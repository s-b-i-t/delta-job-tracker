#!/usr/bin/env python3
"""ATS validation canary runner (metrics-first, stage-aware).

How to run:
  python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 1000 --discover-limit 1000

Quick baseline:
  python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 200 --discover-limit 50

Fresh-domain cohort canary (avoids cache-skip-only domain runs):
  python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 200 --discover-limit 50 --domain-fresh-cohort

Writes raw API/DB captures and derived summaries to ./out/<timestamp>/.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

import efficiency_reporting_contract as erc

DEFAULT_VENDOR_PROBE_SUCCESS_THRESHOLD = 0.45
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "COMPLETED", "CANCELLED", "ABORTED"}
VENDOR_HOST_HINTS = (
    "boards.greenhouse.io",
    "job-boards.greenhouse.io",
    "jobs.lever.co",
    "apply.lever.co",
    "myworkdayjobs.com",
    "smartrecruiters.com",
)
COMMON_PATH_HINTS = (
    "/careers",
    "/jobs",
    "/about/careers",
    "/careers/jobs",
)


@dataclass
class StepResult:
    name: str
    started_at: float
    ended_at: float
    payload: Any

    @property
    def duration_ms(self) -> int:
        return int((self.ended_at - self.started_at) * 1000)


class ApiClient:
    def __init__(self, base_url: str, timeout_seconds: int):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

    def get_json(self, path: str, query: Optional[Dict[str, Any]] = None) -> Any:
        return self._request_json("GET", path, query)

    def post_json(self, path: str, query: Optional[Dict[str, Any]] = None) -> Any:
        return self._request_json("POST", path, query)

    def _request_json(self, method: str, path: str, query: Optional[Dict[str, Any]]) -> Any:
        url = f"{self.base_url}{path}"
        if query:
            query_str = urlencode({k: v for k, v in query.items() if v is not None})
            if query_str:
                url = f"{url}?{query_str}"
        req = Request(url, method=method)
        req.add_header("Accept", "application/json")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {e.code} for {method} {url}: {body}") from e
        except URLError as e:
            raise RuntimeError(f"Request failed for {method} {url}: {e}") from e
        except (TimeoutError, socket.timeout) as e:
            raise RuntimeError(f"Request timed out for {method} {url} after {self.timeout_seconds}s") from e
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON from {method} {url}: {raw[:500]}") from e


class DbClient:
    def __init__(self, container: str, user: str, database: str):
        self.container = container
        self.user = user
        self.database = database

    def _run_psql(self, sql: str, csv_mode: bool) -> str:
        cmd = ["docker", "exec", "-i", self.container, "psql", "-U", self.user, "-d", self.database]
        if csv_mode:
            cmd.append("--csv")
        else:
            cmd.extend(["-t", "-A"])
        cmd.extend(["-c", sql])
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"psql failed ({proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}")
        return proc.stdout

    def query_csv(self, sql: str) -> List[Dict[str, str]]:
        out = self._run_psql(sql, csv_mode=True)
        if not out.strip():
            return []
        reader = csv.DictReader(io.StringIO(out))
        return [{(k or "").strip(): (v or "") for k, v in row.items()} for row in reader]

    def query_single_int(self, sql: str) -> int:
        out = self._run_psql(sql, csv_mode=False).strip()
        return int(out) if out else 0


def ensure_out_dir(base: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = base / ts
    out.mkdir(parents=True, exist_ok=False)
    return out


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def timed_step(name: str, fn) -> StepResult:
    s = time.monotonic()
    payload = fn()
    e = time.monotonic()
    return StepResult(name, s, e, payload)


def timed_step_safe(name: str, fn) -> StepResult:
    s = time.monotonic()
    try:
        payload = fn()
    except Exception as e:  # noqa: BLE001
        payload = {"_error": str(e)}
    e = time.monotonic()
    return StepResult(name, s, e, payload)


def poll_discovery_run(api: ApiClient, run_id: int, max_wait_seconds: int, poll_interval_seconds: int, out_dir: Path, prefix: str) -> Dict[str, Any]:
    deadline = time.monotonic() + max_wait_seconds
    snaps: List[Dict[str, Any]] = []
    last = None
    started = time.monotonic()
    periodic_interval = max(30, poll_interval_seconds * 6)
    next_periodic_at = started + periodic_interval

    def _to_int(value: Any) -> int:
        try:
            return int(value)
        except Exception:
            return 0

    while True:
        status = api.get_json(f"/api/careers/discover/run/{run_id}")
        snaps.append(status)
        state = str(status.get("status") or "")
        if state != last:
            print(f"[{prefix}] status={state}", file=sys.stderr)
            last = state
        now = time.monotonic()
        if state not in TERMINAL_STATUSES and now >= next_periodic_at:
            processed = _to_int(status.get("processedCount"))
            succeeded = _to_int(status.get("succeededCount"))
            failed = _to_int(status.get("failedCount"))
            careers_found = _to_int(status.get("careersUrlFoundCount"))
            vendor_detected = _to_int(status.get("vendorDetectedCount"))
            endpoint_extracted = _to_int(status.get("endpointExtractedCount"))
            remaining = max(0, int(deadline - now))
            elapsed = int(now - started)
            print(
                f"[{prefix}] run_id={run_id} status={state} processed={processed} succeeded={succeeded} failed={failed} "
                f"careers_url_found={careers_found} vendor_detected={vendor_detected} endpoint_extracted={endpoint_extracted} "
                f"elapsed={elapsed}s remaining={remaining}s",
                file=sys.stderr,
            )
            next_periodic_at = now + periodic_interval
        if state in TERMINAL_STATUSES:
            break
        if now >= deadline:
            raise RuntimeError(f"Timed out waiting for discovery run {run_id}")
        time.sleep(poll_interval_seconds)
    write_json(out_dir / f"{prefix}_run_status_snapshots.json", snaps)
    return status


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "t", "true", "y", "yes"}


def safe_rate(num: int, den: int) -> Optional[float]:
    return round(num / den, 4) if den else None


def build_canonical_efficiency_report(
    *,
    args: argparse.Namespace,
    out_dir: Path,
    domain_attempted: int,
    domain_resolved: int,
    domain_before: int,
    domain_after: int,
    domain_resolve_duration_ms: int,
    probe_final_status: Dict[str, Any],
    probe_duration_ms: int,
    stage_failure_buckets: Dict[str, Any],
) -> Dict[str, Any]:
    domain_discovered_net_new = max(0, domain_after - domain_before)
    domain_duration_seconds = max(0.0, domain_resolve_duration_ms / 1000.0)
    probe_duration_seconds = max(0.0, probe_duration_ms / 1000.0)

    probe_attempted = parse_int(probe_final_status.get("companiesAttemptedCount"))
    probe_failed = parse_int(probe_final_status.get("failedCount"))
    probe_succeeded = parse_int(probe_final_status.get("succeededCount"))
    endpoints_extracted = parse_int(probe_final_status.get("endpointExtractedCount"))
    stop_reason_raw = probe_final_status.get("stopReason") or probe_final_status.get("lastError")

    return erc.build_payload(
        source="run_ats_validation_canary.py",
        context={
            "base_url": args.base_url,
            "discover_limit": args.discover_limit,
            "out_dir": str(out_dir),
            "scope_tags": ["run_scope", "global_snapshot"],
        },
        phases={
            "domain_resolution": {
                "scope_tag": "run_scope",
                "duration_seconds": round(domain_duration_seconds, 3),
                "rate_denominator_seconds": round(domain_duration_seconds, 3),
                "counters": {
                    "companies_attempted": domain_attempted,
                    "domains_resolved": domain_resolved,
                    "domains_discovered_net_new_global": domain_discovered_net_new,
                },
                "rates_per_min": {
                    "domains_resolved_per_min": erc.rate_per_min(domain_resolved, domain_duration_seconds),
                    "domains_discovered_per_min": erc.rate_per_min(domain_discovered_net_new, domain_duration_seconds),
                },
                "error_rates": {
                    "unresolved_per_attempt": erc.ratio(max(0, domain_attempted - domain_resolved), domain_attempted),
                },
            },
            "queue_fetch": {
                "scope_tag": "run_scope",
                "availability": "not_collected_in_this_surface",
            },
            "ats_discovery": {
                "scope_tag": "run_scope",
                "duration_seconds": round(probe_duration_seconds, 3),
                "rate_denominator_seconds": round(probe_duration_seconds, 3),
                "counters": {
                    "companies_attempted": probe_attempted,
                    "companies_succeeded": probe_succeeded,
                    "companies_failed": probe_failed,
                    "endpoints_extracted": endpoints_extracted,
                },
                "rates_per_min": {
                    "endpoints_extracted_per_min": erc.rate_per_min(endpoints_extracted, probe_duration_seconds),
                    "companies_attempted_per_min": erc.rate_per_min(probe_attempted, probe_duration_seconds),
                },
                "error_rates": {
                    "failed_per_attempted": erc.ratio(probe_failed, probe_attempted),
                },
                "stop_reason": {
                    "raw": str(stop_reason_raw) if stop_reason_raw is not None else None,
                    "normalized": erc.normalize_stop_reason(stop_reason_raw),
                },
                "error_breakdown": {
                    "stage_failure_buckets": {
                        str(k): erc.parse_int(v) for k, v in (stage_failure_buckets or {}).items()
                    }
                },
            },
        },
        global_snapshots={
            "scope_tag": "global_snapshot",
            "companies_with_domain_count_before": domain_before,
            "companies_with_domain_count_after": domain_after,
            "companies_with_domain_count_delta": domain_after - domain_before,
        },
    )


def top_n_map(m: Any, n: int = 3) -> List[Dict[str, Any]]:
    if not isinstance(m, dict):
        return []
    items = sorted(m.items(), key=lambda kv: (-parse_int(kv[1]), str(kv[0])))
    return [{"key": k, "count": parse_int(v)} for k, v in items[:n]]


def query_domain_counts(db: DbClient) -> Dict[str, Any]:
    total = db.query_single_int("SELECT COUNT(DISTINCT company_id) FROM company_domains;")
    rows = db.query_csv(
        """
        SELECT COALESCE(source, 'UNKNOWN') AS source, COUNT(*) AS count
        FROM (
          SELECT DISTINCT ON (company_id) company_id, source, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        ) latest
        GROUP BY 1
        ORDER BY COUNT(*) DESC, source
        """
    )
    return {"companies_with_domain_count": total, "by_source": [{"source": r["source"], "count": parse_int(r["count"])} for r in rows]}


def query_missing_domain_cohort(db: DbClient, limit: int) -> List[Dict[str, Any]]:
    rows = db.query_csv(
        f"""
        SELECT c.id::text AS company_id,
               c.ticker,
               c.name,
               c.wikipedia_title,
               c.cik,
               c.domain_resolution_status,
               c.domain_resolution_method,
               c.domain_resolution_attempted_at
        FROM companies c
        WHERE NOT EXISTS (SELECT 1 FROM company_domains cd WHERE cd.company_id = c.id)
        ORDER BY c.ticker
        LIMIT {int(limit)}
        """
    )
    return rows


def query_domain_state_for_company_ids(db: DbClient, company_ids: List[int]) -> List[Dict[str, Any]]:
    if not company_ids:
        return []
    ids = ",".join(str(int(i)) for i in company_ids)
    rows = db.query_csv(
        f"""
        WITH latest_domains AS (
          SELECT DISTINCT ON (company_id)
            company_id, domain, source, resolution_method, resolved_at, id
          FROM company_domains
          WHERE company_id IN ({ids})
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        )
        SELECT c.id::text AS company_id,
               c.ticker,
               c.domain_resolution_status,
               c.domain_resolution_method AS company_domain_resolution_method,
               c.domain_resolution_attempted_at,
               ld.domain,
               ld.source,
               ld.resolution_method,
               ld.resolved_at
        FROM companies c
        LEFT JOIN latest_domains ld ON ld.company_id = c.id
        WHERE c.id IN ({ids})
        ORDER BY c.ticker
        """
    )
    return rows


def query_domain_source_funnel_crosstab(db: DbClient, run_id: int) -> Dict[str, Any]:
    rows = db.query_csv(
        f"""
        WITH latest_domains AS (
          SELECT DISTINCT ON (company_id)
            company_id, source, resolution_method, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        )
        SELECT COALESCE(ld.source, 'NONE') AS domain_source,
               COALESCE(ld.resolution_method, 'NONE') AS domain_resolution_method,
               r.status AS discovery_status,
               r.careers_url_found,
               r.vendor_detected,
               r.endpoint_extracted,
               COUNT(*) AS count
        FROM careers_discovery_company_results r
        LEFT JOIN latest_domains ld ON ld.company_id = r.company_id
        WHERE r.discovery_run_id = {int(run_id)}
        GROUP BY 1,2,3,4,5,6
        ORDER BY 1,2,3,4,5,6
        """
    )
    parsed = []
    for r in rows:
        parsed.append(
            {
                "domain_source": r["domain_source"],
                "domain_resolution_method": r["domain_resolution_method"],
                "discovery_status": r["discovery_status"],
                "careers_url_found": parse_bool(r.get("careers_url_found")),
                "vendor_detected": parse_bool(r.get("vendor_detected")),
                "endpoint_extracted": parse_bool(r.get("endpoint_extracted")),
                "count": parse_int(r.get("count")),
            }
        )
    summary_rows = db.query_csv(
        f"""
        WITH latest_domains AS (
          SELECT DISTINCT ON (company_id)
            company_id, source, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        )
        SELECT COALESCE(ld.source, 'NONE') AS domain_source,
               COUNT(*) AS attempted_count,
               SUM(CASE WHEN r.careers_url_found THEN 1 ELSE 0 END) AS careers_url_found_count,
               SUM(CASE WHEN r.vendor_detected THEN 1 ELSE 0 END) AS vendor_detected_count,
               SUM(CASE WHEN r.endpoint_extracted THEN 1 ELSE 0 END) AS endpoint_extracted_count,
               SUM(CASE WHEN r.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
               SUM(CASE WHEN r.status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
        FROM careers_discovery_company_results r
        LEFT JOIN latest_domains ld ON ld.company_id = r.company_id
        WHERE r.discovery_run_id = {int(run_id)}
        GROUP BY 1
        ORDER BY COUNT(*) DESC, domain_source
        """
    )
    summary = []
    for r in summary_rows:
        attempted = parse_int(r["attempted_count"])
        found = parse_int(r["careers_url_found_count"])
        vd = parse_int(r["vendor_detected_count"])
        ex = parse_int(r["endpoint_extracted_count"])
        summary.append(
            {
                "domain_source": r["domain_source"],
                "attempted_count": attempted,
                "careers_url_found_count": found,
                "vendor_detected_count": vd,
                "endpoint_extracted_count": ex,
                "succeeded_count": parse_int(r["succeeded_count"]),
                "failed_count": parse_int(r["failed_count"]),
                "careers_url_found_rate": safe_rate(found, attempted),
                "vendor_detected_rate_among_found": safe_rate(vd, found),
                "endpoint_extracted_rate_among_vendor_detected": safe_rate(ex, vd),
            }
        )
    return {"rows": parsed, "summary": summary}


def query_run_stage_durations(db: DbClient, run_id: int) -> List[Dict[str, Any]]:
    rows = db.query_csv(
        f"""
        SELECT stage,
               COUNT(*) AS count,
               COALESCE(SUM(duration_ms),0) AS total_duration_ms,
               COALESCE(AVG(duration_ms),0) AS avg_duration_ms,
               COALESCE(MIN(duration_ms),0) AS min_duration_ms,
               COALESCE(MAX(duration_ms),0) AS max_duration_ms
        FROM careers_discovery_company_results
        WHERE discovery_run_id = {int(run_id)}
          AND duration_ms IS NOT NULL
        GROUP BY stage
        ORDER BY count DESC, stage
        """
    )
    return [
        {
            "stage": r.get("stage") or "UNKNOWN",
            "count": parse_int(r.get("count")),
            "total_duration_ms": parse_int(r.get("total_duration_ms")),
            "avg_duration_ms": round(float(r.get("avg_duration_ms") or 0), 2),
            "min_duration_ms": parse_int(r.get("min_duration_ms")),
            "max_duration_ms": parse_int(r.get("max_duration_ms")),
        }
        for r in rows
    ]


def query_run_company_funnel_samples(db: DbClient, run_id: int, limit: int) -> List[Dict[str, Any]]:
    rows = db.query_csv(
        f"""
        SELECT c.ticker,
               c.name AS company_name,
               r.status,
               r.stage,
               r.reason_code,
               r.careers_url_found,
               r.careers_url_initial,
               r.careers_url_final,
               r.careers_discovery_method,
               r.careers_discovery_stage_failure,
               r.vendor_detected,
               r.vendor_name,
               r.endpoint_extracted,
               r.endpoint_url,
               r.http_status,
               r.http_status_first_failure,
               r.request_count,
               r.duration_ms,
               r.created_at
        FROM careers_discovery_company_results r
        JOIN companies c ON c.id = r.company_id
        WHERE r.discovery_run_id = {int(run_id)}
        ORDER BY r.created_at DESC
        LIMIT {int(limit)}
        """
    )
    return rows


def query_run_failure_attempts(db: DbClient, run_id: int, limit: int) -> List[Dict[str, Any]]:
    rows = db.query_csv(
        f"""
        WITH run_window AS (
          SELECT started_at, COALESCE(finished_at, NOW()) AS finished_at
          FROM careers_discovery_runs WHERE id = {int(run_id)}
        )
        SELECT c.ticker,
               c.name AS company_name,
               f.reason_code,
               f.candidate_url,
               f.detail,
               f.observed_at,
               r.stage,
               r.careers_discovery_method,
               r.careers_discovery_stage_failure
        FROM careers_discovery_failures f
        JOIN companies c ON c.id = f.company_id
        LEFT JOIN careers_discovery_company_results r
          ON r.company_id = f.company_id AND r.discovery_run_id = {int(run_id)}
        JOIN run_window w ON f.observed_at >= w.started_at AND f.observed_at <= w.finished_at
        ORDER BY f.observed_at DESC
        LIMIT {int(limit)}
        """
    )
    return rows


def classify_attempt_stage(url: Optional[str], row: Dict[str, Any]) -> str:
    u = (url or "").strip()
    reason_code = str(row.get("reason_code") or "")
    method = str(row.get("careers_discovery_method") or "")
    parsed = urlparse(u) if u else None
    host = (parsed.hostname or "").lower() if parsed else ""
    path = (parsed.path or "/").lower() if parsed else ""
    if any(h in host for h in VENDOR_HOST_HINTS):
        return "vendor_probe_attempts"
    if host.startswith("careers.") or host.startswith("jobs."):
        return "subdomain_attempts"
    if any(path.startswith(p) for p in COMMON_PATH_HINTS):
        if method == "HOMEPAGE_LINK":
            return "careers_candidate_attempts"
        return "fallback_path_attempts"
    if path in {"", "/"} and ("homepage" in reason_code or method == "HOMEPAGE_LINK"):
        return "homepage_fetch_attempts"
    if method == "HOMEPAGE_LINK":
        return "careers_candidate_attempts"
    return "careers_candidate_attempts"


def parse_status_code_from_row(row: Dict[str, Any]) -> Optional[int]:
    for key in ("status_code", "http_status", "http_status_first_failure"):
        if row.get(key) not in (None, ""):
            try:
                return int(row[key])
            except Exception:
                pass
    detail = str(row.get("detail") or row.get("errorDetail") or "")
    m = re.search(r"http_(\d{3})", detail, flags=re.I)
    return int(m.group(1)) if m else None


def normalize_url_pattern(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        path = (p.path or "/").lower()
        if not host:
            return None
        segs = [s for s in path.split("/") if s]
        prefix = "/" + segs[0] if segs else "/"
        return f"{host}{prefix}"
    except Exception:
        return None


def build_attempt_evidence(company_rows: List[Dict[str, Any]], failure_rows: List[Dict[str, Any]], limit_per_group: int = 20) -> Dict[str, Any]:
    groups = {
        "homepage_fetch_attempts": [],
        "careers_candidate_attempts": [],
        "fallback_path_attempts": [],
        "subdomain_attempts": [],
        "vendor_probe_attempts": [],
    }
    # failures first (actual attempted URLs)
    for row in failure_rows:
        url = row.get("candidate_url") or ""
        if not url:
            continue
        g = classify_attempt_stage(url, row)
        if len(groups[g]) >= limit_per_group:
            continue
        groups[g].append(
            {
                "ticker": row.get("ticker"),
                "company_name": row.get("company_name"),
                "url": url,
                "status_code": parse_status_code_from_row(row),
                "reason_code": row.get("reason_code"),
                "detail": row.get("detail"),
                "observed_at": row.get("observed_at"),
            }
        )
    # supplement with per-company funnel rows for successful/selected URLs
    for row in company_rows:
        for field, group in (
            ("careers_url_initial", "careers_candidate_attempts"),
            ("careers_url_final", "fallback_path_attempts"),
            ("endpoint_url", "vendor_probe_attempts"),
        ):
            url = row.get(field) or ""
            if not url:
                continue
            if len(groups[group]) >= limit_per_group:
                continue
            status_code = parse_status_code_from_row(row)
            groups[group].append(
                {
                    "ticker": row.get("ticker"),
                    "company_name": row.get("company_name"),
                    "url": url,
                    "status_code": status_code,
                    "reason_code": row.get("reason_code"),
                    "detail": row.get("careers_discovery_stage_failure") or row.get("errorDetail"),
                    "observed_at": row.get("created_at"),
                }
            )
    return groups


def build_top_failing_url_patterns(failure_rows: List[Dict[str, Any]], limit: int = 20) -> List[Dict[str, Any]]:
    counts: Dict[Tuple[str, str], int] = {}
    samples: Dict[Tuple[str, str], str] = {}
    for row in failure_rows:
        url = row.get("candidate_url") or ""
        pat = normalize_url_pattern(url)
        if not pat:
            continue
        status = parse_status_code_from_row(row)
        status_key = str(status if status is not None else "ERR")
        key = (pat, status_key)
        counts[key] = counts.get(key, 0) + 1
        samples.setdefault(key, url)
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0][0], kv[0][1]))[:limit]
    return [
        {"url_pattern": k[0], "status": None if k[1] == "ERR" else int(k[1]), "count": v, "sample_url": samples[k]}
        for k, v in ordered
    ]


def infer_domain_bottleneck(domain_result: Dict[str, Any], probe_status: Dict[str, Any], requested_discover_limit: int) -> Dict[str, Any]:
    metrics = domain_result.get("metrics") or {}
    attempted = parse_int(metrics.get("companiesAttemptedCount"))
    resolved = parse_int(domain_result.get("resolvedCount"))
    processed = parse_int(probe_status.get("processedCount"))
    failures_by_reason = probe_status.get("failuresByReason") or {}
    no_domain_reason_count = sum(parse_int(v) for k, v in (failures_by_reason or {}).items() if "domain" in str(k).lower()) if isinstance(failures_by_reason, dict) else 0
    fill_rate = (processed / requested_discover_limit) if requested_discover_limit else 0.0
    likely = no_domain_reason_count > 0 or (fill_rate < 0.8 and resolved == 0 and attempted > 0)
    return {
        "likely_domain_limited": likely,
        "no_domain_reason_count": no_domain_reason_count,
        "discover_processed_fill_rate_vs_requested": round(fill_rate, 4),
        "notes": "ATS failures are not dominated by domain absence in this canary based on run reasons/fill rate." if not likely else "Flagged domain-limited because ATS run reported domain-related failures or could not fill requested batch.",
    }


def build_fresh_domain_cohort_report(before_rows: List[Dict[str, Any]], after_rows: List[Dict[str, Any]], domain_result: Dict[str, Any], step_duration_ms: int) -> Dict[str, Any]:
    before_ids = {parse_int(r["company_id"]) for r in before_rows}
    after_by_id = {parse_int(r["company_id"]): r for r in after_rows}
    resolved_now = []
    unresolved_after = []
    by_method: Dict[str, int] = {}
    for cid in sorted(before_ids):
        row = after_by_id.get(cid, {})
        domain = (row.get("domain") or "").strip()
        if domain:
            method = row.get("resolution_method") or row.get("company_domain_resolution_method") or "UNKNOWN"
            by_method[method] = by_method.get(method, 0) + 1
            resolved_now.append({
                "company_id": cid,
                "ticker": row.get("ticker"),
                "domain": domain,
                "source": row.get("source"),
                "resolution_method": row.get("resolution_method"),
                "resolved_at": row.get("resolved_at"),
            })
        else:
            unresolved_after.append({
                "company_id": cid,
                "ticker": row.get("ticker"),
                "domain_resolution_status": row.get("domain_resolution_status"),
                "domain_resolution_attempted_at": row.get("domain_resolution_attempted_at"),
            })
    cohort_size = len(before_rows)
    resolved_count = len(resolved_now)
    return {
        "cohort_size": cohort_size,
        "attempted_count_assumed": min(cohort_size, parse_int((domain_result.get("metrics") or {}).get("companiesInputCount")) or cohort_size),
        "resolved_count_in_cohort_after_run": resolved_count,
        "resolution_rate_in_cohort": safe_rate(resolved_count, cohort_size),
        "resolved_by_method_in_cohort": dict(sorted(by_method.items(), key=lambda kv: (-kv[1], kv[0]))),
        "sample_resolved": resolved_now[:20],
        "sample_unresolved": unresolved_after[:20],
        "latency_distribution": {
            "available": False,
            "reason": "Per-company domain resolution latency is not persisted; reporting step-level timing and aggregate resolver metrics instead.",
            "step_duration_ms": step_duration_ms,
            "approx_avg_ms_per_attempt": round(step_duration_ms / max(1, parse_int((domain_result.get('metrics') or {}).get('companiesAttemptedCount') or cohort_size)), 2),
        },
    }


def run(args: argparse.Namespace) -> int:
    out_dir = ensure_out_dir(Path(args.out_dir))
    api = ApiClient(args.base_url, args.http_timeout_seconds)
    db = DbClient(args.postgres_container, args.postgres_user, args.postgres_db)
    print(f"Output directory: {out_dir}", file=sys.stderr)

    status_step = timed_step("api_status", lambda: api.get_json("/api/status"))
    write_json(out_dir / "api_status.json", status_step.payload)

    domain_counts_before = timed_step("domain_counts_before", lambda: query_domain_counts(db))
    write_json(out_dir / "domain_counts_before.json", domain_counts_before.payload)

    ingest_step = timed_step("sec_ingest", lambda: api.post_json("/api/universe/ingest/sec", {"limit": args.sec_limit}))
    write_json(out_dir / "sec_ingest_response.json", ingest_step.payload)

    fresh_domain_before_step = None
    if args.domain_fresh_cohort:
        fresh_domain_before_step = timed_step(
            "domain_fresh_cohort_before",
            lambda: query_missing_domain_cohort(db, args.resolve_limit),
        )
        write_json(out_dir / "domain_fresh_cohort_before.json", fresh_domain_before_step.payload)

    domain_resolve_step = timed_step(
        "domain_resolve",
        lambda: api.post_json(
            "/api/domains/resolve",
            {
                "limit": args.resolve_limit,
                "includeNonEmployer": "true" if args.include_non_employer else None,
            },
        ),
    )
    write_json(out_dir / "domain_resolve_response.json", domain_resolve_step.payload)

    domain_counts_after = timed_step("domain_counts_after", lambda: query_domain_counts(db))
    write_json(out_dir / "domain_counts_after.json", domain_counts_after.payload)

    fresh_domain_after_step = None
    fresh_domain_report = None
    if args.domain_fresh_cohort and fresh_domain_before_step is not None:
        cohort_ids = [parse_int(r.get("company_id")) for r in fresh_domain_before_step.payload]
        fresh_domain_after_step = timed_step(
            "domain_fresh_cohort_after",
            lambda: query_domain_state_for_company_ids(db, cohort_ids),
        )
        write_json(out_dir / "domain_fresh_cohort_after.json", fresh_domain_after_step.payload)
        fresh_domain_report = build_fresh_domain_cohort_report(
            fresh_domain_before_step.payload,
            fresh_domain_after_step.payload,
            domain_resolve_step.payload if isinstance(domain_resolve_step.payload, dict) else {},
            domain_resolve_step.duration_ms,
        )

    discover_probe_start = timed_step(
        "ats_discovery_vendor_probe_start",
        lambda: api.post_json("/api/careers/discover", {"limit": args.discover_limit, "vendorProbeOnly": "true"}),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_start.json", discover_probe_start.payload)
    probe_run_id = parse_int(discover_probe_start.payload.get("discoveryRunId"))

    probe_wait_start = time.monotonic()
    probe_final_status = poll_discovery_run(api, probe_run_id, args.max_wait_seconds, args.poll_interval_seconds, out_dir, "ats_discovery_vendor_probe")
    probe_status_step = StepResult("ats_discovery_vendor_probe_wait", probe_wait_start, time.monotonic(), probe_final_status)
    write_json(out_dir / "ats_discovery_vendor_probe_final_status.json", probe_final_status)

    probe_companies = timed_step_safe(
        "ats_discovery_vendor_probe_companies",
        lambda: api.get_json(f"/api/careers/discover/run/{probe_run_id}/companies", {"limit": args.discovery_samples_limit}),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_companies.json", probe_companies.payload)

    probe_failures = timed_step_safe(
        "ats_discovery_vendor_probe_failures",
        lambda: api.get_json(f"/api/careers/discover/run/{probe_run_id}/failures"),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_failures.json", probe_failures.payload)

    coverage_step = timed_step_safe("coverage_diagnostics", lambda: api.get_json("/api/diagnostics/coverage"))
    write_json(out_dir / "coverage_diagnostics.json", coverage_step.payload)

    discovery_failures_diag_step = timed_step_safe(
        "discovery_failures_diagnostics", lambda: api.get_json("/api/diagnostics/discovery-failures")
    )
    write_json(out_dir / "discovery_failures_diagnostics.json", discovery_failures_diag_step.payload)

    probe_crosstab = timed_step_safe(
        "probe_domain_source_x_ats_funnel",
        lambda: query_domain_source_funnel_crosstab(db, probe_run_id),
    )
    write_json(out_dir / "probe_domain_source_x_ats_funnel.json", probe_crosstab.payload)

    probe_stage_durations = timed_step_safe(
        "probe_stage_durations", lambda: query_run_stage_durations(db, probe_run_id)
    )
    write_json(out_dir / "probe_stage_durations.json", probe_stage_durations.payload)

    probe_company_funnel_samples = timed_step_safe(
        "probe_company_funnel_samples", lambda: query_run_company_funnel_samples(db, probe_run_id, args.discovery_samples_limit)
    )
    write_json(out_dir / "probe_company_funnel_samples.json", probe_company_funnel_samples.payload)

    probe_failure_attempts = timed_step_safe(
        "probe_failure_attempts", lambda: query_run_failure_attempts(db, probe_run_id, max(200, args.failure_samples_limit * 20))
    )
    write_json(out_dir / "probe_failure_attempts.json", probe_failure_attempts.payload)

    company_sample_rows = probe_company_funnel_samples.payload if isinstance(probe_company_funnel_samples.payload, list) else []
    failure_attempt_rows = probe_failure_attempts.payload if isinstance(probe_failure_attempts.payload, list) else []
    attempt_url_samples = build_attempt_evidence(company_sample_rows, failure_attempt_rows, limit_per_group=20)
    top_failing_patterns = build_top_failing_url_patterns(failure_attempt_rows, limit=20)
    write_json(out_dir / "probe_attempt_url_samples.json", attempt_url_samples)
    write_json(out_dir / "probe_top_failing_url_patterns.json", top_failing_patterns)

    probe_processed = parse_int(probe_final_status.get("processedCount"))
    probe_succeeded = parse_int(probe_final_status.get("succeededCount"))
    probe_success_rate = (probe_succeeded / probe_processed) if probe_processed else 0.0

    full_run_summary = None
    full_steps: List[StepResult] = []
    if args.run_full_if_good and probe_success_rate >= args.vendor_probe_success_threshold:
        full_start = timed_step(
            "ats_discovery_full_start",
            lambda: api.post_json("/api/careers/discover", {"limit": args.discover_limit, "vendorProbeOnly": "false"}),
        )
        full_steps.append(full_start)
        write_json(out_dir / "ats_discovery_full_start.json", full_start.payload)
        full_run_id = parse_int(full_start.payload.get("discoveryRunId"))
        full_wait_start = time.monotonic()
        full_final = poll_discovery_run(api, full_run_id, args.max_wait_seconds, args.poll_interval_seconds, out_dir, "ats_discovery_full")
        full_wait = StepResult("ats_discovery_full_wait", full_wait_start, time.monotonic(), full_final)
        full_steps.append(full_wait)
        write_json(out_dir / "ats_discovery_full_final_status.json", full_final)
        full_failures = timed_step_safe("ats_discovery_full_failures", lambda: api.get_json(f"/api/careers/discover/run/{full_run_id}/failures"))
        full_steps.append(full_failures)
        write_json(out_dir / "ats_discovery_full_failures.json", full_failures.payload)
        full_crosstab = timed_step_safe("full_domain_source_x_ats_funnel", lambda: query_domain_source_funnel_crosstab(db, full_run_id))
        full_steps.append(full_crosstab)
        write_json(out_dir / "full_domain_source_x_ats_funnel.json", full_crosstab.payload)
        full_run_summary = {
            "discovery_run_id": full_run_id,
            "final_status": full_final,
            "failures": full_failures.payload,
            "domain_source_x_ats_funnel": full_crosstab.payload,
        }

    domain_result = domain_resolve_step.payload if isinstance(domain_resolve_step.payload, dict) else {}
    domain_metrics = domain_result.get("metrics") or {}
    domain_attempted = parse_int(domain_metrics.get("companiesAttemptedCount"))
    domain_resolved = parse_int(domain_result.get("resolvedCount"))
    domain_resolution_rate = safe_rate(domain_resolved, domain_attempted)
    skipped_not_employer_sample_raw = domain_metrics.get("skippedNotEmployerSample")
    skipped_not_employer_sample = (
        [str(v) for v in skipped_not_employer_sample_raw if str(v)]
        if isinstance(skipped_not_employer_sample_raw, list)
        else []
    )
    domain_selection_metrics = {
        "selectionEligibleCount": parse_int(domain_metrics.get("selectionEligibleCount")),
        "selectionReturnedCount": parse_int(domain_metrics.get("selectionReturnedCount")),
        "skippedNotEmployerCount": parse_int(domain_metrics.get("skippedNotEmployerCount")),
        "skippedNotEmployerSample": skipped_not_employer_sample,
    }

    companies_with_domain_before = parse_int(domain_counts_before.payload.get("companies_with_domain_count"))
    companies_with_domain_after = parse_int(domain_counts_after.payload.get("companies_with_domain_count"))

    coverage_payload = coverage_step.payload if isinstance(coverage_step.payload, dict) else {}
    failures_diag_payload = discovery_failures_diag_step.payload if isinstance(discovery_failures_diag_step.payload, dict) else {}

    requested_discover_count = args.discover_limit
    careers_url_found_count = parse_int(probe_final_status.get("careersUrlFoundCount"))
    vendor_detected_count = parse_int(probe_final_status.get("vendorDetectedCount"))
    endpoint_extracted_count = parse_int(probe_final_status.get("endpointExtractedCount"))
    stage_failure_buckets = probe_final_status.get("careersStageFailuresByReason") or {}
    if not isinstance(stage_failure_buckets, dict):
        stage_failure_buckets = {}

    funnel = {
        "requested_discover_count": requested_discover_count,
        "processed_discover_count": probe_processed,
        "careers_url_found_count": careers_url_found_count,
        "vendor_detected_count": vendor_detected_count,
        "endpoint_extracted_count": endpoint_extracted_count,
        "careers_url_found_rate": safe_rate(careers_url_found_count, probe_processed),
        "vendor_detected_rate_among_found": safe_rate(vendor_detected_count, careers_url_found_count),
        "endpoint_extracted_rate_among_vendor_detected": safe_rate(endpoint_extracted_count, vendor_detected_count),
    }

    domain_bottleneck = infer_domain_bottleneck(domain_result, probe_final_status, args.discover_limit)
    success_thresholds = {
        "vendor_probe_success_rate_gte": args.vendor_probe_success_threshold,
        "vendor_probe_success_rate_actual": round(probe_success_rate, 4),
        "vendor_probe_success_rate_pass": probe_success_rate >= args.vendor_probe_success_threshold,
        "justification": "Threshold applies to the current vendorProbeOnly stage and should be re-tuned after Phase A funnel rates stabilize.",
    }

    step_durations_ms = {
        status_step.name: status_step.duration_ms,
        domain_counts_before.name: domain_counts_before.duration_ms,
        ingest_step.name: ingest_step.duration_ms,
        domain_resolve_step.name: domain_resolve_step.duration_ms,
        domain_counts_after.name: domain_counts_after.duration_ms,
        discover_probe_start.name: discover_probe_start.duration_ms,
        probe_status_step.name: probe_status_step.duration_ms,
        probe_companies.name: probe_companies.duration_ms,
        probe_failures.name: probe_failures.duration_ms,
        coverage_step.name: coverage_step.duration_ms,
        discovery_failures_diag_step.name: discovery_failures_diag_step.duration_ms,
        probe_crosstab.name: probe_crosstab.duration_ms,
        probe_stage_durations.name: probe_stage_durations.duration_ms,
        probe_company_funnel_samples.name: probe_company_funnel_samples.duration_ms,
        probe_failure_attempts.name: probe_failure_attempts.duration_ms,
    }
    if fresh_domain_before_step:
        step_durations_ms[fresh_domain_before_step.name] = fresh_domain_before_step.duration_ms
    if fresh_domain_after_step:
        step_durations_ms[fresh_domain_after_step.name] = fresh_domain_after_step.duration_ms
    for s in full_steps:
        step_durations_ms[s.name] = s.duration_ms

    total_wall_clock_ms = sum(step_durations_ms.values())

    summary = {
        "schema_version": "ats-canary-summary-v2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "base_url": args.base_url,
            "sec_limit": args.sec_limit,
            "resolve_limit": args.resolve_limit,
            "discover_limit": args.discover_limit,
            "vendor_probe_success_threshold": args.vendor_probe_success_threshold,
            "run_full_if_good": args.run_full_if_good,
            "domain_fresh_cohort": args.domain_fresh_cohort,
            "include_non_employer": args.include_non_employer,
            "max_wait_seconds": args.max_wait_seconds,
            "poll_interval_seconds": args.poll_interval_seconds,
        },
        "inputs_and_counts": {
            "companies_ingested_count": parse_int(ingest_step.payload.get("inserted")) + parse_int(ingest_step.payload.get("updated")),
            "sec_ingest_inserted": parse_int(ingest_step.payload.get("inserted")),
            "sec_ingest_updated": parse_int(ingest_step.payload.get("updated")),
            "sec_universe_total_reported": parse_int(ingest_step.payload.get("total")),
            "companies_with_domain_count_before": companies_with_domain_before,
            "companies_with_domain_count_after": companies_with_domain_after,
            "companies_with_domain_net_new": companies_with_domain_after - companies_with_domain_before,
            "companies_with_domain_by_source_before": domain_counts_before.payload.get("by_source") or [],
            "companies_with_domain_by_source_after": domain_counts_after.payload.get("by_source") or [],
        },
        "domain_resolution": {
            "requested_limit": args.resolve_limit,
            "resolved_count": domain_resolved,
            "domain_resolution_rate": domain_resolution_rate,
            "selection_metrics": domain_selection_metrics,
            "result": domain_result,
            "fresh_domain_cohort": fresh_domain_report,
        },
        "ats_vendor_probe": {
            "discovery_run_id": probe_run_id,
            "ats_discovery_attempted_count": probe_processed,
            "ats_discovery_success_count": probe_succeeded,
            "ats_discovery_success_rate": round(probe_success_rate, 4),
            "funnel": funnel,
            "final_status": probe_final_status,
            "run_failures": probe_failures.payload,
            "domain_source_x_ats_funnel": probe_crosstab.payload,
            "stage_failure_buckets": stage_failure_buckets,
            "stage_durations_ms": probe_stage_durations.payload,
        },
        "actionable_failure_evidence": {
            "attempt_url_samples_by_stage": attempt_url_samples,
            "top_failing_url_patterns": top_failing_patterns,
            "company_funnel_samples": company_sample_rows[: min(args.discovery_samples_limit, 50)],
            "failure_attempt_samples_source_error": None if isinstance(probe_failure_attempts.payload, list) else probe_failure_attempts.payload,
        },
        "breakdowns": {
            "vendor_probe_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundVendorProbeByAtsType") or {},
            "homepage_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundHomepageByAtsType") or {},
            "sitemap_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundSitemapByAtsType") or {},
            "coverage_ats_endpoints_by_type": coverage_payload.get("atsEndpointsByType") or {},
            "coverage_ats_endpoints_by_method": coverage_payload.get("atsEndpointsByMethod") or {},
            "top_failure_categories_run": top_n_map(probe_final_status.get("failuresByReason"), 5),
            "top_failure_categories_diagnostics": top_n_map(failures_diag_payload.get("countsByReason"), 5),
            "discovery_failures_recent_samples": (failures_diag_payload.get("recentFailures") or [])[: min(args.failure_samples_limit, 20)],
        },
        "thresholds": success_thresholds,
        "domain_backlog_watch": {
            "heuristic_overfit_risk_check": "Compare ATS funnel rates by domain_source; if HEURISTIC underperforms materially vs WIKIDATA/WIKIPEDIA, inspect host quality/brand alias issues.",
            "probe_domain_source_funnel_summary": probe_crosstab.payload.get("summary") if isinstance(probe_crosstab.payload, dict) else [],
            "domain_resolve_selection_metrics": domain_selection_metrics,
        },
        "bottleneck_assessment": {
            **domain_bottleneck,
            "next_focus": "ATS discovery improvements" if not domain_bottleneck.get("likely_domain_limited") else "Domain resolution / targeting still limiting ATS coverage",
        },
        "optional_full_discovery": {
            "executed": bool(full_run_summary),
            "skipped_reason": None if full_run_summary else ("disabled_by_flag" if not args.run_full_if_good else f"vendor_probe_success_rate_below_threshold ({round(probe_success_rate,4)} < {args.vendor_probe_success_threshold})"),
            "result": full_run_summary,
        },
        "durations_ms": {
            "per_step": step_durations_ms,
            "per_stage": probe_stage_durations.payload if isinstance(probe_stage_durations.payload, list) else {"_error": probe_stage_durations.payload},
            "total_wall_clock_ms": total_wall_clock_ms,
        },
        "optional_errors": {
            "probe_companies": None if not isinstance(probe_companies.payload, dict) or "_error" not in probe_companies.payload else probe_companies.payload,
            "probe_failures": None if not isinstance(probe_failures.payload, dict) or "_error" not in probe_failures.payload else probe_failures.payload,
            "coverage_diagnostics": None if not isinstance(coverage_step.payload, dict) or "_error" not in coverage_step.payload else coverage_step.payload,
            "discovery_failures_diagnostics": None if not isinstance(discovery_failures_diag_step.payload, dict) or "_error" not in discovery_failures_diag_step.payload else discovery_failures_diag_step.payload,
            "probe_domain_source_x_ats_funnel": None if not isinstance(probe_crosstab.payload, dict) or "_error" not in probe_crosstab.payload else probe_crosstab.payload,
        },
        "raw_files": sorted(p.name for p in out_dir.iterdir() if p.is_file()),
    }
    canonical_efficiency_report = build_canonical_efficiency_report(
        args=args,
        out_dir=out_dir,
        domain_attempted=domain_attempted,
        domain_resolved=domain_resolved,
        domain_before=companies_with_domain_before,
        domain_after=companies_with_domain_after,
        domain_resolve_duration_ms=domain_resolve_step.duration_ms,
        probe_final_status=probe_final_status,
        probe_duration_ms=probe_status_step.duration_ms,
        stage_failure_buckets=stage_failure_buckets,
    )
    summary["canonical_efficiency_report"] = canonical_efficiency_report
    write_json(out_dir / "canary_summary.json", summary)
    write_json(out_dir / "canonical_efficiency_report.json", canonical_efficiency_report)

    schema = {
        "schema_version": "ats-canary-summary-schema-v2",
        "required_top_level_fields": [
            "schema_version",
            "generated_at",
            "config",
            "inputs_and_counts",
            "domain_resolution",
            "ats_vendor_probe",
            "actionable_failure_evidence",
            "breakdowns",
            "thresholds",
            "domain_backlog_watch",
            "bottleneck_assessment",
            "optional_full_discovery",
            "durations_ms",
            "optional_errors",
            "raw_files",
        ],
        "key_metrics": [
            "domain_resolution.domain_resolution_rate",
            "domain_resolution.selection_metrics.selectionEligibleCount",
            "domain_resolution.selection_metrics.selectionReturnedCount",
            "domain_resolution.selection_metrics.skippedNotEmployerCount",
            "domain_resolution.fresh_domain_cohort.resolution_rate_in_cohort",
            "ats_vendor_probe.funnel.careers_url_found_rate",
            "ats_vendor_probe.funnel.vendor_detected_rate_among_found",
            "ats_vendor_probe.funnel.endpoint_extracted_rate_among_vendor_detected",
            "ats_vendor_probe.stage_failure_buckets",
            "actionable_failure_evidence.top_failing_url_patterns",
            "domain_backlog_watch.probe_domain_source_funnel_summary",
        ],
    }
    write_json(out_dir / "canary_summary_schema.json", schema)

    print("Canary Summary", file=sys.stderr)
    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "domain_resolution_rate": summary["domain_resolution"]["domain_resolution_rate"],
                "fresh_domain_cohort_rate": (summary["domain_resolution"].get("fresh_domain_cohort") or {}).get("resolution_rate_in_cohort"),
                "ats_vendor_probe_success_rate": summary["ats_vendor_probe"]["ats_discovery_success_rate"],
                "funnel": summary["ats_vendor_probe"]["funnel"],
                "top_stage_failures": top_n_map(summary["ats_vendor_probe"].get("stage_failure_buckets"), 5),
                "top_failing_url_patterns": summary["actionable_failure_evidence"]["top_failing_url_patterns"][:5],
                "next_focus": summary["bottleneck_assessment"]["next_focus"],
            },
            indent=2,
        ),
        file=sys.stderr,
    )
    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bounded ATS/domain validation canary and save JSON artifacts")
    p.add_argument("--base-url", default=os.getenv("DELTA_BASE_URL", "http://localhost:8080"))
    p.add_argument("--sec-limit", type=int, default=int(os.getenv("DELTA_SEC_LIMIT", "5000")))
    p.add_argument("--resolve-limit", type=int, default=int(os.getenv("DELTA_RESOLVE_LIMIT", "1000")))
    p.add_argument("--discover-limit", type=int, default=int(os.getenv("DELTA_DISCOVER_LIMIT", "1000")))
    p.add_argument("--max-wait-seconds", type=int, default=int(os.getenv("DELTA_MAX_WAIT_SECONDS", "1800")))
    p.add_argument("--poll-interval-seconds", type=int, default=int(os.getenv("DELTA_POLL_INTERVAL_SECONDS", "5")))
    p.add_argument("--http-timeout-seconds", type=int, default=int(os.getenv("DELTA_HTTP_TIMEOUT_SECONDS", "120")))
    p.add_argument("--out-dir", default=os.getenv("DELTA_OUT_DIR", "out"))
    p.add_argument("--discovery-samples-limit", type=int, default=int(os.getenv("DELTA_DISCOVERY_SAMPLES_LIMIT", "200")))
    p.add_argument("--failure-samples-limit", type=int, default=int(os.getenv("DELTA_FAILURE_SAMPLES_LIMIT", "20")))
    p.add_argument("--vendor-probe-success-threshold", type=float, default=float(os.getenv("DELTA_VENDOR_PROBE_SUCCESS_THRESHOLD", str(DEFAULT_VENDOR_PROBE_SUCCESS_THRESHOLD))))
    p.add_argument("--run-full-if-good", action="store_true", default=os.getenv("DELTA_RUN_FULL_IF_GOOD", "false").lower() == "true")
    p.add_argument("--domain-fresh-cohort", action="store_true", default=os.getenv("DELTA_DOMAIN_FRESH_COHORT", "false").lower() == "true", help="Sample top missing-domain cohort (ORDER BY ticker) before resolve and report cohort-specific domain outcomes")
    p.add_argument("--include-non-employer", action="store_true", default=os.getenv("DELTA_INCLUDE_NON_EMPLOYER", "false").lower() == "true", help="Include likely non-employer entities (funds/SPACs/trusts/etc.) in /api/domains/resolve candidate selection")
    p.add_argument("--postgres-container", default=os.getenv("DELTA_POSTGRES_CONTAINER", "delta-job-tracker-postgres"))
    p.add_argument("--postgres-user", default=os.getenv("DELTA_POSTGRES_USER", "delta"))
    p.add_argument("--postgres-db", default=os.getenv("DELTA_POSTGRES_DB", "delta_job_tracker"))
    args = p.parse_args(argv)
    for field in ("sec_limit", "resolve_limit", "discover_limit"):
        if getattr(args, field) <= 0:
            p.error(f"{field} must be > 0")
    return args


if __name__ == "__main__":
    sys.exit(run(parse_args()))
