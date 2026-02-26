#!/usr/bin/env python3
"""ATS validation canary runner.

How to run:
  python3 scripts/run_ats_validation_canary.py \
    --sec-limit 5000 \
    --resolve-limit 1000 \
    --discover-limit 1000

This script performs a bounded canary sequence against the local backend API:
1) SEC universe ingest
2) domain resolution
3) ATS discovery (vendorProbeOnly=true)
4) optional ATS discovery full mode (if probe success clears threshold)
5) diagnostics capture + summary generation

Raw endpoint responses and derived summaries are written to ./out/<timestamp>/.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_VENDOR_PROBE_SUCCESS_THRESHOLD = 0.45
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "COMPLETED", "CANCELLED"}


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
        return self._request_json("GET", path, query=query)

    def post_json(self, path: str, query: Optional[Dict[str, Any]] = None) -> Any:
        return self._request_json("POST", path, query=query)

    def _request_json(
        self, method: str, path: str, query: Optional[Dict[str, Any]] = None
    ) -> Any:
        url = f"{self.base_url}{path}"
        if query:
            query_str = urlencode({k: v for k, v in query.items() if v is not None})
            if query_str:
                url = f"{url}?{query_str}"
        req = Request(url, method=method)
        req.add_header("Accept", "application/json")
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                data = resp.read().decode("utf-8")
        except HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {e.code} for {method} {url}: {body}") from e
        except URLError as e:
            raise RuntimeError(f"Request failed for {method} {url}: {e}") from e
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Invalid JSON from {method} {url}: {data[:500]}") from e


class DbClient:
    def __init__(self, container: str, user: str, database: str):
        self.container = container
        self.user = user
        self.database = database

    def query_tsv(self, sql: str) -> List[Dict[str, str]]:
        cmd = [
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            self.user,
            "-d",
            self.database,
            "-A",
            "-P",
            "footer=off",
            "-F",
            "\t",
            "-c",
            sql,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"psql query failed (exit {proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}"
            )
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        if not lines:
            return []
        header = [c.strip() for c in lines[0].split("\t")]
        rows: List[Dict[str, str]] = []
        for line in lines[1:]:
            cols = line.split("\t")
            row = {header[i]: (cols[i] if i < len(cols) else "") for i in range(len(header))}
            rows.append(row)
        return rows

    def query_single_int(self, sql: str) -> int:
        cmd = [
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            self.user,
            "-d",
            self.database,
            "-t",
            "-A",
            "-c",
            sql,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"psql scalar query failed (exit {proc.returncode}): {proc.stderr.strip() or proc.stdout.strip()}"
            )
        out = (proc.stdout or "").strip()
        return int(out) if out else 0


def ensure_out_dir(base: Path) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = base / ts
    out_dir.mkdir(parents=True, exist_ok=False)
    return out_dir


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def timed_step(name: str, fn):
    started = time.monotonic()
    payload = fn()
    ended = time.monotonic()
    return StepResult(name=name, started_at=started, ended_at=ended, payload=payload)


def timed_step_safe(name: str, fn) -> StepResult:
    started = time.monotonic()
    try:
        payload = fn()
    except Exception as e:  # noqa: BLE001 - want canary to continue on non-critical diagnostics failures
        payload = {"_error": str(e)}
    ended = time.monotonic()
    return StepResult(name=name, started_at=started, ended_at=ended, payload=payload)


def poll_discovery_run(
    api: ApiClient,
    run_id: int,
    max_wait_seconds: int,
    poll_interval_seconds: int,
    out_dir: Path,
    prefix: str,
) -> Dict[str, Any]:
    deadline = time.monotonic() + max_wait_seconds
    snapshots: List[Dict[str, Any]] = []
    last_status = None
    while True:
        status = api.get_json(f"/api/careers/discover/run/{run_id}")
        snapshots.append(status)
        status_name = str(status.get("status") or "")
        if status_name != last_status:
            print(f"[{prefix}] status={status_name}", file=sys.stderr)
            last_status = status_name
        if status_name in TERMINAL_STATUSES:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for discovery run {run_id}")
        time.sleep(poll_interval_seconds)
    write_json(out_dir / f"{prefix}_run_status_snapshots.json", snapshots)
    return status


def top_n_map(m: Optional[Dict[str, Any]], n: int = 3) -> List[Dict[str, Any]]:
    if not isinstance(m, dict):
        return []
    items = sorted(m.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return [{"key": k, "count": int(v)} for k, v in items[:n]]


def sum_map(m: Optional[Dict[str, Any]]) -> int:
    if not isinstance(m, dict):
        return 0
    total = 0
    for v in m.values():
        try:
            total += int(v)
        except Exception:
            continue
    return total


def query_domain_counts(db: DbClient) -> Dict[str, Any]:
    total = db.query_single_int("SELECT COUNT(DISTINCT company_id) FROM company_domains;")
    rows = db.query_tsv(
        """
        SELECT COALESCE(source, 'UNKNOWN') AS source, COUNT(*)::text AS count
        FROM (
          SELECT DISTINCT ON (company_id) company_id, source, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        ) latest
        GROUP BY 1
        ORDER BY COUNT(*) DESC, source
        """
    )
    return {
        "companies_with_domain_count": total,
        "by_source": [{"source": r.get("source", "UNKNOWN"), "count": int(r.get("count", "0"))} for r in rows],
    }


def query_discovery_domain_source_crosstab(db: DbClient, run_id: int) -> List[Dict[str, Any]]:
    rows = db.query_tsv(
        f"""
        SELECT COALESCE(ld.source, 'NONE') AS domain_source,
               COALESCE(ld.resolution_method, 'NONE') AS domain_resolution_method,
               r.status AS discovery_status,
               COUNT(*)::text AS count
        FROM careers_discovery_company_results r
        LEFT JOIN (
          SELECT DISTINCT ON (company_id)
            company_id, source, resolution_method, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        ) ld ON ld.company_id = r.company_id
        WHERE r.discovery_run_id = {int(run_id)}
        GROUP BY 1,2,3
        ORDER BY 1,2,3
        """
    )
    return [
        {
            "domain_source": r.get("domain_source", "NONE"),
            "domain_resolution_method": r.get("domain_resolution_method", "NONE"),
            "discovery_status": r.get("discovery_status", "UNKNOWN"),
            "count": int(r.get("count", "0")),
        }
        for r in rows
    ]


def query_discovery_domain_source_summary(db: DbClient, run_id: int) -> List[Dict[str, Any]]:
    rows = db.query_tsv(
        f"""
        SELECT COALESCE(ld.source, 'NONE') AS domain_source,
               COUNT(*)::text AS attempted_count,
               SUM(CASE WHEN r.status = 'SUCCEEDED' THEN 1 ELSE 0 END)::text AS succeeded_count,
               SUM(CASE WHEN r.status = 'FAILED' THEN 1 ELSE 0 END)::text AS failed_count
        FROM careers_discovery_company_results r
        LEFT JOIN (
          SELECT DISTINCT ON (company_id)
            company_id, source, resolved_at, id
          FROM company_domains
          ORDER BY company_id, resolved_at DESC NULLS LAST, id DESC
        ) ld ON ld.company_id = r.company_id
        WHERE r.discovery_run_id = {int(run_id)}
        GROUP BY 1
        ORDER BY COUNT(*) DESC, domain_source
        """
    )
    out: List[Dict[str, Any]] = []
    for r in rows:
        attempted = int(r.get("attempted_count", "0"))
        succeeded = int(r.get("succeeded_count", "0"))
        out.append(
            {
                "domain_source": r.get("domain_source", "NONE"),
                "attempted_count": attempted,
                "succeeded_count": succeeded,
                "failed_count": int(r.get("failed_count", "0")),
                "success_rate": round((succeeded / attempted) if attempted else 0.0, 4),
            }
        )
    return out


def infer_domain_bottleneck(domain_result: Dict[str, Any], probe_status: Dict[str, Any], requested_discover_limit: int) -> Dict[str, Any]:
    metrics = domain_result.get("metrics") or {}
    attempted = int(metrics.get("companiesAttemptedCount") or 0)
    resolved = int(domain_result.get("resolvedCount") or 0)
    processed = int(probe_status.get("processedCount") or 0)
    failures_by_reason = probe_status.get("failuresByReason") or {}
    no_domain_reason_count = 0
    if isinstance(failures_by_reason, dict):
        for k, v in failures_by_reason.items():
            if "domain" in str(k).lower():
                no_domain_reason_count += int(v)
    requested = int(requested_discover_limit)
    processed_fill_rate = (processed / requested) if requested else 0.0
    likely_domain_limited = no_domain_reason_count > 0 or (processed_fill_rate < 0.8 and resolved == 0 and attempted > 0)
    return {
        "likely_domain_limited": likely_domain_limited,
        "no_domain_reason_count": no_domain_reason_count,
        "discover_processed_fill_rate_vs_requested": round(processed_fill_rate, 4),
        "notes": (
            "Flagged domain-limited because ATS run reported domain-related failures or could not fill the requested batch after resolution."
            if likely_domain_limited
            else "ATS failures are not dominated by domain absence in this canary based on run reasons/fill rate."
        ),
    }


def run(args: argparse.Namespace) -> int:
    out_dir = ensure_out_dir(Path(args.out_dir))
    api = ApiClient(args.base_url, args.http_timeout_seconds)
    db = DbClient(args.postgres_container, args.postgres_user, args.postgres_db)

    print(f"Output directory: {out_dir}", file=sys.stderr)

    # Pre-flight readiness
    status_step = timed_step("api_status", lambda: api.get_json("/api/status"))
    write_json(out_dir / "api_status.json", status_step.payload)

    domain_counts_before = timed_step("domain_counts_before", lambda: query_domain_counts(db))
    write_json(out_dir / "domain_counts_before.json", domain_counts_before.payload)

    ingest_step = timed_step(
        "sec_ingest",
        lambda: api.post_json("/api/universe/ingest/sec", {"limit": args.sec_limit}),
    )
    write_json(out_dir / "sec_ingest_response.json", ingest_step.payload)

    domain_resolve_step = timed_step(
        "domain_resolve",
        lambda: api.post_json("/api/domains/resolve", {"limit": args.resolve_limit}),
    )
    write_json(out_dir / "domain_resolve_response.json", domain_resolve_step.payload)

    domain_counts_after = timed_step("domain_counts_after", lambda: query_domain_counts(db))
    write_json(out_dir / "domain_counts_after.json", domain_counts_after.payload)

    # ATS vendor-probe canary
    discover_probe_start = timed_step(
        "ats_discovery_vendor_probe_start",
        lambda: api.post_json(
            "/api/careers/discover",
            {"limit": args.discover_limit, "vendorProbeOnly": "true"},
        ),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_start.json", discover_probe_start.payload)
    probe_run_id = int(discover_probe_start.payload["discoveryRunId"])

    probe_status_step_started = time.monotonic()
    probe_final_status = poll_discovery_run(
        api,
        probe_run_id,
        args.max_wait_seconds,
        args.poll_interval_seconds,
        out_dir,
        "ats_discovery_vendor_probe",
    )
    probe_status_step = StepResult(
        name="ats_discovery_vendor_probe_wait",
        started_at=probe_status_step_started,
        ended_at=time.monotonic(),
        payload=probe_final_status,
    )
    write_json(out_dir / "ats_discovery_vendor_probe_final_status.json", probe_final_status)

    probe_companies = timed_step_safe(
        "ats_discovery_vendor_probe_companies",
        lambda: api.get_json(
            f"/api/careers/discover/run/{probe_run_id}/companies",
            {"limit": args.discovery_samples_limit},
        ),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_companies.json", probe_companies.payload)

    probe_failures = timed_step_safe(
        "ats_discovery_vendor_probe_failures",
        lambda: api.get_json(f"/api/careers/discover/run/{probe_run_id}/failures"),
    )
    write_json(out_dir / "ats_discovery_vendor_probe_failures.json", probe_failures.payload)

    # Diagnostics captured after probe run
    coverage_step = timed_step_safe(
        "coverage_diagnostics", lambda: api.get_json("/api/diagnostics/coverage")
    )
    write_json(out_dir / "coverage_diagnostics.json", coverage_step.payload)

    discovery_failures_diag_step = timed_step_safe(
        "discovery_failures_diagnostics",
        lambda: api.get_json("/api/diagnostics/discovery-failures"),
    )
    write_json(out_dir / "discovery_failures_diagnostics.json", discovery_failures_diag_step.payload)

    probe_crosstab = timed_step(
        "probe_domain_source_x_ats_success",
        lambda: {
            "summary": query_discovery_domain_source_summary(db, probe_run_id),
            "rows": query_discovery_domain_source_crosstab(db, probe_run_id),
        },
    )
    write_json(out_dir / "probe_domain_source_x_ats_success.json", probe_crosstab.payload)

    # Optional full discovery if probe meets threshold
    full_run_summary = None
    full_steps: List[StepResult] = []
    probe_processed = int(probe_final_status.get("processedCount") or 0)
    probe_succeeded = int(probe_final_status.get("succeededCount") or 0)
    probe_success_rate = (probe_succeeded / probe_processed) if probe_processed else 0.0
    should_run_full = args.run_full_if_good and probe_success_rate >= args.vendor_probe_success_threshold

    if should_run_full:
        full_start = timed_step(
            "ats_discovery_full_start",
            lambda: api.post_json(
                "/api/careers/discover",
                {"limit": args.discover_limit, "vendorProbeOnly": "false"},
            ),
        )
        full_steps.append(full_start)
        write_json(out_dir / "ats_discovery_full_start.json", full_start.payload)
        full_run_id = int(full_start.payload["discoveryRunId"])

        full_wait_started = time.monotonic()
        full_final = poll_discovery_run(
            api,
            full_run_id,
            args.max_wait_seconds,
            args.poll_interval_seconds,
            out_dir,
            "ats_discovery_full",
        )
        full_wait = StepResult(
            name="ats_discovery_full_wait",
            started_at=full_wait_started,
            ended_at=time.monotonic(),
            payload=full_final,
        )
        full_steps.append(full_wait)
        write_json(out_dir / "ats_discovery_full_final_status.json", full_final)

        full_failures = timed_step_safe(
            "ats_discovery_full_failures",
            lambda: api.get_json(f"/api/careers/discover/run/{full_run_id}/failures"),
        )
        full_steps.append(full_failures)
        write_json(out_dir / "ats_discovery_full_failures.json", full_failures.payload)

        full_crosstab = timed_step(
            "full_domain_source_x_ats_success",
            lambda: {
                "summary": query_discovery_domain_source_summary(db, full_run_id),
                "rows": query_discovery_domain_source_crosstab(db, full_run_id),
            },
        )
        full_steps.append(full_crosstab)
        write_json(out_dir / "full_domain_source_x_ats_success.json", full_crosstab.payload)

        full_run_summary = {
            "discovery_run_id": full_run_id,
            "final_status": full_final,
            "failures": full_failures.payload,
            "domain_source_x_ats_success": full_crosstab.payload,
        }

    # Build summary
    domain_result = domain_resolve_step.payload
    domain_metrics = domain_result.get("metrics") or {}
    domain_attempted = int(domain_metrics.get("companiesAttemptedCount") or 0)
    domain_resolved = int(domain_result.get("resolvedCount") or 0)
    domain_resolution_rate = (domain_resolved / domain_attempted) if domain_attempted else None

    coverage_payload = coverage_step.payload if isinstance(coverage_step.payload, dict) else {}
    failures_diag_payload = (
        discovery_failures_diag_step.payload if isinstance(discovery_failures_diag_step.payload, dict) else {}
    )

    success_thresholds = {
        "vendor_probe_success_rate_gte": args.vendor_probe_success_threshold,
        "vendor_probe_success_rate_actual": round(probe_success_rate, 4),
        "vendor_probe_success_rate_pass": probe_success_rate >= args.vendor_probe_success_threshold,
        "justification": (
            "Initial threshold is set to 45% because vendorProbeOnly is a precision-first ATS endpoint detection pass and should clear a meaningful fraction of domain-qualified companies before investing in deeper discovery."
        ),
    }

    domain_bottleneck = infer_domain_bottleneck(domain_result, probe_final_status, args.discover_limit)

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
    }
    for s in full_steps:
        step_durations_ms[s.name] = s.duration_ms

    total_wall_clock_ms = sum(step_durations_ms.values())

    companies_with_domain_before = int(domain_counts_before.payload.get("companies_with_domain_count") or 0)
    companies_with_domain_after = int(domain_counts_after.payload.get("companies_with_domain_count") or 0)

    summary = {
        "schema_version": "ats-canary-summary-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "base_url": args.base_url,
            "sec_limit": args.sec_limit,
            "resolve_limit": args.resolve_limit,
            "discover_limit": args.discover_limit,
            "vendor_probe_success_threshold": args.vendor_probe_success_threshold,
            "run_full_if_good": args.run_full_if_good,
            "max_wait_seconds": args.max_wait_seconds,
            "poll_interval_seconds": args.poll_interval_seconds,
        },
        "inputs_and_counts": {
            "companies_ingested_count": int(ingest_step.payload.get("inserted") or 0) + int(ingest_step.payload.get("updated") or 0),
            "sec_ingest_inserted": int(ingest_step.payload.get("inserted") or 0),
            "sec_ingest_updated": int(ingest_step.payload.get("updated") or 0),
            "sec_universe_total_reported": int(ingest_step.payload.get("total") or 0),
            "companies_with_domain_count_before": companies_with_domain_before,
            "companies_with_domain_count_after": companies_with_domain_after,
            "companies_with_domain_net_new": companies_with_domain_after - companies_with_domain_before,
            "companies_with_domain_by_source_before": domain_counts_before.payload.get("by_source") or [],
            "companies_with_domain_by_source_after": domain_counts_after.payload.get("by_source") or [],
        },
        "domain_resolution": {
            "requested_limit": args.resolve_limit,
            "resolved_count": domain_resolved,
            "domain_resolution_rate": round(domain_resolution_rate, 4) if domain_resolution_rate is not None else None,
            "result": domain_result,
        },
        "ats_vendor_probe": {
            "discovery_run_id": probe_run_id,
            "ats_discovery_attempted_count": probe_processed,
            "ats_discovery_success_count": probe_succeeded,
            "ats_discovery_success_rate": round(probe_success_rate, 4),
            "final_status": probe_final_status,
            "run_failures": probe_failures.payload,
            "domain_source_x_ats_success": probe_crosstab.payload,
        },
        "breakdowns": {
            "vendor_probe_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundVendorProbeByAtsType") or {},
            "homepage_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundHomepageByAtsType") or {},
            "sitemap_endpoints_found_by_vendor": probe_final_status.get("endpointsFoundSitemapByAtsType") or {},
            "coverage_ats_endpoints_by_type": coverage_payload.get("atsEndpointsByType") or {},
            "coverage_ats_endpoints_by_method": coverage_payload.get("atsEndpointsByMethod") or {},
            "top_failure_categories_run": top_n_map(probe_final_status.get("failuresByReason"), 3),
            "top_failure_categories_diagnostics": top_n_map(failures_diag_payload.get("countsByReason"), 3),
            "discovery_failures_recent_samples": (failures_diag_payload.get("recentFailures") or [])[: min(args.failure_samples_limit, 20)],
        },
        "thresholds": success_thresholds,
        "domain_backlog_watch": {
            "heuristic_overfit_risk_check": "Compare ATS success by domain_source; if HEURISTIC underperforms materially vs WIKIDATA/WIKIPEDIA, inspect brand alias/host quality before expanding domains further.",
            "probe_domain_source_success_summary": probe_crosstab.payload.get("summary") if isinstance(probe_crosstab.payload, dict) else [],
        },
        "bottleneck_assessment": {
            **domain_bottleneck,
            "next_focus": (
                "ATS discovery improvements" if not domain_bottleneck.get("likely_domain_limited") else "Domain resolution / targeting still limiting ATS coverage"
            ),
        },
        "optional_full_discovery": {
            "executed": bool(full_run_summary),
            "skipped_reason": None
            if full_run_summary
            else (
                "disabled_by_flag" if not args.run_full_if_good else f"vendor_probe_success_rate_below_threshold ({round(probe_success_rate,4)} < {args.vendor_probe_success_threshold})"
            ),
            "result": full_run_summary,
        },
        "durations_ms": {
            "per_step": step_durations_ms,
            "total_wall_clock_ms": total_wall_clock_ms,
        },
        "raw_files": sorted([p.name for p in out_dir.iterdir() if p.is_file()]),
    }

    write_json(out_dir / "canary_summary.json", summary)

    # Minimal machine-readable schema (descriptive keys)
    summary_schema = {
        "schema_version": "ats-canary-summary-schema-v1",
        "required_top_level_fields": [
            "schema_version",
            "generated_at",
            "config",
            "inputs_and_counts",
            "domain_resolution",
            "ats_vendor_probe",
            "breakdowns",
            "thresholds",
            "domain_backlog_watch",
            "bottleneck_assessment",
            "optional_full_discovery",
            "durations_ms",
            "raw_files",
        ],
        "key_metrics": [
            "inputs_and_counts.companies_ingested_count",
            "inputs_and_counts.companies_with_domain_count_before",
            "inputs_and_counts.companies_with_domain_count_after",
            "domain_resolution.domain_resolution_rate",
            "ats_vendor_probe.ats_discovery_attempted_count",
            "ats_vendor_probe.ats_discovery_success_count",
            "ats_vendor_probe.ats_discovery_success_rate",
            "breakdowns.coverage_ats_endpoints_by_type",
            "breakdowns.top_failure_categories_diagnostics",
            "domain_backlog_watch.probe_domain_source_success_summary",
        ],
    }
    write_json(out_dir / "canary_summary_schema.json", summary_schema)

    print("Canary Summary", file=sys.stderr)
    print(json.dumps({
        "out_dir": str(out_dir),
        "domain_resolution_rate": summary["domain_resolution"]["domain_resolution_rate"],
        "ats_vendor_probe_success_rate": summary["ats_vendor_probe"]["ats_discovery_success_rate"],
        "threshold_pass": summary["thresholds"]["vendor_probe_success_rate_pass"],
        "next_focus": summary["bottleneck_assessment"]["next_focus"],
        "top_run_failures": summary["breakdowns"]["top_failure_categories_run"],
        "domain_source_success": summary["domain_backlog_watch"]["probe_domain_source_success_summary"],
    }, indent=2), file=sys.stderr)

    return 0


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bounded ATS validation canary and save JSON artifacts")
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
    p.add_argument(
        "--vendor-probe-success-threshold",
        type=float,
        default=float(os.getenv("DELTA_VENDOR_PROBE_SUCCESS_THRESHOLD", str(DEFAULT_VENDOR_PROBE_SUCCESS_THRESHOLD))),
    )
    p.add_argument(
        "--run-full-if-good",
        action="store_true",
        default=os.getenv("DELTA_RUN_FULL_IF_GOOD", "false").lower() == "true",
        help="Run a full ATS discovery pass after vendor-probe if the probe success rate meets threshold",
    )
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
