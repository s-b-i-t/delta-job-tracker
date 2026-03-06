#!/usr/bin/env python3
"""Run bounded e2e pipeline proof (frontier seed -> fetch -> ATS discovery)."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "COMPLETED", "CANCELLED", "ABORTED"}

PRESETS: Dict[str, Dict[str, Any]] = {
    "S": {
        "frontier_seed_domain_limit": 20,
        "frontier_seed_fetches": 1,
        "frontier_fetch_domain_limit": 1,
        "frontier_fetch_fetches": 1,
        "ats_limit": 3,
        "ats_batch_size": 3,
        "ats_vendor_probe_only": False,
        "poll_interval_seconds": 4,
        "max_wait_seconds": 240,
    }
}

ATS_FIXTURE_COMPANIES: List[Tuple[str, str, str]] = [
    ("000ATSGH1", "Airbnb ATS Fixture", "airbnb.com"),
    ("000ATSGH2", "Stripe ATS Fixture", "stripe.com"),
    ("000ATSGH3", "Coinbase ATS Fixture", "coinbase.com"),
]


@dataclass
class ApiCallRecord:
    stage: str
    method: str
    url: str
    duration_ms: int
    status: int
    ok: bool
    response_preview: str
    error: Optional[str]


class ApiRecorder:
    def __init__(self, base_url: str, timeout_seconds: int):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.calls: List[ApiCallRecord] = []

    def request_json(
        self, stage: str, method: str, path: str, query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            query_str = urlencode({k: v for k, v in query.items() if v is not None})
            if query_str:
                url = f"{url}?{query_str}"
        req = Request(url, method=method)
        req.add_header("Accept", "application/json")
        started = time.monotonic()
        try:
            with urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
                status = int(getattr(resp, "status", 200))
                duration_ms = int((time.monotonic() - started) * 1000)
                self.calls.append(
                    ApiCallRecord(
                        stage=stage,
                        method=method,
                        url=url,
                        duration_ms=duration_ms,
                        status=status,
                        ok=True,
                        response_preview=raw[:600],
                        error=None,
                    )
                )
                return json.loads(raw) if raw.strip() else {}
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            duration_ms = int((time.monotonic() - started) * 1000)
            self.calls.append(
                ApiCallRecord(
                    stage=stage,
                    method=method,
                    url=url,
                    duration_ms=duration_ms,
                    status=int(exc.code),
                    ok=False,
                    response_preview=body[:600],
                    error=f"HTTPError {exc.code}",
                )
            )
            raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body[:300]}") from exc
        except URLError as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            self.calls.append(
                ApiCallRecord(
                    stage=stage,
                    method=method,
                    url=url,
                    duration_ms=duration_ms,
                    status=0,
                    ok=False,
                    response_preview="",
                    error=str(exc),
                )
            )
            raise RuntimeError(f"{method} {url} failed: {exc}") from exc
        except (TimeoutError, socket.timeout) as exc:
            duration_ms = int((time.monotonic() - started) * 1000)
            self.calls.append(
                ApiCallRecord(
                    stage=stage,
                    method=method,
                    url=url,
                    duration_ms=duration_ms,
                    status=0,
                    ok=False,
                    response_preview="",
                    error=f"timeout after {self.timeout_seconds}s",
                )
            )
            raise RuntimeError(f"{method} {url} timed out after {self.timeout_seconds}s") from exc


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def ensure_out_dir(base: Path) -> Path:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stamp_dir = base / stamp
    stamp_dir.mkdir(parents=True, exist_ok=False)
    out_dir = stamp_dir / "e2e_pipeline_run"
    out_dir.mkdir(parents=True, exist_ok=False)
    return out_dir


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_psql_csv(container: str, user: str, database: str, sql: str) -> List[Dict[str, str]]:
    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "psql",
        "-U",
        user,
        "-d",
        database,
        "--csv",
        "-c",
        sql,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed: {proc.stderr.strip() or proc.stdout.strip()}")
    out = proc.stdout.strip()
    if not out:
        return []
    lines = out.splitlines()
    if len(lines) < 2:
        return []
    headers = [h.strip() for h in lines[0].split(",")]
    rows: List[Dict[str, str]] = []
    for line in lines[1:]:
        values = [v.strip() for v in line.split(",")]
        row: Dict[str, str] = {}
        for i, header in enumerate(headers):
            row[header] = values[i] if i < len(values) else ""
        rows.append(row)
    return rows


def run_psql(container: str, user: str, database: str, sql: str) -> None:
    cmd = ["docker", "exec", "-i", container, "psql", "-U", user, "-d", database, "-c", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed: {proc.stderr.strip() or proc.stdout.strip()}")


def seed_deterministic_ats_fixtures(container: str, user: str, database: str) -> Dict[str, Any]:
    seeded: List[Dict[str, Any]] = []
    for ticker, name, domain in ATS_FIXTURE_COMPANIES:
        rows = run_psql_csv(
            container,
            user,
            database,
            f"""
            INSERT INTO companies (ticker, name, sector)
            VALUES ('{ticker}', '{name.replace("'", "''")}', 'Technology')
            ON CONFLICT (ticker)
            DO UPDATE SET name = EXCLUDED.name, sector = EXCLUDED.sector
            RETURNING id
            """,
        )
        if not rows:
            continue
        company_id = parse_int(rows[0].get("id"))
        run_psql(
            container,
            user,
            database,
            f"""
            INSERT INTO company_domains (company_id, domain, careers_hint_url, source, confidence, resolved_at)
            VALUES ({company_id}, '{domain}', NULL, 'MANUAL', 1.0, NOW())
            ON CONFLICT (company_id, domain)
            DO UPDATE SET confidence = EXCLUDED.confidence, resolved_at = NOW()
            """,
        )
        run_psql(container, user, database, f"DELETE FROM ats_endpoints WHERE company_id = {company_id}")
        run_psql(container, user, database, f"DELETE FROM careers_discovery_state WHERE company_id = {company_id}")
        seeded.append({"ticker": ticker, "companyId": company_id, "domain": domain})
    return {"seededFixtures": seeded, "count": len(seeded)}


def get_ats_endpoint_counts(container: str, user: str, database: str) -> Dict[str, int]:
    rows = run_psql_csv(
        container,
        user,
        database,
        """
        SELECT COUNT(*) AS endpoints_total,
               COUNT(DISTINCT company_id) AS companies_with_endpoint
        FROM ats_endpoints
        """,
    )
    if not rows:
        return {"endpoints_total": 0, "companies_with_endpoint": 0}
    return {
        "endpoints_total": parse_int(rows[0].get("endpoints_total")),
        "companies_with_endpoint": parse_int(rows[0].get("companies_with_endpoint")),
    }


def get_companies_with_domain_count(container: str, user: str, database: str) -> int:
    rows = run_psql_csv(
        container,
        user,
        database,
        """
        SELECT COUNT(DISTINCT company_id) AS companies_with_domain
        FROM company_domains
        """,
    )
    if not rows:
        return 0
    return parse_int(rows[0].get("companies_with_domain"))


def build_proof_report(
    run_dir: Path,
    *,
    preset: str,
    go_no_go: str,
    crawl_url_attempts: int | None,
    ats_before: Dict[str, int] | None,
    ats_after: Dict[str, int] | None,
    domains_before: int | None,
    domains_after: int | None,
    error: str | None = None,
) -> Dict[str, Any]:
    stamp_dir = run_dir.parent

    endpoints_before = parse_int((ats_before or {}).get("endpoints_total"))
    endpoints_after = parse_int((ats_after or {}).get("endpoints_total"))
    companies_with_endpoint_before = parse_int((ats_before or {}).get("companies_with_endpoint"))
    companies_with_endpoint_after = parse_int((ats_after or {}).get("companies_with_endpoint"))

    domains_before_i = parse_int(domains_before)
    domains_after_i = parse_int(domains_after)

    payload: Dict[str, Any] = {
        "schemaVersion": "e2e-pipeline-proof-report-v1",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "preset": preset,
        "stampDir": str(stamp_dir),
        "runDir": str(run_dir),
        "go_no_go": go_no_go,
        "evidence": {
            "crawl_url_attempts": crawl_url_attempts,
            "ats_endpoints_count": {
                "before": endpoints_before,
                "after": endpoints_after,
                "delta": endpoints_after - endpoints_before,
            },
            "companies_with_ats_endpoint_count": {
                "before": companies_with_endpoint_before,
                "after": companies_with_endpoint_after,
                "delta": companies_with_endpoint_after - companies_with_endpoint_before,
            },
            "companies_with_domain_count": {
                "before": domains_before_i,
                "after": domains_after_i,
                "delta": domains_after_i - domains_before_i,
            },
        },
        "successCriteria": {
            "crawl_url_attempts_gt_0": bool(crawl_url_attempts is not None and crawl_url_attempts > 0),
            "go_no_go_is_go": go_no_go == "GO",
        },
        "artifacts": {
            "proofReportPath": str(stamp_dir / "proof_report.json"),
            "summaryPath": str(run_dir / "summary.json"),
            "apiCallsPath": str(run_dir / "api_calls.json"),
        },
    }
    if error:
        payload["error"] = {"message": error}
    return payload


def poll_discovery_run(
    api: ApiRecorder,
    run_id: int,
    max_wait_seconds: int,
    poll_interval_seconds: int,
    out_dir: Path,
) -> Dict[str, Any]:
    snapshots: List[Dict[str, Any]] = []
    deadline = time.monotonic() + max_wait_seconds
    last_status = None
    while True:
        status = api.request_json("ats_poll", "GET", f"/api/careers/discover/run/{run_id}")
        snapshots.append(status)
        state = str(status.get("status") or "")
        if state != last_status:
            print(f"[ats_stage] run_id={run_id} status={state}", file=sys.stderr)
            last_status = state
        if state in TERMINAL_STATUSES:
            write_json(out_dir / "ats_run_status_snapshots.json", snapshots)
            return status
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Timed out waiting for discovery run {run_id}")
        time.sleep(max(1, poll_interval_seconds))


def build_histogram(values: List[Any]) -> Dict[str, int]:
    hist: Dict[str, int] = {}
    for value in values:
        if value is None:
            continue
        key = str(value).strip()
        if not key:
            continue
        hist[key] = hist.get(key, 0) + 1
    return dict(sorted(hist.items(), key=lambda kv: (-kv[1], kv[0])))


def build_top_errors(companies_payload: Any, limit: int = 10) -> List[Dict[str, Any]]:
    if not isinstance(companies_payload, list):
        return []
    counts: Dict[str, int] = {}
    for row in companies_payload:
        if not isinstance(row, dict):
            continue
        reason = (row.get("reasonCode") or "UNKNOWN").strip()
        detail = (row.get("errorDetail") or "").strip()
        key = f"{reason}|{detail[:120]}"
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    out: List[Dict[str, Any]] = []
    for key, count in ordered[:limit]:
        reason, detail = key.split("|", 1)
        out.append({"reasonCode": reason, "detail": detail, "count": count})
    return out


def stage_int(stage: Dict[str, Any], *keys: str) -> int:
    for key in keys:
        if key in stage:
            return parse_int(stage.get(key))
    return 0


def classify_blockers(
    frontier_stage: Dict[str, Any], fetch_stage: Dict[str, Any], ats_stage: Dict[str, Any]
) -> List[str]:
    blockers: List[str] = []
    selection_returned = stage_int(ats_stage, "selectionReturnedCount", "selection_returned_count")
    attempted_companies = stage_int(ats_stage, "attemptedCompanies", "attempted_companies")
    succeeded_count = stage_int(ats_stage, "succeededCount", "succeeded_count")
    vendor_detected = stage_int(ats_stage, "vendorDetectedCount", "vendor_detected_count")
    created_endpoints = stage_int(ats_stage, "createdEndpoints", "created_endpoints")

    if selection_returned <= 0:
        blockers.append("no_eligible_companies")
    if parse_int(frontier_stage.get("urlsEnqueued")) <= 0 and parse_int(fetch_stage.get("urlsFetched")) <= 0:
        blockers.append("no_frontier_candidates")
    if parse_int(fetch_stage.get("urlsFetched")) <= 0 and parse_int(fetch_stage.get("httpRequestCount")) > 0:
        blockers.append("all_fetch_attempts_failed")
    if (
        parse_int(fetch_stage.get("urlsFetched")) <= 0
        and parse_int((fetch_stage.get("statusBucketCounts") or {}).get("ROBOTS_BLOCKED")) > 0
    ):
        blockers.append("all_candidates_filtered_robots")
    if attempted_companies > 0 and vendor_detected <= 0:
        blockers.append("vendor_detection_failed")
    if vendor_detected > 0 and created_endpoints <= 0:
        blockers.append("endpoint_extraction_failed")
    if attempted_companies > 0 and succeeded_count <= 0:
        blockers.append("all_ats_attempts_failed")
    error_histogram = ats_stage.get("error_type_histogram") or ats_stage.get("errorTypeHistogram") or {}
    if isinstance(error_histogram, dict):
        fetch_failures = 0
        robots_failures = 0
        total_errors = 0
        for key, value in error_histogram.items():
            reason = str(key).lower()
            count = parse_int(value)
            if count <= 0:
                continue
            total_errors += count
            if "fetch_failed" in reason or "timeout" in reason or "connection" in reason:
                fetch_failures += count
            if "robots" in reason:
                robots_failures += count
        if total_errors > 0 and fetch_failures == total_errors:
            blockers.append("all_fetch_attempts_failed")
        if total_errors > 0 and robots_failures == total_errors:
            blockers.append("all_candidates_filtered_robots")
    stop_reason = str(ats_stage.get("stopReason") or "").strip()
    if stop_reason:
        blockers.append(f"run_aborted_{stop_reason}")
    deduped = []
    for blocker in blockers:
        if blocker not in deduped:
            deduped.append(blocker)
    if created_endpoints <= 0 and not deduped:
        deduped.append("unknown_zero_endpoints")
    return deduped


def build_go_no_go(ats_stage: Dict[str, Any], blockers: List[str]) -> str:
    if parse_int(ats_stage.get("createdEndpoints")) > 0:
        return "GO"
    return "NO-GO" if blockers else "NO-GO"


def run_pipeline(args: argparse.Namespace) -> Tuple[Dict[str, Any], Path]:
    preset = PRESETS[args.preset]
    out_dir = ensure_out_dir(Path(args.out_base))
    stamp_dir = out_dir.parent
    api = ApiRecorder(args.base_url, args.http_timeout_seconds)

    status_payload = api.request_json("preflight", "GET", "/api/status")
    write_json(out_dir / "preflight_api_status.json", status_payload)

    fixtures_payload: Dict[str, Any] = {"seededFixtures": [], "count": 0}
    if not args.skip_fixture_seed:
        fixtures_payload = seed_deterministic_ats_fixtures(
            args.postgres_container, args.postgres_user, args.postgres_db
        )
    write_json(out_dir / "ats_fixture_seed.json", fixtures_payload)

    ats_before = get_ats_endpoint_counts(args.postgres_container, args.postgres_user, args.postgres_db)
    domains_before = get_companies_with_domain_count(
        args.postgres_container, args.postgres_user, args.postgres_db
    )

    frontier_seed_payload = api.request_json(
        "frontier_seed",
        "POST",
        "/api/frontier/seed",
        {
            "domainLimit": preset["frontier_seed_domain_limit"],
            "maxSitemapFetches": preset["frontier_seed_fetches"],
        },
    )
    write_json(out_dir / "frontier_seed_stage.json", frontier_seed_payload)

    frontier_fetch_payload = api.request_json(
        "frontier_fetch",
        "POST",
        "/api/frontier/seed",
        {
            "domainLimit": preset["frontier_fetch_domain_limit"],
            "maxSitemapFetches": preset["frontier_fetch_fetches"],
        },
    )
    write_json(out_dir / "frontier_fetch_stage.json", frontier_fetch_payload)

    discover_start_payload = api.request_json(
        "ats_start",
        "POST",
        "/api/careers/discover",
        {
            "limit": preset["ats_limit"],
            "batchSize": preset["ats_batch_size"],
            "vendorProbeOnly": "true" if preset["ats_vendor_probe_only"] else "false",
        },
    )
    write_json(out_dir / "ats_discovery_start.json", discover_start_payload)
    run_id = parse_int(discover_start_payload.get("discoveryRunId"))
    if run_id <= 0:
        raise RuntimeError(f"Missing discoveryRunId in response: {discover_start_payload}")

    final_status = poll_discovery_run(
        api,
        run_id,
        int(preset["max_wait_seconds"]),
        int(preset["poll_interval_seconds"]),
        out_dir,
    )
    write_json(out_dir / "ats_discovery_final_status.json", final_status)

    companies_payload = api.request_json(
        "ats_companies", "GET", f"/api/careers/discover/run/{run_id}/companies", {"limit": 500}
    )
    failures_payload = api.request_json("ats_failures", "GET", f"/api/careers/discover/run/{run_id}/failures")
    write_json(out_dir / "ats_discovery_companies.json", companies_payload)
    write_json(out_dir / "ats_discovery_failures.json", failures_payload)

    ats_after = get_ats_endpoint_counts(args.postgres_container, args.postgres_user, args.postgres_db)
    domains_after = get_companies_with_domain_count(args.postgres_container, args.postgres_user, args.postgres_db)
    created_endpoints = max(0, ats_after["endpoints_total"] - ats_before["endpoints_total"])
    created_companies = max(
        0, ats_after["companies_with_endpoint"] - ats_before["companies_with_endpoint"]
    )

    attempted_urls = set()
    if isinstance(companies_payload, list):
        for row in companies_payload:
            if not isinstance(row, dict):
                continue
            for key in ("careersUrlFinal", "careersUrlInitial", "endpointUrl"):
                value = row.get(key)
                if value:
                    attempted_urls.add(str(value))

    attempts_from_failures = []
    if isinstance(failures_payload, dict):
        attempts_from_failures = failures_payload.get("failures") or []

    error_type_histogram = build_histogram(
        [row.get("reasonCode") for row in companies_payload if isinstance(row, dict)]
        + [row.get("reasonCode") for row in attempts_from_failures if isinstance(row, dict)]
    )
    http_status_histogram = build_histogram(
        [row.get("httpStatus") for row in companies_payload if isinstance(row, dict)]
        + [row.get("httpStatusFirstFailure") for row in companies_payload if isinstance(row, dict)]
        + [row.get("httpStatus") for row in attempts_from_failures if isinstance(row, dict)]
    )
    top_errors = build_top_errors(companies_payload, limit=10)
    if not top_errors and isinstance(failures_payload, dict):
        top_errors = build_top_errors(attempts_from_failures, limit=10)

    ats_api_calls = []
    for call in api.calls:
        if not call.stage.startswith("ats_"):
            continue
        ats_api_calls.append(
            {
                "url": call.url,
                "duration_ms": call.duration_ms,
                "status": call.status,
                "response_preview": call.response_preview,
                "error": call.error,
            }
        )

    ats_stage: Dict[str, Any] = {
        "runId": run_id,
        "status": final_status.get("status"),
        "stopReason": final_status.get("stopReason") or final_status.get("lastError"),
        "selectionEligibleCount": parse_int(final_status.get("selectionEligibleCount")),
        "selectionReturnedCount": parse_int(final_status.get("selectionReturnedCount")),
        "attemptedCompanies": parse_int(final_status.get("companiesAttemptedCount")),
        "attempted_companies": parse_int(final_status.get("companiesAttemptedCount")),
        "processedCount": parse_int(final_status.get("processedCount")),
        "succeededCount": parse_int(final_status.get("succeededCount")),
        "failedCount": parse_int(final_status.get("failedCount")),
        "vendorDetectedCount": parse_int(final_status.get("vendorDetectedCount")),
        "endpointExtractedCount": parse_int(final_status.get("endpointExtractedCount")),
        "createdEndpoints": created_endpoints,
        "created_endpoints": created_endpoints,
        "createdCompaniesWithEndpoints": created_companies,
        "attemptedUrls": len(attempted_urls),
        "attempted_urls": len(attempted_urls),
        "httpStatusHistogram": http_status_histogram,
        "http_status_histogram": http_status_histogram,
        "errorTypeHistogram": error_type_histogram,
        "error_type_histogram": error_type_histogram,
        "topNErrors": top_errors,
        "top_n_errors": top_errors,
        "backend_calls": ats_api_calls,
    }

    frontier_stage: Dict[str, Any] = {
        "seedDomainsRequested": parse_int(frontier_seed_payload.get("seedDomainsRequested")),
        "seedDomainsUsed": parse_int(frontier_seed_payload.get("seedDomainsUsed")),
        "hostsSeen": parse_int(frontier_seed_payload.get("hostsSeen")),
        "urlsEnqueued": parse_int(frontier_seed_payload.get("urlsEnqueued")),
        "sitemapUrlsEnqueued": parse_int(frontier_seed_payload.get("sitemapUrlsEnqueued")),
        "candidateUrlsEnqueued": parse_int(frontier_seed_payload.get("candidateUrlsEnqueued")),
    }
    fetch_stage: Dict[str, Any] = {
        "urlsFetched": parse_int(frontier_fetch_payload.get("urlsFetched")),
        "blockedByBackoff": parse_int(frontier_fetch_payload.get("blockedByBackoff")),
        "httpRequestCount": parse_int(frontier_fetch_payload.get("httpRequestCount")),
        "http429Count": parse_int(frontier_fetch_payload.get("http429Count")),
        "http429Rate": frontier_fetch_payload.get("http429Rate"),
        "statusBucketCounts": frontier_fetch_payload.get("statusBucketCounts") or {},
        "queueStatusCounts": frontier_fetch_payload.get("queueStatusCounts") or {},
    }

    blockers = classify_blockers(frontier_stage, fetch_stage, ats_stage)
    ats_stage["blockers"] = blockers

    summary: Dict[str, Any] = {
        "schemaVersion": "e2e-pipeline-proof-v1",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "preset": args.preset,
        "config": preset,
        "fixtures": fixtures_payload,
        "frontier_stage": frontier_stage,
        "fetch_stage": fetch_stage,
        "ats_stage": ats_stage,
        "go_no_go": build_go_no_go(ats_stage, blockers),
    }

    write_json(out_dir / "api_calls.json", [call.__dict__ for call in api.calls])
    write_json(out_dir / "summary.json", summary)
    write_json(
        stamp_dir / "proof_report.json",
        build_proof_report(
            out_dir,
            preset=args.preset,
            go_no_go=str(summary.get("go_no_go") or ""),
            crawl_url_attempts=parse_int(fetch_stage.get("httpRequestCount")),
            ats_before=ats_before,
            ats_after=ats_after,
            domains_before=domains_before,
            domains_after=domains_after,
        ),
    )
    return summary, out_dir


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run bounded e2e pipeline proof")
    parser.add_argument("--preset", default="S", choices=sorted(PRESETS.keys()))
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--out-base", default="out")
    parser.add_argument("--http-timeout-seconds", type=int, default=180)
    parser.add_argument("--postgres-container", default="delta-job-tracker-postgres")
    parser.add_argument("--postgres-user", default="delta")
    parser.add_argument("--postgres-db", default="delta_job_tracker")
    parser.add_argument("--skip-fixture-seed", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    try:
        summary, out_dir = run_pipeline(args)
    except Exception as exc:  # noqa: BLE001
        error_payload = {
            "schemaVersion": "e2e-pipeline-proof-v1",
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "preset": args.preset,
            "error": str(exc),
            "go_no_go": "NO-GO",
        }
        fallback_dir = ensure_out_dir(Path(args.out_base))
        write_json(fallback_dir / "summary.json", error_payload)
        write_json(
            fallback_dir.parent / "proof_report.json",
            build_proof_report(
                fallback_dir,
                preset=args.preset,
                go_no_go="NO-GO",
                crawl_url_attempts=None,
                ats_before=None,
                ats_after=None,
                domains_before=None,
                domains_after=None,
                error=str(exc),
            ),
        )
        print(f"e2e_pipeline_proof status=NO-GO error={exc} out_dir={fallback_dir}", file=sys.stderr)
        return 2

    print(
        "e2e_pipeline_proof "
        f"preset={args.preset} "
        f"created_endpoints={parse_int(summary['ats_stage'].get('createdEndpoints'))} "
        f"go_no_go={summary.get('go_no_go')} "
        f"out_dir={out_dir}",
        file=sys.stderr,
    )
    print(
        json.dumps(
            {"out_dir": str(out_dir), "stamp_dir": str(out_dir.parent), "go_no_go": summary.get("go_no_go")},
            indent=2,
        )
    )
    return 0 if summary.get("go_no_go") == "GO" else 1


if __name__ == "__main__":
    raise SystemExit(main())
