#!/usr/bin/env python3
"""Bounded ATS scale canary: frontier seed + ATS discovery + DB-truth metrics."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import efficiency_reporting_contract as erc
import run_ats_validation_canary as canary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run bounded ATS scale canary and emit DB-truth metrics")
    p.add_argument("--base-url", default="http://localhost:8080")
    p.add_argument("--out-dir", default="out")
    p.add_argument("--domain-limit", type=int, default=50)
    p.add_argument("--max-sitemap-fetches", type=int, default=50)
    p.add_argument("--discover-limit", type=int, default=200)
    p.add_argument("--discover-batch-size", type=int, default=25)
    p.add_argument("--vendor-probe-only", action="store_true")
    p.add_argument("--max-wait-seconds", type=int, default=1800)
    p.add_argument("--poll-interval-seconds", type=int, default=5)
    p.add_argument("--http-timeout-seconds", type=int, default=30)
    p.add_argument("--postgres-container", default="delta-job-tracker-postgres")
    p.add_argument("--postgres-user", default="delta")
    p.add_argument("--postgres-db", default="delta_job_tracker")
    return p.parse_args()


def first_int(rows: List[Dict[str, str]], key: str) -> int:
    if not rows:
        return 0
    return canary.parse_int(rows[0].get(key))


def rate_per_hour(count: int, duration_seconds: float) -> float:
    if duration_seconds <= 0:
        return 0.0
    return round((count * 3600.0) / duration_seconds, 3)


def build_canonical_efficiency_report(
    *,
    args: argparse.Namespace,
    out_dir: Path,
    final_status: Dict[str, Any],
    frontier_seed_payload: Dict[str, Any],
    duration_seconds: float,
    attempted: int,
    endpoints_added: int,
    request_count: int,
    error_bucket_counts: Dict[str, int],
    stop_reason_raw: Any,
) -> Dict[str, Any]:
    return erc.build_payload(
        source="run_ats_scale_canary.py",
        context={
            "base_url": args.base_url,
            "discover_limit": args.discover_limit,
            "out_dir": str(out_dir),
            "scope_tags": ["run_scope"],
        },
        phases={
            "domain_resolution": {
                "scope_tag": "run_scope",
                "availability": "not_collected_in_this_surface",
            },
            "queue_fetch": {
                "scope_tag": "run_scope",
                "duration_seconds": None,
                "rate_denominator_seconds": None,
                "counters": {
                    "urls_enqueued": canary.parse_int(frontier_seed_payload.get("urlsEnqueued")),
                    "urls_fetched": canary.parse_int(frontier_seed_payload.get("urlsFetched")),
                    "hosts_seen": canary.parse_int(frontier_seed_payload.get("hostsSeen")),
                },
                "rates_per_min": {
                    "urls_enqueued_per_min": None,
                    "urls_fetched_per_min": None,
                },
            },
            "ats_discovery": {
                "scope_tag": "run_scope",
                "duration_seconds": round(duration_seconds, 3),
                "rate_denominator_seconds": round(duration_seconds, 3),
                "counters": {
                    "companies_attempted": attempted,
                    "endpoints_created": endpoints_added,
                    "request_count_total": request_count,
                },
                "rates_per_min": {
                    "companies_attempted_per_min": erc.rate_per_min(attempted, duration_seconds),
                    "endpoints_created_per_min": erc.rate_per_min(endpoints_added, duration_seconds),
                    "requests_per_min": erc.rate_per_min(request_count, duration_seconds),
                },
                "error_rates": {
                    "error_bucket_count_per_attempted": erc.ratio(
                        sum(erc.parse_int(v) for v in error_bucket_counts.values()),
                        attempted,
                    ),
                },
                "stop_reason": {
                    "raw": str(stop_reason_raw) if stop_reason_raw is not None else None,
                    "normalized": erc.normalize_stop_reason(stop_reason_raw),
                },
                "guardrails": {
                    "request_budget": canary.parse_int(final_status.get("requestBudget")),
                    "hard_timeout_seconds": canary.parse_int(final_status.get("hardTimeoutSeconds")),
                    "host_failure_cutoff_count": canary.parse_int(final_status.get("hostFailureCutoffCount")),
                    "host_failure_cutoff_skips": canary.parse_int(final_status.get("hostFailureCutoffSkips")),
                },
                "error_breakdown": {
                    "bucket_counts": {str(k): erc.parse_int(v) for k, v in error_bucket_counts.items()},
                },
            },
        },
    )


def query_sql_snapshots(db: canary.DbClient, run_id: int) -> Dict[str, Any]:
    table_counts = db.query_csv(
        """
        SELECT
            (SELECT COUNT(*) FROM crawl_hosts) AS crawl_hosts_count,
            (SELECT COUNT(*) FROM crawl_urls) AS crawl_urls_count,
            (SELECT COUNT(*) FROM crawl_url_attempts) AS crawl_url_attempts_count
        """
    )
    endpoints_by_vendor = db.query_csv(
        """
        SELECT ats_type AS vendor, COUNT(*)::text AS count
        FROM ats_endpoints
        GROUP BY ats_type
        ORDER BY COUNT(*) DESC, ats_type
        """
    )
    validation_counts = db.query_csv(
        """
        SELECT CASE WHEN verified THEN 'validated' ELSE 'unvalidated' END AS validation_status,
               COUNT(*)::text AS count
        FROM ats_endpoints
        GROUP BY verified
        ORDER BY validation_status
        """
    )
    attempts_by_error_bucket = db.query_csv(
        f"""
        SELECT COALESCE(reason_code, 'UNKNOWN') AS error_bucket,
               COUNT(*)::text AS count
        FROM careers_discovery_company_results
        WHERE discovery_run_id = {run_id}
          AND reason_code IS NOT NULL
        GROUP BY COALESCE(reason_code, 'UNKNOWN')
        ORDER BY COUNT(*) DESC, error_bucket
        """
    )
    companies_with_endpoint = db.query_csv(
        """
        SELECT COUNT(DISTINCT company_id)::text AS companies_with_endpoint_count
        FROM ats_endpoints
        """
    )
    companies_attempted = db.query_csv(
        f"""
        SELECT companies_attempted_count::text AS companies_attempted_count,
               endpoints_added::text AS endpoints_added,
               request_count_total::text AS request_count_total,
               status,
               stop_reason,
               started_at::text AS started_at,
               finished_at::text AS finished_at
        FROM careers_discovery_runs
        WHERE id = {run_id}
        """
    )
    return {
        "table_counts": table_counts,
        "endpoints_by_vendor": endpoints_by_vendor,
        "validation_counts": validation_counts,
        "attempts_by_error_bucket": attempts_by_error_bucket,
        "companies_with_endpoint": companies_with_endpoint,
        "run_metrics": companies_attempted,
    }


def build_runtime_context(args: argparse.Namespace, out_dir: Path) -> Dict[str, Any]:
    return {
        "base_url": args.base_url,
        "out_dir": str(out_dir),
        "limits": {
            "domain_limit": max(1, args.domain_limit),
            "max_sitemap_fetches": max(1, args.max_sitemap_fetches),
            "discover_limit": max(1, args.discover_limit),
            "discover_batch_size": max(1, args.discover_batch_size),
            "max_wait_seconds": max(1, args.max_wait_seconds),
            "poll_interval_seconds": max(1, args.poll_interval_seconds),
            "http_timeout_seconds": max(1, args.http_timeout_seconds),
        },
        "postgres": {
            "container": args.postgres_container,
            "user": args.postgres_user,
            "db": args.postgres_db,
        },
    }


def write_stage_failure_artifacts(
    *,
    out_dir: Path,
    runtime_context: Dict[str, Any],
    stage_timings_ms: Dict[str, int],
    error_code: str,
    stage_name: str,
    stage_payload: Dict[str, Any],
    request_context: Dict[str, Any],
) -> None:
    stage_failure_summary = {
        "error_code": error_code,
        "stage": stage_name,
        "elapsed_ms": stage_timings_ms.get(stage_name),
        "error_message": stage_payload.get("_error") if isinstance(stage_payload, dict) else None,
        "request_context": request_context,
    }
    stage_diag = {
        "schema_version": "ats-scale-stage-diagnostics-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runtime_context": runtime_context,
        "stage_timings_ms": stage_timings_ms,
        "stage_failure_summary": stage_failure_summary,
    }
    metrics_payload = {
        "error": error_code,
        "details": stage_payload,
        "runtime_context": runtime_context,
        "stage_timings_ms": stage_timings_ms,
        "stage_failure_summary": stage_failure_summary,
        "artifacts": {
            "stage_diagnostics": "stage_diagnostics.json",
        },
    }
    canary.write_json(out_dir / "stage_diagnostics.json", stage_diag)
    canary.write_json(out_dir / "ats_scale_metrics.json", metrics_payload)


def main() -> int:
    args = parse_args()
    out_dir = canary.ensure_out_dir(Path(args.out_dir))
    api = canary.ApiClient(args.base_url, args.http_timeout_seconds)
    db = canary.DbClient(args.postgres_container, args.postgres_user, args.postgres_db)
    runtime_context = build_runtime_context(args, out_dir)
    stage_timings_ms: Dict[str, int] = {}

    preflight = canary.timed_step_safe("preflight_api_status", lambda: api.get_json("/api/status"))
    stage_timings_ms[preflight.name] = preflight.duration_ms
    canary.write_json(out_dir / "preflight_api_status.json", preflight.payload)
    if isinstance(preflight.payload, dict) and preflight.payload.get("_error"):
        write_stage_failure_artifacts(
            out_dir=out_dir,
            runtime_context=runtime_context,
            stage_timings_ms=stage_timings_ms,
            error_code="preflight_failed",
            stage_name=preflight.name,
            stage_payload=preflight.payload,
            request_context={"method": "GET", "path": "/api/status"},
        )
        print(f"ats_scale status=FAILED error=preflight_api_status out_dir={out_dir}", file=sys.stderr)
        return 2

    frontier_seed = canary.timed_step_safe(
        "frontier_seed",
        lambda: api.post_json(
            "/api/frontier/seed",
            {"domainLimit": max(1, args.domain_limit), "maxSitemapFetches": max(1, args.max_sitemap_fetches)},
        ),
    )
    stage_timings_ms[frontier_seed.name] = frontier_seed.duration_ms
    canary.write_json(out_dir / "frontier_seed_response.json", frontier_seed.payload)
    if isinstance(frontier_seed.payload, dict) and frontier_seed.payload.get("_error"):
        write_stage_failure_artifacts(
            out_dir=out_dir,
            runtime_context=runtime_context,
            stage_timings_ms=stage_timings_ms,
            error_code="frontier_seed_failed",
            stage_name=frontier_seed.name,
            stage_payload=frontier_seed.payload,
            request_context={
                "method": "POST",
                "path": "/api/frontier/seed",
                "query": {
                    "domainLimit": max(1, args.domain_limit),
                    "maxSitemapFetches": max(1, args.max_sitemap_fetches),
                },
            },
        )
        print(f"ats_scale status=FAILED error=frontier_seed out_dir={out_dir}", file=sys.stderr)
        return 2

    discover_start = canary.timed_step(
        "ats_discovery_start",
        lambda: api.post_json(
            "/api/careers/discover",
            {
                "limit": max(1, args.discover_limit),
                "batchSize": max(1, args.discover_batch_size),
                "vendorProbeOnly": "true" if args.vendor_probe_only else "false",
            },
        ),
    )
    stage_timings_ms[discover_start.name] = discover_start.duration_ms
    canary.write_json(out_dir / "ats_discovery_start.json", discover_start.payload)
    run_id = canary.parse_int((discover_start.payload or {}).get("discoveryRunId"))
    if run_id <= 0:
        write_stage_failure_artifacts(
            out_dir=out_dir,
            runtime_context=runtime_context,
            stage_timings_ms=stage_timings_ms,
            error_code="missing_discovery_run_id",
            stage_name=discover_start.name,
            stage_payload=discover_start.payload if isinstance(discover_start.payload, dict) else {},
            request_context={
                "method": "POST",
                "path": "/api/careers/discover",
                "query": {
                    "limit": max(1, args.discover_limit),
                    "batchSize": max(1, args.discover_batch_size),
                    "vendorProbeOnly": "true" if args.vendor_probe_only else "false",
                },
            },
        )
        print(f"ats_scale status=FAILED error=missing_discovery_run_id out_dir={out_dir}", file=sys.stderr)
        return 2

    final_status = canary.poll_discovery_run(
        api, run_id, args.max_wait_seconds, args.poll_interval_seconds, out_dir, "ats_scale_discovery"
    )
    canary.write_json(out_dir / "ats_discovery_final_status.json", final_status)

    companies_payload = canary.timed_step_safe(
        "ats_discovery_companies",
        lambda: api.get_json(f"/api/careers/discover/run/{run_id}/companies", {"limit": 500}),
    )
    canary.write_json(out_dir / "ats_discovery_companies.json", companies_payload.payload)
    failures_payload = canary.timed_step_safe(
        "ats_discovery_failures", lambda: api.get_json(f"/api/careers/discover/run/{run_id}/failures")
    )
    canary.write_json(out_dir / "ats_discovery_failures.json", failures_payload.payload)

    sql_snapshots = query_sql_snapshots(db, run_id)
    canary.write_json(out_dir / "ats_scale_sql_snapshots.json", sql_snapshots)

    run_row = (sql_snapshots.get("run_metrics") or [{}])[0]
    attempted = canary.parse_int(run_row.get("companies_attempted_count"))
    endpoints_added = canary.parse_int(run_row.get("endpoints_added"))
    request_count = canary.parse_int(run_row.get("request_count_total"))
    started_at = run_row.get("started_at")
    finished_at = run_row.get("finished_at")

    duration_seconds = 0.0
    if started_at and finished_at:
        try:
            s = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            f = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
            duration_seconds = max(0.0, (f - s).total_seconds())
        except Exception:
            duration_seconds = 0.0

    endpoints_by_vendor = {
        str(row.get("vendor")): canary.parse_int(row.get("count"))
        for row in (sql_snapshots.get("endpoints_by_vendor") or [])
    }
    validation_counts = {
        str(row.get("validation_status")): canary.parse_int(row.get("count"))
        for row in (sql_snapshots.get("validation_counts") or [])
    }
    error_bucket_counts = {
        str(row.get("error_bucket")): canary.parse_int(row.get("count"))
        for row in (sql_snapshots.get("attempts_by_error_bucket") or [])
    }

    metrics = {
        "schema_version": "ats-scale-metrics-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "run_status": final_status.get("status"),
        "stop_reason": run_row.get("stop_reason") or final_status.get("lastError"),
        "guardrails": {
            "request_budget": canary.parse_int(final_status.get("requestBudget")),
            "hard_timeout_seconds": canary.parse_int(final_status.get("hardTimeoutSeconds")),
            "host_failure_cutoff_count": canary.parse_int(final_status.get("hostFailureCutoffCount")),
            "host_failure_cutoff_skips": canary.parse_int(final_status.get("hostFailureCutoffSkips")),
        },
        "frontier_seed": {
            "hostsSeen": canary.parse_int(frontier_seed.payload.get("hostsSeen")),
            "urlsEnqueued": canary.parse_int(frontier_seed.payload.get("urlsEnqueued")),
            "urlsFetched": canary.parse_int(frontier_seed.payload.get("urlsFetched")),
            "blockedByBackoff": canary.parse_int(frontier_seed.payload.get("blockedByBackoff")),
            "http429Rate": float(frontier_seed.payload.get("http429Rate") or 0.0),
        },
        "db_truth": {
            "companiesAttempted": attempted,
            "companiesWithAtLeastOneAtsEndpoint": first_int(
                sql_snapshots.get("companies_with_endpoint") or [], "companies_with_endpoint_count"
            ),
            "endpointsByVendor": endpoints_by_vendor,
            "validatedVsUnvalidated": {
                "validated": validation_counts.get("validated", 0),
                "unvalidated": validation_counts.get("unvalidated", 0),
            },
            "attemptsByErrorBucket": error_bucket_counts,
        },
        "throughput": {
            "durationSeconds": round(duration_seconds, 3),
            "domainsPerHour": rate_per_hour(attempted, duration_seconds),
            "endpointsPerHour": rate_per_hour(endpoints_added, duration_seconds),
            "requestsPerHour": rate_per_hour(request_count, duration_seconds),
            "requestCount": request_count,
            "endpointsAdded": endpoints_added,
        },
        "artifacts": {
            "frontier_seed_response": "frontier_seed_response.json",
            "ats_discovery_final_status": "ats_discovery_final_status.json",
            "ats_scale_sql_snapshots": "ats_scale_sql_snapshots.json",
            "stage_diagnostics": "stage_diagnostics.json",
        },
        "runtime_context": runtime_context,
        "stage_timings_ms": stage_timings_ms,
    }
    canonical_efficiency_report = build_canonical_efficiency_report(
        args=args,
        out_dir=out_dir,
        final_status=final_status,
        frontier_seed_payload=frontier_seed.payload if isinstance(frontier_seed.payload, dict) else {},
        duration_seconds=duration_seconds,
        attempted=attempted,
        endpoints_added=endpoints_added,
        request_count=request_count,
        error_bucket_counts=error_bucket_counts,
        stop_reason_raw=run_row.get("stop_reason") or final_status.get("lastError"),
    )
    metrics["canonical_efficiency_report"] = canonical_efficiency_report

    canary.write_json(
        out_dir / "stage_diagnostics.json",
        {
            "schema_version": "ats-scale-stage-diagnostics-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "runtime_context": runtime_context,
            "stage_timings_ms": stage_timings_ms,
            "stage_failure_summary": None,
        },
    )
    canary.write_json(out_dir / "ats_scale_metrics.json", metrics)
    canary.write_json(out_dir / "canonical_efficiency_report.json", canonical_efficiency_report)
    print(
        "ats_scale "
        f"run_id={run_id} "
        f"status={final_status.get('status')} "
        f"attempted={attempted} "
        f"endpoints_added={endpoints_added} "
        f"requests={request_count} "
        f"domains_per_hour={metrics['throughput']['domainsPerHour']:.3f} "
        f"endpoints_per_hour={metrics['throughput']['endpointsPerHour']:.3f} "
        f"out_dir={out_dir}",
        file=sys.stderr,
    )
    print(json.dumps({"out_dir": str(out_dir), "run_id": run_id, "status": final_status.get("status")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
