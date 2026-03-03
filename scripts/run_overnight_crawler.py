#!/usr/bin/env python3
"""Overnight unattended runner for SEC -> domain -> ATS discovery.

Flow:
1) SEC ingest
2) Domain resolve (optional missing-domain cohort sampling before/after)
3) ATS discovery vendorProbeOnly in batches
4) ATS discovery full mode in batches if vendorProbe stage passes threshold
5) Writes summary + raw captures to out/<timestamp>/

This script reuses helpers from scripts/run_ats_validation_canary.py.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import run_ats_validation_canary as canary

EXIT_OK = 0
EXIT_PREREQ_FAILED = 2
EXIT_TIMEOUT = 3
EXIT_RUNTIME_FAILED = 4

RUNNING_STATUSES = {"RUNNING"}
DOMAIN_RESOLVE_TOP_LEVEL_INT_FIELDS = (
    "resolvedCount",
    "noWikipediaTitleCount",
    "noItemCount",
    "noP856Count",
    "wdqsErrorCount",
    "wdqsTimeoutCount",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_running_status(status: Any) -> bool:
    return str(status or "").upper() in RUNNING_STATUSES


def remaining_seconds(deadline: Optional[float]) -> Optional[int]:
    if deadline is None:
        return None
    return max(0, int(deadline - time.monotonic()))


def ensure_within_deadline(deadline: Optional[float], context: str) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        raise TimeoutError(f"Overall timeout reached while {context}")


def try_get_latest_run(api: canary.ApiClient) -> Optional[Dict[str, Any]]:
    try:
        return api.get_json("/api/careers/discover/runs/latest")
    except Exception:
        return None


def run_discovery_batch(
    *,
    api: canary.ApiClient,
    db: canary.DbClient,
    out_dir: Path,
    batch_index: int,
    discover_batch_size: int,
    vendor_probe_only: bool,
    poll_interval_seconds: int,
    max_run_wait_seconds: int,
    overall_deadline: Optional[float],
    company_samples_limit: int,
) -> Dict[str, Any]:
    mode = "vendor_probe" if vendor_probe_only else "full"
    label = f"{mode}_batch_{batch_index:02d}"

    ensure_within_deadline(overall_deadline, f"starting {label}")

    latest = try_get_latest_run(api)
    resumed = False
    if latest and is_running_status(latest.get("status")):
        run_id = canary.parse_int(latest.get("discoveryRunId"))
        resumed = True
        start_payload: Dict[str, Any] = {
            "discoveryRunId": run_id,
            "status": latest.get("status"),
            "statusUrl": f"/api/careers/discover/run/{run_id}",
            "resumeMode": True,
            "resumeReason": "latest_run_already_running",
        }
    else:
        start_payload = api.post_json(
            "/api/careers/discover",
            {"limit": discover_batch_size, "vendorProbeOnly": str(vendor_probe_only).lower()},
        )
        run_id = canary.parse_int(start_payload.get("discoveryRunId"))

    canary.write_json(out_dir / f"{label}_start.json", start_payload)

    run_wait_limit = max_run_wait_seconds
    remaining = remaining_seconds(overall_deadline)
    if remaining is not None:
        run_wait_limit = min(run_wait_limit, remaining)
    if run_wait_limit <= 0:
        raise TimeoutError(f"No remaining time budget to wait for {label}")

    wait_started = time.monotonic()
    final_status = canary.poll_discovery_run(
        api,
        run_id,
        run_wait_limit,
        poll_interval_seconds,
        out_dir,
        label,
    )
    wait_duration_ms = int((time.monotonic() - wait_started) * 1000)
    canary.write_json(out_dir / f"{label}_final_status.json", final_status)

    companies_step = canary.timed_step_safe(
        f"{label}_companies",
        lambda: api.get_json(
            f"/api/careers/discover/run/{run_id}/companies",
            {"limit": company_samples_limit},
        ),
    )
    canary.write_json(out_dir / f"{label}_companies.json", companies_step.payload)

    failures_step = canary.timed_step_safe(
        f"{label}_failures",
        lambda: api.get_json(f"/api/careers/discover/run/{run_id}/failures"),
    )
    canary.write_json(out_dir / f"{label}_failures.json", failures_step.payload)

    stage_durations_step = canary.timed_step_safe(
        f"{label}_stage_durations",
        lambda: canary.query_run_stage_durations(db, run_id),
    )
    canary.write_json(out_dir / f"{label}_stage_durations.json", stage_durations_step.payload)

    failure_attempts_step = canary.timed_step_safe(
        f"{label}_failure_attempts",
        lambda: canary.query_run_failure_attempts(db, run_id, max(200, company_samples_limit * 10)),
    )
    canary.write_json(out_dir / f"{label}_failure_attempts.json", failure_attempts_step.payload)

    return {
        "mode": mode,
        "batch": batch_index,
        "run_id": run_id,
        "resumed": resumed,
        "start": start_payload,
        "final": final_status,
        "wait_duration_ms": wait_duration_ms,
        "companies": companies_step.payload,
        "failures": failures_step.payload,
        "stage_durations": stage_durations_step.payload,
        "failure_attempts": failure_attempts_step.payload,
    }


def _merge_count_maps(target: Dict[str, int], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        target[str(key)] = target.get(str(key), 0) + canary.parse_int(value)


def _merge_metric_values(target: Dict[str, Any], source: Any) -> None:
    if not isinstance(source, dict):
        return
    for key, value in source.items():
        if isinstance(value, dict):
            existing = target.get(key)
            if not isinstance(existing, dict):
                existing = {}
                target[key] = existing
            _merge_count_maps(existing, value)
            continue
        if isinstance(value, (int, float)) or str(value).lstrip("-").isdigit():
            target[key] = canary.parse_int(target.get(key)) + canary.parse_int(value)
        elif key not in target:
            target[key] = value


def summarize_domain_resolve_batch(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = payload.get("metrics") or {}
    return {
        "resolvedCount": canary.parse_int(payload.get("resolvedCount")),
        "attemptedCount": canary.parse_int(metrics.get("companiesAttemptedCount")),
        "companiesInputCount": canary.parse_int(metrics.get("companiesInputCount")),
        "selectionReturnedCount": canary.parse_int(metrics.get("selectionReturnedCount")),
        "selectionEligibleCount": canary.parse_int(metrics.get("selectionEligibleCount")),
        "cachedSkipCount": canary.parse_int(metrics.get("cachedSkipCount")),
        "skippedNotEmployerCount": canary.parse_int(metrics.get("skippedNotEmployerCount")),
        "wdqsTimeoutCount": canary.parse_int(payload.get("wdqsTimeoutCount")),
        "wdqsErrorCount": canary.parse_int(payload.get("wdqsErrorCount")),
    }


def run_domain_resolution_in_batches(
    *,
    api: canary.ApiClient,
    out_dir: Path,
    resolve_limit: int,
    resolve_batch_size: int,
    overall_deadline: Optional[float],
    durations_ms: Dict[str, int],
) -> Dict[str, Any]:
    if resolve_limit <= 0:
        return {
            "requestedLimit": 0,
            "resolveBatchSize": resolve_batch_size,
            "batchCount": 0,
            "resolvedCount": 0,
            "noWikipediaTitleCount": 0,
            "noItemCount": 0,
            "noP856Count": 0,
            "wdqsErrorCount": 0,
            "wdqsTimeoutCount": 0,
            "sampleErrors": [],
            "metrics": {
                "companiesInputCount": 0,
                "companiesAttemptedCount": 0,
                "selectionReturnedCount": 0,
                "selectionEligibleCount": 0,
                "skippedNotEmployerCount": 0,
            },
            "batches": [],
        }

    batch_size = max(1, resolve_batch_size)
    batch_count = (resolve_limit + batch_size - 1) // batch_size
    remaining = resolve_limit
    rollup_metrics: Dict[str, Any] = {}
    rollup_counts: Dict[str, int] = {field: 0 for field in DOMAIN_RESOLVE_TOP_LEVEL_INT_FIELDS}
    sample_errors: List[str] = []
    batch_summaries: List[Dict[str, Any]] = []
    started = time.monotonic()

    for batch in range(1, batch_count + 1):
        ensure_within_deadline(overall_deadline, f"starting domain resolve batch {batch}/{batch_count}")
        requested = min(batch_size, remaining)
        batch_label = f"domain_resolve_batch_{batch:02d}"
        batch_step = canary.timed_step(
            batch_label,
            lambda req=requested: api.post_json("/api/domains/resolve", {"limit": req}),
        )
        durations_ms[batch_step.name] = batch_step.duration_ms
        payload = batch_step.payload if isinstance(batch_step.payload, dict) else {}
        canary.write_json(out_dir / f"{batch_label}_response.json", payload)

        for field in DOMAIN_RESOLVE_TOP_LEVEL_INT_FIELDS:
            rollup_counts[field] += canary.parse_int(payload.get(field))
        _merge_metric_values(rollup_metrics, payload.get("metrics") or {})
        for err in (payload.get("sampleErrors") or []):
            err_txt = str(err)
            if err_txt and err_txt not in sample_errors:
                sample_errors.append(err_txt)
                if len(sample_errors) >= 50:
                    break

        summary = summarize_domain_resolve_batch(payload)
        summary_with_meta = {
            "batch": batch,
            "batchCount": batch_count,
            "requestedLimit": requested,
            "elapsedMs": batch_step.duration_ms,
            **summary,
        }
        batch_summaries.append(summary_with_meta)
        print(
            f"[domain_resolve_batch_{batch:02d}] batch={batch}/{batch_count} requested={requested} "
            f"resolved={summary['resolvedCount']} attempted={summary['attemptedCount']} "
            f"selection_returned={summary['selectionReturnedCount']} selection_eligible={summary['selectionEligibleCount']} "
            f"skipped_not_employer={summary['skippedNotEmployerCount']} elapsed_ms={batch_step.duration_ms}",
            file=sys.stderr,
        )
        remaining -= requested

    total_elapsed_ms = int((time.monotonic() - started) * 1000)
    durations_ms["domain_resolve"] = total_elapsed_ms

    return {
        "requestedLimit": resolve_limit,
        "resolveBatchSize": batch_size,
        "batchCount": batch_count,
        **rollup_counts,
        "sampleErrors": sample_errors,
        "metrics": rollup_metrics,
        "batches": batch_summaries,
    }


def summarize_batch_statuses(statuses: List[Dict[str, Any]]) -> Dict[str, Any]:
    processed = sum(canary.parse_int(s.get("processedCount")) for s in statuses)
    succeeded = sum(canary.parse_int(s.get("succeededCount")) for s in statuses)
    failed = sum(canary.parse_int(s.get("failedCount")) for s in statuses)
    careers_url_found = sum(canary.parse_int(s.get("careersUrlFoundCount")) for s in statuses)
    vendor_detected = sum(canary.parse_int(s.get("vendorDetectedCount")) for s in statuses)
    endpoint_extracted = sum(canary.parse_int(s.get("endpointExtractedCount")) for s in statuses)

    stage_failures: Dict[str, int] = {}
    failures_by_reason: Dict[str, int] = {}
    for s in statuses:
        for k, v in (s.get("careersStageFailuresByReason") or {}).items():
            stage_failures[k] = stage_failures.get(k, 0) + canary.parse_int(v)
        for k, v in (s.get("failuresByReason") or {}).items():
            failures_by_reason[k] = failures_by_reason.get(k, 0) + canary.parse_int(v)

    return {
        "processed": processed,
        "succeeded": succeeded,
        "failed": failed,
        "success_rate": canary.safe_rate(succeeded, processed),
        "careers_url_found_count": careers_url_found,
        "vendor_detected_count": vendor_detected,
        "endpoint_extracted_count": endpoint_extracted,
        "careers_url_found_rate": canary.safe_rate(careers_url_found, processed),
        "vendor_detected_rate_among_found": canary.safe_rate(vendor_detected, careers_url_found),
        "endpoint_extracted_rate_among_vendor_detected": canary.safe_rate(
            endpoint_extracted, vendor_detected
        ),
        "careers_stage_failures_by_reason": dict(
            sorted(stage_failures.items(), key=lambda kv: (-kv[1], kv[0]))
        ),
        "failures_by_reason": dict(
            sorted(failures_by_reason.items(), key=lambda kv: (-kv[1], kv[0]))
        ),
    }


def generate_canary_summary(
    *,
    args: argparse.Namespace,
    out_dir: Path,
    db: canary.DbClient,
    domain_counts_before: Dict[str, Any],
    domain_counts_after: Dict[str, Any],
    domain_resolve_payload: Dict[str, Any],
    fresh_domain_report: Optional[Dict[str, Any]],
    vendor_results: List[Dict[str, Any]],
    full_results: List[Dict[str, Any]],
    optional_payloads: Dict[str, Any],
    durations_ms: Dict[str, int],
) -> Dict[str, Any]:
    vendor_statuses = [r.get("final") or {} for r in vendor_results]
    full_statuses = [r.get("final") or {} for r in full_results]
    vendor_rollup = summarize_batch_statuses(vendor_statuses)

    latest_vendor_run_id = canary.parse_int((vendor_results[-1].get("run_id") if vendor_results else 0))
    domain_source_funnel = {}
    company_funnel_samples: List[Dict[str, Any]] = []
    failure_attempt_rows: List[Dict[str, Any]] = []
    top_failing_patterns: List[Dict[str, Any]] = []
    attempt_samples: Dict[str, Any] = {}

    if latest_vendor_run_id > 0:
        funnel_step = canary.timed_step_safe(
            "overnight_probe_domain_source_x_ats_funnel",
            lambda: canary.query_domain_source_funnel_crosstab(db, latest_vendor_run_id),
        )
        durations_ms[funnel_step.name] = funnel_step.duration_ms
        domain_source_funnel = funnel_step.payload
        canary.write_json(out_dir / "probe_domain_source_x_ats_funnel.json", funnel_step.payload)

        samples_step = canary.timed_step_safe(
            "overnight_probe_company_funnel_samples",
            lambda: canary.query_run_company_funnel_samples(db, latest_vendor_run_id, args.company_samples_limit),
        )
        durations_ms[samples_step.name] = samples_step.duration_ms
        company_funnel_samples = (
            samples_step.payload if isinstance(samples_step.payload, list) else []
        )
        canary.write_json(out_dir / "probe_company_funnel_samples.json", samples_step.payload)

        attempts_step = canary.timed_step_safe(
            "overnight_probe_failure_attempts",
            lambda: canary.query_run_failure_attempts(db, latest_vendor_run_id, max(200, args.company_samples_limit * 20)),
        )
        durations_ms[attempts_step.name] = attempts_step.duration_ms
        failure_attempt_rows = attempts_step.payload if isinstance(attempts_step.payload, list) else []
        canary.write_json(out_dir / "probe_failure_attempts.json", attempts_step.payload)

        attempt_samples = canary.build_attempt_evidence(
            company_funnel_samples,
            failure_attempt_rows,
            limit_per_group=20,
        )
        top_failing_patterns = canary.build_top_failing_url_patterns(failure_attempt_rows, limit=20)

    canary.write_json(out_dir / "probe_attempt_url_samples.json", attempt_samples)
    canary.write_json(out_dir / "probe_top_failing_url_patterns.json", top_failing_patterns)

    domain_metrics = domain_resolve_payload.get("metrics") or {}
    attempted = canary.parse_int(domain_metrics.get("companiesAttemptedCount"))
    resolved = canary.parse_int(domain_resolve_payload.get("resolvedCount"))

    domain_bottleneck = canary.infer_domain_bottleneck(
        domain_resolve_payload,
        vendor_statuses[-1] if vendor_statuses else {},
        args.discover_batch_size * args.num_batches,
    )

    canary_summary = {
        "schema_version": "ats-canary-summary-v2",
        "generated_at": utc_now_iso(),
        "config": {
            "base_url": args.base_url,
            "sec_limit": args.sec_limit,
            "resolve_limit": args.resolve_limit,
            "resolve_batch_size": args.resolve_batch_size,
            "discover_batch_size": args.discover_batch_size,
            "num_batches": args.num_batches,
            "sleep_between_batches": args.sleep_between_batches,
            "vendor_probe_success_threshold": args.vendor_probe_threshold_for_full,
            "run_full_mode": args.run_full_mode,
            "domain_fresh_cohort_first": args.domain_fresh_cohort_first,
        },
        "inputs_and_counts": {
            "companies_with_domain_count_before": canary.parse_int(
                domain_counts_before.get("companies_with_domain_count")
            ),
            "companies_with_domain_count_after": canary.parse_int(
                domain_counts_after.get("companies_with_domain_count")
            ),
            "companies_with_domain_by_source_before": domain_counts_before.get("by_source") or [],
            "companies_with_domain_by_source_after": domain_counts_after.get("by_source") or [],
        },
        "domain_resolution": {
            "requested_limit": args.resolve_limit,
            "attempted_count": attempted,
            "resolved_count": resolved,
            "domain_resolution_rate": canary.safe_rate(resolved, attempted),
            "result": domain_resolve_payload,
            "fresh_domain_cohort": fresh_domain_report,
        },
        "ats_vendor_probe": {
            "requested_discover_count": args.discover_batch_size * args.num_batches,
            "runs": [
                {
                    "batch": r.get("batch"),
                    "run_id": r.get("run_id"),
                    "resumed": r.get("resumed"),
                    "status": (r.get("final") or {}).get("status"),
                }
                for r in vendor_results
            ],
            "funnel": {
                "requested_discover_count": args.discover_batch_size * args.num_batches,
                "careers_url_found_count": vendor_rollup["careers_url_found_count"],
                "vendor_detected_count": vendor_rollup["vendor_detected_count"],
                "endpoint_extracted_count": vendor_rollup["endpoint_extracted_count"],
                "careers_url_found_rate": vendor_rollup["careers_url_found_rate"],
                "vendor_detected_rate_among_found": vendor_rollup["vendor_detected_rate_among_found"],
                "endpoint_extracted_rate_among_vendor_detected": vendor_rollup[
                    "endpoint_extracted_rate_among_vendor_detected"
                ],
            },
            "ats_discovery_attempted_count": vendor_rollup["processed"],
            "ats_discovery_success_count": vendor_rollup["succeeded"],
            "ats_discovery_success_rate": vendor_rollup["success_rate"],
            "stage_failure_buckets": vendor_rollup["careers_stage_failures_by_reason"],
            "failures_by_reason": vendor_rollup["failures_by_reason"],
            "latest_run_status": vendor_statuses[-1] if vendor_statuses else {},
            "domain_source_x_ats_funnel": domain_source_funnel,
        },
        "actionable_failure_evidence": {
            "attempt_url_samples_by_stage": attempt_samples,
            "top_failing_url_patterns": top_failing_patterns,
            "company_funnel_samples": company_funnel_samples[: min(args.company_samples_limit, 50)],
        },
        "breakdowns": {
            "coverage_ats_endpoints_by_type": (optional_payloads.get("coverage") or {}).get(
                "atsEndpointsByType", {}
            ),
            "coverage_ats_endpoints_by_method": (optional_payloads.get("coverage") or {}).get(
                "atsEndpointsByMethod", {}
            ),
            "top_failure_categories_run": canary.top_n_map(vendor_rollup["failures_by_reason"], 5),
            "top_stage_failures_run": canary.top_n_map(
                vendor_rollup["careers_stage_failures_by_reason"], 5
            ),
            "top_failure_categories_diagnostics": canary.top_n_map(
                (optional_payloads.get("discovery_failures") or {}).get("countsByReason"),
                5,
            ),
        },
        "thresholds": {
            "vendor_probe_success_rate_gte": args.vendor_probe_threshold_for_full,
            "vendor_probe_success_rate_actual": vendor_rollup["success_rate"],
            "vendor_probe_success_rate_pass": (
                vendor_rollup["success_rate"] is not None
                and vendor_rollup["success_rate"] >= args.vendor_probe_threshold_for_full
            ),
            "full_mode_executed": bool(full_results),
        },
        "domain_backlog_watch": {
            "probe_domain_source_funnel_summary": (
                domain_source_funnel.get("summary")
                if isinstance(domain_source_funnel, dict)
                else []
            )
        },
        "bottleneck_assessment": {
            **domain_bottleneck,
            "next_focus": (
                "ATS discovery improvements"
                if not domain_bottleneck.get("likely_domain_limited")
                else "Domain resolution / targeting still limiting ATS coverage"
            ),
        },
        "optional_full_discovery": {
            "executed": bool(full_results),
            "runs": [
                {
                    "batch": r.get("batch"),
                    "run_id": r.get("run_id"),
                    "resumed": r.get("resumed"),
                    "status": (r.get("final") or {}).get("status"),
                }
                for r in full_results
            ],
            "totals": summarize_batch_statuses(full_statuses),
        },
        "durations_ms": {
            "per_step": durations_ms,
            "total_wall_clock_ms": sum(durations_ms.values()),
        },
        "optional_errors": {
            "coverage": optional_payloads.get("coverage_error"),
            "discovery_failures": optional_payloads.get("discovery_failures_error"),
        },
        "raw_files": sorted(p.name for p in out_dir.iterdir() if p.is_file()),
    }
    canary.write_json(out_dir / "canary_summary.json", canary_summary)
    return canary_summary


def run(args: argparse.Namespace) -> int:
    out_dir = canary.ensure_out_dir(Path(args.out_dir))
    api = canary.ApiClient(args.base_url, args.http_timeout_seconds)
    db = canary.DbClient(args.postgres_container, args.postgres_user, args.postgres_db)
    print(f"Output directory: {out_dir}", file=sys.stderr)
    print(
        "Effective time budgets: "
        f"overall_timeout_seconds={args.overall_timeout_seconds}, "
        f"max_run_wait_seconds={args.max_run_wait_seconds}, "
        f"poll_interval_seconds={args.poll_interval_seconds}, "
        f"http_timeout_seconds={args.http_timeout_seconds}",
        file=sys.stderr,
    )

    durations_ms: Dict[str, int] = {}

    def dump_running_discovery_diagnostics(trigger: str) -> Dict[str, Any]:
        running_step = canary.timed_step_safe(
            "exit_running_discovery_count",
            lambda: db.query_single_int("SELECT COUNT(*) FROM careers_discovery_runs WHERE status='RUNNING';"),
        )
        latest_step = canary.timed_step_safe(
            "exit_latest_discovery_run",
            lambda: api.get_json("/api/careers/discover/runs/latest"),
        )

        running_count = (
            running_step.payload
            if isinstance(running_step.payload, int)
            else canary.parse_int((running_step.payload or {}).get("value"))
        )

        payload = {
            "trigger": trigger,
            "generated_at": utc_now_iso(),
            "running_count": running_count,
            "running_count_query": (
                {"value": running_step.payload}
                if not isinstance(running_step.payload, dict)
                else running_step.payload
            ),
            "latest_discovery_run": latest_step.payload,
        }
        canary.write_json(out_dir / "exit_discovery_diagnostics.json", payload)
        if running_count > 0:
            print(f"[exit-diagnostics] running_discovery_runs={running_count}", file=sys.stderr)
            print(
                f"[exit-diagnostics] latest_discovery_run={json.dumps(latest_step.payload, sort_keys=True)}",
                file=sys.stderr,
            )
        return payload

    preflight_api = canary.timed_step_safe("preflight_api_status", lambda: api.get_json("/api/status"))
    durations_ms[preflight_api.name] = preflight_api.duration_ms
    canary.write_json(out_dir / "preflight_api_status.json", preflight_api.payload)

    preflight_db = canary.timed_step_safe("preflight_db_connectivity", lambda: db.query_single_int("SELECT 1"))
    durations_ms[preflight_db.name] = preflight_db.duration_ms
    canary.write_json(
        out_dir / "preflight_db_connectivity.json",
        {
            "ok": isinstance(preflight_db.payload, int) and preflight_db.payload == 1,
            "result": preflight_db.payload,
        }
        if not isinstance(preflight_db.payload, dict)
        else preflight_db.payload,
    )

    preflight_errors: List[str] = []
    if isinstance(preflight_api.payload, dict) and preflight_api.payload.get("_error"):
        preflight_errors.append(str(preflight_api.payload.get("_error")))
    if isinstance(preflight_db.payload, dict) and preflight_db.payload.get("_error"):
        preflight_errors.append(str(preflight_db.payload.get("_error")))
    if not isinstance(preflight_db.payload, dict) and preflight_db.payload != 1:
        preflight_errors.append(f"DB connectivity expected SELECT 1 == 1, got {preflight_db.payload!r}")

    if args.dry_run:
        dry_summary = {
            "schema_version": "overnight-summary-v1",
            "mode": "dry-run",
            "generated_at": utc_now_iso(),
            "config": vars(args),
            "preflight": {
                "ok": not preflight_errors,
                "errors": preflight_errors,
                "api_status": preflight_api.payload,
                "db_connectivity": preflight_db.payload,
            },
            "durations_ms": {"per_step": durations_ms, "total_wall_clock_ms": sum(durations_ms.values())},
        }
        canary.write_json(out_dir / "runs_started.json", {"vendor_probe": [], "full": []})
        canary.write_json(out_dir / "final_run_statuses.json", {"vendor_probe": [], "full": []})
        canary.write_json(out_dir / "canary_summary.json", {"schema_version": "ats-canary-summary-v2", "mode": "dry-run"})
        canary.write_json(out_dir / "overnight_summary.json", dry_summary)
        return EXIT_OK if not preflight_errors else EXIT_PREREQ_FAILED

    if preflight_errors:
        canary.write_json(out_dir / "runs_started.json", {"vendor_probe": [], "full": []})
        canary.write_json(out_dir / "final_run_statuses.json", {"vendor_probe": [], "full": []})
        canary.write_json(out_dir / "canary_summary.json", {"schema_version": "ats-canary-summary-v2", "error": "preflight_failed", "errors": preflight_errors})
        canary.write_json(
            out_dir / "overnight_summary.json",
            {
                "schema_version": "overnight-summary-v1",
                "generated_at": utc_now_iso(),
                "config": vars(args),
                "preflight": {
                    "ok": False,
                    "errors": preflight_errors,
                    "api_status": preflight_api.payload,
                    "db_connectivity": preflight_db.payload,
                },
            },
        )
        print("Preflight failed. Check preflight_api_status.json and preflight_db_connectivity.json", file=sys.stderr)
        return EXIT_PREREQ_FAILED

    overall_started = time.monotonic()
    overall_deadline = None
    if args.overall_timeout_seconds > 0:
        overall_deadline = overall_started + args.overall_timeout_seconds

    vendor_results: List[Dict[str, Any]] = []
    full_results: List[Dict[str, Any]] = []

    try:
        ensure_within_deadline(overall_deadline, "before domain counts")
        domain_counts_before_step = canary.timed_step(
            "domain_counts_before", lambda: canary.query_domain_counts(db)
        )
        durations_ms[domain_counts_before_step.name] = domain_counts_before_step.duration_ms
        domain_counts_before = domain_counts_before_step.payload
        canary.write_json(out_dir / "domain_counts_before.json", domain_counts_before)

        ensure_within_deadline(overall_deadline, "SEC ingest")
        ingest_step = canary.timed_step(
            "sec_ingest",
            lambda: api.post_json("/api/universe/ingest/sec", {"limit": args.sec_limit}),
        )
        durations_ms[ingest_step.name] = ingest_step.duration_ms
        canary.write_json(out_dir / "sec_ingest_response.json", ingest_step.payload)

        fresh_before_rows: Optional[List[Dict[str, Any]]] = None
        if args.domain_fresh_cohort_first:
            ensure_within_deadline(overall_deadline, "sampling missing-domain cohort before resolve")
            fresh_before_step = canary.timed_step(
                "domain_fresh_cohort_before",
                lambda: canary.query_missing_domain_cohort(db, args.resolve_limit),
            )
            durations_ms[fresh_before_step.name] = fresh_before_step.duration_ms
            fresh_before_rows = fresh_before_step.payload
            canary.write_json(out_dir / "domain_fresh_cohort_before.json", fresh_before_rows)

        ensure_within_deadline(overall_deadline, "domain resolve")
        domain_resolve_payload = run_domain_resolution_in_batches(
            api=api,
            out_dir=out_dir,
            resolve_limit=args.resolve_limit,
            resolve_batch_size=args.resolve_batch_size,
            overall_deadline=overall_deadline,
            durations_ms=durations_ms,
        )
        canary.write_json(out_dir / "domain_resolve_response.json", domain_resolve_payload)

        ensure_within_deadline(overall_deadline, "domain counts after")
        domain_counts_after_step = canary.timed_step(
            "domain_counts_after", lambda: canary.query_domain_counts(db)
        )
        durations_ms[domain_counts_after_step.name] = domain_counts_after_step.duration_ms
        domain_counts_after = domain_counts_after_step.payload
        canary.write_json(out_dir / "domain_counts_after.json", domain_counts_after)

        fresh_domain_report = None
        if args.domain_fresh_cohort_first and fresh_before_rows is not None:
            cohort_ids = [canary.parse_int(r.get("company_id")) for r in fresh_before_rows]
            fresh_after_step = canary.timed_step(
                "domain_fresh_cohort_after",
                lambda: canary.query_domain_state_for_company_ids(db, cohort_ids),
            )
            durations_ms[fresh_after_step.name] = fresh_after_step.duration_ms
            fresh_after_rows = fresh_after_step.payload
            canary.write_json(out_dir / "domain_fresh_cohort_after.json", fresh_after_rows)
            fresh_domain_report = canary.build_fresh_domain_cohort_report(
                fresh_before_rows,
                fresh_after_rows,
                domain_resolve_payload if isinstance(domain_resolve_payload, dict) else {},
                durations_ms.get("domain_resolve", 0),
            )
            canary.write_json(out_dir / "domain_fresh_cohort_report.json", fresh_domain_report)

        for batch in range(1, args.num_batches + 1):
            ensure_within_deadline(overall_deadline, f"running vendor batch {batch}")
            batch_step = canary.timed_step(
                f"vendor_probe_batch_{batch:02d}",
                lambda b=batch: run_discovery_batch(
                    api=api,
                    db=db,
                    out_dir=out_dir,
                    batch_index=b,
                    discover_batch_size=args.discover_batch_size,
                    vendor_probe_only=True,
                    poll_interval_seconds=args.poll_interval_seconds,
                    max_run_wait_seconds=args.max_run_wait_seconds,
                    overall_deadline=overall_deadline,
                    company_samples_limit=args.company_samples_limit,
                ),
            )
            durations_ms[batch_step.name] = batch_step.duration_ms
            vendor_results.append(batch_step.payload)
            canary.write_json(out_dir / f"vendor_probe_batch_{batch:02d}_result.json", batch_step.payload)
            if batch < args.num_batches and args.sleep_between_batches > 0:
                ensure_within_deadline(overall_deadline, "sleeping between vendor batches")
                time.sleep(args.sleep_between_batches)

        vendor_statuses = [r.get("final") or {} for r in vendor_results]
        vendor_rollup = summarize_batch_statuses(vendor_statuses)
        vendor_success_rate = vendor_rollup.get("success_rate") or 0.0
        run_full = bool(args.run_full_mode and vendor_success_rate >= args.vendor_probe_threshold_for_full)

        if run_full:
            for batch in range(1, args.num_batches + 1):
                ensure_within_deadline(overall_deadline, f"running full batch {batch}")
                full_step = canary.timed_step(
                    f"full_batch_{batch:02d}",
                    lambda b=batch: run_discovery_batch(
                        api=api,
                        db=db,
                        out_dir=out_dir,
                        batch_index=b,
                        discover_batch_size=args.discover_batch_size,
                        vendor_probe_only=False,
                        poll_interval_seconds=args.poll_interval_seconds,
                        max_run_wait_seconds=args.max_run_wait_seconds,
                        overall_deadline=overall_deadline,
                        company_samples_limit=args.company_samples_limit,
                    ),
                )
                durations_ms[full_step.name] = full_step.duration_ms
                full_results.append(full_step.payload)
                canary.write_json(out_dir / f"full_batch_{batch:02d}_result.json", full_step.payload)
                if batch < args.num_batches and args.sleep_between_batches > 0:
                    ensure_within_deadline(overall_deadline, "sleeping between full batches")
                    time.sleep(args.sleep_between_batches)

        runs_started = {
            "vendor_probe": [
                {
                    "batch": r.get("batch"),
                    "run_id": r.get("run_id"),
                    "resumed": r.get("resumed"),
                    "mode": r.get("mode"),
                }
                for r in vendor_results
            ],
            "full": [
                {
                    "batch": r.get("batch"),
                    "run_id": r.get("run_id"),
                    "resumed": r.get("resumed"),
                    "mode": r.get("mode"),
                }
                for r in full_results
            ],
        }
        canary.write_json(out_dir / "runs_started.json", runs_started)

        final_run_statuses = {
            "vendor_probe": [r.get("final") for r in vendor_results],
            "full": [r.get("final") for r in full_results],
        }
        canary.write_json(out_dir / "final_run_statuses.json", final_run_statuses)

        coverage_step = canary.timed_step_safe(
            "coverage_diagnostics", lambda: api.get_json("/api/diagnostics/coverage")
        )
        durations_ms[coverage_step.name] = coverage_step.duration_ms
        canary.write_json(out_dir / "coverage_diagnostics.json", coverage_step.payload)

        discovery_failures_step = canary.timed_step_safe(
            "discovery_failures_diagnostics",
            lambda: api.get_json("/api/diagnostics/discovery-failures"),
        )
        durations_ms[discovery_failures_step.name] = discovery_failures_step.duration_ms
        canary.write_json(
            out_dir / "discovery_failures_diagnostics.json", discovery_failures_step.payload
        )

        optional_payloads = {
            "coverage": coverage_step.payload if isinstance(coverage_step.payload, dict) and "_error" not in coverage_step.payload else {},
            "coverage_error": coverage_step.payload if isinstance(coverage_step.payload, dict) and "_error" in coverage_step.payload else None,
            "discovery_failures": discovery_failures_step.payload if isinstance(discovery_failures_step.payload, dict) and "_error" not in discovery_failures_step.payload else {},
            "discovery_failures_error": discovery_failures_step.payload if isinstance(discovery_failures_step.payload, dict) and "_error" in discovery_failures_step.payload else None,
        }

        canary_summary = generate_canary_summary(
            args=args,
            out_dir=out_dir,
            db=db,
            domain_counts_before=domain_counts_before,
            domain_counts_after=domain_counts_after,
            domain_resolve_payload=domain_resolve_payload if isinstance(domain_resolve_payload, dict) else {},
            fresh_domain_report=fresh_domain_report,
            vendor_results=vendor_results,
            full_results=full_results,
            optional_payloads=optional_payloads,
            durations_ms=durations_ms,
        )

        overnight_summary = {
            "schema_version": "overnight-summary-v1",
            "generated_at": utc_now_iso(),
            "config": vars(args),
            "preflight": {
                "ok": True,
                "api_status": preflight_api.payload,
                "db_connectivity": {"ok": True, "result": preflight_db.payload},
            },
            "pipeline": {
                "sec_ingest": ingest_step.payload,
                "domain_resolve": domain_resolve_payload,
                "domain_counts_before": domain_counts_before,
                "domain_counts_after": domain_counts_after,
                "vendor_probe_batches_completed": len(vendor_results),
                "full_batches_completed": len(full_results),
                "full_mode_executed": bool(full_results),
                "full_mode_skip_reason": (
                    None
                    if full_results
                    else (
                        "disabled_by_flag"
                        if not args.run_full_mode
                        else f"vendor_probe_success_rate_below_threshold ({summarize_batch_statuses([r.get('final') or {} for r in vendor_results]).get('success_rate')} < {args.vendor_probe_threshold_for_full})"
                    )
                ),
            },
            "batch_rollup": {
                "vendor_probe": summarize_batch_statuses([r.get("final") or {} for r in vendor_results]),
                "full": summarize_batch_statuses([r.get("final") or {} for r in full_results]),
            },
            "durations_ms": {
                "per_step": durations_ms,
                "total_wall_clock_ms": int((time.monotonic() - overall_started) * 1000),
            },
            "artifacts": {
                "runs_started": "runs_started.json",
                "final_run_statuses": "final_run_statuses.json",
                "canary_summary": "canary_summary.json",
                "overnight_summary": "overnight_summary.json",
            },
            "canary_summary_highlights": {
                "ats_vendor_probe_success_rate": canary_summary.get("ats_vendor_probe", {}).get(
                    "ats_discovery_success_rate"
                ),
                "funnel": canary_summary.get("ats_vendor_probe", {}).get("funnel", {}),
                "top_stage_failures": canary.top_n_map(
                    (canary_summary.get("ats_vendor_probe", {}) or {}).get(
                        "stage_failure_buckets"
                    ),
                    5,
                ),
            },
        }
        canary.write_json(out_dir / "overnight_summary.json", overnight_summary)
        print(f"Overnight run completed. Artifacts: {out_dir}", file=sys.stderr)
        return EXIT_OK

    except TimeoutError as e:
        exit_diag = dump_running_discovery_diagnostics("timeout")
        canary.write_json(out_dir / "runs_started.json", {
            "vendor_probe": [
                {"batch": r.get("batch"), "run_id": r.get("run_id"), "resumed": r.get("resumed")}
                for r in vendor_results
            ],
            "full": [
                {"batch": r.get("batch"), "run_id": r.get("run_id"), "resumed": r.get("resumed")}
                for r in full_results
            ],
        })
        canary.write_json(out_dir / "final_run_statuses.json", {
            "vendor_probe": [r.get("final") for r in vendor_results],
            "full": [r.get("final") for r in full_results],
        })
        canary.write_json(out_dir / "canary_summary.json", {
            "schema_version": "ats-canary-summary-v2",
            "generated_at": utc_now_iso(),
            "error": "timeout",
            "message": str(e),
        })
        canary.write_json(
            out_dir / "overnight_summary.json",
            {
                "schema_version": "overnight-summary-v1",
                "generated_at": utc_now_iso(),
                "config": vars(args),
                "error": "timeout",
                "message": str(e),
                "partial_runs": {
                    "vendor_probe": len(vendor_results),
                    "full": len(full_results),
                },
                "exit_discovery_diagnostics": exit_diag,
            },
        )
        print(str(e), file=sys.stderr)
        return EXIT_TIMEOUT
    except Exception as e:  # noqa: BLE001
        exit_diag = dump_running_discovery_diagnostics("runtime_failed")
        canary.write_json(out_dir / "runs_started.json", {
            "vendor_probe": [
                {"batch": r.get("batch"), "run_id": r.get("run_id"), "resumed": r.get("resumed")}
                for r in vendor_results
            ],
            "full": [
                {"batch": r.get("batch"), "run_id": r.get("run_id"), "resumed": r.get("resumed")}
                for r in full_results
            ],
        })
        canary.write_json(out_dir / "final_run_statuses.json", {
            "vendor_probe": [r.get("final") for r in vendor_results],
            "full": [r.get("final") for r in full_results],
        })
        canary.write_json(out_dir / "canary_summary.json", {
            "schema_version": "ats-canary-summary-v2",
            "generated_at": utc_now_iso(),
            "error": "runtime_failed",
            "message": str(e),
        })
        canary.write_json(
            out_dir / "overnight_summary.json",
            {
                "schema_version": "overnight-summary-v1",
                "generated_at": utc_now_iso(),
                "config": vars(args),
                "error": "runtime_failed",
                "message": str(e),
                "partial_runs": {
                    "vendor_probe": len(vendor_results),
                    "full": len(full_results),
                },
                "exit_discovery_diagnostics": exit_diag,
            },
        )
        print(f"Overnight run failed: {e}", file=sys.stderr)
        return EXIT_RUNTIME_FAILED


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Overnight SEC/domain/ATS runner with resume mode and hard timeouts"
    )
    p.add_argument("--base-url", default=os.getenv("DELTA_BASE_URL", "http://localhost:8080"))
    p.add_argument("--sec-limit", type=int, default=int(os.getenv("DELTA_SEC_LIMIT", "10000")))
    p.add_argument("--resolve-limit", type=int, default=int(os.getenv("DELTA_RESOLVE_LIMIT", "2000")))
    p.add_argument("--resolve-batch-size", type=int, default=int(os.getenv("DELTA_RESOLVE_BATCH_SIZE", "300")))
    p.add_argument("--discover-batch-size", type=int, default=int(os.getenv("DELTA_DISCOVER_BATCH_SIZE", "500")))
    p.add_argument("--num-batches", type=int, default=int(os.getenv("DELTA_NUM_BATCHES", "10")))
    p.add_argument("--sleep-between-batches", type=int, default=int(os.getenv("DELTA_SLEEP_BETWEEN_BATCHES", "30")))
    p.add_argument("--vendor-probe-threshold-for-full", type=float, default=float(os.getenv("DELTA_VENDOR_PROBE_THRESHOLD_FOR_FULL", "0.45")))
    p.add_argument("--run-full-mode", action="store_true", default=os.getenv("DELTA_RUN_FULL_MODE", "true").lower() == "true")
    p.add_argument("--domain-fresh-cohort-first", action="store_true", default=os.getenv("DELTA_DOMAIN_FRESH_COHORT_FIRST", "false").lower() == "true")
    p.add_argument("--company-samples-limit", type=int, default=int(os.getenv("DELTA_COMPANY_SAMPLES_LIMIT", "100")))
    p.add_argument("--max-run-wait-seconds", type=int, default=int(os.getenv("DELTA_MAX_RUN_WAIT_SECONDS", "7200")))
    p.add_argument("--overall-timeout-seconds", type=int, default=int(os.getenv("DELTA_OVERALL_TIMEOUT_SECONDS", "43200")))
    p.add_argument("--poll-interval-seconds", type=int, default=int(os.getenv("DELTA_POLL_INTERVAL_SECONDS", "10")))
    p.add_argument("--http-timeout-seconds", type=int, default=int(os.getenv("DELTA_HTTP_TIMEOUT_SECONDS", "600")))
    p.add_argument("--out-dir", default=os.getenv("DELTA_OUT_DIR", "out"))
    p.add_argument("--dry-run", action="store_true", default=False, help="Only check /api/status and DB connectivity")
    p.add_argument("--postgres-container", default=os.getenv("DELTA_POSTGRES_CONTAINER", "delta-job-tracker-postgres"))
    p.add_argument("--postgres-user", default=os.getenv("DELTA_POSTGRES_USER", "delta"))
    p.add_argument("--postgres-db", default=os.getenv("DELTA_POSTGRES_DB", "delta_job_tracker"))
    args = p.parse_args(argv)

    for field in (
        "sec_limit",
        "resolve_limit",
        "resolve_batch_size",
        "discover_batch_size",
        "num_batches",
        "max_run_wait_seconds",
        "poll_interval_seconds",
        "http_timeout_seconds",
    ):
        if getattr(args, field) <= 0:
            p.error(f"{field} must be > 0")
    if args.overall_timeout_seconds < 0:
        p.error("overall_timeout_seconds must be >= 0")
    if not (0.0 <= args.vendor_probe_threshold_for_full <= 1.0):
        p.error("vendor_probe_threshold_for_full must be between 0 and 1")
    return args


if __name__ == "__main__":
    try:
        raise SystemExit(run(parse_args()))
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        raise SystemExit(EXIT_RUNTIME_FAILED)
