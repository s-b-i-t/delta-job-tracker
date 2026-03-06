#!/usr/bin/env python3
"""Run a bounded end-to-end pipeline proof: frontier seed -> fetch -> ATS discovery.

Outputs:
  out/<timestamp>/e2e_pipeline_run/
    run_config.json
    db_pre_counts.csv
    db_post_seed_counts.csv
    db_post_fetch_counts.csv
    db_post_ats_counts.csv
    summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import db_snapshot as dbsnap

SCHEMA_VERSION = "e2e-pipeline-proof-v2"
CURL_CONNECT_TIMEOUT_SECONDS = 10

DEFAULT_CONFIG: dict[str, Any] = {
    "seed_domain_limit": 1200,
    "seed_max_sitemap_fetches": 1,
    "seed_timeout_seconds": 900,
    "fetch_cycles": 20,
    "fetch_domain_limit": 1,
    "fetch_batch_size": 120,
    "fetch_timeout_seconds": 180,
    "fetch_sleep_seconds": 2,
    "fetch_min_attempts_before_ats": 200,
    "sec_limit": 0,
    "sec_ingest_timeout_seconds": 180,
    "resolve_batch_size": 75,
    "resolve_cycles": 8,
    "resolve_timeout_seconds": 600,
    "discover_limit": 1000,
    "discover_batch_size": 25,
    "discover_max_wait_seconds": 1800,
    "discover_poll_interval_seconds": 5,
}

PRESETS: dict[str, dict[str, Any]] = {
    "S": {
        "seed_domain_limit": 50,
        "seed_max_sitemap_fetches": 1,
        "seed_timeout_seconds": 60,
        "fetch_cycles": 6,
        "fetch_domain_limit": 1,
        "fetch_batch_size": 20,
        "fetch_timeout_seconds": 60,
        "fetch_sleep_seconds": 1,
        "fetch_min_attempts_before_ats": 20,
        "sec_limit": 0,
        "sec_ingest_timeout_seconds": 90,
        "resolve_batch_size": 1,
        "resolve_cycles": 2,
        "resolve_timeout_seconds": 60,
        "discover_limit": 50,
        "discover_batch_size": 10,
        "discover_max_wait_seconds": 120,
        "discover_poll_interval_seconds": 5,
    },
    "M": {
        "seed_domain_limit": 500,
        "seed_max_sitemap_fetches": 1,
        "seed_timeout_seconds": 120,
        "fetch_cycles": 12,
        "fetch_domain_limit": 1,
        "fetch_batch_size": 60,
        "fetch_timeout_seconds": 90,
        "fetch_sleep_seconds": 2,
        "fetch_min_attempts_before_ats": 120,
        "sec_limit": 0,
        "sec_ingest_timeout_seconds": 120,
        "resolve_batch_size": 1,
        "resolve_cycles": 5,
        "resolve_timeout_seconds": 90,
        "discover_limit": 200,
        "discover_batch_size": 20,
        "discover_max_wait_seconds": 300,
        "discover_poll_interval_seconds": 5,
    },
    "L": {
        "seed_domain_limit": 2000,
        "seed_max_sitemap_fetches": 1,
        "seed_timeout_seconds": 180,
        "fetch_cycles": 24,
        "fetch_domain_limit": 1,
        "fetch_batch_size": 120,
        "fetch_timeout_seconds": 120,
        "fetch_sleep_seconds": 2,
        "fetch_min_attempts_before_ats": 300,
        "sec_limit": 0,
        "sec_ingest_timeout_seconds": 180,
        "resolve_batch_size": 1,
        "resolve_cycles": 8,
        "resolve_timeout_seconds": 120,
        "discover_limit": 800,
        "discover_batch_size": 25,
        "discover_max_wait_seconds": 600,
        "discover_poll_interval_seconds": 5,
    },
}

CONFIG_KEYS = list(DEFAULT_CONFIG.keys())


@dataclass
class StageError(Exception):
    stage: str
    error_type: str
    message: str
    rc: int | None = None
    command: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "error_type": self.error_type,
            "message": self.message,
            "rc": self.rc,
            "command": self.command,
        }


@dataclass
class StageResult:
    ok: bool
    error: dict[str, Any] | None
    payload: dict[str, Any]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(ts: datetime | None = None) -> str:
    value = ts or utc_now()
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ts_dir_name(ts: datetime | None = None) -> str:
    value = ts or utc_now()
    return value.strftime("%Y%m%dT%H%M%SZ")


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def detect_explicit_cli_options(argv: list[str] | None) -> set[str]:
    explicit: set[str] = set()
    if not argv:
        return explicit
    for token in argv:
        if not token.startswith("--"):
            continue
        if token == "--":
            break
        key = token.split("=", 1)[0]
        explicit.add(key)
    return explicit


def resolve_effective_config(args: argparse.Namespace, explicit_options: set[str]) -> tuple[dict[str, Any], list[str]]:
    config = dict(DEFAULT_CONFIG)
    applied: list[str] = ["default"]

    preset = args.preset
    if preset:
        config.update(PRESETS[preset])
        applied.append(f"preset:{preset}")

    option_to_key = {
        "--seed-domain-limit": "seed_domain_limit",
        "--seed-max-sitemap-fetches": "seed_max_sitemap_fetches",
        "--seed-timeout-seconds": "seed_timeout_seconds",
        "--fetch-cycles": "fetch_cycles",
        "--fetch-domain-limit": "fetch_domain_limit",
        "--fetch-batch-size": "fetch_batch_size",
        "--fetch-timeout-seconds": "fetch_timeout_seconds",
        "--fetch-sleep-seconds": "fetch_sleep_seconds",
        "--fetch-min-attempts-before-ats": "fetch_min_attempts_before_ats",
        "--sec-limit": "sec_limit",
        "--sec-ingest-timeout-seconds": "sec_ingest_timeout_seconds",
        "--resolve-batch-size": "resolve_batch_size",
        "--resolve-cycles": "resolve_cycles",
        "--resolve-timeout-seconds": "resolve_timeout_seconds",
        "--discover-limit": "discover_limit",
        "--discover-batch-size": "discover_batch_size",
        "--discover-max-wait-seconds": "discover_max_wait_seconds",
        "--discover-poll-interval-seconds": "discover_poll_interval_seconds",
    }

    for option, key in option_to_key.items():
        value = getattr(args, key)
        if option in explicit_options and value is not None:
            config[key] = value
            applied.append(f"explicit:{option}")

    return config, applied


def check_runtime_dependencies(args: argparse.Namespace) -> dict[str, Any]:
    checks: dict[str, Any] = {}

    curl_path = shutil.which("curl")
    checks["curl"] = {"ok": bool(curl_path), "path": curl_path}
    if not curl_path:
        raise StageError(
            stage="startup_dependency_check",
            error_type="MissingDependency",
            message="curl is required for deterministic HTTP timeouts but was not found in PATH",
            rc=127,
            command=["curl", "--version"],
        )

    docker_path = shutil.which("docker")
    checks["docker"] = {"ok": bool(docker_path), "path": docker_path}
    if not docker_path:
        raise StageError(
            stage="startup_dependency_check",
            error_type="MissingDependency",
            message="docker is required but was not found in PATH",
            rc=127,
            command=["docker", "--version"],
        )

    ps_cmd = [
        "docker",
        "ps",
        "--filter",
        f"name=^/{args.postgres_container}$",
        "--filter",
        "status=running",
        "--format",
        "{{.Names}}",
    ]
    ps_cp = run_cmd(ps_cmd, timeout_seconds=15, stage="startup_dependency_check")
    if ps_cp.returncode != 0:
        raise StageError(
            stage="startup_dependency_check",
            error_type="MissingDependency",
            message=f"failed to query running docker containers: {(ps_cp.stderr or ps_cp.stdout).strip()}",
            rc=ps_cp.returncode,
            command=ps_cmd,
        )
    running_names = [line.strip() for line in (ps_cp.stdout or "").splitlines() if line.strip()]
    container_ok = args.postgres_container in running_names
    checks["postgres_container_running"] = {
        "ok": container_ok,
        "container": args.postgres_container,
        "running_matches": running_names,
    }
    if not container_ok:
        raise StageError(
            stage="startup_dependency_check",
            error_type="MissingDependency",
            message=f"postgres container '{args.postgres_container}' is not running",
            rc=1,
            command=ps_cmd,
        )

    psql_cmd = ["docker", "exec", args.postgres_container, "psql", "--version"]
    psql_cp = run_cmd(psql_cmd, timeout_seconds=15, stage="startup_dependency_check")
    psql_out = (psql_cp.stdout or psql_cp.stderr or "").strip()
    psql_ok = psql_cp.returncode == 0 and bool(psql_out)
    checks["psql_in_container"] = {
        "ok": psql_ok,
        "container": args.postgres_container,
        "version": psql_out if psql_cp.returncode == 0 else None,
    }
    if not psql_ok:
        raise StageError(
            stage="startup_dependency_check",
            error_type="MissingDependency",
            message=f"psql is not available in container '{args.postgres_container}': {psql_out}",
            rc=psql_cp.returncode,
            command=psql_cmd,
        )

    return checks


def compute_report_paths(run_root: Path, report_date: str, write_docs_report: bool) -> dict[str, str | None]:
    run_root_report = run_root / "release_report.md"
    docs_report = (
        Path("docs") / f"release_e2e_pipeline_proof_{report_date}.md"
        if write_docs_report
        else None
    )
    return {
        "run_root_report": str(run_root_report),
        "docs_report": str(docs_report) if docs_report else None,
    }


def snapshot_error_payload(mode: str, stage: str, exc: Exception) -> dict[str, Any]:
    return {
        "schema_version": "db-snapshot-v1",
        "captured_at": utc_iso(),
        "ok": False,
        "mode_requested": mode,
        "mode_used": None,
        "error": {
            "stage": stage,
            "message": f"snapshot capture failed: {exc}",
            "exception_type": type(exc).__name__,
            "attempts": [],
        },
    }


def capture_db_snapshot_safe(repo_root: Path, mode: str, stage: str) -> dict[str, Any]:
    try:
        snapshot = dbsnap.capture_db_snapshot(repo_root, mode=mode)
        if not isinstance(snapshot, dict):
            raise RuntimeError("capture_db_snapshot returned non-dict payload")
        return snapshot
    except Exception as exc:  # noqa: BLE001
        return snapshot_error_payload(mode, stage, exc)


def build_db_truth_key_metrics(db_truth_raw: dict[str, Any]) -> dict[str, Any]:
    start = (db_truth_raw.get("start") or {}).get("metrics") or {}
    end = (db_truth_raw.get("end") or {}).get("metrics") or {}
    delta = db_truth_raw.get("delta") or {}
    return {
        "start": {
            "ats_endpoints_count": int(start.get("ats_endpoints_count") or 0),
            "companies_with_ats_endpoint_count": int(start.get("companies_with_ats_endpoint_count") or 0),
            "companies_with_domain_count": int(start.get("companies_with_domain_count") or 0),
            "endpoints_by_type": start.get("endpoints_by_type") or [],
        },
        "end": {
            "ats_endpoints_count": int(end.get("ats_endpoints_count") or 0),
            "companies_with_ats_endpoint_count": int(end.get("companies_with_ats_endpoint_count") or 0),
            "companies_with_domain_count": int(end.get("companies_with_domain_count") or 0),
            "endpoints_by_type": end.get("endpoints_by_type") or [],
        },
        "delta": {
            "ats_endpoints_count": int(delta.get("ats_endpoints_count") or 0)
            if db_truth_raw.get("delta_ok")
            else None,
            "companies_with_ats_endpoint_count": int(delta.get("companies_with_ats_endpoint_count") or 0)
            if db_truth_raw.get("delta_ok")
            else None,
            "companies_with_domain_count": int(delta.get("companies_with_domain_count") or 0)
            if db_truth_raw.get("delta_ok")
            else None,
            "endpoints_by_type": delta.get("endpoints_by_type")
            if db_truth_raw.get("delta_ok")
            else None,
        },
    }


def build_db_truth_section(
    enabled: bool,
    start_snapshot: dict[str, Any] | None,
    end_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "start": None,
            "end": None,
            "delta_ok": False,
            "delta": None,
            "errors": [{"message": "db snapshots disabled by flag"}],
            "key_metrics": {},
        }
    raw = dbsnap.build_db_truth(start_snapshot, end_snapshot)
    return {
        "enabled": True,
        "start": raw.get("start"),
        "end": raw.get("end"),
        "delta_ok": bool(raw.get("delta_ok")),
        "delta": raw.get("delta"),
        "errors": raw.get("errors") or [],
        "key_metrics": build_db_truth_key_metrics(raw),
    }


def run_cmd(cmd: list[str], timeout_seconds: int, stage: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(1, timeout_seconds),
        )
    except subprocess.TimeoutExpired as exc:
        raise StageError(
            stage=stage,
            error_type="TimeoutExpired",
            message=f"command timed out after {exc.timeout}s",
            rc=124,
            command=list(cmd),
        ) from exc


def _curl_json(
    method: str,
    base_url: str,
    path: str,
    query: dict[str, Any],
    timeout_seconds: int,
    stage: str,
) -> dict[str, Any]:
    clean_query = {k: v for k, v in query.items() if v is not None}
    encoded = urllib.parse.urlencode(clean_query)
    url = f"{base_url.rstrip('/')}{path}"
    if encoded:
        url = f"{url}?{encoded}"

    cmd = [
        "curl",
        "-sS",
        "--fail",
        "--connect-timeout",
        str(CURL_CONNECT_TIMEOUT_SECONDS),
        "--max-time",
        str(max(1, timeout_seconds)),
        "-X",
        method.upper(),
        url,
    ]
    cp = run_cmd(cmd, timeout_seconds=max(5, timeout_seconds + 5), stage=stage)
    if cp.returncode != 0:
        stderr = (cp.stderr or "").strip() or (cp.stdout or "").strip()
        raise StageError(
            stage=stage,
            error_type="HttpCommandError",
            message=f"curl {method.upper()} failed: {stderr}",
            rc=cp.returncode,
            command=cmd,
        )

    body = (cp.stdout or "").strip()
    if not body:
        raise StageError(
            stage=stage,
            error_type="EmptyHttpBody",
            message="HTTP response body was empty",
            rc=cp.returncode,
            command=cmd,
        )
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise StageError(
            stage=stage,
            error_type="InvalidJson",
            message=f"failed to parse JSON response: {exc}",
            rc=cp.returncode,
            command=cmd,
        ) from exc


def post_json(base_url: str, path: str, query: dict[str, Any], timeout_seconds: int, stage: str) -> dict[str, Any]:
    return _curl_json("POST", base_url, path, query, timeout_seconds, stage)


def get_json(base_url: str, path: str, query: dict[str, Any], timeout_seconds: int, stage: str) -> dict[str, Any]:
    return _curl_json("GET", base_url, path, query, timeout_seconds, stage)


def psql_csv(
    postgres_container: str,
    postgres_user: str,
    postgres_db: str,
    sql: str,
    stage: str,
    timeout_seconds: int = 30,
) -> str:
    cmd = [
        "docker",
        "exec",
        "-i",
        postgres_container,
        "psql",
        "-U",
        postgres_user,
        "-d",
        postgres_db,
        "--csv",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    cp = run_cmd(cmd, timeout_seconds=timeout_seconds, stage=stage)
    if cp.returncode != 0:
        raise StageError(
            stage=stage,
            error_type="SqlCommandError",
            message=(cp.stderr or cp.stdout or "psql failed").strip(),
            rc=cp.returncode,
            command=cmd,
        )
    output = cp.stdout
    if not output or "metric" not in output:
        raise StageError(
            stage=stage,
            error_type="SqlOutputError",
            message="snapshot query returned empty/invalid csv payload",
            rc=cp.returncode,
            command=cmd,
        )
    return output


def psql_scalar_int(
    postgres_container: str,
    postgres_user: str,
    postgres_db: str,
    sql: str,
    stage: str,
    timeout_seconds: int = 30,
) -> int:
    cmd = [
        "docker",
        "exec",
        "-i",
        postgres_container,
        "psql",
        "-U",
        postgres_user,
        "-d",
        postgres_db,
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    cp = run_cmd(cmd, timeout_seconds=timeout_seconds, stage=stage)
    if cp.returncode != 0:
        raise StageError(
            stage=stage,
            error_type="SqlCommandError",
            message=(cp.stderr or cp.stdout or "psql scalar failed").strip(),
            rc=cp.returncode,
            command=cmd,
        )
    text = (cp.stdout or "").strip()
    if not text:
        return 0
    try:
        return int(text)
    except ValueError:
        raise StageError(
            stage=stage,
            error_type="SqlParseError",
            message=f"expected integer scalar but got: {text!r}",
            rc=cp.returncode,
            command=cmd,
        )


def truncate_frontier_tables(postgres_container: str, postgres_user: str, postgres_db: str) -> None:
    cmd = [
        "docker",
        "exec",
        "-i",
        postgres_container,
        "psql",
        "-U",
        postgres_user,
        "-d",
        postgres_db,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        "TRUNCATE TABLE crawl_url_attempts, crawl_urls, crawl_hosts;",
    ]
    cp = run_cmd(cmd, timeout_seconds=60, stage="reset_frontier")
    if cp.returncode != 0:
        raise StageError(
            stage="reset_frontier",
            error_type="SqlCommandError",
            message=(cp.stderr or cp.stdout or "truncate failed").strip(),
            rc=cp.returncode,
            command=cmd,
        )


def snapshot_counts_csv(
    out_path: Path,
    postgres_container: str,
    postgres_user: str,
    postgres_db: str,
    pre_attempt_id: int,
    pre_ats_id: int,
    stage: str,
) -> None:
    sql = f"""
WITH metrics AS (
  SELECT 'crawl_hosts_count'::text AS metric, ''::text AS dim1, ''::text AS dim2, COUNT(*)::bigint AS value FROM crawl_hosts
  UNION ALL
  SELECT 'crawl_urls_count', '', '', COUNT(*)::bigint FROM crawl_urls
  UNION ALL
  SELECT 'crawl_url_attempts_count', '', '', COUNT(*)::bigint FROM crawl_url_attempts
  UNION ALL
  SELECT 'attempts_created_run_scope', '', '', COUNT(*)::bigint FROM crawl_url_attempts WHERE id > {int(pre_attempt_id)}
  UNION ALL
  SELECT 'queued_total', '', '', COUNT(*)::bigint FROM crawl_urls WHERE status = 'QUEUED'
  UNION ALL
  SELECT 'fetching_rows', '', '', COUNT(*)::bigint FROM crawl_urls WHERE status = 'FETCHING'
  UNION ALL
  SELECT 'locked_rows', '', '', COUNT(*)::bigint FROM crawl_urls WHERE locked_until IS NOT NULL AND locked_until > now()
  UNION ALL
  SELECT 'inflight_sum', '', '', COALESCE(SUM(inflight_count), 0)::bigint FROM crawl_hosts
  UNION ALL
  SELECT 'ats_endpoints_count', '', '', COUNT(*)::bigint FROM ats_endpoints
  UNION ALL
  SELECT 'ats_companies_count', '', '', COUNT(DISTINCT company_id)::bigint FROM ats_endpoints
  UNION ALL
  SELECT 'ats_endpoints_created_run_scope', '', '', COUNT(*)::bigint FROM ats_endpoints WHERE id > {int(pre_ats_id)}
  UNION ALL
  SELECT 'ats_companies_created_run_scope', '', '', COUNT(DISTINCT company_id)::bigint FROM ats_endpoints WHERE id > {int(pre_ats_id)}
),
url_status AS (
  SELECT 'crawl_urls_status'::text AS metric,
         COALESCE(url_kind, '<null>')::text AS dim1,
         COALESCE(status, '<null>')::text AS dim2,
         COUNT(*)::bigint AS value
  FROM crawl_urls
  GROUP BY 2, 3
),
error_buckets AS (
  SELECT 'attempt_error_bucket_run_scope'::text AS metric,
         COALESCE(error_bucket, '<null>')::text AS dim1,
         ''::text AS dim2,
         COUNT(*)::bigint AS value
  FROM crawl_url_attempts
  WHERE id > {int(pre_attempt_id)}
  GROUP BY 2
  ORDER BY value DESC, dim1 ASC
  LIMIT 20
),
ats_type_total AS (
  SELECT 'ats_type_total'::text AS metric,
         COALESCE(ats_type, '<null>')::text AS dim1,
         ''::text AS dim2,
         COUNT(*)::bigint AS value
  FROM ats_endpoints
  GROUP BY 2
),
ats_type_run AS (
  SELECT 'ats_type_created_run_scope'::text AS metric,
         COALESCE(ats_type, '<null>')::text AS dim1,
         ''::text AS dim2,
         COUNT(*)::bigint AS value
  FROM ats_endpoints
  WHERE id > {int(pre_ats_id)}
  GROUP BY 2
)
SELECT metric, dim1, dim2, value FROM metrics
UNION ALL SELECT metric, dim1, dim2, value FROM url_status
UNION ALL SELECT metric, dim1, dim2, value FROM error_buckets
UNION ALL SELECT metric, dim1, dim2, value FROM ats_type_total
UNION ALL SELECT metric, dim1, dim2, value FROM ats_type_run
ORDER BY metric, dim1, dim2;
"""
    csv_text = psql_csv(postgres_container, postgres_user, postgres_db, sql, stage=stage)
    out_path.write_text(csv_text, encoding="utf-8")


def load_counts_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(
                {
                    "metric": (row.get("metric") or "").strip(),
                    "dim1": (row.get("dim1") or "").strip(),
                    "dim2": (row.get("dim2") or "").strip(),
                    "value": int(row.get("value") or 0),
                }
            )
    return rows


def metric_value(rows: list[dict[str, Any]], metric: str) -> int:
    for row in rows:
        if row["metric"] == metric and row["dim1"] == "" and row["dim2"] == "":
            return int(row["value"])
    return 0


def metric_map(rows: list[dict[str, Any]], metric: str) -> dict[str, int]:
    output: dict[str, int] = {}
    for row in rows:
        if row["metric"] != metric:
            continue
        key = row["dim1"] if not row["dim2"] else f"{row['dim1']}::{row['dim2']}"
        output[key] = int(row["value"])
    return output


def rate_per_min(count: int, duration_seconds: float) -> float | None:
    if duration_seconds <= 0:
        return None
    return round((count * 60.0) / duration_seconds, 4)


def ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def normalize_stop_reason(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return "NONE"
    if "request_budget" in text or "budget_exceeded" in text:
        return "REQUEST_BUDGET_EXCEEDED"
    if "hard_timeout" in text or ("timeout" in text and "wdqs" not in text):
        return "TIMEOUT"
    if "host_failure_cutoff" in text:
        return "HOST_FAILURE_CUTOFF"
    if "cancel" in text:
        return "CANCELLED"
    if "abort" in text:
        return "ABORTED"
    if "fail" in text or "error" in text:
        return "FAILED"
    if "complete" in text or "succeed" in text:
        return "COMPLETED"
    return "OTHER"


def build_canonical_efficiency_report(
    *,
    run_root: Path,
    preset: str | None,
    fetch_elapsed_seconds: float,
    fetch_attempts: int,
    fetch_successes: int,
    queued_before_fetch: int,
    queued_after_fetch: int,
    fetch_error_buckets: dict[str, int],
    ats_stage: dict[str, Any],
    ats_endpoints_created: int,
    domain_before: int | None,
    domain_after: int | None,
) -> dict[str, Any]:
    resolve_batches = ats_stage.get("resolve_batches") if isinstance(ats_stage.get("resolve_batches"), list) else []
    domain_attempted = sum(parse_int((batch or {}).get("attempted")) for batch in resolve_batches)
    domain_resolved = sum(parse_int((batch or {}).get("resolvedCount")) for batch in resolve_batches)
    domain_duration_seconds = round(
        sum(float((batch or {}).get("duration_seconds") or 0.0) for batch in resolve_batches), 3
    )

    discovery_final = ats_stage.get("discovery_final") if isinstance(ats_stage.get("discovery_final"), dict) else {}
    discovery_elapsed_seconds = float(ats_stage.get("discovery_wait_seconds") or 0.0)
    attempted_companies = parse_int(discovery_final.get("companiesAttemptedCount"))
    succeeded_companies = parse_int(discovery_final.get("succeededCount"))
    failed_companies = parse_int(discovery_final.get("failedCount"))
    endpoints_extracted = parse_int(discovery_final.get("endpointExtractedCount"))
    stop_reason_raw = discovery_final.get("stopReason") or discovery_final.get("lastError")
    normalized_stop_reason = normalize_stop_reason(stop_reason_raw)

    fetch_error_total = sum(parse_int(v) for v in fetch_error_buckets.values())
    fetch_queue_delta = queued_after_fetch - queued_before_fetch

    return {
        "schema_version": "canonical-efficiency-report-v1",
        "generated_at": utc_iso(),
        "context": {
            "source": "run_e2e_pipeline_proof.py",
            "preset": preset,
            "run_root": str(run_root),
        },
        "phases": {
            "domain_resolution": {
                "scope_tag": "run_scope",
                "duration_seconds": domain_duration_seconds,
                "counters": {
                    "companies_attempted": domain_attempted,
                    "domains_resolved": domain_resolved,
                },
                "rates_per_min": {
                    "domains_resolved_per_min": rate_per_min(domain_resolved, domain_duration_seconds),
                },
                "error_rates": {
                    "unresolved_per_attempt": ratio(max(0, domain_attempted - domain_resolved), domain_attempted),
                },
            },
            "queue_fetch": {
                "scope_tag": "run_scope",
                "duration_seconds": round(fetch_elapsed_seconds, 3),
                "counters": {
                    "urls_fetch_attempted": fetch_attempts,
                    "urls_fetch_succeeded": fetch_successes,
                    "urls_queued_before": queued_before_fetch,
                    "urls_queued_after": queued_after_fetch,
                    "urls_queued_net_delta": fetch_queue_delta,
                },
                "rates_per_min": {
                    "urls_fetch_attempted_per_min": rate_per_min(fetch_attempts, fetch_elapsed_seconds),
                    "urls_fetch_succeeded_per_min": rate_per_min(fetch_successes, fetch_elapsed_seconds),
                    "urls_queued_net_delta_per_min": rate_per_min(fetch_queue_delta, fetch_elapsed_seconds),
                },
                "error_breakdown": {
                    "bucket_counts": fetch_error_buckets,
                    "total_errors": fetch_error_total,
                    "error_rate_per_attempt": ratio(fetch_error_total, fetch_attempts),
                },
            },
            "ats_discovery": {
                "scope_tag": "run_scope",
                "duration_seconds": round(discovery_elapsed_seconds, 3),
                "counters": {
                    "companies_attempted": attempted_companies,
                    "companies_succeeded": succeeded_companies,
                    "companies_failed": failed_companies,
                    "endpoints_extracted": endpoints_extracted,
                    "endpoints_created_run_scope": ats_endpoints_created,
                },
                "rates_per_min": {
                    "endpoints_extracted_per_min": rate_per_min(endpoints_extracted, discovery_elapsed_seconds),
                    "endpoints_created_per_min": rate_per_min(ats_endpoints_created, discovery_elapsed_seconds),
                },
                "error_rates": {
                    "failed_per_attempted": ratio(failed_companies, attempted_companies),
                },
                "stop_reason": {
                    "raw": str(stop_reason_raw) if stop_reason_raw is not None else None,
                    "normalized": normalized_stop_reason,
                },
            },
        },
        "global_snapshots": {
            "scope_tag": "global_snapshot",
            "companies_with_domain_count_before": domain_before,
            "companies_with_domain_count_after": domain_after,
            "companies_with_domain_count_delta": (
                None if domain_before is None or domain_after is None else (domain_after - domain_before)
            ),
        },
    }


def poll_discovery_run(
    base_url: str,
    run_id: int,
    poll_interval_seconds: int,
    max_wait_seconds: int,
) -> dict[str, Any]:
    start = time.monotonic()
    last: dict[str, Any] = {}
    while True:
        last = get_json(
            base_url,
            f"/api/careers/discover/run/{run_id}",
            {},
            timeout_seconds=60,
            stage="ats_discovery_poll",
        )
        status = str(last.get("status") or "").upper()
        if status and status not in {"RUNNING", "PENDING"}:
            return last
        if time.monotonic() - start >= max_wait_seconds:
            return {
                "status": "TIMEOUT",
                "runId": run_id,
                "elapsedSeconds": round(time.monotonic() - start, 1),
                "lastStatus": last,
            }
        time.sleep(max(1, poll_interval_seconds))


def build_failure_summary(
    run_root: Path,
    started_at: datetime,
    run_config: dict[str, Any],
    error: StageError | Exception,
    dependency_checks: dict[str, Any] | None = None,
    warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if isinstance(error, StageError):
        err_obj = error.to_dict()
    else:
        err_obj = {
            "stage": "unknown",
            "error_type": type(error).__name__,
            "message": str(error),
            "rc": None,
            "command": None,
        }

    summary = {
        "schema_version": SCHEMA_VERSION,
        "run_root": str(run_root),
        "started_at": utc_iso(started_at),
        "ended_at": utc_iso(),
        "elapsed_seconds": round((utc_now() - started_at).total_seconds(), 2),
        "fatal_error": err_obj,
        "go_no_go": {
            "frontier_pass": False,
            "fetch_pass": False,
            "ats_pass": False,
            "decision": "NO-GO",
            "reason": "fatal_error",
        },
        "dependency_checks": dependency_checks or {},
        "warnings": warnings or [],
        "run_config": run_config,
    }
    return summary


def run(args: argparse.Namespace, explicit_options: set[str]) -> int:
    started_at = utc_now()
    resolved_config, applied_rules = resolve_effective_config(args, explicit_options)
    warnings: list[dict[str, Any]] = []
    dependency_checks: dict[str, Any] = {}
    repo_root = Path(__file__).resolve().parents[1]
    db_snapshot_start: dict[str, Any] | None = None
    db_snapshot_end: dict[str, Any] | None = None

    run_root = Path(args.out_dir) / ts_dir_name(started_at) / "e2e_pipeline_run"
    run_root.mkdir(parents=True, exist_ok=True)

    run_config = {
        "schema_version": SCHEMA_VERSION,
        "started_at": utc_iso(started_at),
        "base_url": args.base_url,
        "preset_requested": args.preset,
        "resolution_order": applied_rules,
        "reset_frontier": bool(args.reset_frontier),
        "write_docs_report": bool(args.write_docs_report),
        "db_snapshots": bool(args.db_snapshots),
        "db_snapshot_mode": args.db_snapshot_mode,
        "dry_run": bool(args.dry_run),
        "http_transport": {
            "mode": "curl",
            "connect_timeout_seconds": CURL_CONNECT_TIMEOUT_SECONDS,
            "max_time_source": "per-stage timeout flags",
        },
        "dependency_checks": {"status": "pending"},
        "resolved": resolved_config,
        "postgres": {
            "container": args.postgres_container,
            "user": args.postgres_user,
            "db": args.postgres_db,
        },
    }
    write_json(run_root / "run_config.json", run_config)

    try:
        dependency_checks = check_runtime_dependencies(args)
        run_config["dependency_checks"] = dependency_checks
        write_json(run_root / "run_config.json", run_config)

        status_payload = get_json(
            args.base_url,
            "/api/status",
            {},
            timeout_seconds=20,
            stage="startup_api_status",
        )
        write_json(run_root / "api_status.json", status_payload)

        if args.db_snapshots:
            db_snapshot_start = capture_db_snapshot_safe(
                repo_root,
                mode=args.db_snapshot_mode,
                stage="db_snapshot_start",
            )
            write_json(run_root / "db_snapshot_start.json", db_snapshot_start)

        if args.dry_run:
            if args.db_snapshots:
                db_snapshot_end = capture_db_snapshot_safe(
                    repo_root,
                    mode=args.db_snapshot_mode,
                    stage="db_snapshot_end",
                )
                write_json(run_root / "db_snapshot_end.json", db_snapshot_end)
            db_truth = build_db_truth_section(
                enabled=bool(args.db_snapshots),
                start_snapshot=db_snapshot_start,
                end_snapshot=db_snapshot_end,
            )
            summary = {
                "schema_version": SCHEMA_VERSION,
                "run_root": str(run_root),
                "started_at": utc_iso(started_at),
                "ended_at": utc_iso(),
                "elapsed_seconds": round((utc_now() - started_at).total_seconds(), 2),
                "dry_run": True,
                "dependency_checks": {
                    **dependency_checks,
                    "api_status_ok": bool(status_payload),
                },
                "db_truth": db_truth,
                "go_no_go": {
                    "frontier_pass": False,
                    "fetch_pass": False,
                    "ats_pass": False,
                    "decision": "NO-GO",
                    "reason": "dry_run_only",
                },
            }
            key_metrics = db_truth.get("key_metrics") if isinstance(db_truth.get("key_metrics"), dict) else {}
            domain_before_global = None
            domain_after_global = None
            if isinstance(key_metrics.get("start"), dict):
                domain_before_global = int((key_metrics.get("start") or {}).get("companies_with_domain_count") or 0)
            if isinstance(key_metrics.get("end"), dict):
                domain_after_global = int((key_metrics.get("end") or {}).get("companies_with_domain_count") or 0)
            canonical_efficiency = build_canonical_efficiency_report(
                run_root=run_root,
                preset=args.preset,
                fetch_elapsed_seconds=0.0,
                fetch_attempts=0,
                fetch_successes=0,
                queued_before_fetch=0,
                queued_after_fetch=0,
                fetch_error_buckets={},
                ats_stage={},
                ats_endpoints_created=0,
                domain_before=domain_before_global,
                domain_after=domain_after_global,
            )
            summary["canonical_efficiency_report"] = canonical_efficiency
            write_json(run_root / "summary.json", summary)
            write_json(run_root / "canonical_efficiency_report.json", canonical_efficiency)
            print(json.dumps({"run_root": str(run_root), "dry_run": True}, indent=2))
            return 0

        if args.reset_frontier:
            truncate_frontier_tables(args.postgres_container, args.postgres_user, args.postgres_db)

        pre_attempt_id = psql_scalar_int(
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            "SELECT COALESCE(MAX(id), 0) FROM crawl_url_attempts;",
            stage="snapshot_pre_attempt_id",
        )
        pre_ats_id = psql_scalar_int(
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            "SELECT COALESCE(MAX(id), 0) FROM ats_endpoints;",
            stage="snapshot_pre_ats_id",
        )
        snapshot_counts_csv(
            run_root / "db_pre_counts.csv",
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            pre_attempt_id,
            pre_ats_id,
            stage="snapshot_pre",
        )

        frontier_seed_start = time.monotonic()
        frontier_seed_result: StageResult
        try:
            payload = post_json(
                args.base_url,
                "/api/frontier/seed",
                {
                    "domainLimit": resolved_config["seed_domain_limit"],
                    "maxSitemapFetches": resolved_config["seed_max_sitemap_fetches"],
                },
                timeout_seconds=resolved_config["seed_timeout_seconds"],
                stage="frontier_seed",
            )
            frontier_seed_result = StageResult(ok=True, error=None, payload=payload)
        except StageError as exc:
            frontier_seed_result = StageResult(ok=False, error=exc.to_dict(), payload={})

        frontier_seed_elapsed = round(time.monotonic() - frontier_seed_start, 2)
        write_json(
            run_root / "frontier_seed_stage.json",
            {
                "ok": frontier_seed_result.ok,
                "error": frontier_seed_result.error,
                "elapsed_seconds": frontier_seed_elapsed,
                "payload": frontier_seed_result.payload,
            },
        )

        snapshot_counts_csv(
            run_root / "db_post_seed_counts.csv",
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            pre_attempt_id,
            pre_ats_id,
            stage="snapshot_post_seed",
        )

        fetch_progress: list[dict[str, Any]] = []
        fetch_started_at = utc_now()
        fetch_start_monotonic = time.monotonic()
        for cycle in range(1, int(resolved_config["fetch_cycles"]) + 1):
            cycle_started = utc_now()
            response: dict[str, Any] = {}
            error_obj: dict[str, Any] | None = None
            t0 = time.monotonic()
            try:
                response = post_json(
                    args.base_url,
                    "/api/frontier/seed",
                    {
                        "domainLimit": resolved_config["fetch_domain_limit"],
                        "maxSitemapFetches": resolved_config["fetch_batch_size"],
                    },
                    timeout_seconds=resolved_config["fetch_timeout_seconds"],
                    stage=f"fetch_cycle_{cycle}",
                )
            except StageError as exc:
                error_obj = exc.to_dict()

            attempts_run_scope = psql_scalar_int(
                args.postgres_container,
                args.postgres_user,
                args.postgres_db,
                f"SELECT COUNT(*) FROM crawl_url_attempts WHERE id > {pre_attempt_id};",
                stage=f"fetch_cycle_{cycle}_attempt_count",
                timeout_seconds=30,
            )
            queued_total = psql_scalar_int(
                args.postgres_container,
                args.postgres_user,
                args.postgres_db,
                "SELECT COUNT(*) FROM crawl_urls WHERE status = 'QUEUED';",
                stage=f"fetch_cycle_{cycle}_queued_count",
                timeout_seconds=30,
            )

            fetch_progress.append(
                {
                    "cycle": cycle,
                    "started_at": utc_iso(cycle_started),
                    "elapsed_seconds": round(time.monotonic() - t0, 2),
                    "ok": error_obj is None,
                    "error": error_obj,
                    "attempts_created_run_scope": attempts_run_scope,
                    "queued_remaining": queued_total,
                    "seed_response": response,
                }
            )

            if attempts_run_scope >= int(resolved_config["fetch_min_attempts_before_ats"]):
                break
            if int(resolved_config["fetch_sleep_seconds"]) > 0:
                time.sleep(int(resolved_config["fetch_sleep_seconds"]))

        fetch_elapsed_seconds = round(time.monotonic() - fetch_start_monotonic, 2)
        write_json(
            run_root / "fetch_stage.json",
            {
                "started_at": utc_iso(fetch_started_at),
                "ended_at": utc_iso(),
                "elapsed_seconds": fetch_elapsed_seconds,
                "cycles_executed": len(fetch_progress),
                "progress": fetch_progress,
            },
        )

        snapshot_counts_csv(
            run_root / "db_post_fetch_counts.csv",
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            pre_attempt_id,
            pre_ats_id,
            stage="snapshot_post_fetch",
        )
        post_fetch_rows = load_counts_csv(run_root / "db_post_fetch_counts.csv")
        attempts_after_fetch = metric_value(post_fetch_rows, "attempts_created_run_scope")

        ats_stage: dict[str, Any] = {
            "ok": False,
            "skipped": False,
            "error": None,
            "sec_ingest": None,
            "resolve_batches": [],
            "discovery_start": None,
            "discovery_final": None,
            "discovery_failures": None,
        }

        if attempts_after_fetch <= 0:
            ats_stage["skipped"] = True
            ats_stage["error"] = {
                "stage": "ats_stage",
                "error_type": "PrerequisiteFailed",
                "message": "No crawl fetch attempts were created; ATS stage not started.",
                "rc": None,
                "command": None,
            }
        else:
            try:
                if int(resolved_config["sec_limit"]) > 0:
                    ats_stage["sec_ingest"] = post_json(
                        args.base_url,
                        "/api/universe/ingest/sec",
                        {"limit": int(resolved_config["sec_limit"])},
                        timeout_seconds=int(resolved_config["sec_ingest_timeout_seconds"]),
                        stage="ats_sec_ingest",
                    )
                else:
                    ats_stage["sec_ingest"] = {"skipped": True, "reason": "sec_limit <= 0"}

                for idx in range(1, int(resolved_config["resolve_cycles"]) + 1):
                    resolve_started = time.monotonic()
                    batch_resp = post_json(
                        args.base_url,
                        "/api/domains/resolve",
                        {"limit": int(resolved_config["resolve_batch_size"])},
                        timeout_seconds=int(resolved_config["resolve_timeout_seconds"]),
                        stage=f"ats_resolve_batch_{idx}",
                    )
                    metrics = batch_resp.get("metrics") if isinstance(batch_resp, dict) else {}
                    attempted = 0
                    if isinstance(metrics, dict):
                        attempted = int(metrics.get("companiesAttemptedCount") or 0)
                    ats_stage["resolve_batches"].append(
                        {
                            "batch": idx,
                            "attempted": attempted,
                            "resolvedCount": int(batch_resp.get("resolvedCount") or 0)
                            if isinstance(batch_resp, dict)
                            else 0,
                            "metrics": metrics if isinstance(metrics, dict) else {},
                            "duration_seconds": round(time.monotonic() - resolve_started, 3),
                        }
                    )
                    if attempted <= 0:
                        break

                discover_start = post_json(
                    args.base_url,
                    "/api/careers/discover",
                    {
                        "limit": int(resolved_config["discover_limit"]),
                        "batchSize": int(resolved_config["discover_batch_size"]),
                        "vendorProbeOnly": "false",
                    },
                    timeout_seconds=120,
                    stage="ats_discovery_start",
                )
                ats_stage["discovery_start"] = discover_start

                run_id = int(discover_start.get("discoveryRunId") or 0)
                if run_id <= 0:
                    raise StageError(
                        stage="ats_discovery_start",
                        error_type="InvalidResponse",
                        message=f"Missing discoveryRunId in response: {discover_start}",
                    )

                discover_wait_started = time.monotonic()
                ats_stage["discovery_final"] = poll_discovery_run(
                    args.base_url,
                    run_id,
                    poll_interval_seconds=int(resolved_config["discover_poll_interval_seconds"]),
                    max_wait_seconds=int(resolved_config["discover_max_wait_seconds"]),
                )
                ats_stage["discovery_wait_seconds"] = round(time.monotonic() - discover_wait_started, 3)

                try:
                    ats_stage["discovery_failures"] = get_json(
                        args.base_url,
                        f"/api/careers/discover/run/{run_id}/failures",
                        {},
                        timeout_seconds=90,
                        stage="ats_discovery_failures",
                    )
                except StageError as fail_exc:
                    ats_stage["discovery_failures"] = {"error": fail_exc.to_dict()}

                ats_stage["ok"] = True
            except StageError as exc:
                ats_stage["error"] = exc.to_dict()

        write_json(run_root / "ats_stage.json", ats_stage)

        snapshot_counts_csv(
            run_root / "db_post_ats_counts.csv",
            args.postgres_container,
            args.postgres_user,
            args.postgres_db,
            pre_attempt_id,
            pre_ats_id,
            stage="snapshot_post_ats",
        )

        pre_rows = load_counts_csv(run_root / "db_pre_counts.csv")
        post_seed_rows = load_counts_csv(run_root / "db_post_seed_counts.csv")
        post_fetch_rows = load_counts_csv(run_root / "db_post_fetch_counts.csv")
        post_ats_rows = load_counts_csv(run_root / "db_post_ats_counts.csv")

        queued_before_fetch = metric_value(post_seed_rows, "queued_total")
        queued_after_fetch = metric_value(post_fetch_rows, "queued_total")
        fetch_attempts = metric_value(post_fetch_rows, "attempts_created_run_scope")
        fetch_successes = sum(
            parse_int((item.get("seed_response") or {}).get("urlsFetched")) for item in fetch_progress
        )

        attempts_per_min = 0.0
        if fetch_elapsed_seconds > 0:
            attempts_per_min = fetch_attempts / (fetch_elapsed_seconds / 60.0)

        ats_endpoints_created = metric_value(post_ats_rows, "ats_endpoints_created_run_scope")
        ats_companies_created = metric_value(post_ats_rows, "ats_companies_created_run_scope")

        frontier_hosts_count = metric_value(post_seed_rows, "crawl_hosts_count")
        frontier_urls_count = metric_value(post_seed_rows, "crawl_urls_count")

        frontier_pass = frontier_hosts_count >= 1000 and frontier_urls_count >= 2000
        fetch_pass = fetch_attempts > 0 and attempts_per_min > 10.0
        ats_pass = ats_endpoints_created > 0 and ats_companies_created > 0

        summary = {
            "schema_version": SCHEMA_VERSION,
            "run_root": str(run_root),
            "started_at": utc_iso(started_at),
            "ended_at": utc_iso(),
            "elapsed_seconds": round((utc_now() - started_at).total_seconds(), 2),
            "dependency_checks": dependency_checks,
            "frontier_stage": {
                "seed_ok": frontier_seed_result.ok,
                "seed_error": frontier_seed_result.error,
                "seed_elapsed_seconds": frontier_seed_elapsed,
                "crawl_hosts_count_post_seed": frontier_hosts_count,
                "crawl_urls_count_post_seed": frontier_urls_count,
                "acceptance": {
                    "hosts_gte_1000": frontier_hosts_count >= 1000,
                    "urls_gte_2000": frontier_urls_count >= 2000,
                },
            },
            "fetch_stage": {
                "cycles_executed": len(fetch_progress),
                "queued_before_fetch": queued_before_fetch,
                "queued_after_fetch": queued_after_fetch,
                "queue_delta": queued_after_fetch - queued_before_fetch,
                "fetch_attempts_created": fetch_attempts,
                "attempts_per_minute": round(attempts_per_min, 2),
                "fetch_rate_per_min": round(attempts_per_min, 2),
                "error_bucket_distribution": metric_map(post_fetch_rows, "attempt_error_bucket_run_scope"),
                "queued_remaining_over_time": [
                    {
                        "cycle": item["cycle"],
                        "queued_remaining": item["queued_remaining"],
                        "attempts_created_run_scope": item["attempts_created_run_scope"],
                    }
                    for item in fetch_progress
                ],
                "acceptance": {
                    "attempts_gt_0": fetch_attempts > 0,
                    "attempts_per_min_gt_10": attempts_per_min > 10.0,
                },
            },
            "ats_stage": {
                "ok": ats_stage["ok"],
                "skipped": ats_stage["skipped"],
                "error": ats_stage["error"],
                "ats_endpoints_created": ats_endpoints_created,
                "ats_companies_created": ats_companies_created,
                "ats_types_created_distribution": metric_map(post_ats_rows, "ats_type_created_run_scope"),
                "acceptance": {
                    "ats_endpoints_created_gt_0": ats_endpoints_created > 0,
                    "ats_companies_created_gt_0": ats_companies_created > 0,
                },
            },
            "post_counts": {
                "crawl_hosts_count": metric_value(post_ats_rows, "crawl_hosts_count"),
                "crawl_urls_count": metric_value(post_ats_rows, "crawl_urls_count"),
                "crawl_url_attempts_count": metric_value(post_ats_rows, "crawl_url_attempts_count"),
                "ats_endpoints_count": metric_value(post_ats_rows, "ats_endpoints_count"),
                "ats_companies_count": metric_value(post_ats_rows, "ats_companies_count"),
                "fetching_rows": metric_value(post_ats_rows, "fetching_rows"),
                "locked_rows": metric_value(post_ats_rows, "locked_rows"),
                "inflight_sum": metric_value(post_ats_rows, "inflight_sum"),
            },
            "pre_counts": {
                "crawl_hosts_count": metric_value(pre_rows, "crawl_hosts_count"),
                "crawl_urls_count": metric_value(pre_rows, "crawl_urls_count"),
                "crawl_url_attempts_count": metric_value(pre_rows, "crawl_url_attempts_count"),
                "ats_endpoints_count": metric_value(pre_rows, "ats_endpoints_count"),
                "ats_companies_count": metric_value(pre_rows, "ats_companies_count"),
            },
            "run_config": run_config,
            "warnings": warnings,
            "go_no_go": {
                "frontier_pass": frontier_pass,
                "fetch_pass": fetch_pass,
                "ats_pass": ats_pass,
                "decision": "GO" if (frontier_pass and fetch_pass and ats_pass) else "NO-GO",
            },
        }
        if args.db_snapshots:
            db_snapshot_end = capture_db_snapshot_safe(
                repo_root,
                mode=args.db_snapshot_mode,
                stage="db_snapshot_end",
            )
            write_json(run_root / "db_snapshot_end.json", db_snapshot_end)
        db_truth = build_db_truth_section(
            enabled=bool(args.db_snapshots),
            start_snapshot=db_snapshot_start,
            end_snapshot=db_snapshot_end,
        )
        summary["db_truth"] = db_truth

        domain_before_global: int | None = None
        domain_after_global: int | None = None
        key_metrics = db_truth.get("key_metrics") if isinstance(db_truth.get("key_metrics"), dict) else {}
        if isinstance(key_metrics.get("start"), dict):
            domain_before_global = int((key_metrics.get("start") or {}).get("companies_with_domain_count") or 0)
        if isinstance(key_metrics.get("end"), dict):
            domain_after_global = int((key_metrics.get("end") or {}).get("companies_with_domain_count") or 0)

        canonical_efficiency = build_canonical_efficiency_report(
            run_root=run_root,
            preset=args.preset,
            fetch_elapsed_seconds=fetch_elapsed_seconds,
            fetch_attempts=fetch_attempts,
            fetch_successes=fetch_successes,
            queued_before_fetch=queued_before_fetch,
            queued_after_fetch=queued_after_fetch,
            fetch_error_buckets=metric_map(post_fetch_rows, "attempt_error_bucket_run_scope"),
            ats_stage=ats_stage,
            ats_endpoints_created=ats_endpoints_created,
            domain_before=domain_before_global,
            domain_after=domain_after_global,
        )
        summary["canonical_efficiency_report"] = canonical_efficiency
        write_json(run_root / "summary.json", summary)
        write_json(run_root / "canonical_efficiency_report.json", canonical_efficiency)

        report_date = utc_now().strftime("%Y-%m-%d")
        report_paths = compute_report_paths(run_root, report_date, bool(args.write_docs_report))
        run_root_report_path = Path(report_paths["run_root_report"])
        lines = [
            f"# Release E2E Pipeline Proof - {report_date}",
            "",
            f"Run root: `{run_root}`",
            "",
            "## Stage Metrics",
            "",
            f"- Frontier hosts post-seed: `{frontier_hosts_count}`",
            f"- Frontier urls post-seed: `{frontier_urls_count}`",
            f"- Fetch attempts created (run scope): `{fetch_attempts}`",
            f"- Attempts/minute: `{round(attempts_per_min, 2)}`",
            f"- Queue before fetch: `{queued_before_fetch}`",
            f"- Queue after fetch: `{queued_after_fetch}`",
            f"- ATS endpoints created (run scope): `{ats_endpoints_created}`",
            f"- ATS companies created (run scope): `{ats_companies_created}`",
            "",
            "## DB Truth",
            "",
            f"- Snapshots enabled: `{db_truth.get('enabled')}`",
            f"- Delta ok: `{db_truth.get('delta_ok')}`",
            f"- Start ATS endpoints: `{((db_truth.get('key_metrics') or {}).get('start') or {}).get('ats_endpoints_count')}`",
            f"- End ATS endpoints: `{((db_truth.get('key_metrics') or {}).get('end') or {}).get('ats_endpoints_count')}`",
            f"- ATS endpoints delta: `{((db_truth.get('key_metrics') or {}).get('delta') or {}).get('ats_endpoints_count')}`",
            f"- Start ATS companies: `{((db_truth.get('key_metrics') or {}).get('start') or {}).get('companies_with_ats_endpoint_count')}`",
            f"- End ATS companies: `{((db_truth.get('key_metrics') or {}).get('end') or {}).get('companies_with_ats_endpoint_count')}`",
            f"- ATS companies delta: `{((db_truth.get('key_metrics') or {}).get('delta') or {}).get('companies_with_ats_endpoint_count')}`",
            f"- Start companies_with_domain_count: `{((db_truth.get('key_metrics') or {}).get('start') or {}).get('companies_with_domain_count')}`",
            f"- End companies_with_domain_count: `{((db_truth.get('key_metrics') or {}).get('end') or {}).get('companies_with_domain_count')}`",
            f"- companies_with_domain_count delta: `{((db_truth.get('key_metrics') or {}).get('delta') or {}).get('companies_with_domain_count')}`",
            "",
            "## Failure Modes",
            "",
            f"- Frontier seed ok: `{frontier_seed_result.ok}` error=`{frontier_seed_result.error}`",
            f"- ATS stage ok: `{ats_stage['ok']}` skipped=`{ats_stage['skipped']}` error=`{ats_stage['error']}`",
            f"- Fetch error buckets (run scope): `{metric_map(post_fetch_rows, 'attempt_error_bucket_run_scope')}`",
            "",
            "## Decision",
            "",
            f"- Frontier PASS: `{frontier_pass}`",
            f"- Fetch PASS: `{fetch_pass}`",
            f"- ATS PASS: `{ats_pass}`",
            f"- Final: **{summary['go_no_go']['decision']}**",
            "",
            "## Artifacts",
            "",
            "- `run_config.json`",
            "- `db_pre_counts.csv`",
            "- `db_post_seed_counts.csv`",
            "- `db_post_fetch_counts.csv`",
            "- `db_post_ats_counts.csv`",
            "- `summary.json`",
        ]
        run_root_report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        if args.write_docs_report and report_paths["docs_report"]:
            docs_report_path = Path(report_paths["docs_report"])
            try:
                docs_report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            except Exception as docs_exc:  # noqa: BLE001
                warnings.append(
                    {
                        "stage": "report_docs_write",
                        "error_type": type(docs_exc).__name__,
                        "message": str(docs_exc),
                        "path": str(docs_report_path),
                    }
                )
                summary["warnings"] = warnings
                write_json(run_root / "summary.json", summary)

        summary["report_paths"] = report_paths
        summary["warnings"] = warnings
        write_json(run_root / "summary.json", summary)

        print(json.dumps({"run_root": str(run_root), "report_paths": report_paths}, indent=2))
        return 0

    except Exception as exc:  # noqa: BLE001
        if args.db_snapshots and db_snapshot_start is not None and db_snapshot_end is None:
            db_snapshot_end = capture_db_snapshot_safe(
                repo_root,
                mode=args.db_snapshot_mode,
                stage="db_snapshot_end",
            )
            write_json(run_root / "db_snapshot_end.json", db_snapshot_end)
        db_truth = build_db_truth_section(
            enabled=bool(args.db_snapshots),
            start_snapshot=db_snapshot_start,
            end_snapshot=db_snapshot_end,
        )
        failure_summary = build_failure_summary(
            run_root,
            started_at,
            run_config,
            exc,
            dependency_checks=dependency_checks,
            warnings=warnings,
        )
        failure_summary["db_truth"] = db_truth
        canonical_efficiency = build_canonical_efficiency_report(
            run_root=run_root,
            preset=args.preset,
            fetch_elapsed_seconds=0.0,
            fetch_attempts=0,
            fetch_successes=0,
            queued_before_fetch=0,
            queued_after_fetch=0,
            fetch_error_buckets={},
            ats_stage={},
            ats_endpoints_created=0,
            domain_before=None,
            domain_after=None,
        )
        failure_summary["canonical_efficiency_report"] = canonical_efficiency
        write_json(run_root / "summary.json", failure_summary)
        write_json(run_root / "canonical_efficiency_report.json", canonical_efficiency)
        print(json.dumps({"run_root": str(run_root), "fatal_error": failure_summary["fatal_error"]}, indent=2), file=sys.stderr)
        return 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run end-to-end pipeline proof: frontier seed -> fetch -> ATS"
    )
    p.add_argument("--base-url", default=os.getenv("DELTA_BASE_URL", "http://localhost:8080"))
    p.add_argument("--out-dir", default=os.getenv("DELTA_OUT_DIR", "out"))
    p.add_argument("--preset", choices=["S", "M", "L"], help="Apply preset tuning (explicit flags override)")
    p.add_argument(
        "--write-docs-report",
        action="store_true",
        help="Also write docs/release_e2e_pipeline_proof_<date>.md (best-effort).",
    )
    p.add_argument(
        "--db-snapshots",
        dest="db_snapshots",
        action="store_true",
        default=True,
        help="Capture DB snapshots at start/end and include db_truth in outputs (default: on).",
    )
    p.add_argument(
        "--no-db-snapshots",
        dest="db_snapshots",
        action="store_false",
        help="Disable DB snapshot capture and db_truth deltas.",
    )
    p.add_argument(
        "--db-snapshot-mode",
        choices=["auto", "direct", "docker"],
        default="auto",
        help="DB snapshot capture mode used by db_snapshot.py helpers.",
    )
    p.add_argument("--dry-run", action="store_true", help="Validate config/dependencies/api status only")
    p.add_argument(
        "--reset-frontier",
        action="store_true",
        default=False,
        help="Truncate crawl_hosts/crawl_urls/crawl_url_attempts before run",
    )

    p.add_argument("--seed-domain-limit", type=int, default=None)
    p.add_argument("--seed-max-sitemap-fetches", type=int, default=None)
    p.add_argument("--seed-timeout-seconds", type=int, default=None)

    p.add_argument("--fetch-cycles", type=int, default=None)
    p.add_argument("--fetch-domain-limit", type=int, default=None)
    p.add_argument("--fetch-batch-size", type=int, default=None)
    p.add_argument("--fetch-timeout-seconds", type=int, default=None)
    p.add_argument("--fetch-sleep-seconds", type=int, default=None)
    p.add_argument("--fetch-min-attempts-before-ats", type=int, default=None)

    p.add_argument("--sec-limit", type=int, default=None)
    p.add_argument("--sec-ingest-timeout-seconds", type=int, default=None)
    p.add_argument("--resolve-batch-size", type=int, default=None)
    p.add_argument("--resolve-cycles", type=int, default=None)
    p.add_argument("--resolve-timeout-seconds", type=int, default=None)
    p.add_argument("--discover-limit", type=int, default=None)
    p.add_argument("--discover-batch-size", type=int, default=None)
    p.add_argument("--discover-max-wait-seconds", type=int, default=None)
    p.add_argument("--discover-poll-interval-seconds", type=int, default=None)

    p.add_argument("--postgres-container", default="delta-job-tracker-postgres")
    p.add_argument("--postgres-user", default="delta")
    p.add_argument("--postgres-db", default="delta_job_tracker")

    args = p.parse_args(argv)
    for key in CONFIG_KEYS:
        value = getattr(args, key)
        if value is not None and int(value) < 0:
            p.error(f"{key} must be >= 0")
    return args


def main(argv: list[str] | None = None) -> int:
    explicit = detect_explicit_cli_options(argv if argv is not None else sys.argv[1:])
    args = parse_args(argv)
    return run(args, explicit)


if __name__ == "__main__":
    sys.exit(main())
