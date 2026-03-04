#!/usr/bin/env python3
"""Capture DB-truth snapshots for soak runs."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


QUERY_ATS_ENDPOINT_ROWS = "SELECT COUNT(*) AS value FROM ats_endpoints;"
QUERY_ATS_COMPANIES = "SELECT COUNT(DISTINCT company_id) AS value FROM ats_endpoints;"
QUERY_ENDPOINTS_BY_TYPE = """
SELECT COALESCE(ats_type, 'UNKNOWN') AS ats_type, COUNT(*) AS count
FROM ats_endpoints
GROUP BY COALESCE(ats_type, 'UNKNOWN')
ORDER BY COUNT(*) DESC, COALESCE(ats_type, 'UNKNOWN');
"""
QUERY_COMPANIES_WITH_DOMAIN = "SELECT COUNT(DISTINCT company_id) AS value FROM company_domains;"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def run_psql_csv(cmd: List[str], env: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(stderr or stdout or f"command failed: {' '.join(cmd)}")
    out = proc.stdout or ""
    if not out.strip():
        return []
    return list(csv.DictReader(io.StringIO(out)))


def try_direct_psql(sql: str, env: Dict[str, str]) -> List[Dict[str, str]]:
    cmd = ["psql", "--csv", "-v", "ON_ERROR_STOP=1", "-c", sql]
    return run_psql_csv(cmd, env=env)


def detect_compose_postgres_container(repo_root: Path) -> Optional[str]:
    candidates = []
    explicit = os.getenv("SOAK_POSTGRES_CONTAINER")
    if explicit:
        candidates.append(explicit)

    compose_file = repo_root / "infra" / "docker-compose.yml"
    if compose_file.exists():
        try:
            cid = (
                subprocess.check_output(
                    [
                        "docker",
                        "compose",
                        "-f",
                        str(compose_file),
                        "ps",
                        "-q",
                        "postgres",
                    ],
                    text=True,
                )
                .strip()
            )
            if cid:
                candidates.append(cid)
        except Exception:
            pass

    try:
        fallback = (
            subprocess.check_output(
                [
                    "docker",
                    "ps",
                    "--filter",
                    "name=delta-job-tracker-postgres",
                    "--format",
                    "{{.ID}}",
                ],
                text=True,
            )
            .strip()
        )
        if fallback:
            candidates.append(fallback.splitlines()[0].strip())
    except Exception:
        pass

    for c in candidates:
        if c:
            return c
    return None


def try_docker_psql(repo_root: Path, sql: str) -> List[Dict[str, str]]:
    container = detect_compose_postgres_container(repo_root)
    if not container:
        raise RuntimeError("Could not find postgres docker container")
    user = os.getenv("SOAK_POSTGRES_USER", os.getenv("DELTA_POSTGRES_USER", "delta"))
    database = os.getenv("SOAK_POSTGRES_DB", os.getenv("DELTA_POSTGRES_DB", "delta_job_tracker"))
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
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        sql,
    ]
    return run_psql_csv(cmd)


def build_metrics(queries: Dict[str, List[Dict[str, str]]]) -> Dict[str, Any]:
    endpoints_by_type_rows: List[Dict[str, Any]] = []
    by_type_map: Dict[str, int] = {}
    for row in queries.get("endpoints_by_type", []):
        ats_type = str(row.get("ats_type") or "UNKNOWN")
        count = parse_int(row.get("count"))
        endpoints_by_type_rows.append({"ats_type": ats_type, "count": count})
        by_type_map[ats_type] = count
    return {
        "ats_endpoints_count": parse_int((queries.get("ats_endpoints_count") or [{}])[0].get("value")),
        "companies_with_ats_endpoint_count": parse_int(
            (queries.get("companies_with_ats_endpoint_count") or [{}])[0].get("value")
        ),
        "endpoints_by_type": endpoints_by_type_rows,
        "endpoints_by_type_map": by_type_map,
        "companies_with_domain_count": parse_int(
            (queries.get("companies_with_domain_count") or [{}])[0].get("value")
        ),
    }


def capture_db_snapshot(repo_root: Path, mode: str = "auto") -> Dict[str, Any]:
    attempts: List[Dict[str, Any]] = []
    modes = [mode] if mode != "auto" else ["direct", "docker"]

    query_plan = {
        "ats_endpoints_count": QUERY_ATS_ENDPOINT_ROWS,
        "companies_with_ats_endpoint_count": QUERY_ATS_COMPANIES,
        "endpoints_by_type": QUERY_ENDPOINTS_BY_TYPE,
        "companies_with_domain_count": QUERY_COMPANIES_WITH_DOMAIN,
    }

    for candidate in modes:
        try:
            rows: Dict[str, List[Dict[str, str]]] = {}
            for name, sql in query_plan.items():
                if candidate == "direct":
                    rows[name] = try_direct_psql(sql, dict(os.environ))
                elif candidate == "docker":
                    rows[name] = try_docker_psql(repo_root, sql)
                else:
                    raise RuntimeError(f"Unsupported mode: {candidate}")
            return {
                "schema_version": "db-snapshot-v1",
                "captured_at": utc_iso(),
                "mode": candidate,
                "metrics": build_metrics(rows),
            }
        except Exception as exc:
            attempts.append({"mode": candidate, "error": str(exc)})

    return {
        "schema_version": "db-snapshot-v1",
        "captured_at": utc_iso(),
        "mode": mode,
        "error": {
            "message": "Could not capture DB snapshot in any supported mode",
            "attempts": attempts,
        },
    }


def metrics_for_delta(snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not snapshot or not isinstance(snapshot.get("metrics"), dict):
        return {
            "ats_endpoints_count": 0,
            "companies_with_ats_endpoint_count": 0,
            "companies_with_domain_count": 0,
            "endpoints_by_type_map": {},
        }
    metrics = snapshot["metrics"]
    return {
        "ats_endpoints_count": parse_int(metrics.get("ats_endpoints_count")),
        "companies_with_ats_endpoint_count": parse_int(metrics.get("companies_with_ats_endpoint_count")),
        "companies_with_domain_count": parse_int(metrics.get("companies_with_domain_count")),
        "endpoints_by_type_map": {
            str(k): parse_int(v) for k, v in (metrics.get("endpoints_by_type_map") or {}).items()
        },
    }


def build_db_truth(
    start_snapshot: Optional[Dict[str, Any]], end_snapshot: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    start_metrics = metrics_for_delta(start_snapshot)
    end_metrics = metrics_for_delta(end_snapshot)

    type_keys = sorted(
        set(start_metrics["endpoints_by_type_map"].keys())
        | set(end_metrics["endpoints_by_type_map"].keys())
    )
    endpoints_by_type_delta = {
        key: end_metrics["endpoints_by_type_map"].get(key, 0)
        - start_metrics["endpoints_by_type_map"].get(key, 0)
        for key in type_keys
    }
    errors: List[Dict[str, Any]] = []
    if start_snapshot and start_snapshot.get("error"):
        errors.append({"snapshot": "start", "error": start_snapshot.get("error")})
    if end_snapshot and end_snapshot.get("error"):
        errors.append({"snapshot": "end", "error": end_snapshot.get("error")})

    return {
        "start": start_snapshot,
        "end": end_snapshot,
        "delta": {
            "ats_endpoints_count": end_metrics["ats_endpoints_count"]
            - start_metrics["ats_endpoints_count"],
            "companies_with_ats_endpoint_count": end_metrics["companies_with_ats_endpoint_count"]
            - start_metrics["companies_with_ats_endpoint_count"],
            "companies_with_domain_count": end_metrics["companies_with_domain_count"]
            - start_metrics["companies_with_domain_count"],
            "endpoints_by_type": endpoints_by_type_delta,
        },
        "errors": errors,
    }


def patch_soak_report(out_dir: Path, db_truth: Dict[str, Any]) -> bool:
    report_path = out_dir / "soak_report.json"
    report = read_json(report_path)
    if report is None:
        return False
    report["db_truth"] = db_truth
    write_json(report_path, report)
    return True


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture/patch DB-truth snapshots for soak artifacts.")
    parser.add_argument("--out-dir", required=True, help="Soak output directory (out/<timestamp>)")
    parser.add_argument("--mode", choices=["auto", "direct", "docker"], default="auto")
    parser.add_argument(
        "--emit-db-truth",
        action="store_true",
        default=False,
        help="Patch soak_report.json with db_truth section when available.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    repo_root = Path(__file__).resolve().parents[1]

    start_path = out_dir / "db_snapshot_start.json"
    end_path = out_dir / "db_snapshot_end.json"

    start_snapshot = read_json(start_path)
    if start_snapshot is None:
        start_snapshot = capture_db_snapshot(repo_root, mode=args.mode)
        start_snapshot["posthoc"] = True
        start_snapshot["note"] = "No existing start snapshot; captured current DB state."
        write_json(start_path, start_snapshot)

    end_snapshot = capture_db_snapshot(repo_root, mode=args.mode)
    write_json(end_path, end_snapshot)

    db_truth = build_db_truth(start_snapshot, end_snapshot)
    if args.emit_db_truth:
        patched = patch_soak_report(out_dir, db_truth)
        if not patched:
            write_json(out_dir / "db_truth.json", db_truth)
    else:
        write_json(out_dir / "db_truth.json", db_truth)

    print(json.dumps({"out_dir": str(out_dir), "db_truth": db_truth}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
