#!/usr/bin/env python3
"""Run warmup + ATS-heavy soak cycles and emit aggregate soak artifacts."""

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
from urllib.error import URLError
from urllib.request import Request, urlopen

import db_snapshot as dbsnap


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def utc_stamp() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_cmd(cmd: List[str], cwd: Path, env: Dict[str, str], log_path: Path) -> int:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as fh:
        fh.write(f"$ {' '.join(cmd)}\n")
        fh.flush()
        proc = subprocess.run(cmd, cwd=str(cwd), env=env, stdout=fh, stderr=subprocess.STDOUT)
    return proc.returncode


def api_status_ok(base_url: str, timeout_seconds: int = 5) -> bool:
    url = base_url.rstrip("/") + "/api/status"
    req = Request(url, method="GET")
    req.add_header("Accept", "application/json")
    try:
        with urlopen(req, timeout=timeout_seconds) as resp:
            return 200 <= int(getattr(resp, "status", 0)) < 300
    except (TimeoutError, URLError, ValueError):
        return False


def safe_read_json(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def capture_db_snapshot_safe(repo_root: Path) -> Dict[str, Any]:
    try:
        return dbsnap.capture_db_snapshot(repo_root, mode="auto")
    except Exception as exc:
        return {
            "schema_version": "db-snapshot-v1",
            "captured_at": utc_iso(),
            "mode": "auto",
            "error": {
                "message": "DB snapshot capture failed with unexpected exception",
                "attempts": [{"mode": "auto", "error": str(exc)}],
            },
        }


def collect_new_run_dir(base_dir: Path, before: set[str]) -> Optional[Path]:
    after = {p.name for p in base_dir.iterdir() if p.is_dir()} if base_dir.exists() else set()
    created = sorted(after - before)
    if created:
        return base_dir / created[-1]
    all_dirs = [p for p in base_dir.iterdir() if p.is_dir()] if base_dir.exists() else []
    if not all_dirs:
        return None
    return max(all_dirs, key=lambda p: p.stat().st_mtime)


def top_n_counts(counts: Dict[str, int], n: int = 10) -> List[Dict[str, Any]]:
    rows = [{"key": k, "count": parse_int(v)} for k, v in counts.items() if parse_int(v) > 0]
    rows.sort(key=lambda r: (-r["count"], r["key"]))
    return rows[:n]


def git_value(repo_root: Path, args: List[str], default: str) -> str:
    try:
        return (
            subprocess.check_output(["git", *args], cwd=str(repo_root), text=True)
            .strip()
            .strip("'")
        )
    except Exception:
        return default


JQ_SNIPPETS = [
    ".pipeline.domain_counts_before.companies_with_domain_count",
    ".pipeline.domain_counts_after.companies_with_domain_count",
    ".pipeline.ats_selection_metrics.vendor_probe.selectionEligibleCount",
    ".pipeline.ats_selection_metrics.vendor_probe.selectionReturnedCount",
    ".pipeline.ats_selection_metrics.vendor_probe.companiesAttemptedCount",
    ".pipeline.ats_selection_metrics.vendor_probe.vendorDetectedCount",
    ".pipeline.ats_selection_metrics.vendor_probe.endpointExtractedCount",
]


@dataclass
class PhaseConfig:
    name: str
    duration_seconds: float
    sec_limit: int
    resolve_limit: int
    resolve_batch_size: int
    discover_batch_size: int
    num_batches: int
    sleep_between_batches: int
    domain_fresh_cohort_first: bool


def build_runner_args(phase_cfg: PhaseConfig, run_full_mode: bool) -> List[str]:
    args = [
        "--sec-limit",
        str(phase_cfg.sec_limit),
        "--resolve-limit",
        str(phase_cfg.resolve_limit),
        "--resolve-batch-size",
        str(phase_cfg.resolve_batch_size),
        "--discover-batch-size",
        str(phase_cfg.discover_batch_size),
        "--num-batches",
        str(phase_cfg.num_batches),
        "--sleep-between-batches",
        str(phase_cfg.sleep_between_batches),
    ]
    if phase_cfg.domain_fresh_cohort_first:
        args.append("--domain-fresh-cohort-first")
    if run_full_mode:
        args.append("--run-full-mode")
    else:
        args.append("--no-run-full-mode")
    return args


def load_cycle_metrics(run_dir: Path) -> Dict[str, Any]:
    overnight_path = run_dir / "overnight_summary.json"
    overnight = safe_read_json(overnight_path) or {}
    pipeline = overnight.get("pipeline") if isinstance(overnight, dict) else {}
    pipeline = pipeline if isinstance(pipeline, dict) else {}

    before = (
        parse_int(((pipeline.get("domain_counts_before") or {}).get("companies_with_domain_count")))
        if isinstance(pipeline.get("domain_counts_before"), dict)
        else 0
    )
    after = (
        parse_int(((pipeline.get("domain_counts_after") or {}).get("companies_with_domain_count")))
        if isinstance(pipeline.get("domain_counts_after"), dict)
        else 0
    )
    ats = pipeline.get("ats_selection_metrics") if isinstance(pipeline.get("ats_selection_metrics"), dict) else {}
    vendor = ats.get("vendor_probe") if isinstance(ats.get("vendor_probe"), dict) else {}
    full = ats.get("full") if isinstance(ats.get("full"), dict) else {}

    discover_failures_path = run_dir / "discovery_failures_diagnostics.json"
    discover_failures = safe_read_json(discover_failures_path) or {}
    counts_by_reason = (
        discover_failures.get("countsByReason", {})
        if isinstance(discover_failures.get("countsByReason"), dict)
        else {}
    )
    failure_types = (
        discover_failures.get("failureTypeBreakdown", {})
        if isinstance(discover_failures.get("failureTypeBreakdown"), dict)
        else {}
    )

    return {
        "domains_before": before,
        "domains_after": after,
        "domains_delta": after - before,
        "selectionEligibleCount": parse_int(vendor.get("selectionEligibleCount"))
        + parse_int(full.get("selectionEligibleCount")),
        "selectionReturnedCount": parse_int(vendor.get("selectionReturnedCount"))
        + parse_int(full.get("selectionReturnedCount")),
        "companiesAttemptedCount": parse_int(vendor.get("companiesAttemptedCount"))
        + parse_int(full.get("companiesAttemptedCount")),
        "vendorDetectedCount": parse_int(vendor.get("vendorDetectedCount"))
        + parse_int(full.get("vendorDetectedCount")),
        "endpointExtractedCount": parse_int(vendor.get("endpointExtractedCount"))
        + parse_int(full.get("endpointExtractedCount")),
        "failureTypeBreakdown": {str(k): parse_int(v) for k, v in failure_types.items()},
        "failureCountsByReason": {str(k): parse_int(v) for k, v in counts_by_reason.items()},
        "artifacts": {
            "overnight_summary": str(overnight_path),
            "discovery_failures_diagnostics": str(discover_failures_path),
        },
    }


def write_report(
    report_path: Path,
    started_at: datetime,
    finished_at: Optional[datetime],
    cycles: List[Dict[str, Any]],
    db_truth: Dict[str, Any],
    status: str,
) -> None:
    completed = [c for c in cycles if c.get("metrics")]
    domain_before = parse_int(completed[0]["metrics"].get("domains_before")) if completed else 0
    domain_after = parse_int(completed[-1]["metrics"].get("domains_after")) if completed else 0

    totals = {
        "selectionEligibleCount": 0,
        "selectionReturnedCount": 0,
        "companiesAttemptedCount": 0,
        "vendorDetectedCount": 0,
        "endpointExtractedCount": 0,
    }
    failure_type_totals: Dict[str, int] = {}
    failure_reason_totals: Dict[str, int] = {}
    artifact_pointers: List[Dict[str, Any]] = []

    for cycle in completed:
        metrics = cycle["metrics"]
        for key in totals:
            totals[key] += parse_int(metrics.get(key))
        for key, value in (metrics.get("failureTypeBreakdown") or {}).items():
            failure_type_totals[str(key)] = failure_type_totals.get(str(key), 0) + parse_int(value)
        for key, value in (metrics.get("failureCountsByReason") or {}).items():
            failure_reason_totals[str(key)] = failure_reason_totals.get(str(key), 0) + parse_int(value)
        artifact_pointers.append(
            {
                "phase": cycle.get("phase"),
                "cycle": cycle.get("cycle"),
                "run_dir": cycle.get("run_dir"),
                "overnight_summary": cycle.get("metrics", {}).get("artifacts", {}).get("overnight_summary"),
                "discovery_failures_diagnostics": cycle.get("metrics", {})
                .get("artifacts", {})
                .get("discovery_failures_diagnostics"),
            }
        )

    end_ts = finished_at or utc_now()
    elapsed_seconds = max(0.0, (end_ts - started_at).total_seconds())
    elapsed_hours = elapsed_seconds / 3600 if elapsed_seconds > 0 else 0.0
    attempts_per_hour = (
        round(totals["companiesAttemptedCount"] / elapsed_hours, 2) if elapsed_hours > 0 else 0.0
    )
    endpoints_per_hour = (
        round(totals["endpointExtractedCount"] / elapsed_hours, 2) if elapsed_hours > 0 else 0.0
    )

    payload = {
        "schema_version": "soak-report-v1",
        "status": status,
        "generated_at": utc_iso(),
        "started_at": utc_iso(started_at),
        "finished_at": utc_iso(finished_at) if finished_at else None,
        "elapsed_seconds": int(elapsed_seconds),
        "elapsed_hours": round(elapsed_hours, 4),
        "domains": {
            "domains_before": domain_before,
            "domains_after": domain_after,
            "domains_delta": domain_after - domain_before,
        },
        "ats_selection_metrics_totals": totals,
        "throughput": {
            "attempts_per_hour": attempts_per_hour,
            "endpoints_per_hour": endpoints_per_hour,
        },
        "failure_type_top_buckets": top_n_counts(failure_type_totals, 10),
        "failure_reason_top_buckets": top_n_counts(failure_reason_totals, 10),
        "cycles_completed": len(completed),
        "cycles": cycles,
        "artifact_pointers": artifact_pointers,
        "jq_snippets_used": JQ_SNIPPETS,
        "db_truth": db_truth,
    }
    write_json(report_path, payload)


def run_phase(
    repo_root: Path,
    root_out: Path,
    phase_cfg: PhaseConfig,
    base_url: str,
    max_duration_seconds: int,
    run_full_mode: bool,
    stop_backend: bool,
    backend_running: bool,
    cycles: List[Dict[str, Any]],
) -> tuple[bool, bool]:
    if phase_cfg.duration_seconds <= 0:
        return True, backend_running

    phase_dir = root_out / "runs" / phase_cfg.name
    phase_dir.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + phase_cfg.duration_seconds
    cycle_index = 0

    while time.monotonic() < deadline:
        cycle_index += 1
        cycle_started_at = utc_now()
        before = {p.name for p in phase_dir.iterdir() if p.is_dir()}
        cycle_log = root_out / "logs" / f"{phase_cfg.name}_cycle_{cycle_index:03d}.log"
        env = dict(os.environ)
        env["OUT_BASE"] = str(phase_dir)
        env["CRAWLER_CAREERS_DISCOVERY_MAX_DURATION_SECONDS"] = str(max_duration_seconds)
        runner_args = build_runner_args(phase_cfg, run_full_mode)
        if backend_running and not api_status_ok(base_url):
            backend_running = False

        if stop_backend or not backend_running:
            cmd = [
                "./scripts/run_overnight_stack.sh",
                "--base-url",
                base_url,
            ]
            if stop_backend:
                cmd.append("--stop-backend")
            cmd.extend(runner_args)
            runner_type = "stack"
        else:
            cmd = [
                "python3",
                "scripts/run_overnight_crawler.py",
                "--base-url",
                base_url,
                "--out-dir",
                str(phase_dir),
            ]
            cmd.extend(runner_args)
            runner_type = "crawler"

        rc = run_cmd(cmd, repo_root, env, cycle_log)
        if rc == 0 and not stop_backend:
            backend_running = True
        if stop_backend:
            backend_running = False
        cycle_finished_at = utc_now()
        run_dir = collect_new_run_dir(phase_dir, before)
        metrics = load_cycle_metrics(run_dir) if (rc == 0 and run_dir) else None

        cycles.append(
            {
                "phase": phase_cfg.name,
                "cycle": cycle_index,
                "started_at": utc_iso(cycle_started_at),
                "finished_at": utc_iso(cycle_finished_at),
                "return_code": rc,
                "run_dir": str(run_dir) if run_dir else None,
                "log": str(cycle_log),
                "runner": runner_type,
                "metrics": metrics,
            }
        )

        if rc != 0:
            return False, backend_running
    return True, backend_running


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run warmup + ATS-heavy soak test cycles.")
    parser.add_argument("--base-url", default=os.getenv("SOAK_BASE_URL", "http://localhost:8080"))
    parser.add_argument("--out-base", default=os.getenv("SOAK_OUT_BASE", "out"))
    parser.add_argument("--warmup-hours", type=float, default=float(os.getenv("SOAK_WARMUP_HOURS", "0")))
    parser.add_argument("--soak-hours", type=float, default=float(os.getenv("SOAK_HOURS", "12")))
    parser.add_argument(
        "--run-full-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Forward full-mode gate override to overnight runner (default: enabled).",
    )
    parser.add_argument(
        "--stop-backend",
        action="store_true",
        default=False,
        help="Pass --stop-backend to run_overnight_stack.sh; otherwise backend stays up between cycles.",
    )
    parser.add_argument(
        "--crawler-max-duration-seconds",
        type=int,
        default=int(os.getenv("SOAK_CRAWLER_MAX_DURATION_SECONDS", "45")),
        help="Passed via CRAWLER_CAREERS_DISCOVERY_MAX_DURATION_SECONDS.",
    )

    parser.add_argument("--warmup-sec-limit", type=int, default=5000)
    parser.add_argument("--warmup-resolve-limit", type=int, default=400)
    parser.add_argument("--warmup-resolve-batch-size", type=int, default=100)
    parser.add_argument("--warmup-discover-batch-size", type=int, default=8)
    parser.add_argument("--warmup-num-batches", type=int, default=1)
    parser.add_argument("--warmup-sleep-between-batches", type=int, default=2)
    parser.add_argument("--warmup-domain-fresh-cohort-first", action="store_true", default=False)

    parser.add_argument("--soak-sec-limit", type=int, default=2000)
    parser.add_argument("--soak-resolve-limit", type=int, default=0)
    parser.add_argument("--soak-resolve-batch-size", type=int, default=100)
    parser.add_argument("--soak-discover-batch-size", type=int, default=20)
    parser.add_argument("--soak-num-batches", type=int, default=2)
    parser.add_argument("--soak-sleep-between-batches", type=int, default=5)
    parser.add_argument("--soak-domain-fresh-cohort-first", action="store_true", default=False)

    args = parser.parse_args(argv)
    if args.warmup_hours < 0 or args.soak_hours <= 0:
        parser.error("warmup-hours must be >= 0 and soak-hours must be > 0")
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    out_root = Path(args.out_base) / utc_stamp()
    out_root.mkdir(parents=True, exist_ok=False)

    started_at = utc_now()
    db_snapshot_start = capture_db_snapshot_safe(repo_root)
    write_json(out_root / "db_snapshot_start.json", db_snapshot_start)
    db_truth_running = dbsnap.build_db_truth(db_snapshot_start, None)
    manifest = {
        "schema_version": "soak-manifest-v1",
        "started_at": utc_iso(started_at),
        "git_commit": git_value(repo_root, ["rev-parse", "HEAD"], "unknown"),
        "git_branch": git_value(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"], "unknown"),
        "args": vars(args),
        "jq_snippets_used": JQ_SNIPPETS,
    }
    write_json(out_root / "soak_manifest.json", manifest)

    cycles: List[Dict[str, Any]] = []
    warmup = PhaseConfig(
        name="warmup",
        duration_seconds=args.warmup_hours * 3600,
        sec_limit=args.warmup_sec_limit,
        resolve_limit=args.warmup_resolve_limit,
        resolve_batch_size=args.warmup_resolve_batch_size,
        discover_batch_size=args.warmup_discover_batch_size,
        num_batches=args.warmup_num_batches,
        sleep_between_batches=args.warmup_sleep_between_batches,
        domain_fresh_cohort_first=bool(args.warmup_domain_fresh_cohort_first),
    )
    soak = PhaseConfig(
        name="soak",
        duration_seconds=args.soak_hours * 3600,
        sec_limit=args.soak_sec_limit,
        resolve_limit=args.soak_resolve_limit,
        resolve_batch_size=args.soak_resolve_batch_size,
        discover_batch_size=args.soak_discover_batch_size,
        num_batches=args.soak_num_batches,
        sleep_between_batches=args.soak_sleep_between_batches,
        domain_fresh_cohort_first=bool(args.soak_domain_fresh_cohort_first),
    )

    backend_running = False
    ok, backend_running = run_phase(
        repo_root,
        out_root,
        warmup,
        args.base_url,
        args.crawler_max_duration_seconds,
        bool(args.run_full_mode),
        bool(args.stop_backend),
        backend_running,
        cycles,
    )
    write_report(out_root / "soak_report.json", started_at, None, cycles, db_truth_running, "RUNNING")
    if ok:
        ok, backend_running = run_phase(
            repo_root,
            out_root,
            soak,
            args.base_url,
            args.crawler_max_duration_seconds,
            bool(args.run_full_mode),
            bool(args.stop_backend),
            backend_running,
            cycles,
        )

    finished_at = utc_now()
    db_snapshot_end = capture_db_snapshot_safe(repo_root)
    write_json(out_root / "db_snapshot_end.json", db_snapshot_end)
    db_truth = dbsnap.build_db_truth(db_snapshot_start, db_snapshot_end)
    write_report(
        out_root / "soak_report.json",
        started_at,
        finished_at,
        cycles,
        db_truth,
        "SUCCEEDED" if ok else "FAILED",
    )

    print(f"Soak artifacts: {out_root}", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
