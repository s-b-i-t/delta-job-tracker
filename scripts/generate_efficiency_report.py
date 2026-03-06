#!/usr/bin/env python3
"""Generate crawler efficiency artifacts by sampling DB truth during a wrapped command."""

from __future__ import annotations

import argparse
import csv
import json
import shlex
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import db_snapshot as dbsnap


REPORT_SCHEMA_VERSION = "efficiency-report-v1"
SUMMARY_SCHEMA_VERSION = "efficiency-report-summary-v1"
CSV_HEADERS = [
    "sample_index",
    "timestamp_utc",
    "elapsed_seconds",
    "snapshot_ok",
    "mode_used",
    "ats_endpoints_count",
    "companies_with_domain_count",
    "companies_with_ats_endpoint_count",
    "ats_endpoints_delta",
    "ats_endpoints_rate_per_min",
    "companies_with_domain_delta",
    "companies_with_domain_rate_per_min",
    "companies_with_ats_endpoint_delta",
    "companies_with_ats_endpoint_rate_per_min",
    "endpoints_by_type_json",
    "error_message",
]
METRIC_KEYS = [
    "ats_endpoints_count",
    "companies_with_domain_count",
    "companies_with_ats_endpoint_count",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).isoformat()


def utc_stamp(dt: Optional[datetime] = None) -> str:
    return (dt or utc_now()).strftime("%Y%m%dT%H%M%SZ")


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def format_float(value: Optional[float]) -> str:
    if value is None:
        return ""
    return f"{value:.6f}"


def maybe_metric(snapshot: Dict[str, Any], key: str) -> Optional[int]:
    metrics = snapshot.get("metrics") if isinstance(snapshot, dict) else None
    if not isinstance(metrics, dict):
        return None
    if key not in metrics:
        return None
    return parse_int(metrics.get(key))


def sample_once(
    repo_root: Path,
    mode: str,
    sample_index: int,
    started_mono: float,
    now_mono: float,
) -> Dict[str, Any]:
    try:
        snapshot = dbsnap.capture_db_snapshot(repo_root, mode=mode)
    except Exception as exc:  # noqa: BLE001
        snapshot = {
            "schema_version": "db-snapshot-v1",
            "captured_at": utc_iso(),
            "ok": False,
            "mode_requested": mode,
            "mode_used": None,
            "error": {
                "message": "DB snapshot capture raised unexpected exception",
                "attempts": [{"mode": mode, "error": str(exc)}],
            },
        }

    metrics_obj = snapshot.get("metrics") if isinstance(snapshot.get("metrics"), dict) else {}
    endpoints_by_type_map = metrics_obj.get("endpoints_by_type_map")
    if not isinstance(endpoints_by_type_map, dict):
        endpoints_by_type_map = {}

    return {
        "sample_index": sample_index,
        "timestamp_utc": str(snapshot.get("captured_at") or utc_iso()),
        "elapsed_seconds": round(max(0.0, now_mono - started_mono), 3),
        "snapshot_ok": bool(snapshot.get("ok")),
        "mode_requested": snapshot.get("mode_requested"),
        "mode_used": snapshot.get("mode_used"),
        "metrics": {
            "ats_endpoints_count": maybe_metric(snapshot, "ats_endpoints_count"),
            "companies_with_domain_count": maybe_metric(snapshot, "companies_with_domain_count"),
            "companies_with_ats_endpoint_count": maybe_metric(snapshot, "companies_with_ats_endpoint_count"),
            "endpoints_by_type_map": {
                str(k): parse_int(v)
                for k, v in endpoints_by_type_map.items()
                if str(k).strip()
            },
        },
        "error": snapshot.get("error"),
    }


def find_rate(prev: Dict[str, Any], curr: Dict[str, Any], key: str) -> Tuple[Optional[int], Optional[float]]:
    dt_seconds = parse_float(curr.get("elapsed_seconds")) - parse_float(prev.get("elapsed_seconds"))
    if dt_seconds <= 0:
        return None, None
    prev_value = (prev.get("metrics") or {}).get(key)
    curr_value = (curr.get("metrics") or {}).get(key)
    if prev_value is None or curr_value is None:
        return None, None
    delta = parse_int(curr_value) - parse_int(prev_value)
    return delta, (delta * 60.0) / dt_seconds


def endpoints_type_delta(first_ok: Dict[str, Any], last_ok: Dict[str, Any]) -> Dict[str, int]:
    first_map = (first_ok.get("metrics") or {}).get("endpoints_by_type_map") or {}
    last_map = (last_ok.get("metrics") or {}).get("endpoints_by_type_map") or {}
    keys = sorted(set(first_map.keys()) | set(last_map.keys()))
    return {
        key: parse_int(last_map.get(key)) - parse_int(first_map.get(key))
        for key in keys
        if parse_int(last_map.get(key)) - parse_int(first_map.get(key)) != 0
    }


def sorted_counter(counter: Counter[str]) -> List[Dict[str, Any]]:
    rows = [{"key": key, "count": count} for key, count in counter.items() if count > 0]
    rows.sort(key=lambda row: (-parse_int(row["count"]), row["key"]))
    return rows


def build_throughput_stats(samples: List[Dict[str, Any]]) -> Dict[str, Any]:
    ok_samples = [sample for sample in samples if sample.get("snapshot_ok")]
    stats: Dict[str, Any] = {}
    if len(ok_samples) < 2:
        for key in METRIC_KEYS:
            stats[key] = {
                "start": None,
                "end": None,
                "delta": None,
                "avg_rate_per_min": None,
                "peak_rate_per_min": None,
                "latest_rate_per_min": None,
            }
        return stats

    first_ok = ok_samples[0]
    last_ok = ok_samples[-1]
    elapsed_minutes = max(
        0.0,
        (parse_float(last_ok.get("elapsed_seconds")) - parse_float(first_ok.get("elapsed_seconds"))) / 60.0,
    )

    for key in METRIC_KEYS:
        start_value = (first_ok.get("metrics") or {}).get(key)
        end_value = (last_ok.get("metrics") or {}).get(key)
        if start_value is None or end_value is None:
            stats[key] = {
                "start": None,
                "end": None,
                "delta": None,
                "avg_rate_per_min": None,
                "peak_rate_per_min": None,
                "latest_rate_per_min": None,
            }
            continue

        rate_series: List[float] = []
        latest_rate: Optional[float] = None
        prev = ok_samples[0]
        for curr in ok_samples[1:]:
            delta, rate = find_rate(prev, curr, key)
            if rate is not None:
                rate_series.append(rate)
                latest_rate = rate
            prev = curr

        delta_total = parse_int(end_value) - parse_int(start_value)
        avg_rate = (delta_total / elapsed_minutes) if elapsed_minutes > 0 else None
        peak_rate = max(rate_series) if rate_series else None

        stats[key] = {
            "start": parse_int(start_value),
            "end": parse_int(end_value),
            "delta": delta_total,
            "avg_rate_per_min": round(avg_rate, 4) if avg_rate is not None else None,
            "peak_rate_per_min": round(peak_rate, 4) if peak_rate is not None else None,
            "latest_rate_per_min": round(latest_rate, 4) if latest_rate is not None else None,
        }

    return stats


def build_concurrency_justification(samples: List[Dict[str, Any]], throughput: Dict[str, Any]) -> Dict[str, Any]:
    ok_samples = [sample for sample in samples if sample.get("snapshot_ok")]
    snapshot_errors = len(samples) - len(ok_samples)
    endpoints = throughput.get("ats_endpoints_count") or {}
    domains = throughput.get("companies_with_domain_count") or {}

    statements: List[str] = []
    if len(ok_samples) < 2:
        statements.append("Insufficient successful DB samples for a reliable concurrency inference.")
    else:
        endpoint_avg = endpoints.get("avg_rate_per_min")
        endpoint_peak = endpoints.get("peak_rate_per_min")
        domain_avg = domains.get("avg_rate_per_min")

        if endpoint_avg is not None and endpoint_avg > 0:
            statements.append(
                "Endpoint throughput remained positive during execution, indicating productive parallel crawling."
            )
        else:
            statements.append(
                "Endpoint throughput was flat or negative; concurrency likely constrained by upstream selection or failures."
            )

        if endpoint_peak is not None and endpoint_avg is not None and endpoint_peak > (endpoint_avg * 1.8):
            statements.append(
                "Short bursts exceeded the average rate, consistent with batched work release under bounded concurrency."
            )
        elif endpoint_peak is not None and endpoint_avg is not None:
            statements.append(
                "Peak and average rates were close, consistent with steady-state bounded concurrency over the sample window."
            )

        if domain_avg is not None and domain_avg > 0:
            statements.append(
                "Domain coverage growth confirms throughput was not limited to repeated endpoint updates only."
            )

    if snapshot_errors > 0:
        statements.append(
            f"{snapshot_errors} snapshot samples failed, so rate precision is lower than the nominal sampling interval."
        )

    return {
        "summary": " ".join(statements).strip(),
        "evidence": {
            "successful_samples": len(ok_samples),
            "failed_samples": snapshot_errors,
            "ats_avg_rate_per_min": endpoints.get("avg_rate_per_min"),
            "ats_peak_rate_per_min": endpoints.get("peak_rate_per_min"),
            "domains_avg_rate_per_min": domains.get("avg_rate_per_min"),
        },
    }


def build_error_breakdown(
    samples: List[Dict[str, Any]],
    command_exit_code: int,
    command_timed_out: bool,
) -> Dict[str, Any]:
    message_counter: Counter[str] = Counter()
    mode_counter: Counter[str] = Counter()

    for sample in samples:
        if sample.get("snapshot_ok"):
            continue
        err = sample.get("error")
        if isinstance(err, dict):
            msg = str(err.get("message") or "unknown_snapshot_error").strip()
            if msg:
                message_counter[msg] += 1
            attempts = err.get("attempts")
            if isinstance(attempts, list):
                for attempt in attempts:
                    if not isinstance(attempt, dict):
                        continue
                    mode = str(attempt.get("mode") or "unknown").strip() or "unknown"
                    mode_counter[mode] += 1
        else:
            message_counter["unknown_snapshot_error"] += 1

    return {
        "snapshot_failure_count": sum(message_counter.values()),
        "snapshot_failure_messages": sorted_counter(message_counter),
        "snapshot_failure_modes": sorted_counter(mode_counter),
        "wrapped_command_exit_code": command_exit_code,
        "wrapped_command_timed_out": command_timed_out,
    }


def write_metrics_csv(path: Path, samples: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    previous_ok: Optional[Dict[str, Any]] = None

    for sample in samples:
        row: Dict[str, Any] = {
            "sample_index": sample.get("sample_index"),
            "timestamp_utc": sample.get("timestamp_utc"),
            "elapsed_seconds": format_float(parse_float(sample.get("elapsed_seconds"))),
            "snapshot_ok": "true" if sample.get("snapshot_ok") else "false",
            "mode_used": sample.get("mode_used") or "",
            "ats_endpoints_count": "",
            "companies_with_domain_count": "",
            "companies_with_ats_endpoint_count": "",
            "ats_endpoints_delta": "",
            "ats_endpoints_rate_per_min": "",
            "companies_with_domain_delta": "",
            "companies_with_domain_rate_per_min": "",
            "companies_with_ats_endpoint_delta": "",
            "companies_with_ats_endpoint_rate_per_min": "",
            "endpoints_by_type_json": "",
            "error_message": "",
        }

        metrics = sample.get("metrics") if isinstance(sample.get("metrics"), dict) else {}
        if metrics:
            row["ats_endpoints_count"] = (
                parse_int(metrics.get("ats_endpoints_count")) if metrics.get("ats_endpoints_count") is not None else ""
            )
            row["companies_with_domain_count"] = (
                parse_int(metrics.get("companies_with_domain_count"))
                if metrics.get("companies_with_domain_count") is not None
                else ""
            )
            row["companies_with_ats_endpoint_count"] = (
                parse_int(metrics.get("companies_with_ats_endpoint_count"))
                if metrics.get("companies_with_ats_endpoint_count") is not None
                else ""
            )
            row["endpoints_by_type_json"] = json.dumps(
                metrics.get("endpoints_by_type_map") or {}, sort_keys=True
            )

        if sample.get("snapshot_ok") and previous_ok is not None:
            for key in METRIC_KEYS:
                delta, rate = find_rate(previous_ok, sample, key)
                if key == "ats_endpoints_count":
                    row["ats_endpoints_delta"] = "" if delta is None else delta
                    row["ats_endpoints_rate_per_min"] = format_float(rate)
                elif key == "companies_with_domain_count":
                    row["companies_with_domain_delta"] = "" if delta is None else delta
                    row["companies_with_domain_rate_per_min"] = format_float(rate)
                elif key == "companies_with_ats_endpoint_count":
                    row["companies_with_ats_endpoint_delta"] = "" if delta is None else delta
                    row["companies_with_ats_endpoint_rate_per_min"] = format_float(rate)

        if sample.get("snapshot_ok"):
            previous_ok = sample

        err = sample.get("error")
        if isinstance(err, dict):
            row["error_message"] = str(err.get("message") or "")

        rows.append(row)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)


def load_matplotlib() -> Tuple[Optional[Any], Optional[str]]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        return plt, None
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)


def generate_plots(report_dir: Path, samples: List[Dict[str, Any]], type_delta: Dict[str, int]) -> Dict[str, Any]:
    plt, error = load_matplotlib()
    plots_dir = report_dir / "plots"
    if plt is None:
        return {
            "enabled": False,
            "generated": [],
            "skipped_reason": f"matplotlib unavailable: {error}",
        }

    plots_dir.mkdir(parents=True, exist_ok=True)
    generated: List[str] = []

    ok_samples = [sample for sample in samples if sample.get("snapshot_ok")]
    if ok_samples:
        x_minutes = [parse_float(sample.get("elapsed_seconds")) / 60.0 for sample in ok_samples]
        ats_counts = [
            parse_int((sample.get("metrics") or {}).get("ats_endpoints_count"))
            for sample in ok_samples
        ]
        domain_counts = [
            parse_int((sample.get("metrics") or {}).get("companies_with_domain_count"))
            for sample in ok_samples
        ]

        fig = plt.figure(figsize=(8, 4.5))
        ax = fig.add_subplot(111)
        ax.plot(x_minutes, ats_counts, marker="o", label="ATS endpoints")
        ax.plot(x_minutes, domain_counts, marker="o", label="Companies with domain")
        ax.set_xlabel("Elapsed minutes")
        ax.set_ylabel("Count")
        ax.set_title("DB truth counts over time")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best")
        fig.tight_layout()
        out_path = plots_dir / "counts_over_time.png"
        fig.savefig(out_path, dpi=140)
        plt.close(fig)
        generated.append(str(out_path.relative_to(report_dir)))

    if len(ok_samples) >= 2:
        x_rate = []
        ats_rates = []
        domain_rates = []
        prev = ok_samples[0]
        for curr in ok_samples[1:]:
            _, ats_rate = find_rate(prev, curr, "ats_endpoints_count")
            _, dom_rate = find_rate(prev, curr, "companies_with_domain_count")
            x_rate.append(parse_float(curr.get("elapsed_seconds")) / 60.0)
            ats_rates.append(ats_rate if ats_rate is not None else 0.0)
            domain_rates.append(dom_rate if dom_rate is not None else 0.0)
            prev = curr

        fig = plt.figure(figsize=(8, 4.5))
        ax = fig.add_subplot(111)
        ax.plot(x_rate, ats_rates, marker="o", label="ATS endpoints / min")
        ax.plot(x_rate, domain_rates, marker="o", label="Domains / min")
        ax.set_xlabel("Elapsed minutes")
        ax.set_ylabel("Rate per minute")
        ax.set_title("Throughput rates by sample interval")
        ax.grid(True, alpha=0.3)
        ax.legend(loc="best")
        fig.tight_layout()
        out_path = plots_dir / "rates_per_minute.png"
        fig.savefig(out_path, dpi=140)
        plt.close(fig)
        generated.append(str(out_path.relative_to(report_dir)))

    if type_delta:
        names = list(type_delta.keys())
        values = [type_delta[name] for name in names]
        fig = plt.figure(figsize=(8, 4.5))
        ax = fig.add_subplot(111)
        ax.bar(names, values)
        ax.set_xlabel("ATS type")
        ax.set_ylabel("Endpoint delta")
        ax.set_title("Endpoint delta by ATS type")
        ax.tick_params(axis="x", labelrotation=30)
        fig.tight_layout()
        out_path = plots_dir / "endpoints_by_type_delta.png"
        fig.savefig(out_path, dpi=140)
        plt.close(fig)
        generated.append(str(out_path.relative_to(report_dir)))

    if not generated:
        return {
            "enabled": False,
            "generated": [],
            "skipped_reason": "No plottable successful samples were available.",
        }

    return {"enabled": True, "generated": generated, "skipped_reason": None}


def build_readme(
    report_dir: Path,
    metrics: Dict[str, Any],
) -> str:
    throughput = metrics.get("throughput") or {}
    endpoints = throughput.get("ats_endpoints_count") or {}
    domains = throughput.get("companies_with_domain_count") or {}
    errors = metrics.get("error_breakdown") or {}
    plots = metrics.get("plots") or {}

    lines = [
        "# Efficiency Report",
        "",
        f"Generated at: `{metrics.get('generated_at')}`",
        f"Schema: `{metrics.get('schema_version')}`",
        f"Wrapped command: `{shlex.join(metrics.get('wrapped_command', {}).get('argv') or [])}`",
        f"Wrapped exit code: `{metrics.get('wrapped_command', {}).get('exit_code')}`",
        "",
        "## What this run proves",
        "",
        (
            f"- ATS endpoints changed from `{endpoints.get('start')}` to `{endpoints.get('end')}` "
            f"(delta `{endpoints.get('delta')}`), average `{endpoints.get('avg_rate_per_min')}` per minute."
        ),
        (
            f"- Companies with domain changed from `{domains.get('start')}` to `{domains.get('end')}` "
            f"(delta `{domains.get('delta')}`), average `{domains.get('avg_rate_per_min')}` per minute."
        ),
        (
            "- Concurrency justification: "
            f"{(metrics.get('concurrency_justification') or {}).get('summary') or 'No inference available.'}"
        ),
        (
            f"- Snapshot failures observed: `{errors.get('snapshot_failure_count')}`. "
            "This bounds confidence in interval-level rates."
        ),
        "",
        "## Chart interpretation",
        "",
        "- `counts_over_time.png`: demonstrates monotonic growth (or stalls) in DB-truth counters across the run.",
        "- `rates_per_minute.png`: shows interval throughput and burst/plateau behavior relevant to concurrency tuning.",
        "- `endpoints_by_type_delta.png`: shows which ATS integrations account for net endpoint growth.",
        "",
        "## Artifacts",
        "",
        "- `metrics.json`: full schema payload with all samples, deltas, and derived metrics.",
        "- `metrics.csv`: sample-level flat table for spreadsheet review.",
        "- `plots/`: chart images when plotting dependencies are available.",
        "",
        "## Reproduce",
        "",
        "```bash",
        shlex.join(metrics.get("reproduce_command") or []),
        "```",
        "",
    ]

    if plots.get("enabled"):
        lines.extend(["## Plot status", "", "- Plots generated:"])
        for rel in plots.get("generated") or []:
            lines.append(f"  - `{rel}`")
    else:
        lines.extend(
            [
                "## Plot status",
                "",
                "- Plots were skipped.",
                f"- Reason: `{plots.get('skipped_reason')}`",
            ]
        )

    lines.extend(["", f"Report directory: `{report_dir}`", ""])
    return "\n".join(lines)


def run_wrapped_command(
    command: Sequence[str],
    report_dir: Path,
    duration_seconds: Optional[int],
) -> Tuple[int, bool, str]:
    log_path = report_dir / "wrapped_command.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    timed_out = False
    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"$ {shlex.join(command)}\n")
        log_handle.flush()

        proc = subprocess.Popen(
            list(command),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

        start_mono = time.monotonic()
        deadline = (start_mono + max(0, duration_seconds)) if duration_seconds is not None else None

        while proc.poll() is None:
            if deadline is not None and time.monotonic() >= deadline:
                timed_out = True
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                break
            time.sleep(0.2)

        if proc.returncode is None:
            proc.wait(timeout=5)

        exit_code = int(proc.returncode or 0)

    return exit_code, timed_out, str(log_path)


def run_sampling_loop(
    repo_root: Path,
    mode: str,
    interval_seconds: int,
    command: Sequence[str],
    report_dir: Path,
    duration_seconds: Optional[int],
) -> Dict[str, Any]:
    samples: List[Dict[str, Any]] = []
    interval = max(1, interval_seconds)

    start_wall = utc_now()
    started_mono = time.monotonic()

    samples.append(
        sample_once(
            repo_root=repo_root,
            mode=mode,
            sample_index=0,
            started_mono=started_mono,
            now_mono=started_mono,
        )
    )

    log_path = report_dir / "wrapped_command.log"
    with log_path.open("w", encoding="utf-8") as log_handle:
        log_handle.write(f"$ {shlex.join(command)}\n")
        log_handle.flush()

        proc = subprocess.Popen(
            list(command),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )

        next_sample_at = started_mono + interval
        deadline = (started_mono + max(0, duration_seconds)) if duration_seconds is not None else None
        timed_out = False

        while proc.poll() is None:
            now_mono = time.monotonic()
            if deadline is not None and now_mono >= deadline:
                timed_out = True
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
                break

            if now_mono >= next_sample_at:
                samples.append(
                    sample_once(
                        repo_root=repo_root,
                        mode=mode,
                        sample_index=len(samples),
                        started_mono=started_mono,
                        now_mono=now_mono,
                    )
                )
                next_sample_at += interval
                continue

            sleep_for = min(0.25, max(0.01, next_sample_at - now_mono))
            if deadline is not None:
                sleep_for = min(sleep_for, max(0.01, deadline - now_mono))
            time.sleep(sleep_for)

        if proc.returncode is None:
            proc.wait(timeout=5)

    finish_mono = time.monotonic()
    samples.append(
        sample_once(
            repo_root=repo_root,
            mode=mode,
            sample_index=len(samples),
            started_mono=started_mono,
            now_mono=finish_mono,
        )
    )

    end_wall = utc_now()
    elapsed_seconds = max(0.0, finish_mono - started_mono)

    return {
        "started_at": utc_iso(start_wall),
        "finished_at": utc_iso(end_wall),
        "elapsed_seconds": round(elapsed_seconds, 3),
        "samples": samples,
        "wrapped_exit_code": int(proc.returncode or 0),
        "wrapped_timed_out": timed_out,
        "wrapped_log_path": str(log_path),
    }


def ensure_report_dir(base_out: Path) -> Path:
    stamp = utc_stamp()
    report_dir = base_out / stamp / "efficiency_report"
    report_dir.mkdir(parents=True, exist_ok=False)
    return report_dir


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a command and generate crawler efficiency metrics from DB snapshots."
    )
    parser.add_argument("--out-dir", default="out", help="Output root directory (default: out/)")
    parser.add_argument("--interval-seconds", type=int, default=30, help="Sampling interval in seconds")
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=None,
        help="Optional maximum run duration; process is terminated after this many seconds",
    )
    parser.add_argument(
        "--db-snapshot-mode",
        choices=["auto", "direct", "docker"],
        default="auto",
        help="Pass-through mode for db_snapshot.capture_db_snapshot",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Wrapped command to run. Use `--` before command.",
    )
    args = parser.parse_args(argv)

    command = list(args.command or [])
    if command and command[0] == "--":
        command = command[1:]
    args.command = command

    if not args.command:
        parser.error("missing wrapped command; provide it after `--`")

    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be > 0")

    return args


def build_report_payload(
    report_dir: Path,
    sampling_result: Dict[str, Any],
    args: argparse.Namespace,
    reproduce_command: Sequence[str],
) -> Dict[str, Any]:
    samples = sampling_result.get("samples") or []
    ok_samples = [sample for sample in samples if sample.get("snapshot_ok")]

    throughput = build_throughput_stats(samples)
    type_delta: Dict[str, int] = {}
    if len(ok_samples) >= 2:
        type_delta = endpoints_type_delta(ok_samples[0], ok_samples[-1])

    plots = generate_plots(report_dir, samples, type_delta)
    errors = build_error_breakdown(
        samples=samples,
        command_exit_code=parse_int(sampling_result.get("wrapped_exit_code")),
        command_timed_out=bool(sampling_result.get("wrapped_timed_out")),
    )
    concurrency = build_concurrency_justification(samples, throughput)

    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": utc_iso(),
        "report_dir": str(report_dir),
        "started_at": sampling_result.get("started_at"),
        "finished_at": sampling_result.get("finished_at"),
        "elapsed_seconds": sampling_result.get("elapsed_seconds"),
        "sampling": {
            "interval_seconds": args.interval_seconds,
            "db_snapshot_mode": args.db_snapshot_mode,
            "sample_count": len(samples),
            "sample_ok_count": len(ok_samples),
            "sample_failure_count": len(samples) - len(ok_samples),
        },
        "wrapped_command": {
            "argv": list(args.command),
            "exit_code": parse_int(sampling_result.get("wrapped_exit_code")),
            "timed_out": bool(sampling_result.get("wrapped_timed_out")),
            "duration_seconds_cap": args.duration_seconds,
            "log_path": sampling_result.get("wrapped_log_path"),
        },
        "throughput": throughput,
        "delta": {
            "endpoints_by_type": type_delta,
        },
        "error_breakdown": errors,
        "concurrency_justification": concurrency,
        "plots": plots,
        "time_series": samples,
        "reproduce_command": list(reproduce_command),
    }


def build_stable_summary(metrics: Dict[str, Any]) -> Dict[str, Any]:
    throughput = metrics.get("throughput") if isinstance(metrics.get("throughput"), dict) else {}
    sampling = metrics.get("sampling") if isinstance(metrics.get("sampling"), dict) else {}
    wrapped = metrics.get("wrapped_command") if isinstance(metrics.get("wrapped_command"), dict) else {}
    delta = metrics.get("delta") if isinstance(metrics.get("delta"), dict) else {}

    def throughput_block(key: str) -> Dict[str, Any]:
        block = throughput.get(key) if isinstance(throughput.get(key), dict) else {}
        return {
            "start": block.get("start"),
            "end": block.get("end"),
            "delta": block.get("delta"),
            "avg_rate_per_min": block.get("avg_rate_per_min"),
            "peak_rate_per_min": block.get("peak_rate_per_min"),
            "latest_rate_per_min": block.get("latest_rate_per_min"),
        }

    report_dir = Path(str(metrics.get("report_dir") or ""))
    source_metrics_path = str(report_dir / "metrics.json") if str(metrics.get("report_dir") or "") else None

    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": utc_iso(),
        "report_dir": str(metrics.get("report_dir") or ""),
        "source_metrics_path": source_metrics_path,
        "sampling": {
            "interval_seconds": sampling.get("interval_seconds"),
            "db_snapshot_mode": sampling.get("db_snapshot_mode"),
            "sample_count": sampling.get("sample_count"),
            "sample_ok_count": sampling.get("sample_ok_count"),
            "sample_failure_count": sampling.get("sample_failure_count"),
        },
        "throughput": {
            "ats_endpoints_count": throughput_block("ats_endpoints_count"),
            "companies_with_domain_count": throughput_block("companies_with_domain_count"),
            "companies_with_ats_endpoint_count": throughput_block("companies_with_ats_endpoint_count"),
        },
        "delta": {
            "endpoints_by_type": delta.get("endpoints_by_type") if isinstance(delta.get("endpoints_by_type"), dict) else {},
        },
        "wrapped_command": {
            "argv": wrapped.get("argv"),
            "exit_code": wrapped.get("exit_code"),
            "timed_out": wrapped.get("timed_out"),
            "duration_seconds_cap": wrapped.get("duration_seconds_cap"),
            "log_path": wrapped.get("log_path"),
        },
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    out_base = Path(args.out_dir)

    report_dir = ensure_report_dir(out_base)

    reproduce_command = [
        "python3",
        "scripts/generate_efficiency_report.py",
        "--out-dir",
        args.out_dir,
        "--interval-seconds",
        str(args.interval_seconds),
        "--db-snapshot-mode",
        args.db_snapshot_mode,
    ]
    if args.duration_seconds is not None:
        reproduce_command.extend(["--duration-seconds", str(args.duration_seconds)])
    reproduce_command.append("--")
    reproduce_command.extend(list(args.command))

    try:
        sampling_result = run_sampling_loop(
            repo_root=repo_root,
            mode=args.db_snapshot_mode,
            interval_seconds=args.interval_seconds,
            command=args.command,
            report_dir=report_dir,
            duration_seconds=args.duration_seconds,
        )
        metrics = build_report_payload(report_dir, sampling_result, args, reproduce_command)
    except Exception as exc:  # noqa: BLE001
        stub = {
            "schema_version": REPORT_SCHEMA_VERSION,
            "generated_at": utc_iso(),
            "report_dir": str(report_dir),
            "sampling": {
                "interval_seconds": args.interval_seconds,
                "db_snapshot_mode": args.db_snapshot_mode,
                "sample_count": 0,
                "sample_ok_count": 0,
                "sample_failure_count": 0,
            },
            "wrapped_command": {
                "argv": list(args.command),
                "exit_code": None,
                "timed_out": False,
                "duration_seconds_cap": args.duration_seconds,
                "log_path": str(report_dir / "wrapped_command.log"),
            },
            "throughput": {},
            "delta": {"endpoints_by_type": {}},
            "error_breakdown": {
                "snapshot_failure_count": 0,
                "snapshot_failure_messages": [],
                "snapshot_failure_modes": [],
                "wrapped_command_exit_code": None,
                "wrapped_command_timed_out": False,
            },
            "concurrency_justification": {
                "summary": "Report generation failed before sampling completed.",
                "evidence": {},
            },
            "plots": {
                "enabled": False,
                "generated": [],
                "skipped_reason": "Report generation failed",
            },
            "time_series": [],
            "reproduce_command": list(reproduce_command),
            "error": {
                "message": "Efficiency report generation failed",
                "detail": str(exc),
            },
        }
        write_json(report_dir / "metrics.json", stub)
        write_json(report_dir.parent / "efficiency_report.json", build_stable_summary(stub))

        with (report_dir / "metrics.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
            writer.writeheader()

        readme = "\n".join(
            [
                "# Efficiency Report",
                "",
                "Report generation failed before metrics could be computed.",
                "",
                f"Error: `{exc}`",
                "",
                "A stub `metrics.json` and `metrics.csv` were written for postmortem continuity.",
                "",
            ]
        )
        (report_dir / "README.md").write_text(readme, encoding="utf-8")
        print(json.dumps({"out_dir": str(report_dir), "status": "error", "error": str(exc)}, indent=2))
        return 2

    write_json(report_dir / "metrics.json", metrics)
    write_json(report_dir.parent / "efficiency_report.json", build_stable_summary(metrics))
    write_metrics_csv(report_dir / "metrics.csv", metrics.get("time_series") or [])
    (report_dir / "README.md").write_text(build_readme(report_dir, metrics), encoding="utf-8")

    print(
        json.dumps(
            {
                "out_dir": str(report_dir),
                "wrapped_exit_code": metrics.get("wrapped_command", {}).get("exit_code"),
                "timed_out": metrics.get("wrapped_command", {}).get("timed_out"),
                "sample_count": metrics.get("sampling", {}).get("sample_count"),
                "plot_count": len((metrics.get("plots") or {}).get("generated") or []),
            },
            indent=2,
        )
    )
    return parse_int(metrics.get("wrapped_command", {}).get("exit_code"), default=1)


if __name__ == "__main__":
    raise SystemExit(main())
