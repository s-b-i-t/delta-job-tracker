#!/usr/bin/env python3
"""Generate crawler efficiency proof artifacts by sampling DB-truth during a wrapped command run."""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import db_snapshot as dbsnap

SCHEMA_VERSION = "efficiency-report-v1"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(ts: datetime | None = None) -> str:
    value = ts or utc_now()
    return value.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ts_dir_name(ts: datetime | None = None) -> str:
    value = ts or utc_now()
    return value.strftime("%Y%m%dT%H%M%SZ")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def write_samples_jsonl(path: Path, samples: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for sample in samples:
            f.write(json.dumps(sample, sort_keys=True) + "\n")


def metric_int(metrics: dict[str, Any], key: str) -> int | None:
    raw = metrics.get(key)
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


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


def capture_snapshot_safe(repo_root: Path, mode: str, stage: str) -> dict[str, Any]:
    try:
        snapshot = dbsnap.capture_db_snapshot(repo_root, mode=mode)
        if not isinstance(snapshot, dict):
            raise RuntimeError("capture_db_snapshot returned non-dict payload")
        return snapshot
    except Exception as exc:  # noqa: BLE001
        return snapshot_error_payload(mode=mode, stage=stage, exc=exc)


def sample_row(index: int, started_monotonic: float, snapshot: dict[str, Any]) -> dict[str, Any]:
    metrics = snapshot.get("metrics") if isinstance(snapshot.get("metrics"), dict) else {}
    error_obj = snapshot.get("error") if isinstance(snapshot.get("error"), dict) else None
    endpoint_type_map = metrics.get("endpoints_by_type_map")
    if not isinstance(endpoint_type_map, dict):
        endpoint_type_map = {}

    return {
        "sample_index": index,
        "captured_at": snapshot.get("captured_at") or utc_iso(),
        "elapsed_seconds": round(max(0.0, time.monotonic() - started_monotonic), 2),
        "ok": bool(snapshot.get("ok")),
        "mode_requested": snapshot.get("mode_requested"),
        "mode_used": snapshot.get("mode_used"),
        "metrics": {
            "ats_endpoints_count": metric_int(metrics, "ats_endpoints_count"),
            "companies_with_ats_endpoint_count": metric_int(metrics, "companies_with_ats_endpoint_count"),
            "companies_with_domain_count": metric_int(metrics, "companies_with_domain_count"),
            "endpoints_by_type_map": endpoint_type_map,
        },
        "error": error_obj,
        "snapshot": snapshot,
    }


def rate_per_min(prev_val: int | None, curr_val: int | None, delta_seconds: float) -> float | None:
    if prev_val is None or curr_val is None or delta_seconds <= 0:
        return None
    return (curr_val - prev_val) / (delta_seconds / 60.0)


def build_endpoints_by_type_delta(
    start_map: dict[str, int] | None,
    end_map: dict[str, int] | None,
) -> dict[str, int]:
    s = start_map or {}
    e = end_map or {}
    keys = sorted(set(s.keys()) | set(e.keys()))
    return {k: int(e.get(k, 0)) - int(s.get(k, 0)) for k in keys}


def build_metrics(samples: list[dict[str, Any]]) -> dict[str, Any]:
    successful = [s for s in samples if s.get("ok")]

    for idx, sample in enumerate(samples):
        prev_ok = None
        for back in range(idx - 1, -1, -1):
            if samples[back].get("ok"):
                prev_ok = samples[back]
                break

        if prev_ok is not None and sample.get("ok"):
            delta_seconds = float(sample.get("elapsed_seconds", 0.0)) - float(prev_ok.get("elapsed_seconds", 0.0))
            sample["rates_per_min_from_prev"] = {
                "ats_endpoints_count": rate_per_min(
                    prev_ok["metrics"].get("ats_endpoints_count"),
                    sample["metrics"].get("ats_endpoints_count"),
                    delta_seconds,
                ),
                "companies_with_domain_count": rate_per_min(
                    prev_ok["metrics"].get("companies_with_domain_count"),
                    sample["metrics"].get("companies_with_domain_count"),
                    delta_seconds,
                ),
            }
        else:
            sample["rates_per_min_from_prev"] = {
                "ats_endpoints_count": None,
                "companies_with_domain_count": None,
            }

    summary: dict[str, Any] = {
        "sample_count": len(samples),
        "sample_success_count": len(successful),
        "sample_failure_count": len(samples) - len(successful),
        "start_metrics": None,
        "end_metrics": None,
        "delta": None,
        "avg_rates_per_min": None,
    }

    if len(successful) >= 1:
        first = successful[0]
        last = successful[-1]
        start_metrics = first.get("metrics") or {}
        end_metrics = last.get("metrics") or {}
        summary["start_metrics"] = start_metrics
        summary["end_metrics"] = end_metrics

        delta: dict[str, Any] = {
            "ats_endpoints_count": None,
            "companies_with_ats_endpoint_count": None,
            "companies_with_domain_count": None,
            "endpoints_by_type": build_endpoints_by_type_delta(
                start_metrics.get("endpoints_by_type_map"),
                end_metrics.get("endpoints_by_type_map"),
            ),
        }

        if start_metrics.get("ats_endpoints_count") is not None and end_metrics.get("ats_endpoints_count") is not None:
            delta["ats_endpoints_count"] = int(end_metrics["ats_endpoints_count"]) - int(start_metrics["ats_endpoints_count"])
        if (
            start_metrics.get("companies_with_ats_endpoint_count") is not None
            and end_metrics.get("companies_with_ats_endpoint_count") is not None
        ):
            delta["companies_with_ats_endpoint_count"] = int(end_metrics["companies_with_ats_endpoint_count"]) - int(
                start_metrics["companies_with_ats_endpoint_count"]
            )
        if (
            start_metrics.get("companies_with_domain_count") is not None
            and end_metrics.get("companies_with_domain_count") is not None
        ):
            delta["companies_with_domain_count"] = int(end_metrics["companies_with_domain_count"]) - int(
                start_metrics["companies_with_domain_count"]
            )

        summary["delta"] = delta

        elapsed = float(last.get("elapsed_seconds", 0.0)) - float(first.get("elapsed_seconds", 0.0))
        if elapsed > 0:
            summary["avg_rates_per_min"] = {
                "ats_endpoints_count": rate_per_min(
                    start_metrics.get("ats_endpoints_count"),
                    end_metrics.get("ats_endpoints_count"),
                    elapsed,
                ),
                "companies_with_domain_count": rate_per_min(
                    start_metrics.get("companies_with_domain_count"),
                    end_metrics.get("companies_with_domain_count"),
                    elapsed,
                ),
            }

    return summary


def write_metrics_csv(path: Path, samples: list[dict[str, Any]], first_ok: dict[str, Any] | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "sample_index",
        "captured_at",
        "elapsed_seconds",
        "ok",
        "mode_used",
        "ats_endpoints_count",
        "companies_with_ats_endpoint_count",
        "companies_with_domain_count",
        "ats_endpoints_delta_from_first",
        "companies_with_domain_delta_from_first",
        "ats_endpoints_rate_per_min",
        "companies_with_domain_rate_per_min",
        "endpoints_by_type_map_json",
        "error_message",
    ]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()

        for sample in samples:
            metrics = sample.get("metrics") or {}
            rates = sample.get("rates_per_min_from_prev") or {}
            delta_ep = None
            delta_domains = None
            if first_ok is not None and sample.get("ok"):
                first_metrics = first_ok.get("metrics") or {}
                if first_metrics.get("ats_endpoints_count") is not None and metrics.get("ats_endpoints_count") is not None:
                    delta_ep = int(metrics["ats_endpoints_count"]) - int(first_metrics["ats_endpoints_count"])
                if (
                    first_metrics.get("companies_with_domain_count") is not None
                    and metrics.get("companies_with_domain_count") is not None
                ):
                    delta_domains = int(metrics["companies_with_domain_count"]) - int(first_metrics["companies_with_domain_count"])

            error_message = None
            if isinstance(sample.get("error"), dict):
                error_message = sample["error"].get("message")

            writer.writerow(
                {
                    "sample_index": sample.get("sample_index"),
                    "captured_at": sample.get("captured_at"),
                    "elapsed_seconds": sample.get("elapsed_seconds"),
                    "ok": sample.get("ok"),
                    "mode_used": sample.get("mode_used"),
                    "ats_endpoints_count": metrics.get("ats_endpoints_count"),
                    "companies_with_ats_endpoint_count": metrics.get("companies_with_ats_endpoint_count"),
                    "companies_with_domain_count": metrics.get("companies_with_domain_count"),
                    "ats_endpoints_delta_from_first": delta_ep,
                    "companies_with_domain_delta_from_first": delta_domains,
                    "ats_endpoints_rate_per_min": rates.get("ats_endpoints_count"),
                    "companies_with_domain_rate_per_min": rates.get("companies_with_domain_count"),
                    "endpoints_by_type_map_json": json.dumps(metrics.get("endpoints_by_type_map") or {}, sort_keys=True),
                    "error_message": error_message,
                }
            )


def maybe_generate_plots(report_dir: Path, samples: list[dict[str, Any]]) -> dict[str, Any]:
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "reason": f"matplotlib unavailable: {type(exc).__name__}: {exc}",
            "files": [],
        }

    ok_samples = [s for s in samples if s.get("ok")]
    if not ok_samples:
        return {
            "ok": False,
            "reason": "no successful samples available for plotting",
            "files": [],
        }

    minutes = [float(s.get("elapsed_seconds", 0.0)) / 60.0 for s in ok_samples]
    ats = [s["metrics"].get("ats_endpoints_count") for s in ok_samples]
    domains = [s["metrics"].get("companies_with_domain_count") for s in ok_samples]

    plot_files: list[str] = []

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(minutes, ats, marker="o")
    ax.set_title("ATS Endpoints Over Time")
    ax.set_xlabel("Elapsed minutes")
    ax.set_ylabel("ats_endpoints_count")
    ax.grid(True, alpha=0.3)
    p1 = report_dir / "ats_endpoints_over_time.png"
    fig.tight_layout()
    fig.savefig(p1)
    plt.close(fig)
    plot_files.append(str(p1))

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(minutes, domains, marker="o")
    ax.set_title("Companies With Domain Over Time")
    ax.set_xlabel("Elapsed minutes")
    ax.set_ylabel("companies_with_domain_count")
    ax.grid(True, alpha=0.3)
    p2 = report_dir / "companies_with_domain_over_time.png"
    fig.tight_layout()
    fig.savefig(p2)
    plt.close(fig)
    plot_files.append(str(p2))

    return {"ok": True, "reason": None, "files": plot_files}


def write_readme(
    path: Path,
    command: list[str],
    args: argparse.Namespace,
    metrics: dict[str, Any],
    plots: dict[str, Any],
    files_written: dict[str, str],
) -> None:
    summary = metrics.get("summary") or {}
    lines = [
        "# Efficiency Report",
        "",
        "## Run",
        "",
        f"- Wrapped command: `{ ' '.join(command) }`",
        f"- Sampling interval seconds: `{args.interval_seconds}`",
        f"- Duration cap seconds: `{args.duration_seconds}`",
        f"- DB snapshot mode: `{args.db_snapshot_mode}`",
        "",
        "## What This Proves",
        "",
        "- Throughput trend for `ats_endpoints_count` over time.",
        "- Growth trend for `companies_with_domain_count` over time.",
        "- Delta by ATS endpoint type across the sampled run window.",
        "- Snapshot reliability (success/failure counts) for DB-truth sampling.",
        "",
        "## Key Summary",
        "",
        f"- Sample count: `{summary.get('sample_count')}`",
        f"- Successful samples: `{summary.get('sample_success_count')}`",
        f"- Failed samples: `{summary.get('sample_failure_count')}`",
        f"- Delta (ats_endpoints_count): `{((summary.get('delta') or {}).get('ats_endpoints_count'))}`",
        f"- Delta (companies_with_domain_count): `{((summary.get('delta') or {}).get('companies_with_domain_count'))}`",
        f"- Delta (endpoints_by_type): `{((summary.get('delta') or {}).get('endpoints_by_type'))}`",
        f"- Avg rate/min (ats_endpoints_count): `{((summary.get('avg_rates_per_min') or {}).get('ats_endpoints_count'))}`",
        f"- Avg rate/min (companies_with_domain_count): `{((summary.get('avg_rates_per_min') or {}).get('companies_with_domain_count'))}`",
        "",
        "## Files",
        "",
        f"- metrics.json: `{files_written.get('metrics_json')}`",
        f"- metrics.csv: `{files_written.get('metrics_csv')}`",
        f"- samples.jsonl: `{files_written.get('samples_jsonl')}`",
        f"- wrapped stdout log: `{files_written.get('stdout_log')}`",
        f"- wrapped stderr log: `{files_written.get('stderr_log')}`",
    ]

    if plots.get("ok"):
        lines.extend(["", "## Plots", ""])
        for plot in plots.get("files") or []:
            lines.append(f"- `{plot}`")
    else:
        lines.extend(
            [
                "",
                "## Plots",
                "",
                f"Plots skipped: `{plots.get('reason')}`",
            ]
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    if args.interval_seconds <= 0:
        print("--interval-seconds must be > 0", file=sys.stderr)
        return 2

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        print("Missing wrapped command. Usage: ... -- <command>", file=sys.stderr)
        return 2

    started_at = utc_now()
    report_dir = Path(args.out_dir) / ts_dir_name(started_at) / "efficiency_report"
    report_dir.mkdir(parents=True, exist_ok=True)

    repo_root = Path(__file__).resolve().parents[1]
    stdout_log_path = report_dir / "wrapped_command.stdout.log"
    stderr_log_path = report_dir / "wrapped_command.stderr.log"

    samples: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    command_timeout_reached = False

    started_monotonic = time.monotonic()
    next_sample_due = started_monotonic

    with stdout_log_path.open("w", encoding="utf-8") as stdout_f, stderr_log_path.open("w", encoding="utf-8") as stderr_f:
        proc = subprocess.Popen(command, stdout=stdout_f, stderr=stderr_f, text=True)  # noqa: S603

        sample_idx = 0
        while True:
            now = time.monotonic()
            if now >= next_sample_due:
                snap = capture_snapshot_safe(repo_root, args.db_snapshot_mode, stage=f"sample_{sample_idx}")
                sample = sample_row(sample_idx, started_monotonic, snap)
                samples.append(sample)
                sample_idx += 1
                next_sample_due = now + args.interval_seconds

            rc = proc.poll()
            if rc is not None:
                break

            if args.duration_seconds is not None and (now - started_monotonic) >= args.duration_seconds:
                command_timeout_reached = True
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                break

            time.sleep(min(1.0, max(0.1, args.interval_seconds / 5.0)))

        if not samples or samples[-1].get("elapsed_seconds") is None or (
            float(samples[-1]["elapsed_seconds"]) < round(max(0.0, time.monotonic() - started_monotonic), 2)
        ):
            snap = capture_snapshot_safe(repo_root, args.db_snapshot_mode, stage="final")
            samples.append(sample_row(sample_idx, started_monotonic, snap))

        exit_code = proc.returncode
        if exit_code is None:
            exit_code = proc.wait(timeout=5)

    write_samples_jsonl(report_dir / "samples.jsonl", samples)

    summary = build_metrics(samples)
    first_ok = next((s for s in samples if s.get("ok")), None)

    for s in samples:
        if not s.get("ok") and isinstance(s.get("error"), dict):
            errors.append(
                {
                    "sample_index": s.get("sample_index"),
                    "captured_at": s.get("captured_at"),
                    "error": s.get("error"),
                }
            )

    plots = maybe_generate_plots(report_dir, samples)

    metrics_payload = {
        "schema_version": SCHEMA_VERSION,
        "started_at": utc_iso(started_at),
        "ended_at": utc_iso(),
        "elapsed_seconds": round((utc_now() - started_at).total_seconds(), 2),
        "report_dir": str(report_dir),
        "command": {
            "argv": command,
            "exit_code": int(exit_code),
            "duration_capped": bool(command_timeout_reached),
        },
        "sampling": {
            "interval_seconds": args.interval_seconds,
            "duration_seconds": args.duration_seconds,
            "db_snapshot_mode": args.db_snapshot_mode,
        },
        "summary": summary,
        "samples": samples,
        "errors": errors,
        "plots": plots,
        "files": {
            "samples_jsonl": str(report_dir / "samples.jsonl"),
            "metrics_json": str(report_dir / "metrics.json"),
            "metrics_csv": str(report_dir / "metrics.csv"),
            "readme": str(report_dir / "README.md"),
            "stdout_log": str(stdout_log_path),
            "stderr_log": str(stderr_log_path),
        },
    }

    write_json(report_dir / "metrics.json", metrics_payload)
    write_metrics_csv(report_dir / "metrics.csv", samples, first_ok)
    write_readme(
        report_dir / "README.md",
        command=command,
        args=args,
        metrics=metrics_payload,
        plots=plots,
        files_written=metrics_payload["files"],
    )

    print(json.dumps({"report_dir": str(report_dir), "exit_code": int(exit_code)}, indent=2))
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate efficiency report by sampling DB snapshots while running a wrapped command."
    )
    p.add_argument("--out-dir", default=os.getenv("DELTA_OUT_DIR", "out"))
    p.add_argument("--interval-seconds", type=int, default=30)
    p.add_argument("--duration-seconds", type=int, default=None)
    p.add_argument("--db-snapshot-mode", choices=["auto", "direct", "docker"], default="auto")
    p.add_argument("command", nargs=argparse.REMAINDER)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
