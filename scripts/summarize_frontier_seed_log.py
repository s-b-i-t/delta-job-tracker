#!/usr/bin/env python3
"""Summarize frontier seed observability logs for a single runId."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

EVENT_PREFIXES = (
    "frontier.seed.start",
    "frontier.seed.findSeedDomains",
    "frontier.seed.domain.start",
    "frontier.seed.domain.ensureHost",
    "frontier.seed.domain.robots.begin",
    "frontier.seed.domain.robots.finish",
    "frontier.seed.domain.finish",
    "frontier.seed.scheduler.begin",
    "frontier.seed.scheduler.start",
    "frontier.seed.scheduler.fetch.begin",
    "frontier.seed.scheduler.robots.begin",
    "frontier.seed.scheduler.robots.finish",
    "frontier.seed.scheduler.http.begin",
    "frontier.seed.scheduler.http.finish",
    "frontier.seed.scheduler.fetch.finish",
    "frontier.seed.scheduler.idle",
    "frontier.seed.scheduler.summary",
    "frontier.seed.summary",
)

KEY_VALUE_RE = re.compile(r"([A-Za-z][A-Za-z0-9]*)=([^ ]+)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize frontier seed timing logs for a single runId"
    )
    parser.add_argument("log_path", type=Path)
    parser.add_argument("--run-id", default=None)
    return parser.parse_args(argv)


def convert_value(raw: str) -> Any:
    if raw == "null":
        return None
    if raw == "true":
        return True
    if raw == "false":
        return False
    try:
        if "." in raw:
            return float(raw)
        return int(raw)
    except ValueError:
        return raw


def parse_line(line: str) -> tuple[str, dict[str, Any]] | None:
    for event in EVENT_PREFIXES:
        marker = f"{event} "
        if marker not in line:
            continue
        payload = {key: convert_value(value) for key, value in KEY_VALUE_RE.findall(line)}
        run_id = payload.get("runId")
        if not isinstance(run_id, str) or not run_id:
            return None
        return event, payload
    return None


def summarize_runs(lines: list[str]) -> dict[str, dict[str, Any]]:
    runs: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for line in lines:
        parsed = parse_line(line)
        if parsed is None:
            continue
        event, payload = parsed
        run_id = payload["runId"]
        if run_id not in runs:
            runs[run_id] = {"runId": run_id, "events": {}, "eventSequence": []}
            order.append(run_id)
        runs[run_id]["events"][event] = payload
        runs[run_id]["eventSequence"].append(event)

    for run_id in order:
        run = runs[run_id]
        events = run["events"]
        run["seed"] = events.get("frontier.seed.summary")
        run["scheduler"] = events.get("frontier.seed.scheduler.summary")
        run["seedStart"] = events.get("frontier.seed.start")
        run["schedulerStart"] = events.get("frontier.seed.scheduler.start")
    return runs


def select_run(
    runs: dict[str, dict[str, Any]], requested_run_id: str | None
) -> dict[str, Any] | None:
    if requested_run_id:
        return runs.get(requested_run_id)
    if not runs:
        return None
    return next(reversed(runs.values()))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if not args.log_path.exists():
        print(f"log file not found: {args.log_path}", file=sys.stderr)
        return 1

    lines = args.log_path.read_text(encoding="utf-8").splitlines()
    runs = summarize_runs(lines)
    selected = select_run(runs, args.run_id)
    if selected is None:
        if args.run_id:
            print(f"runId not found: {args.run_id}", file=sys.stderr)
        else:
            print("no frontier seed runs found", file=sys.stderr)
        return 1

    print(json.dumps(selected, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
