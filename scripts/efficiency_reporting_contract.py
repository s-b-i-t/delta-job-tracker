#!/usr/bin/env python3
"""Shared canonical efficiency reporting helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

SCHEMA_VERSION = "canonical-efficiency-report-v1"
STOP_REASON_UNKNOWN = "UNKNOWN"


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def rate_per_min(count: int, duration_seconds: float | None) -> float | None:
    if duration_seconds is None or duration_seconds <= 0:
        return None
    return round((count * 60.0) / duration_seconds, 4)


def ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def normalize_stop_reason(stop_reason: Any) -> str:
    if stop_reason is None:
        return STOP_REASON_UNKNOWN
    reason = str(stop_reason).strip()
    if not reason:
        return STOP_REASON_UNKNOWN
    upper = reason.upper()
    if any(token in upper for token in ("TIMEOUT", "TIMED OUT", "DEADLINE")):
        return "TIMEOUT"
    if any(token in upper for token in ("BUDGET", "REQUEST_LIMIT", "REQUEST LIMIT", "LIMIT_EXCEEDED")):
        return "REQUEST_BUDGET_EXCEEDED"
    if any(token in upper for token in ("HOST_FAILURE", "HOST_FAIL", "HOST_GUARDRAIL")):
        return "HOST_FAILURE_GUARDRAIL"
    if any(token in upper for token in ("CANCEL", "ABORT")):
        return "CANCELLED"
    return "OTHER"


def build_payload(
    *,
    source: str,
    context: Dict[str, Any],
    phases: Dict[str, Any],
    global_snapshots: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "context": context,
        "phases": phases,
    }
    if global_snapshots is not None:
        payload["global_snapshots"] = global_snapshots
    return payload
