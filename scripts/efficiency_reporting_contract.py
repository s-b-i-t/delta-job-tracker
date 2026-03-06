#!/usr/bin/env python3
"""Shared helpers for canonical/compatible efficiency reporting payloads."""

from __future__ import annotations

from typing import Any, Dict, Optional


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def rate_per_min(count: int, duration_seconds: float) -> Optional[float]:
    if duration_seconds <= 0:
        return None
    return round((count * 60.0) / duration_seconds, 4)


def ratio(numerator: int, denominator: int) -> Optional[float]:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def normalize_stop_reason(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return "NONE"
    if "request_budget" in text or "budget_exceeded" in text:
        return "REQUEST_BUDGET_EXCEEDED"
    if "hard_timeout" in text or "timeout" in text:
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


def build_payload(
    *,
    source: str,
    context: Dict[str, Any],
    phases: Dict[str, Any],
    global_snapshots: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "schema_version": "canonical-efficiency-report-v1",
        "source": source,
        "context": context,
        "phases": phases,
    }
    if global_snapshots is not None:
        payload["global_snapshots"] = global_snapshots
    return payload

