# Reporting Convergence Contract

This document defines canonical/compatible deployment-facing reporting semantics
for script-layer benchmark surfaces.

Implemented surfaces:
- `scripts/run_ats_validation_canary.py`
  - emits `canonical_efficiency_report.json`
  - embeds payload at `canary_summary.json.canonical_efficiency_report`
- `scripts/run_ats_scale_canary.py`
  - emits `canonical_efficiency_report.json`
  - embeds payload at `ats_scale_metrics.json.canonical_efficiency_report`

Shared contract helper:
- `scripts/efficiency_reporting_contract.py`
  - stop reason normalization
  - denominator-safe rates
  - common payload envelope

## Converged semantics
- `schema_version`: `canonical-efficiency-report-v1`
- `source`: script name
- `context`: includes `base_url`, `out_dir`, `scope_tags`
- `phases`:
  - `domain_resolution`
  - `queue_fetch`
  - `ats_discovery`
- per phase:
  - `scope_tag`
  - `duration_seconds` / `rate_denominator_seconds` when available
  - `counters`
  - `rates_per_min`
  - `error_rates` and/or `error_breakdown` when available
  - `stop_reason` (`raw`, `normalized`) for discovery phases

## Denominator meanings
- `*_per_min` is always based on that phase's `rate_denominator_seconds`.
- A `None` rate means denominator is unavailable/zero for that surface.
- `error_rate_per_attempt` and similar ratios must include an explicit denominator from the same phase.

## Stop reason normalization
Normalized buckets:
- `REQUEST_BUDGET_EXCEEDED`
- `TIMEOUT`
- `HOST_FAILURE_CUTOFF`
- `CANCELLED`
- `ABORTED`
- `FAILED`
- `COMPLETED`
- `OTHER`
- `NONE`

## First-class domain discovery metric
- `run_ats_validation_canary.py` now emits:
  - `phases.domain_resolution.counters.domains_discovered_net_new_global`
  - `phases.domain_resolution.rates_per_min.domains_discovered_per_min`

## Compatibility note
- Surfaces may report `availability: not_collected_in_this_surface` for phases not directly measured there.
