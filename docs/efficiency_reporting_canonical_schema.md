# Canonical Efficiency Reporting Schema

Status: v1 implemented in `scripts/run_e2e_pipeline_proof.py` as:
- `out/<ts>/e2e_pipeline_run/canonical_efficiency_report.json`
- `summary.json.canonical_efficiency_report`

## Goals
- Phase-separated reporting for:
  - `domain_resolution`
  - `queue_fetch`
  - `ats_discovery`
- Explicit denominator-aware rate fields (`rates_per_min`) and error rates (`error_rates`)
- Scope tags to prevent run-scope/global-snapshot ambiguity

## Schema (v1)
- `schema_version`: `canonical-efficiency-report-v1`
- `context`
  - `source`, `preset`, `run_root`
- `phases`
  - `domain_resolution`
    - `scope_tag`: `run_scope`
    - `duration_seconds`
    - `counters`: `companies_attempted`, `domains_resolved`
    - `rates_per_min`: `domains_resolved_per_min`
    - `error_rates`: `unresolved_per_attempt`
  - `queue_fetch`
    - `scope_tag`: `run_scope`
    - `duration_seconds`
    - `counters`:
      - `urls_fetch_attempted`
      - `urls_fetch_succeeded`
      - `urls_queued_before`
      - `urls_queued_after`
      - `urls_queued_net_delta`
    - `rates_per_min`:
      - `urls_fetch_attempted_per_min`
      - `urls_fetch_succeeded_per_min`
      - `urls_queued_net_delta_per_min`
    - `error_breakdown`:
      - `bucket_counts`
      - `total_errors`
      - `error_rate_per_attempt`
  - `ats_discovery`
    - `scope_tag`: `run_scope`
    - `duration_seconds`
    - `counters`:
      - `companies_attempted`
      - `companies_succeeded`
      - `companies_failed`
      - `endpoints_extracted`
      - `endpoints_created_run_scope`
    - `rates_per_min`:
      - `endpoints_extracted_per_min`
      - `endpoints_created_per_min`
    - `error_rates`: `failed_per_attempted`
    - `stop_reason`: `raw`, `normalized`
- `global_snapshots`
  - `scope_tag`: `global_snapshot`
  - `companies_with_domain_count_before`
  - `companies_with_domain_count_after`
  - `companies_with_domain_count_delta`

## Metric Semantics
- `*_per_min` rates are computed as `(count * 60) / duration_seconds` for the same phase.
- `error_rate_per_attempt` uses phase-local attempt denominator.
- `failed_per_attempted` uses ATS-discovery-local company-attempt denominator.
- `NONE` or null rates indicate denominator is unavailable/zero.

## Stop Reason Normalization (v1)
- Normalized buckets:
  - `REQUEST_BUDGET_EXCEEDED`
  - `TIMEOUT`
  - `HOST_FAILURE_CUTOFF`
  - `CANCELLED`
  - `ABORTED`
  - `FAILED`
  - `COMPLETED`
  - `OTHER`
  - `NONE`

## Compatibility Notes
- Existing summary fields are retained; canonical report is additive.
- Dry-run/failure paths still emit canonical report with available/zeroed phase values to keep schema stable.
