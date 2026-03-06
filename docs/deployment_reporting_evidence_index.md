# Deployment Reporting Evidence Index

This index maps deployment-facing PM questions to canonical artifact fields with scope and trust semantics.

## Canonical Artifacts

- `out/<timestamp>/e2e_pipeline_run/canonical_efficiency_report.json` from `scripts/run_e2e_pipeline_proof.py`
- `out/<timestamp>/canonical_efficiency_report.json` from `scripts/run_ats_validation_canary.py`
- `out/<timestamp>/canonical_efficiency_report.json` from `scripts/run_ats_scale_canary.py`

Schema:
- `schema_version`: `canonical-efficiency-report-v1`
- `phases`: `domain_resolution`, `queue_fetch`, `ats_discovery`
- Scope tags:
  - `run_scope`: emitted counter maps only to work initiated by this run.
  - `global_snapshot`: emitted counter is a global DB snapshot delta across the run window.

Rate semantics:
- `rates_per_min.*` values always use that phase's `rate_denominator_seconds`.
- If a phase does not have a stable denominator, rates are emitted as `null`.

## PM Question Index

| PM question | Canonical field path(s) | Surface / script | Direct vs inferred | Trustworthy |
|---|---|---|---|---|
| Domains discovered per minute | `phases.domain_resolution.rates_per_min.domains_discovered_per_min` | e2e, validation canary | direct | yes |
| Domains resolved per minute | `phases.domain_resolution.rates_per_min.domains_resolved_per_min` | e2e, validation canary | direct | yes |
| URLs queued per minute | `phases.queue_fetch.rates_per_min.urls_enqueued_per_min` (scale/e2e), `phases.queue_fetch.rates_per_min.crawl_urls_queued_delta_per_min` (validation) | e2e, scale canary, validation canary | direct on scale/e2e, inferred-from-global-delta on validation | partial |
| URLs fetched per minute | `phases.queue_fetch.rates_per_min.urls_fetched_per_min` (scale), `phases.queue_fetch.rates_per_min.urls_fetch_succeeded_per_min` (e2e) | e2e, scale canary | direct | yes |
| ATS endpoints discovered per minute | `phases.ats_discovery.rates_per_min.endpoints_extracted_per_min` (validation), `phases.ats_discovery.rates_per_min.endpoints_created_per_min` (e2e/scale) | e2e, validation canary, scale canary | direct | yes |
| Error-rate breakdown | `phases.*.error_rates.*`, `phases.*.error_breakdown.*` | e2e, validation canary, scale canary | direct | yes |
| Timeout/guardrail stop reasons | `phases.ats_discovery.stop_reason.raw`, `phases.ats_discovery.stop_reason.normalized` | validation canary, scale canary | direct | yes |

## Known Remaining Limits

- Validation canary queue/fetch is measured as `global_snapshot` deltas because that surface does not own frontier run identifiers for strict run-scoped fetch attribution.
- Field naming remains endpoint-*specific* (`endpoints_created` vs `endpoints_extracted`) by surface, but both are normalized as ATS discovery phase counters with explicit source script in payload context.
