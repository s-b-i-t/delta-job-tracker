# Efficiency Metric Fidelity Audit (Domain Resolution + ATS Discovery)

Date: 2026-03-06 (repo reality audit against committed `master`-line state)

## Scope
- Domain resolution efficiency measurement fidelity
- ATS discovery efficiency measurement fidelity
- Related queue/fetch activity efficiency measurement fidelity

Out of scope:
- Implementing Team C blocker fixes
- Product feature behavior changes
- Workflow changes

## Repo Reality Notes
- Canonical reporting utility `scripts/generate_efficiency_report.py` is **not present** in this workspace snapshot.
- Metric/reporting coverage is currently distributed across:
  - `scripts/db_snapshot.py`
  - `scripts/run_e2e_pipeline_proof.py`
  - `scripts/run_ats_validation_canary.py`
  - `scripts/run_ats_scale_canary.py`
  - `scripts/run_soak_test.py`
- Local uncommitted drift in `scripts/run_soak_test.py` was treated as non-authoritative; committed-state content was audited via `git show HEAD:scripts/run_soak_test.py`.

## Commands Run
- `git stash push -m "temp: local run_soak_test drift non-authoritative for audit"`
- `git checkout -b teamb-efficiency-audit-20260306 origin/master`
- `rg --files scripts | rg "generate_efficiency_report|db_snapshot|run_e2e_pipeline_proof|run_ats_validation_canary|run_ats_scale_canary|run_soak_test"`
- `rg -n "companies_with_domain_count|ats_endpoints_count|attempts_per_minute|fetch_attempts_created|queued_total|error_bucket_distribution|stopReason|stop_reason|hard_timeout|request_budget|timeouts|delta_ok|db_truth" scripts/...`
- `rg -n "duration_ms|domain_resolution_rate|resolved_count|funnel|stage_failure_buckets|final_status|...|stopReason" scripts/run_ats_validation_canary.py`
- `rg -n "throughput|attempts_per_hour|endpoints_per_hour|domainsPerHour|endpointsPerHour|requestsPerHour|...|failure" docs/soak_test.md docs/e2e_pipeline_proof.md docs/ats_scale_canary.md`
- `sed -n` / `git show HEAD:...` inspections for:
  - `scripts/db_snapshot.py`
  - `scripts/run_e2e_pipeline_proof.py`
  - `scripts/run_ats_validation_canary.py`
  - `scripts/run_ats_scale_canary.py`
  - `scripts/run_soak_test.py`

## Source References (Exact)
- `scripts/db_snapshot.py`
  - Direct DB metrics: `build_metrics` (`ats_endpoints_count`, `companies_with_domain_count`, `companies_with_ats_endpoint_count`)
  - Delta model: `build_db_truth` (`delta_ok`, count deltas, endpoints-by-type delta)
- `scripts/run_e2e_pipeline_proof.py`
  - Fetch/run-scope counters: `snapshot_counts_csv` (includes `attempts_created_run_scope`, `queued_total`)
  - Summary rates: `fetch_stage.attempts_per_minute`
  - ATS endpoint run-scope creation: `ats_endpoints_created_run_scope`
  - Error buckets: `fetch_stage.error_bucket_distribution`
- `scripts/run_ats_validation_canary.py`
  - Domain before/after counts and net-new domains
  - Domain resolution rate (`resolved_count / companiesAttemptedCount`)
  - ATS funnel rates and stage failure buckets
  - Discovery final status payload (stop reason context in `final_status`)
- `scripts/run_ats_scale_canary.py`
  - Guardrails (`request_budget`, `hard_timeout_seconds`, cutoff metrics)
  - Stop reason (`stop_reason` fallback from run row / final status error)
  - Throughput estimates (`domainsPerHour`, `endpointsPerHour`, `requestsPerHour`)
- `scripts/run_soak_test.py`
  - Aggregated throughput (`attempts_per_hour`, `endpoints_per_hour`) and failure buckets
  - DB-truth linkage (`db_truth`) from snapshots

## Coverage Matrix
| metric/question | current source | direct / inferred / missing | trustworthy? yes/no | gap/risk |
|---|---|---|---|---|
| domains discovered per minute | `run_ats_validation_canary.py` (`companies_with_domain_count_before/after` + step durations); `run_ats_scale_canary.py` (`domainsPerHour`) | inferred | no | Not emitted as stable per-minute direct metric; denominator varies by script (stage duration vs run duration). |
| domains resolved per minute | `run_ats_validation_canary.py` (`resolved_count`, `domain_resolution_rate`, step duration metadata); `docs/efficiency_baseline_2026-03-06.md` inference pattern | inferred | no | Can be approximated, but no canonical explicit per-minute field and no shared denominator contract. |
| URLs queued per minute | `run_e2e_pipeline_proof.py` (`queued_total` snapshots pre/post fetch) | inferred | no | Queue values are point-in-time counts; no direct queue ingest/dequeue rate stream. Can mislead if queue oscillates intra-window. |
| URLs fetched per minute | `run_e2e_pipeline_proof.py` (`fetch_attempts_created`, `attempts_per_minute`) | inferred proxy | no | Uses attempts created, not successful fetch completions. “fetched/min” and “attempted/min” are blended today. |
| ATS endpoints discovered per minute | `run_ats_scale_canary.py` (`endpointsPerHour`), `run_e2e_pipeline_proof.py` (`ats_endpoints_created_run_scope`) | inferred | no | Per-hour only in scale canary; no canonical per-minute across modes; run-scope vs global DB deltas vary by script. |
| error-rate breakdown | `run_e2e_pipeline_proof.py` (`error_bucket_distribution` run-scope), `run_ats_validation_canary.py` (`stage_failure_buckets`, `countsByReason`) | partial direct + inferred | no | Counts exist, but true error **rate** denominators are inconsistent/missing (attempted URLs vs attempted companies vs processed rows). |
| timeout / guardrail stop reasons | `run_ats_scale_canary.py` (`stop_reason`, guardrails), `run_ats_validation_canary.py` (`final_status`), backend run status fields | partial direct | no | Stop reason semantics differ across scripts (null, lastError fallback, free-text categories). Not normalized for PM rollups. |

## Top Measurement Gaps
1. No single canonical payload emits phase-separated per-minute metrics across domain resolve, queue/fetch, and ATS discovery.
2. “Fetched per minute” is not measured directly; current proxy is attempts created.
3. Queue throughput is represented as count snapshots (`queued_total`) rather than direct enqueue/dequeue rates.
4. Error “rates” are not stable because denominators differ by script and stage.
5. Stop reason / guardrail semantics are not normalized; PM rollups can blend null/lastError/enum-like values incorrectly.
6. Run-scope deltas and global DB counts are both used, but not consistently labeled to prevent interpretation drift.

## Misleading-Rollup / False-Confidence Risks
- A run can show positive ATS endpoint delta while domain-resolution work quality is ambiguous (blended stage windows).
- `attempts_per_minute` may be read as fetch success throughput, even though it reflects attempts creation.
- Net domain growth (`companies_with_domain_count` delta) can appear healthy even when resolution attempts are sparse or heavily cached.
- Error bucket counts without denominator normalization can understate degradation when volume changes.
- Stop reason nulls and fallback last-error usage can hide guardrail-triggered early exits in aggregate PM views.

## Minimum Recommended Next Changes (Prioritized)
### P0 (required for trustworthiness)
1. Add a canonical reporting JSON payload (single schema) with explicit phase blocks:
   - `domain_resolution`
   - `queue_fetch`
   - `ats_discovery`
   Each block must emit: `work_count`, `duration_seconds`, `rate_per_min`.
2. In `run_e2e_pipeline_proof.py`, add direct queue/fetch counters per fetch cycle:
   - `urls_queued_delta`
   - `urls_fetch_succeeded_delta` (not just attempts)
   - `urls_fetch_attempted_delta`
3. In `run_ats_validation_canary.py`, emit explicit `domains_resolved_per_min` and `ats_endpoints_discovered_per_min` fields, derived with fixed denominator fields in same payload.
4. Enforce explicit “scope tags” in all reported metrics:
   - `run_scope`
   - `global_snapshot`
   to prevent blending confusion.

### P1 (stability and auditability)
5. Normalize stop reason taxonomy across scripts:
   - canonical enum-like bucket + optional raw detail
   - explicit guardrail flags (`hard_timeout_hit`, `request_budget_hit`, `host_cutoff_triggered`)
6. Add denominator-aligned error-rate fields:
   - `error_rate_per_attempt`
   - `error_rate_per_company_attempted`
   with counts + denominator included together.
7. Add tests (script-level) to lock metric semantics:
   - rate fields present and numerically coherent
   - denominator fields always present
   - scope tags always present
   - stop reason normalization mapping stable

## Final Verdict
- Current efficiency reporting for domain resolution: **PARTIAL**
- Current efficiency reporting for ATS discovery: **PARTIAL**
- Overall reporting quality for deployment decisions: **INSUFFICIENT**

Rationale:
- Useful signals exist, but core PM questions rely on inferred or blended metrics without a single canonical denominator/phase model. This is adequate for engineering diagnostics, not for deployment-confidence decisions.
