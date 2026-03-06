# Efficiency Baseline Report (2026-03-06)

Branch baseline: `master` (artifact-backed measurement pass).

## Scope and method
This is a validation/evidence report, not a product-change report.

- Measured directly from generated artifacts:
  - domain resolution counts/durations
  - ATS discovery counts/funnel/failures
  - fetch attempts and queue deltas
  - DB-sampled throughput from efficiency report tooling
- Inferred (explicitly marked):
  - per-minute rates derived from step durations or wall-clock durations
  - bottleneck classification from stage timing + failure buckets

## Commands run
Executed from repo root unless noted.

```bash
git switch master
git pull --ff-only origin master
git switch -c teama-efficiency-baseline-20260306

# Environment checks and attempted runtime campaign
cd backend && ./gradlew bootRun
which docker
systemctl status postgresql
pg_isready
psql --version

# Bounded scale canary attempt on current environment
python3 scripts/run_ats_scale_canary.py

# Artifact evidence extraction
jq '{elapsed_seconds, go_no_go, fetch_stage, ats_stage}' out/20260306T031223Z/e2e_pipeline_run/summary.json
jq '{domain_resolution, ats_vendor_probe, durations_ms}' out/20260306T031524Z/canary_summary.json
cat out/20260306T031524Z/domain_counts_before.json
cat out/20260306T031524Z/domain_counts_after.json
sed -n '1,80p' out/20260306T031221Z/efficiency_report/metrics.json
sed -n '1,80p' out/20260306T031522Z/efficiency_report/metrics.json
cat out/20260306T170654Z/preflight_api_status.json
cat out/20260306T170654Z/ats_scale_metrics.json
```

## Artifact paths used
- E2E proof run (current master):
  - `out/20260306T031223Z/e2e_pipeline_run/summary.json`
- ATS validation canary run (current master):
  - `out/20260306T031524Z/canary_summary.json`
  - `out/20260306T031524Z/domain_counts_before.json`
  - `out/20260306T031524Z/domain_counts_after.json`
- Efficiency time-series wrappers:
  - `out/20260306T031221Z/efficiency_report/metrics.json`
  - `out/20260306T031522Z/efficiency_report/metrics.json`
- Fresh local attempt showing environment preflight failure:
  - `out/20260306T170654Z/preflight_api_status.json`
  - `out/20260306T170654Z/ats_scale_metrics.json`

## Environment reality observed
- `backend ./gradlew bootRun` failed due to DB connection refused.
- `docker` binary unavailable in this runtime context.
- `psql` tooling unavailable in this runtime context.
- Because of that, this baseline is based on real current-master artifacts already present in `out/` plus one fresh failed preflight artifact documenting runtime constraints.

## Metric summary (baseline)
| Metric | Value | Direct vs inferred | Source |
|---|---:|---|---|
| Domains resolved (canary) | 37 | Direct | `canary_summary.json` |
| Domain resolution stage duration | 139.851s | Direct | `canary_summary.json` |
| Domains discovered delta (companies_with_domain_count) | +37 (1806 → 1843) | Direct | `domain_counts_before/after.json` |
| Domains discovered per minute (wall-clock) | 2.3668/min | Inferred | `total_wall_clock_ms`, domain delta |
| Domains resolved per minute (resolve step) | 15.8740/min | Inferred | resolve duration, resolved count |
| ATS discovery attempted | 50 | Direct | `canary_summary.json` |
| ATS endpoints extracted (vendor probe) | 29 | Direct | `canary_summary.json` |
| ATS discovery success rate | 0.58 | Direct | `canary_summary.json` |
| ATS endpoint extraction rate during probe wait | 2.2827/min | Inferred | probe wait duration, extracted count |
| URL fetch attempts created (e2e fetch stage) | 39 | Direct | `e2e_pipeline_run/summary.json` |
| URL fetch attempts/min (fetch stage window) | 161.6022/min | Inferred (stage-local) | attempts and stage elapsed |
| URL queue delta during fetch stage | +24 | Direct | `e2e_pipeline_run/summary.json` |
| URL queue delta/min (fetch stage window) | +99.4475/min | Inferred (stage-local) | queue delta and stage elapsed |
| ATS endpoints created (e2e run-scope) | +5 | Direct | `e2e_pipeline_run/summary.json` |
| ATS companies created (e2e run-scope) | +5 | Direct | `e2e_pipeline_run/summary.json` |
| ATS endpoints created/min (full e2e wall) | 1.8218/min | Inferred | e2e elapsed, created count |

## Error-rate and stop-reason evidence
- ATS stage failure buckets (`canary_summary.json`):
  - `BOT_PROTECTION_403`: 19
  - `CAREERS_PAGE_200_NO_VENDOR_SIGNATURE`: 18
  - `NETWORK_ERROR`: 6
  - `NO_CAREERS_LINK_ON_HOMEPAGE`: 2
- Discover run stop/guardrail (`final_status`):
  - `status=SUCCEEDED`
  - `stopReason=null`
  - `hardTimeoutSeconds=900`
  - `timeBudgetExceededCount=0`
  - `hostFailureCutoffCount=6`
  - `hostFailureCutoffSkips=1`

## Bottleneck analysis
1. Timeout-related: **moderate risk**
   - High load canary has long waits (`ats_discovery_vendor_probe_wait=762251 ms`), and historical runs required larger HTTP timeout for stability.
2. Concurrency-related: **mixed**
   - Fetch attempts/min appears high in short fetch-stage window, but queue still grew (+24), indicating enqueue can outpace drain in this sampled run.
3. Guardrail-related: **present but controlled**
   - Host failure cutoff engaged (`hostFailureCutoffCount=6`) with skips tracked; run still completed.
4. Under-instrumented areas: **high**
   - Lack of fresh, repeatable local run in this environment (DB/container unavailable) limits confidence in day-of efficiency reproducibility.

## PM verdicts
- Domain resolution efficiency: **CONDITIONAL**
  - Positive throughput and +37 domain growth in canary, but strong dependency on long-running canary timing and environment readiness.
- ATS discovery efficiency: **CONDITIONAL**
  - Yield is positive (29 extracted in canary, +5 run-scoped in e2e), but significant 403/no-signature failure buckets and long probe wait dominate runtime.
- Overall current-master efficiency: **CONDITIONAL**
  - System is operationally productive in captured runs, but not yet robustly efficient under constrained/variable runtime environments without explicit tuning (timeouts, bounded limits, and stable backend+DB availability).

## What PM should use this for
- Use this baseline as the "current master" reference before guardrail/fetch scheduling changes.
- Treat improvements as meaningful only if they beat both:
  - domain discovery wall-rate (`2.3668/min`) and
  - ATS created run-scope rate (`1.8218/min` on e2e, `2.2827/min` on probe window)
  while maintaining low guardrail/timeout pressure.
