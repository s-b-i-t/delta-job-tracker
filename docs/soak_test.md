# Soak Test Harness

`scripts/run_soak_test.py` runs unattended warmup + ATS-heavy soak cycles using existing runners:
- `scripts/run_overnight_stack.sh`
- `scripts/run_overnight_crawler.py`

It writes all artifacts under `out/<timestamp>/`.

Defaults tuned for ATS-heavy soak:
- `--soak-resolve-limit 0` (domain resolution mostly disabled during soak)
- `--run-full-mode` enabled
- backend is kept running between cycles unless `--stop-backend` is set

## How To Run

### 1 hour domain warmup + 12 hour ATS soak
```bash
python3 scripts/run_soak_test.py \
  --warmup-hours 1 \
  --soak-hours 12 \
  --run-full-mode \
  --crawler-max-duration-seconds 45 \
  --warmup-sec-limit 5000 \
  --warmup-resolve-limit 400 \
  --warmup-resolve-batch-size 100 \
  --warmup-discover-batch-size 8 \
  --warmup-num-batches 1 \
  --warmup-sleep-between-batches 2 \
  --soak-sec-limit 2000 \
  --soak-resolve-limit 0 \
  --soak-resolve-batch-size 100 \
  --soak-discover-batch-size 20 \
  --soak-num-batches 2 \
  --soak-sleep-between-batches 5
```

### Pure 12 hour ATS soak (no warmup)
```bash
python3 scripts/run_soak_test.py \
  --warmup-hours 0 \
  --soak-hours 12 \
  --run-full-mode \
  --crawler-max-duration-seconds 45 \
  --soak-sec-limit 2000 \
  --soak-resolve-limit 0 \
  --soak-resolve-batch-size 100 \
  --soak-discover-batch-size 20 \
  --soak-num-batches 2 \
  --soak-sleep-between-batches 5
```

## Artifacts

In `out/<timestamp>/`:
- `soak_manifest.json`: git commit, timestamps, args
- `soak_report.json`: aggregated metrics and throughput
- `runs/warmup/<run_ts>/...`: per-cycle warmup overnight artifacts
- `runs/soak/<run_ts>/...`: per-cycle soak overnight artifacts
- `logs/*.log`: stack-run command logs per cycle

Use `--stop-backend` if you want each cycle to tear down backend after completion.
Without `--stop-backend`, the harness starts backend via `run_overnight_stack.sh` and reuses it
in subsequent cycles by calling `run_overnight_crawler.py` directly.

## Metrics In `soak_report.json`

- `domains.domains_before/after/delta`: from each cycle’s `overnight_summary.json`
- `ats_selection_metrics_totals`:
  - `selectionEligibleCount`
  - `selectionReturnedCount`
  - `companiesAttemptedCount`
  - `vendorDetectedCount`
  - `endpointExtractedCount`
- `failure_type_top_buckets` / `failure_reason_top_buckets`:
  - aggregated from `discovery_failures_diagnostics.json` when present
- `throughput.attempts_per_hour`
- `throughput.endpoints_per_hour`

## Interpreting Results

Good signs:
- `domains_delta` stays positive or stable after warmup
- `selectionReturnedCount` tracks close to `selectionEligibleCount`
- `endpointExtractedCount` grows steadily
- `endpoints_per_hour` stable or improving

Bad signs:
- `selectionReturnedCount` far below `selectionEligibleCount` for long periods
- `companiesAttemptedCount` high but `vendorDetectedCount` and `endpointExtractedCount` flat
- `failure_type_top_buckets` dominated by timeout/rate-limit buckets
- repeated cycle failures (`status=FAILED`)

## JQ snippets used in aggregation

The harness reads these fields from each cycle’s `overnight_summary.json`:

```jq
.pipeline.domain_counts_before.companies_with_domain_count
.pipeline.domain_counts_after.companies_with_domain_count
.pipeline.ats_selection_metrics.vendor_probe.selectionEligibleCount
.pipeline.ats_selection_metrics.vendor_probe.selectionReturnedCount
.pipeline.ats_selection_metrics.vendor_probe.companiesAttemptedCount
.pipeline.ats_selection_metrics.vendor_probe.vendorDetectedCount
.pipeline.ats_selection_metrics.vendor_probe.endpointExtractedCount
```
