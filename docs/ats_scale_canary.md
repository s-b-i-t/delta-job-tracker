# ATS Scale Canary (Frontier + Discovery)

This canary is a bounded reliability run for ATS discovery at scale.

It performs:
1. Frontier seed (`/api/frontier/seed`) to enqueue sitemap/job-signal URLs from company domains.
2. ATS discovery run (`/api/careers/discover`) with run-level guardrails.
3. DB-truth SQL snapshots and a consolidated `ats_scale_metrics.json`.

## Guardrail config keys

Under `crawler.careers-discovery`:

- `hard-timeout-seconds` (default `900`)
  - Hard wall-clock timeout for the discovery run.
  - Run terminates with `status=ABORTED` and `stop_reason=time_budget_exceeded` when exceeded.
- `global-request-budget-per-run` (default `5000`)
  - Maximum total HTTP requests in a discovery run.
  - Run terminates with `status=ABORTED` and `stop_reason=request_budget_exceeded` when exceeded.
- `per-host-failure-cutoff` (default `6`)
  - If a host accumulates repeated failures to this cutoff, discovery skips further attempts for that host in-run.
  - Skip count is surfaced as `hostFailureCutoffSkips`.

## Run command

```bash
python3 scripts/run_ats_scale_canary.py \
  --domain-limit 50 \
  --max-sitemap-fetches 50 \
  --discover-limit 200 \
  --discover-batch-size 25
```

Optional DB container args:

```bash
--postgres-container delta-job-tracker-postgres \
--postgres-user delta \
--postgres-db delta_job_tracker
```

## Outputs

The script writes `out/<timestamp>/` with:

- `frontier_seed_response.json`
- `ats_discovery_start.json`
- `ats_discovery_final_status.json`
- `ats_discovery_companies.json`
- `ats_discovery_failures.json`
- `ats_scale_sql_snapshots.json`
- `ats_scale_metrics.json`

`ats_scale_metrics.json` includes:

- companies attempted
- companies with >= 1 ATS endpoint
- endpoints by vendor
- validated vs unvalidated endpoints
- attempts by error bucket
- throughput estimates (`domainsPerHour`, `endpointsPerHour`, `requestCount`, `requestsPerHour`)

The script also prints a single-line stderr summary:

`ats_scale run_id=... status=... attempted=... endpoints_added=... requests=... domains_per_hour=... endpoints_per_hour=...`
