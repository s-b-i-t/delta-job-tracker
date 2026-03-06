# Live Benchmark Campaign (Current Master) — 2026-03-06

## Scope
Fresh live-run benchmark campaign on current `master` for:
1. domain resolution
2. ATS discovery
3. queue/fetch behavior

No product logic was changed. This is runtime setup + measurement only.

## Runtime setup steps (fresh)
Executed on this machine, from repo root:

```bash
# branch prep
git fetch origin --prune
git switch master
git pull --ff-only origin master
git switch -c teama-live-benchmark-20260306

# Docker daemon startup (WSL fallback to host Docker Desktop)
docker --version                    # showed WSL integration missing
powershell.exe -NoProfile -Command "Start-Process 'C:\\Program Files\\Docker\\Docker\\Docker Desktop.exe'"

# Start postgres from project compose file (via docker.exe)
docker.exe compose -f "$(wslpath -w "$PWD/infra/docker-compose.yml")" up -d
docker.exe compose -f "$(wslpath -w "$PWD/infra/docker-compose.yml")" ps -a
docker.exe exec delta-job-tracker-postgres pg_isready -U app -d delta_job_tracker

# Backend (kept running during benchmark)
cd backend && ./gradlew bootRun

# API readiness check
python3 - <<'PY'
import urllib.request
with urllib.request.urlopen('http://localhost:8080/api/status', timeout=10) as r:
    print(r.status)
PY
```

## Fresh live benchmark commands
```bash
# Queue/fetch + ATS pipeline proof (bounded preset)
python3 scripts/run_e2e_pipeline_proof.py --preset S

# Domain resolution + ATS discovery canary (bounded, explicit timeout)
python3 scripts/run_ats_validation_canary.py \
  --sec-limit 5000 \
  --resolve-limit 200 \
  --discover-limit 50 \
  --domain-fresh-cohort \
  --http-timeout-seconds 300
```

## Fresh artifact pack
- E2E pipeline proof run:
  - `out/20260306T175847Z/e2e_pipeline_run/summary.json`
  - `out/20260306T175847Z/e2e_pipeline_run/release_report.md`
- ATS validation canary run:
  - `out/20260306T180049Z/canary_summary.json`
  - `out/20260306T180049Z/domain_counts_before.json`
  - `out/20260306T180049Z/domain_counts_after.json`
  - plus diagnostic artifacts in same folder (`coverage_diagnostics.json`, `discovery_failures_diagnostics.json`, etc.)

## Directly measured fresh metrics
| Area | Metric | Value | Source |
|---|---|---:|---|
| Domain resolution | resolved_count | 36 | `canary_summary.json` |
| Domain resolution | domain_resolution_rate | 0.18 | `canary_summary.json` |
| Domain resolution | resolve step duration | 171.503s | `canary_summary.json` |
| Domain coverage | companies_with_domain_count | 1892 -> 1928 (`+36`) | `domain_counts_before/after.json` |
| ATS discovery | attempted | 50 | `canary_summary.json` |
| ATS discovery | success_count | 25 | `canary_summary.json` |
| ATS discovery | endpoint_extracted_count | 25 | `canary_summary.json` |
| ATS discovery | success_rate | 0.50 | `canary_summary.json` |
| ATS discovery | request_count_total | 452 | `canary_summary.json` |
| ATS discovery | stopReason | `null` (SUCCEEDED) | `canary_summary.json` |
| Queue/fetch (e2e) | crawl_hosts_count_post_seed | 1794 | `e2e summary.json` |
| Queue/fetch (e2e) | crawl_urls_count_post_seed | 14014 | `e2e summary.json` |
| Queue/fetch (e2e) | fetch_attempts_created | 21 | `e2e summary.json` |
| Queue/fetch (e2e) | attempts_per_minute | 159.49 | `e2e summary.json` |
| Queue/fetch (e2e) | queue_delta | -8 | `e2e summary.json` |
| ATS in e2e | ats_endpoints_created | 0 | `e2e summary.json` |
| ATS in e2e | ats_stage status | timeout error on `/api/domains/resolve?limit=1` (`curl rc=28`, 60s max-time) | `e2e summary.json` |

## Derived fresh efficiency metrics (inferred)
- Domains discovered per minute (wall-clock): `36 / (843.959 / 60) = 2.56/min`
- Domains resolved per minute (resolve-step): `36 / (171.503 / 60) = 12.59/min`
- ATS endpoints discovered per minute (ATS wait step): `25 / (646.355 / 60) = 2.32/min`
- URLs queued per minute (e2e fetch window): `-8 / ((21 / 159.49) min) = -60.76/min`
- URLs fetched per minute (e2e): `159.49/min` (direct metric)

## Error-rate / bottleneck evidence
Top ATS stage failure buckets (fresh canary):
- `CAREERS_PAGE_200_NO_VENDOR_SIGNATURE`: 20
- `BOT_PROTECTION_403`: 16
- `NETWORK_ERROR`: 12

Observed bottlenecks:
1. Timeout-related: e2e ATS stage failed due to 60s domain resolve HTTP timeout (`curl rc=28`).
2. ATS quality bottlenecks: high no-signature + 403 share limits endpoint yield.
3. Repro/setup friction: Docker daemon had to be started via host `docker.exe` because native WSL Docker integration is not active.
4. Instrumentation gap on current master: `scripts/generate_efficiency_report.py` is absent, so time-series efficiency had to be inferred from stage summaries and durations.

## Fresh vs fallback evidence
- Fresh live-run evidence: **all metrics above** came from new runs started during this task (`out/20260306T175847Z`, `out/20260306T180049Z`).
- Fallback/reference evidence: **none required** for the final numbers in this report.

## Reproducibility and benchmark reliability verdicts
- Live benchmark reproducibility: **CONDITIONAL**
  - Reproducible with explicit runtime steps (start Docker Desktop, compose postgres, bootRun backend), but not one-command due to WSL Docker integration gap.
- Current-master runtime efficiency evidence quality: **CONDITIONAL**
  - Fresh domain and ATS canary evidence is strong; queue/fetch evidence is fresh.
  - Canonical e2e harness ATS step is currently sensitive to timeout (`60s`) and yielded `NO-GO` on this run even though standalone canary succeeded with `--http-timeout-seconds 300`.

## Can master be benchmarked reliably for future branch comparisons?
**Yes, conditionally.** Use this standardized operator recipe:
1. Start Docker Desktop daemon first.
2. Start compose Postgres with `docker.exe compose ... up -d`.
3. Start backend with `./gradlew bootRun`.
4. Run both benchmark commands above, with canary using `--http-timeout-seconds 300`.

With those controls fixed, comparisons across future branches are reliable enough for trend tracking.
