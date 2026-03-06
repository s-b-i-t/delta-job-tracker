# Deployment Rehearsal Validation (2026-03-06)

## Branch / scope
- Branch: `teama-deploy-rehearsal-20260306`
- Scope: live runtime + bounded real runs + artifact-backed stability check
- No crawler/product logic changes

## Fresh runtime setup (Docker-native)
```bash
git fetch origin --prune
git switch master
git pull --ff-only origin master
git switch -c teama-deploy-rehearsal-20260306

# Docker in this WSL env is available directly
docker --version
docker compose version

# Fresh postgres runtime
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
docker exec delta-job-tracker-postgres pg_isready -U app -d delta_job_tracker

# Fresh backend runtime
cd backend && ./gradlew bootRun
```

## Commands run (fresh bounded rehearsal)
```bash
# 1) Canonical E2E proof harness
python3 scripts/run_e2e_pipeline_proof.py --preset S

# 2) ATS validation canary run 1
python3 scripts/run_ats_validation_canary.py \
  --sec-limit 5000 \
  --resolve-limit 200 \
  --discover-limit 50 \
  --domain-fresh-cohort \
  --http-timeout-seconds 300

# 3) ATS validation canary run 2 (repeat of run 1, unchanged command)
python3 scripts/run_ats_validation_canary.py \
  --sec-limit 5000 \
  --resolve-limit 200 \
  --discover-limit 50 \
  --domain-fresh-cohort \
  --http-timeout-seconds 300
```

## Fresh artifact directories
- E2E proof: `out/20260306T194022Z/e2e_pipeline_run`
- ATS canary run 1: `out/20260306T194319Z`
- ATS canary run 2: `out/20260306T195519Z`

## Key outcomes
### E2E proof (single run)
Source: `out/20260306T194022Z/e2e_pipeline_run/summary.json`

- `go_no_go.decision`: `GO`
- Frontier:
  - `crawl_hosts_count_post_seed`: `1794`
  - `crawl_urls_count_post_seed`: `14026`
- Fetch:
  - `fetch_attempts_created`: `21`
  - `attempts_per_minute`: `148.76`
  - `queue_delta`: `-16` (drain)
- ATS stage:
  - `ats_endpoints_created`: `7`
  - `ats_companies_created`: `7`
  - `ok=true`, no timeout error

### ATS canary repeatability (run1 vs run2)
| Metric | Run 1 (`out/20260306T194319Z`) | Run 2 (`out/20260306T195519Z`) | Delta / note |
|---|---:|---:|---|
| domain_resolved_count | 53 | 53 | stable |
| domain_resolution_rate | 0.265 | 0.265 | stable |
| companies_with_domain_count before -> after | 1930 -> 1983 | 1983 -> 2036 | +53 both runs |
| ats_discovery_success_count | 24 | 31 | +7 run2 |
| ats_discovery_success_rate | 0.48 | 0.62 | improved |
| endpoint_extracted_count | 24 | 31 | +7 run2 |
| vendor_detected_count | 25 | 31 | +6 run2 |
| requestCountTotal | 441 | 374 | lower request load run2 |
| total_wall_clock_ms | 720388 | 637682 | run2 faster by ~82.7s |
| final_status.status | SUCCEEDED | SUCCEEDED | stable |
| final_status.stopReason | null | null | no timeout stop |
| timeBudgetExceededCount | 0 | 0 | no guardrail timeout |

## ATS non-zero expectation check
- Expected non-zero ATS outcomes in canary runs: **met**.
- Run 1 and Run 2 both produced non-zero endpoint extraction (`24`, `31`) and succeeded.

## Throughput delta interpretation
- Domain resolution throughput remained stable across repeats.
- ATS discovery throughput improved in run 2 (higher extracted endpoints, lower wall-clock, lower request count).
- This indicates repeatable operation with normal run-to-run variance and no emergent timeout instability under current bounded settings.

## Blocker classification
Current status is not fully blocked, but has caveats:
1. Operational timeout tuning: **primary residual risk**
   - Rehearsal was stable with explicit `--http-timeout-seconds 300`.
   - Without this control, high-load path may regress to timeout sensitivity.
2. ATS yield/quality: **secondary risk**
   - Failure buckets remain dominated by `CAREERS_PAGE_200_NO_VENDOR_SIGNATURE`, plus bot/network failures.
3. Backend/runtime setup: **not a blocker now**
   - Docker-native + backend setup was repeatable on this machine.
4. Measurement/reporting gap: **minor**
   - Core artifacts are sufficient for deployment rehearsal decisions.

## Deployment rehearsal verdicts
- Stable rerun behavior: **GO**
- Bounded deployment rehearsal: **CONDITIONAL**
  - Works and is repeatable with explicit timeout control.
- Ready for larger bounded ingestion cycle: **YES (CONDITIONAL)**
  - Proceed if operational guardrails are fixed in runbook:
    - keep explicit HTTP timeout control
    - keep bounded limits
    - monitor top ATS failure buckets during first larger run
