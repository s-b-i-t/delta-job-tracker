# Deployment Candidate Closing Validation (2026-03-06)

## Candidate branch
- Branch: `teama-deploy-candidate-20260306`
- Base: current `origin/master`
- Integration method: cherry-pick (smallest clean integration)

## Integrated source matrix
| Source branch | Source head commit | Included? | Integrated commit(s) on candidate | Notes/conflicts |
|---|---|---|---|---|
| `origin/teamb-efficiency-reporting-trust-20260306` | `020cb34` | Yes | `3b117cb`, `3be43e1` | clean cherry-pick |
| `origin/teamb-reporting-convergence-20260306` | `ff23847` | Yes | `a172036`, `1870b3a`, `e151d04` | clean cherry-pick |
| `origin/teamc-pr6-blocker-fixes` | `c2bd922` | Yes | `73569a0` | clean cherry-pick |
| `origin/teamc-pr6-budget-enforcement-followup` | `6e0ae1f` | Yes | `9fd9ffa` | clean cherry-pick; includes budget enforcement follow-up |

## Narrow verification after integration
Commands run:
```bash
./gradlew test --tests CareersDiscoveryRunServiceTest --tests CareersDiscoveryServiceTest
python3 -m unittest scripts.tests.test_e2e_pipeline_proof scripts.tests.test_reporting_convergence -v
```

Results:
- Python tests: **PASS** (11/11)
- Gradle targeted tests: **FAIL**
  - `CareersDiscoveryRunServiceTest.discoveryRunAbortsWhenTimeBudgetExceeded()`
  - `CareersDiscoveryRunServiceTest.discoveryRunAbortsWhenRequestBudgetExceeded()`
  - failure mode: Mockito argument mismatch (`ArgumentsAreDifferent`)

## Fresh runtime setup (Docker-native WSL)
```bash
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps

docker exec delta-job-tracker-postgres pg_isready -U app -d delta_job_tracker

cd backend && ./gradlew bootRun
```

## Fresh rehearsal commands run
```bash
# canonical e2e proof
python3 scripts/run_e2e_pipeline_proof.py --preset S

# ATS canary run 1
python3 scripts/run_ats_validation_canary.py \
  --sec-limit 5000 \
  --resolve-limit 200 \
  --discover-limit 50 \
  --domain-fresh-cohort \
  --http-timeout-seconds 300

# ATS canary run 2 (repeat)
python3 scripts/run_ats_validation_canary.py \
  --sec-limit 5000 \
  --resolve-limit 200 \
  --discover-limit 50 \
  --domain-fresh-cohort \
  --http-timeout-seconds 300

# larger bounded ingestion-oriented run (high bounded settings attempted)
python3 scripts/run_ats_scale_canary.py \
  --domain-limit 500 \
  --max-sitemap-fetches 120 \
  --discover-limit 300 \
  --discover-batch-size 50 \
  --http-timeout-seconds 300 \
  --max-wait-seconds 1200 \
  --postgres-user app
```

## Fresh artifact paths
- E2E proof: `out/20260306T205823Z/e2e_pipeline_run`
- ATS canary run 1: `out/20260306T210157Z`
- ATS canary run 2: `out/20260306T211026Z`
- Larger bounded scale run attempt (partial only): `out/20260306T212653Z`
- Initial scale run attempt with default DB user (aborted): `out/20260306T212240Z`

## Rehearsal metrics summary
### Canonical e2e proof (`out/20260306T205823Z/e2e_pipeline_run/summary.json`)
- `go_no_go.decision`: `GO`
- frontier: `hosts=1794`, `urls=14030`
- fetch: `attempts_created=20`, `attempts_per_min=92.52`, `queue_delta=+13`
- ATS: `ats_endpoints_created=5`, `ats_companies_created=5`
- note: frontier seed endpoint showed a timeout error in `seed_error`, but run continued and overall GO decision remained true

### ATS canary repeatability
| Metric | Run 1 (`out/20260306T210157Z`) | Run 2 (`out/20260306T211026Z`) |
|---|---:|---:|
| domain_resolved_count | 58 | 38 |
| domain_resolution_rate | 0.29 | 0.19 |
| domain_delta (companies_with_domain_count) | +58 | +38 |
| ats_discovery_success_count | 25 | 25 |
| ats_discovery_success_rate | 0.50 | 0.50 |
| endpoint_extracted_count | 25 | 25 |
| vendor_detected_count | 26 | 26 |
| requestCountTotal | 279 | 436 |
| wall_clock_seconds | 508.12 | 734.658 |
| final_status | SUCCEEDED | SUCCEEDED |
| stopReason | null | null |

### Larger bounded ingestion-oriented run
- Attempted higher bounded configuration (`domain-limit=500`, `max-sitemap-fetches=120`, `discover-limit=300`).
- Run failed to progress past preflight artifact stage (only `preflight_api_status.json` written).
- No seed/discovery completion artifacts were produced for this large run.

## Blocker classification
1. **Operational/runtime blocker (primary):** larger bounded scale run did not complete/progress under elevated settings.
2. **Integration confidence blocker (secondary):** targeted backend test regressions after Team C integration (`CareersDiscoveryRunServiceTest` failures).
3. **ATS yield/quality:** canary remains productive (non-zero ATS outcomes), but high variance in domain throughput between repeats persists.

## Final verdict
- Integration readiness: **CONDITIONAL**
  - Code integrated cleanly by cherry-pick, but targeted backend tests currently fail.
- Larger bounded ingestion readiness: **NO-GO**
  - Elevated bounded scale run did not complete and did not produce full artifacts.
- Deployment candidate status: **NOT READY**
