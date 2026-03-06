# E2E Pipeline Proof Runbook (canonical)

This runbook proves the end-to-end pipeline is working **and** produces two machine-readable artifacts:

- `out/<ts>/proof_report.json` (pipeline correctness proof)
- `out/<ts>/efficiency_report.json` (efficiency summary)

`<ts>` is a UTC timestamp directory like `20260306T031500Z`.

## Prerequisites

- Docker running locally
- Java 21 (for backend)
- Python 3.10+ (for scripts)

## 1) Start dependencies

```bash
cd infra
docker compose up -d
```

## 2) Start the backend API

```bash
cd backend
./gradlew bootRun
```

Wait until the API is up:

```bash
curl -sS http://localhost:8080/api/status | jq .
```

## 3) Run the deterministic proof harness

In a separate terminal from `bootRun`:

```bash
cd /path/to/delta-job-tracker
python3 scripts/run_e2e_pipeline_proof.py --preset S
```

The script prints a JSON object that includes:

- `stamp_dir`: `out/<ts>`
- `out_dir`: `out/<ts>/e2e_pipeline_run`
- `go_no_go`: `GO` or `NO-GO`

### Expected artifacts (pipeline proof)

Under `out/<ts>/`:

- `proof_report.json`
- `e2e_pipeline_run/summary.json`
- `e2e_pipeline_run/api_calls.json`
- `e2e_pipeline_run/frontier_seed_stage.json`
- `e2e_pipeline_run/frontier_fetch_stage.json`
- `e2e_pipeline_run/ats_discovery_start.json`
- `e2e_pipeline_run/ats_discovery_final_status.json`

## 4) Generate the Efficiency Report artifact

Wrap the proof harness so the DB is sampled while it runs:

```bash
cd /path/to/delta-job-tracker
python3 scripts/generate_efficiency_report.py \
  --out-dir out \
  --interval-seconds 5 \
  --db-snapshot-mode auto \
  -- \
  python3 scripts/run_e2e_pipeline_proof.py --preset S
```

### Expected artifacts (efficiency report)

Under `out/<ts>/`:

- `efficiency_report.json` (stable JSON summary)
- `efficiency_report/metrics.json`
- `efficiency_report/metrics.csv`
- `efficiency_report/README.md`

## Success criteria (merge-ready)

Inspect `out/<ts>/proof_report.json`:

- `go_no_go` must be `GO`
- `successCriteria.crawl_url_attempts_gt_0` must be `true`
- Evidence present (all non-null):
  - `evidence.ats_endpoints_count.before/after/delta`
  - `evidence.companies_with_domain_count.before/after/delta`

Inspect `out/<ts>/efficiency_report.json`:

- `schema_version` is `efficiency-report-summary-v1`
- `throughput.ats_endpoints_count` contains `start`, `end`, `delta`

