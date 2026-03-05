# E2E Pipeline Proof Release Gate

## Purpose
`run_e2e_pipeline_proof.py` is the release-gate harness for proving:
1. Frontier enqueue scale
2. Crawler fetch throughput
3. ATS discovery progress

It writes deterministic artifacts and a `GO/NO-GO` decision.

## Prerequisites
- Backend running at `http://localhost:8080`
- Docker Postgres container available (default: `delta-job-tracker-postgres`)
- `curl` installed (required; script fails fast if missing)

## Presets
- `S`: quick gate check
- `M`: medium scale
- `L`: larger bounded scale

Preset values are embedded in the script and written to `run_config.json`.

## Run Commands
Quick sanity / dependency check (no DB mutation):
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S --dry-run
```

Small gate run:
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S
```

Medium gate run:
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset M
```

Large bounded gate run:
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset L
```

Optional docs report write (best-effort):
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S --write-docs-report
```

Disable DB snapshots if needed:
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S --no-db-snapshots
```

Override any preset value explicitly (explicit flag wins):
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset M --fetch-cycles 6 --discover-limit 300
```

## Artifacts
Each run writes:
`out/<timestamp>/e2e_pipeline_run/`

Required outputs:
- `run_config.json`
- `db_snapshot_start.json` (default; unless `--no-db-snapshots`)
- `db_snapshot_end.json` (default; unless `--no-db-snapshots`)
- `db_pre_counts.csv`
- `db_post_seed_counts.csv`
- `db_post_fetch_counts.csv`
- `db_post_ats_counts.csv`
- `summary.json`
- `release_report.md` (default report location)

Optional output:
- `docs/release_e2e_pipeline_proof_<date>.md` only when `--write-docs-report` is set.

## GO Interpretation
`summary.json` contains:
- `go_no_go.frontier_pass`
- `go_no_go.fetch_pass`
- `go_no_go.ats_pass`
- `go_no_go.decision`

Gate semantics:
- Frontier PASS: `crawl_hosts_count_post_seed >= 1000` and `crawl_urls_count_post_seed >= 2000`
- Fetch PASS: `fetch_attempts_created > 0` and `attempts_per_minute > 10`
- ATS PASS: `ats_endpoints_created > 0` and `ats_companies_created > 0`
- Decision is `GO` only if all three pass.
