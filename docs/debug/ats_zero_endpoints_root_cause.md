# ATS zero-endpoints root cause (preset `S`)

## Command run
```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S
```

## Expected
- Bounded local run.
- `summary.json` contains ATS diagnostics.
- At least one ATS endpoint created (or explicit blocker categories on NO-GO).

## What happened (before fix)
- Run failed with timeout before ATS proof could complete:
  - stderr: `e2e_pipeline_proof status=NO-GO error=POST http://localhost:8080/api/frontier/seed?domainLimit=1&maxSitemapFetches=30 timed out after 180s`
  - artifact: `out/20260305T021959Z/e2e_pipeline_run/summary.json`
  - summary fields were only:
    - `go_no_go = "NO-GO"`
    - `error = "POST ... timed out after 180s"`

## Specific failure point + evidence
- Preset `S` was spending too long in frontier fetch (`maxSitemapFetches=30`), causing harness HTTP timeout before full ATS stage evidence was captured.
- Preset `S` also used vendor-probe-only mode, which is weaker for endpoint creation than full discovery mode.

## Fix
- Updated `scripts/run_e2e_pipeline_proof.py` preset `S`:
  - `ats_vendor_probe_only: false`
  - `frontier_fetch_fetches: 1` (bounded fetch stage)
  - `ats_limit: 3`, `ats_batch_size: 3`, `max_wait_seconds: 240`
- Added explicit timeout handling in API client:
  - captures timeout in `api_calls.json`
  - raises clear `... timed out after <n>s` errors
- Added ATS diagnostics schema fields under `ats_stage`:
  - `attempted_urls`, `attempted_companies`
  - `http_status_histogram`, `error_type_histogram`
  - `top_n_errors`
  - `backend_calls` (url, duration, status, response preview, error)
- Kept deterministic fixture seeding for ATS candidate companies.

## Why this works
- The run is now bounded and deterministic enough for local proof.
- Full ATS discovery path executes (not vendor-probe-only), so endpoint creation is possible.
- If endpoint creation fails, NO-GO now has structured blocker/error context instead of a silent `0/0`.

## Post-fix evidence
- Successful proof run:
  - stderr: `e2e_pipeline_proof preset=S created_endpoints=1 go_no_go=GO out_dir=out/20260305T025149Z/e2e_pipeline_run`
  - `summary.json` key fields:
    - `ats_stage.created_endpoints = 1`
    - `ats_stage.attempted_companies = 3`
    - `ats_stage.error_type_histogram = {"HTTP_404": 4}`
    - `ats_stage.backend_calls` present
    - `go_no_go = "GO"`
