# PR #6 Validation Matrix (Master vs Candidate)

## Context
- Baseline master ref: `a01b3ac`
- Candidate ref: `42890ef` (`team2-ats-scale-guardrails` + Team C head at run time)
- Worktrees:
  - `/tmp/djt-master-validate`
  - `/tmp/djt-pr6-validate2`

## Commands Executed
1. `python3 scripts/run_e2e_pipeline_proof.py --preset S` (master)
2. `python3 /tmp/djt-master-validate/scripts/run_e2e_pipeline_proof.py --preset S --out-dir out` (candidate backend)
3. `python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 200 --discover-limit 50 --domain-fresh-cohort` (candidate)
4. `python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 200 --discover-limit 50 --domain-fresh-cohort --http-timeout-seconds 300` (candidate)
5. `python3 scripts/run_ats_scale_canary.py` (candidate)

## Run Matrix
| ID | Worktree | Status | Stop reason | Artifact |
|---|---|---|---|---|
| A | master (`/tmp/djt-master-validate`) | success | - | `/tmp/djt-master-validate/out/20260306T031231Z/e2e_pipeline_run` |
| B | candidate (`/tmp/djt-pr6-validate2`) | success | - | `/tmp/djt-pr6-validate2/out/20260306T031604Z/e2e_pipeline_run` |
| C | candidate | failed | `TimeoutError` during `/api/domains/resolve` | `/tmp/djt-pr6-validate2/out/20260306T031749Z` |
| D | candidate | success | - | `/tmp/djt-pr6-validate2/out/20260306T032009Z` |
| E | candidate | failed | `TimeoutError` during `/api/frontier/seed` | `/tmp/djt-pr6-validate2/out/20260306T033653Z` |

## Key Comparison Findings
- Harness compatibility:
  - A and B both executed end-to-end without API/schema crash.
  - A produced `GO` and B produced `NO-GO` due ATS delta gate (`ats_endpoints_created_gt_0=false`), not endpoint contract break.
- Timeout behavior:
  - C failed at default timeout with no final `canary_summary.json`.
  - D succeeded under `--http-timeout-seconds 300` and produced full canary artifacts.
- Scale canary:
  - E failed at default timeout on first frontier seed request (`TimeoutError`).

## GO / NO-GO Recommendations
- Harness compatibility: **GO**
- Canary operational readiness: **NO-GO** on default timeout; **GO (conditional)** with `--http-timeout-seconds 300` or reduced `--resolve-limit`
- Scale-canary readiness: **NO-GO** on default timeout
- Overall PR #6 merge readiness: **GO for compatibility/correctness**, **NO-GO for default operational release-gate commands until timeout/runbook tuning is applied**

## Failure Classification
- C: operational tuning issue (client timeout), not schema incompatibility.
- E: operational tuning issue (client timeout), not harness logic incompatibility.
- B ATS gate miss: correctness/data-yield outcome for this sample window, not API contract break.

## Smallest Actionable Fix List
1. Raise default timeout for canary/scale runner HTTP client, or add stage-specific timeout overrides.
2. Keep release docs explicit: use `--http-timeout-seconds 300` for high-load resolve runs or lower `--resolve-limit`.
3. Ensure timeout failures are summarized in machine-readable output (not just traceback) for release-gate automation.
