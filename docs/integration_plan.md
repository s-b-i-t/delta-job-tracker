# Integration Plan: E2E Proof + Efficiency + ATS Guardrails

This plan is written to minimize merge conflicts and keep `master` runnable while we integrate:

- Team A: `team1-e2e-pipeline-gate` (DB snapshots + DB-truth in E2E proof harness)
- Team B: `team2-e2e-ats-proof` (alternative E2E harness + efficiency report generator)
- Team D: `teamd-efficiency-report` (efficiency report generator)
- PR #6: `team2-ats-scale-guardrails` (ATS guardrails + deterministic canary/scale diagnostics)

## Current State (Source of Truth: Remote Branches)

- `origin/master` already contains Team A’s E2E proof harness changes (merge commit `6a826fe`, PR #7).
- Team B and Team D both introduce an efficiency report script with the same path: `scripts/generate_efficiency_report.py`.
- Team B introduces an alternative `scripts/run_e2e_pipeline_proof.py` that would conflict with Team A’s canonical harness.

## Canonical Ownership Decisions

To avoid duplicate tools and path conflicts, we treat these as canonical:

- `scripts/run_e2e_pipeline_proof.py`: Team A version (already on `master`).
  - Team B’s version should be renamed (preferred) or dropped before merge into `master`.
- `scripts/generate_efficiency_report.py`: Team B version (more “management-ready” output: richer CSV schema + charts + narrative README).
  - Team D’s version should be renamed (preferred) or dropped to avoid path conflict.
- Canary/scale scripts (`scripts/run_ats_validation_canary.py`, `scripts/ats_canary_summary_schema.json`): PR #6 is canonical for guardrails and deterministic run behavior.

## Acceptance Criteria: E2E Proof Harness

Run:

```bash
python3 scripts/run_e2e_pipeline_proof.py --preset S
```

Artifacts required under `out/<ts>/e2e_pipeline_run/`:

- `run_config.json`
- `api_status.json`
- `db_pre_counts.csv`
- `db_post_seed_counts.csv`
- `db_post_fetch_counts.csv`
- `db_post_ats_counts.csv`
- `release_report.md`
- `summary.json`

DB-truth snapshots required (when enabled, default on):

- `db_snapshot_start.json`
- `db_snapshot_end.json`

Summary required fields:

- `summary.json` contains `db_truth.start`, `db_truth.end`, and `db_truth.delta_ok` and `db_truth.delta`.
- When snapshots are disabled: `summary.db_truth.enabled == false`, `delta_ok == false`, `delta == null`.

Non-hanging / safety requirements:

- Any snapshot failure must be recorded as a structured error payload and must not crash the harness by itself.

## Acceptance Criteria: Efficiency Report Generator

Run (example):

```bash
python3 scripts/generate_efficiency_report.py --interval-seconds 15 -- \\
  python3 scripts/run_e2e_pipeline_proof.py --preset S
```

Artifacts required under `out/<ts>/efficiency_report/`:

- `metrics.json` (schema versioned)
- `metrics.csv` (flat, spreadsheet friendly)
- `README.md` (what it proves + how to reproduce)
- Logs for wrapped command (`*.stdout.log`, `*.stderr.log`)

Behavior requirements:

- If DB snapshot capture fails: still writes report stub with structured errors (does not crash).
- If plotting dependencies are missing: skips plots; README must explicitly state plots were skipped and why.

Minimum metrics required:

- ATS endpoints sampled over time and rate (per minute)
- companies-with-domain sampled over time and rate (per minute)
- endpoints-by-type deltas when available from snapshots

## How PR #6 Interacts With Proof Harness

PR #6 changes ATS discovery behavior and canary/scale metrics collection. After merging PR #6, re-run:

- `python3 -m unittest scripts.tests.test_e2e_pipeline_proof -v`
- `python3 scripts/run_e2e_pipeline_proof.py --preset S`
- `python3 scripts/run_ats_validation_canary.py --sec-limit 5000 --resolve-limit 200 --discover-limit 50 --domain-fresh-cohort`

Key risk to re-verify:

- Response schema and behavior of `/api/domains/resolve` and `/api/careers/discover` remains compatible with the harness.

## Merge Order Proposal (Minimize Conflicts)

1. Merge PR #6 (`team2-ats-scale-guardrails`) into `master`.
   - Rationale: mostly backend and canary guardrails; low overlap with Team A E2E harness; improves determinism before we scale proof runs.
2. Merge Team B’s efficiency tooling into `master`, but:
   - Must not overwrite `scripts/run_e2e_pipeline_proof.py`.
   - Must include a rename or deletion of Team B’s `scripts/run_e2e_pipeline_proof.py` before merge.
3. Close/rename Team D’s efficiency script branch (or re-scope it to improvements on the canonical efficiency tool).

## Owners / Responsibilities

- Team A: E2E proof harness (already in `master`), integration checklist ownership, and post-merge re-test.
- Team B: efficiency report generator canonicalization and any required renames to avoid clobbering Team A harness.
- Team D: either withdraw duplicate efficiency script or re-target as improvements to the canonical efficiency tool.
- Team 2 / PR #6 owner: confirm guardrails do not break harness calls and document any behavior changes.

