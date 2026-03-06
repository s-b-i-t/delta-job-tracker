# PR #6 Final Review Checklist

Purpose: final closure framework for PR #6 so reviewers can decide merge-readiness quickly and consistently once Team C (blocker fixes) and Team A (validation artifacts) publish updates.

Reference inputs:
- PR #6: https://github.com/s-b-i-t/delta-job-tracker/pull/6
- Team C findings: [docs/teamc_pr6_review.md](/home/bhatt/programming/delta-job-tracker/docs/teamc_pr6_review.md)
- Blocker matrix: [docs/pr6_blocker_closure_matrix.md](/home/bhatt/programming/delta-job-tracker/docs/pr6_blocker_closure_matrix.md)

## 1) Verify when Team C publishes blocker-fix work

Required evidence package from Team C:
- PR/commit link(s) and commit SHAs addressing each blocker A-D.
- Updated blocker matrix with each row marked `OPEN`/`PARTIAL`/`CLOSED`.
- Test command(s) and pass output for newly added/updated tests.
- Short note mapping each code change to blocker ID(s).

Exact verification steps:
- Confirm only intended areas were changed for blocker fixes (services/metrics/tests/docs), with no unrelated behavioral scope creep.
- For blocker A: verify host-cutoff path persists as `SKIPPED` (not `FAILED`) with clear reason mapping.
- For blocker B: verify `hostFailureCutoffSkips` includes both pre-check and in-discovery host-cutoff paths.
- For blocker C: verify strict mid-company request-budget enforcement and `stopReason=request_budget_exceeded`.
- For blocker D: verify sitemap fetches (and equivalent paths) increment request counting used by run guardrails.
- Confirm PR #6 description/body remains accurate after fixes (acceptance criteria, artifacts, worst-case behavior, rollback note).

## 2) Verify when Team A publishes validation evidence

Required evidence package from Team A:
- Independent validation artifact path(s) from bounded canary/validation runs.
- Run-status or metrics snapshots showing stop-reason and guardrail fields (`requestBudget`, `requestCountTotal`, `hardTimeoutSeconds`, `stopReason`, `hostFailureCutoffCount`, `hostFailureCutoffSkips`).
- Evidence that blocker-sensitive scenarios were exercised (host cutoff path, budget pressure path, sitemap-heavy/request-count path).

Exact verification steps:
- Cross-check Team A artifacts against Team C claimed closures in the blocker matrix.
- Confirm observed status/metrics align with intended behavior in worst-case scenarios.
- Confirm no contradiction between Team A runtime evidence and Team C unit/integration test claims.

## 3) Blocker vs Follow-up classification

Classify as **Blocker** (must block merge) if any of the following is true:
- A blocker row A-D is still `OPEN` or `PARTIAL`.
- Stop-reason semantics are inconsistent or non-deterministic for guardrail abort conditions.
- Request budget/time budget accounting is materially inaccurate for enforcement decisions.
- Team A validation evidence contradicts Team C closure claims.

Classify as **Follow-up** (can merge with tracked issue) only if:
- All blocker rows A-D are `CLOSED` with required evidence.
- Remaining gaps are non-critical (wording, docs polish, optional indexing/perf tuning, additional observability not needed for correctness).
- A concrete follow-up issue is created with owner and scope.

## 4) Final merge gate checklist for PR #6

Mark each gate `PASS` or `FAIL`:
- Gate 1: All blocker rows A-D in `pr6_blocker_closure_matrix.md` are `CLOSED`.
- Gate 2: Required tests for each blocker are present and passing in published evidence.
- Gate 3: Team A validation evidence confirms Team C closures in runtime data/artifacts.
- Gate 4: PR #6 body is still accurate (acceptance criteria, artifacts, worst-case behavior, rollback plan).
- Gate 5: No new high-severity regressions or scope drift introduced by blocker fixes.

Merge decision rule:
- Merge only if all five gates are `PASS`.
- If any gate is `FAIL`, outcome is `REQUEST CHANGES`.

## 5) Quick decision template

Use this verbatim in review comment:
- `GO`: "All blocker matrix rows A-D are CLOSED with matching code+tests+runtime evidence. Merge gate checks all PASS."
- `REQUEST CHANGES`: "At least one blocker row is OPEN/PARTIAL or evidence is inconsistent with claimed closure; merge gates not satisfied."
