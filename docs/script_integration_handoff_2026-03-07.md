# Script Integration Handoff - 2026-03-07

This memo is a docs-only handoff for the next two agents. It is anchored to repo history, not prior narrative summaries.

## Accepted Script-Layer Anchors

| Accepted commit / branch | Files changed | Purpose | Tests that should pass after integration | Integration note |
| --- | --- | --- | --- | --- |
| `ef6ec8805e31251499cfd1562c32a18cd992f955` on `origin/teamb-diagnostics-timeout-wrap-20260306` | `scripts/run_ats_validation_canary.py` | Wrap `TimeoutError` from canary API requests as structured `RuntimeError("Request timed out ...")` so script tests and failure handling are stable. | `python3 -m unittest scripts.tests.test_ats_scale_canary -v` | Safe, minimal first pick. This is the clean timeout-wrap fix. |
| `d17654c5e12f62a24f36dcd4db383c61bd0c1c1e` on `origin/teamb-scale-canary-diagnostics-integration-20260306` | Commit-local: `scripts/run_ats_scale_canary.py`, `scripts/tests/test_ats_scale_canary.py` | Add `stage_diagnostics.json`, `stage_failure_summary`, `runtime_context`, and `stage_timings_ms` for easier classification of preflight vs frontier-seed failures. | `python3 -m unittest scripts.tests.test_ats_scale_canary -v` | This commit is the second diagnostics layer only. It sits on top of prior branch commit `f876577`. |
| `f8765771221053a81becdd06f5ecb6383e10d64d` on `origin/teamb-scale-canary-diagnostics-integration-20260306` | `scripts/run_ats_scale_canary.py`, `scripts/run_ats_validation_canary.py`, `scripts/tests/test_ats_scale_canary.py` | Add structured frontier-timeout failure handling to ATS scale canary and initial test coverage. | `python3 -m unittest scripts.tests.test_ats_scale_canary -v` | Required prerequisite if taking the diagnostics branch as cherry-picks instead of merging the branch. |

## Exact Files Touched By Accepted Work

- Timeout-wrap fix:
  - `scripts/run_ats_validation_canary.py`
- Diagnostics branch cumulative diff (`cbf344d..d17654c`):
  - `scripts/run_ats_scale_canary.py`
  - `scripts/run_ats_validation_canary.py`
  - `scripts/tests/test_ats_scale_canary.py`

## Recommended Integration Order

1. Cherry-pick `ef6ec8805e31251499cfd1562c32a18cd992f955`.
2. Cherry-pick `f8765771221053a81becdd06f5ecb6383e10d64d`.
3. Cherry-pick `d17654c5e12f62a24f36dcd4db383c61bd0c1c1e`.

Alternative:
- Merge `origin/teamb-scale-canary-diagnostics-integration-20260306` after first cherry-picking `ef6ec8805e31251499cfd1562c32a18cd992f955`.

Reason for this order:
- `ef6ec88` is the smallest accepted timeout-wrap fix and unblocks the existing script test surface by itself.
- `d17654c` is not a standalone replacement for the full diagnostics branch. It assumes the earlier branch commit `f876577`.
- Putting `ef6ec88` first makes the timeout behavior explicit and lets the next agent resolve the duplicated `run_ats_validation_canary.py` hunk from `f876577` as already applied.

## Overlap / Conflict Surface

- Direct overlap between the two accepted anchors:
  - `ef6ec88` and `d17654c` do not modify the same file.
- Practical overlap during integration:
  - `ef6ec88` and prerequisite commit `f876577` both modify `scripts/run_ats_validation_canary.py`.
  - Expected outcome after `ef6ec88` is applied first: the `run_ats_validation_canary.py` hunk from `f876577` should be duplicate or empty.
- No workflow overlap:
  - None of these accepted commits touch `.github/workflows`.

## Exact Verification Surface

After integrating the script-layer stack above, the next two agents should run:

1. `python3 -m unittest scripts.tests.test_ats_scale_canary -v`
2. `cd backend && ./gradlew test --tests com.delta.jobtracker.crawl.http.CanaryHttpBudgetDeadlineTest`

Notes:
- The first command verifies the timeout-wrap behavior and the structured ATS scale failure artifacts.
- The second command is a narrow guardrail cross-check that has already been used on milestone-line verification and should remain green.
- On lines without a repo-root Gradle wrapper, run the Gradle command from `backend/`.

## Practical Handoff For Next 2 Agents

- Agent 1:
  - Integrate `ef6ec88`, then land the diagnostics branch commits in order.
  - Resolve the `scripts/run_ats_validation_canary.py` duplicate hunk by keeping the already-applied timeout-wrap behavior.
- Agent 2:
  - Run the exact verification surface above.
  - If there is a runtime ATS scale failure, inspect `stage_diagnostics.json` and `ats_scale_metrics.json` first; that is the accepted diagnostic artifact contract.

## Final Recommendation

Safe merge order for script-layer work:

1. `ef6ec8805e31251499cfd1562c32a18cd992f955`
2. `f8765771221053a81becdd06f5ecb6383e10d64d`
3. `d17654c5e12f62a24f36dcd4db383c61bd0c1c1e`

If the next agent wants a branch-level integration instead of cherry-picks, the safest equivalent is:

1. cherry-pick `ef6ec88`
2. merge `origin/teamb-scale-canary-diagnostics-integration-20260306`
3. keep the existing timeout-wrap hunk in `scripts/run_ats_validation_canary.py` during conflict resolution
