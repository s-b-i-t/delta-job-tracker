# Team C Blocker-Fix Audit (PR #6)

Audit date: 2026-03-06

Compared branches:
- Base: `team2-ats-scale-guardrails` @ `42890ef8351a8d02d5dca93a4cfb08b948fc0bed`
- Candidate fix branch: `teamc-pr6-blocker-fixes` @ `c2bd92284f8071cb47a23750660c43866db429b2`
- Team C blocker source: [teamc_pr6_review.md](/home/bhatt/programming/delta-job-tracker/docs/teamc_pr6_review.md)
- Closure framework reference: [pr6_blocker_closure_matrix.md](/home/bhatt/programming/delta-job-tracker/docs/pr6_blocker_closure_matrix.md)

## Scope and method
- Reviewed only code and docs diffs between base and candidate branches.
- No backend services, bootRun, Docker, or runtime-heavy validation executed.
- Verdicts are based on repository evidence (diff + tests), not team claims.

## Branch diff reality
Files changed in `teamc-pr6-blocker-fixes` vs `team2-ats-scale-guardrails`:
- `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java`
- `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryService.java`
- `backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunServiceTest.java`
- `backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryServiceTest.java`

No diff observed for:
- `backend/src/main/java/com/delta/jobtracker/crawl/sitemap/SitemapService.java`
- `backend/src/main/java/com/delta/jobtracker/crawl/http/CanaryHttpBudget.java`

## Strict blocker closure table

| Blocker | Expected fix | Observed code/test evidence | Verdict |
|---|---|---|---|
| A: Host cutoff reported as `FAILED` instead of `SKIPPED` | Host-cutoff failure paths produce skip semantics and persist `SKIPPED` outcome. | `CareersDiscoveryService` now marks host-cutoff failure as skipped via `isHostFailureCutoffFailure(failure)` in `DiscoveryOutcome` ([CareersDiscoveryService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryService.java):685). `CareersDiscoveryRunService` additionally forces `status="SKIPPED"` when host-cutoff failure appears in non-skipped branch ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):241). Tests added: `vendorProbeHostFailureCutoffReturnsSkippedOutcome` and `discoveryRunTreatsHostFailureCutoffOutcomeAsSkippedAndCountsSkip` ([CareersDiscoveryServiceTest.java](/home/bhatt/programming/delta-job-tracker/backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryServiceTest.java):216, [CareersDiscoveryRunServiceTest.java](/home/bhatt/programming/delta-job-tracker/backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunServiceTest.java):254). | **CLOSED** |
| B: `hostFailureCutoffSkips` undercounted | Count host-cutoff skips from all relevant paths (pre-check and in-discovery outcome mapping). | Increment added when skipped outcome is host-cutoff ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):230) and when host-cutoff is detected in non-skipped branch ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):241). Existing pre-check increment remains ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):188). New run-service test asserts skip count increments for host-cutoff outcome path ([CareersDiscoveryRunServiceTest.java](/home/bhatt/programming/delta-job-tracker/backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunServiceTest.java):304). | **CLOSED** |
| C: Request budget not strict mid-company | Enforce hard run budget during in-company execution (not only between companies), with deterministic abort reason. | Guardrails are now frozen at run start and passed through explicitly ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):74), and test added for frozen values ([CareersDiscoveryRunServiceTest.java](/home/bhatt/programming/delta-job-tracker/backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunServiceTest.java):331). However strict mid-company enforcement still not implemented: budget checks remain at loop boundaries/post-company processing ([CareersDiscoveryRunService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java):162, :251), and no `CanaryHttpBudget` integration change is present in this branch diff. | **PARTIAL** |
| D: Request counting misses sitemap/network paths | Ensure request accounting includes sitemap fetch paths used by discovery. | No changes to sitemap request path accounting files in branch diff; `SitemapService` still performs direct `httpClient.get(...)` without new request-metric hook ([SitemapService.java](/home/bhatt/programming/delta-job-tracker/backend/src/main/java/com/delta/jobtracker/crawl/sitemap/SitemapService.java):69). No new test added that proves sitemap fetches are included in `request_count_total`. | **OPEN** |

## Proof/reporting gaps in Team C deliverable
- No published runtime artifact in this branch proving blocker C (strict mid-company budget enforcement) or blocker D (sitemap-inclusive request counting).
- No update to closure status docs (matrix/checklist) in this branch showing A-D closure evidence formally recorded.
- No added test demonstrating request-budget interruption during a single company execution path.
- No added test demonstrating sitemap fetches increment the run-level request counter used for budgeting.

## Mismatch: prior blocker review claims vs actual blocker-fix changes
- Prior review requested hard run-budget enforcement via HTTP budget context and/or equivalent strict in-request mechanism; current diff does not modify `CanaryHttpBudget` or add such in-request enforcement.
- Prior review requested sitemap request counting inclusion; current diff does not modify `SitemapService` request accounting path.
- Prior review requested frozen per-run guardrail values; current diff does implement this (aligned).
- Prior review requested host-cutoff skip semantics and skip counting fixes; current diff implements both with direct tests (aligned).

## Final recommendation
Team C blocker-fix task is **incomplete**.

Reason:
- Blockers A and B are closed with code+test evidence.
- Blocker C is only partial (frozen config improved, but strict mid-company request-budget enforcement still unproven/absent).
- Blocker D remains open (no sitemap request-accounting fix evidence).
