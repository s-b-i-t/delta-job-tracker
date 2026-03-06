# Team C Review: PR #6 (`team2-ats-scale-guardrails`)

Reviewer focus: safety + scale guardrails.

Reviewed commit: `06b7cae`

## Executive summary

This PR is directionally good (additive schema changes, clear run-level guardrail fields, and expanded ATS detection), but I see **4 blocking correctness issues** in guardrail enforcement/reporting and **several medium-risk operational/API concerns**.

## 1) Schema/migration risk assessment

### Backwards compatibility
- `V25__careers_discovery_guardrail_metrics.sql` is additive only (new columns), so old code can still write rows due defaults.
- New nullable `stop_reason` is safe for legacy readers.

### Nullability/defaults
- `request_budget`, `request_count_total`, `hard_timeout_seconds`, `host_failure_cutoff_count`, `host_failure_cutoff_skips` are `NOT NULL DEFAULT 0`, which is safe for read paths.
- Risk: `0` has overloaded semantics ("unset" vs "disabled" vs "not collected") and can blur analytics/reporting.

### Indexes
- No index added for likely new query dimensions (`status`, `stop_reason`, run recency). If dashboards/API start filtering aborted reasons, this will full-scan `careers_discovery_runs`.

### Migration operational risk
- Multiple `ALTER TABLE ... ADD COLUMN ... NOT NULL DEFAULT ...` statements can take stronger locks on older Postgres versions and may still block writes during rollout on large tables.

## 2) Guardrail default sanity

Defaults introduced:
- `hard-timeout-seconds: 900`
- `global-request-budget-per-run: 5000`
- `per-host-failure-cutoff: 6`

Assessment:
- Defaults are broadly reasonable as a starting point.
- `perHostFailureCutoff` is clamped to `>=1`; setting `0` cannot disable the feature, which is a rollback foot-gun.
- `hardTimeoutSeconds <= 0` falls back to `maxDurationSeconds`; if both are `0`, hard timeout effectively disables with no explicit warning.

## 3) Failure modes and reporting correctness

### Blocking issue A: host cutoff can be reported as FAILED (not SKIPPED)
- In `CareersDiscoveryService.probeVendor(...)`, host cutoff adds failure `discovery_host_failure_cutoff` and returns `null`.
- Final `DiscoveryOutcome` for that path is returned with `skipped=false`.
- `CareersDiscoveryRunService.runDiscovery(...)` treats it as `FAILED` unless `outcome.skipped()==true`.

Impact:
- Violates the intended behavior documented in PR (`skip further attempts`).
- Distorts `failedCount`, failure reason distribution, and run outcome quality.

### Blocking issue B: `hostFailureCutoffSkips` is undercounted
- Counter increments only in `CareersDiscoveryRunService` pre-check (`isHostFailureCutoffReached(company.domain(), ...)`).
- Vendor-probe host-cutoff events inside `CareersDiscoveryService.probeVendor(...)` do not increment this counter.

Impact:
- Reported `hostFailureCutoffSkips` is incomplete/inaccurate.

### Blocking issue C: request budget is not strict/enforced mid-company
- `requestBudget` check occurs at company-loop boundaries in `CareersDiscoveryRunService`.
- A single company can overshoot budget significantly before aborting.

Impact:
- Safety guardrail is best-effort, not hard-limit.

### Blocking issue D: request counting misses network paths
- `requestCountTotal` depends on `DiscoveryMetrics.requestsIssuedCount()` deltas.
- Several HTTP paths are not instrumented through this metric (notably sitemap fetches in `SitemapService.discover(...)` via direct `httpClient.get(...)`).

Impact:
- `request_count_total` can under-report, and request-budget abort can be delayed/never triggered despite high real traffic.

## 4) Naming/API surface concerns

### `AtsDiscoveryResult` semantics are ambiguous
- `validationStatus` is a free string (not enum) with mixed producer paths.
- `evidence` carries heterogeneous values (`careers_landing`, method names, `none`) without schema.
- `errorBucket` is derived differently depending on path (`reason_code` from persistence vs classifier at runtime).

Impact:
- Consumers cannot reliably reason about contract/stability.

### Duplication with existing fields
- `CareersDiscoveryCompanyResultView` now returns both legacy vendor/endpoint fields and nested `atsDiscoveryResult`; these can drift in meaning unless explicitly defined as source-of-truth.

## 5) Tests sufficiency

Added tests cover:
- detector/extractor happy paths for ICIMS/TALEO/SUCCESSFACTORS,
- ICIMS normalization dedupe,
- request-budget abort trigger in run service.

Missing/high-value tests:
- Host-failure-cutoff path should produce `SKIPPED` and increment `hostFailureCutoffSkips`.
- Budget accounting should include sitemap HTTP requests.
- Strictness test for mid-company request-budget exceed.
- Run-config consistency test: persisted guardrail values should equal enforced values.
- API contract tests for `AtsDiscoveryResult` value-set and compatibility.

## 6) Concrete change requests

1. **Make host-cutoff outcomes explicit skips.**
   - File: `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryService.java`
   - Change: when failure reason is `discovery_host_failure_cutoff`, return `DiscoveryOutcome(..., skipped=true, ...)` (or add a dedicated `skipReason` field in `DiscoveryOutcome`).
   - Why: aligns behavior/reporting with documented skip semantics.

2. **Accurately count host-cutoff skips from all paths.**
   - File: `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java`
   - Change: increment `hostFailureCutoffSkips` when mapped failure reason is host cutoff (not only pre-check branch).
   - Why: fixes undercount in run status.

3. **Enforce true run-level request/time budgets via HTTP budget context.**
   - Files:
     - `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java`
     - `backend/src/main/java/com/delta/jobtracker/crawl/http/CanaryHttpBudget.java`
   - Change: activate `CanaryHttpBudgetContext` for the entire discovery run with `maxTotalRequests=requestBudget` and `deadline=runStarted+hardTimeout`; surface abort reason mapping in run status.
   - Why: guarantees hard guardrails and catches in-company overshoot.

4. **Include sitemap requests in request metrics if context approach is not adopted.**
   - Files:
     - `backend/src/main/java/com/delta/jobtracker/crawl/sitemap/SitemapService.java`
     - `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryService.java`
   - Change: plumb a request counter callback / metrics hook into sitemap fetch loop.
   - Why: prevents under-reporting of `request_count_total`.

5. **Freeze guardrail values per run at start.**
   - File: `backend/src/main/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunService.java`
   - Change: pass `requestBudget`, `hardTimeoutSeconds`, and `hostFailureCutoff` captured in `startAsync(...)` into `runDiscovery(...)` parameters; do not re-read mutable properties mid-run.
   - Why: prevents config drift between persisted settings and actual enforcement.

6. **Allow explicit disable for host-failure cutoff.**
   - File: `backend/src/main/java/com/delta/jobtracker/config/CrawlerProperties.java`
   - Field: `perHostFailureCutoff`
   - Change: permit `0` as disabled (`Math.max(0, ...)`) and handle disabled branch in services.
   - Why: safer rollback/operational control.

7. **Clarify/normalize ATS discovery API contract.**
   - Files:
     - `backend/src/main/java/com/delta/jobtracker/crawl/model/AtsDiscoveryResult.java`
     - `backend/src/main/java/com/delta/jobtracker/crawl/model/CareersDiscoveryCompanyResultView.java`
   - Change: convert `validationStatus` to enum, define allowed `evidence` values centrally, and document whether legacy vendor fields or `atsDiscoveryResult` is canonical.
   - Why: avoid consumer ambiguity and drift.

8. **Add index for stop-reason analytics if queried by API/dashboard.**
   - File: `backend/src/main/resources/db/migration/V25__careers_discovery_guardrail_metrics.sql` (or new migration)
   - Change: add index like `(status, stop_reason, started_at DESC)` if this is a query pattern.
   - Why: prevents regressions as run volume scales.

9. **Harden migration for large-table deploys.**
   - File: `backend/src/main/resources/db/migration/V25__careers_discovery_guardrail_metrics.sql`
   - Change: document/version-gate Postgres behavior or split into phased migration (nullable add -> backfill -> not null) for low-lock rollout.
   - Why: reduce production migration risk.

10. **Add targeted regression tests for the above.**
    - Files:
      - `backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryRunServiceTest.java`
      - `backend/src/test/java/com/delta/jobtracker/crawl/service/CareersDiscoveryServiceTest.java` (new/expanded)
      - `backend/src/test/java/com/delta/jobtracker/crawl/persistence/CrawlJdbcRepository...` (run-status/reporting assertions)
    - Change: assert skip-vs-fail semantics, full request counting, stop-reason correctness, and frozen per-run guardrail values.
    - Why: catch regressions in guardrail behavior and reporting.

## Verdict

Request changes before merge, primarily on guardrail correctness/reporting (items 1-5).
