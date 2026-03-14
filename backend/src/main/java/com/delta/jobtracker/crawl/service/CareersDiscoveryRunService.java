package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryAbortException;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyFailureView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyResultView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryFailuresResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunStatus;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class CareersDiscoveryRunService {
  private static final Logger log = LoggerFactory.getLogger(CareersDiscoveryRunService.class);
  private static final int DEFAULT_BATCH_SIZE = 25;

  private final CrawlJdbcRepository repository;
  private final CareersDiscoveryService discoveryService;
  private final ExecutorService discoveryExecutor;
  private final CrawlerProperties properties;

  public CareersDiscoveryRunService(
      CrawlJdbcRepository repository,
      CareersDiscoveryService discoveryService,
      @Qualifier("discoveryExecutor") ExecutorService discoveryExecutor,
      CrawlerProperties properties) {
    this.repository = repository;
    this.discoveryService = discoveryService;
    this.discoveryExecutor = discoveryExecutor;
    this.properties = properties;
  }

  public CareersDiscoveryRunResponse startAsync(
      Integer limit, Integer batchSize, Boolean vendorProbeOnly) {
    int companyLimit =
        limit == null ? properties.getCareersDiscovery().getDefaultLimit() : Math.max(1, limit);
    int safeBatchSize = batchSize == null ? DEFAULT_BATCH_SIZE : Math.max(1, batchSize);
    boolean vendorProbeOnlyMode = vendorProbeOnly != null && vendorProbeOnly;

    int selectionEligibleCount =
        repository.countCompaniesWithDomainWithoutAtsEligible(vendorProbeOnlyMode);
    List<CompanyTarget> companies =
        repository.findCompaniesWithDomainWithoutAts(companyLimit, vendorProbeOnlyMode);
    int selectionReturnedCount = companies == null ? 0 : companies.size();
    int companiesInputCount = selectionReturnedCount;
    int requestBudget = properties.getCareersDiscovery().getGlobalRequestBudgetPerRun();
    int configuredHardTimeoutSeconds = properties.getCareersDiscovery().getHardTimeoutSeconds();
    if (configuredHardTimeoutSeconds <= 0) {
      configuredHardTimeoutSeconds = properties.getCareersDiscovery().getMaxDurationSeconds();
    }
    final int hardTimeoutSeconds = configuredHardTimeoutSeconds;
    int hostFailureCutoff = properties.getCareersDiscovery().getPerHostFailureCutoff();
    long runId =
        repository.insertCareersDiscoveryRun(
            companyLimit,
            selectionEligibleCount,
            selectionReturnedCount,
            companiesInputCount,
            requestBudget,
            hardTimeoutSeconds,
            hostFailureCutoff);
    discoveryExecutor.submit(
        () ->
            runDiscovery(
                runId,
                companies,
                safeBatchSize,
                vendorProbeOnlyMode,
                requestBudget,
                hardTimeoutSeconds,
                hostFailureCutoff));
    return new CareersDiscoveryRunResponse(runId, "RUNNING", "/api/careers/discover/run/" + runId);
  }

  public CareersDiscoveryRunStatus getRunStatus(long runId) {
    CareersDiscoveryRunStatus status = repository.findCareersDiscoveryRun(runId);
    if (status == null) {
      return null;
    }
    Map<String, Long> failuresByReason = repository.countCareersDiscoveryCompanyFailures(runId);
    return withFailures(
        status,
        failuresByReason,
        repository.countCareersDiscoveryRunFunnels(runId),
        repository.countCareersDiscoveryStageFailures(runId));
  }

  public CareersDiscoveryRunStatus getLatestRunStatus() {
    CareersDiscoveryRunStatus status = repository.findLatestCareersDiscoveryRun();
    if (status == null) {
      return null;
    }
    Map<String, Long> failuresByReason =
        repository.countCareersDiscoveryCompanyFailures(status.discoveryRunId());
    return withFailures(
        status,
        failuresByReason,
        repository.countCareersDiscoveryRunFunnels(status.discoveryRunId()),
        repository.countCareersDiscoveryStageFailures(status.discoveryRunId()));
  }

  public List<CareersDiscoveryCompanyResultView> getCompanyResults(
      long runId, String status, Integer limit) {
    int safeLimit = limit == null ? 200 : Math.max(1, Math.min(limit, 500));
    return repository.findCareersDiscoveryCompanyResults(runId, normalizeStatus(status), safeLimit);
  }

  public CareersDiscoveryFailuresResponse getFailures(long runId) {
    Map<String, Long> counts = repository.countCareersDiscoveryCompanyFailures(runId);
    List<CareersDiscoveryCompanyFailureView> failures =
        repository.findRecentCareersDiscoveryCompanyFailures(runId, 20);
    return new CareersDiscoveryFailuresResponse(counts, failures);
  }

  private void runDiscovery(
      long runId,
      List<CompanyTarget> companies,
      int batchSize,
      boolean vendorProbeOnly,
      int requestBudget,
      int hardTimeoutSeconds,
      int hostFailureCutoff) {
    int processed = 0;
    int succeeded = 0;
    int failed = 0;
    int endpointsAdded = 0;
    int endpointsPromoted = 0;
    int endpointsConfirmed = 0;
    String lastError = null;
    Instant runStarted = Instant.now();
    int companiesConsidered = 0;
    int companiesAttemptedCount = 0;
    int cachedSkipCount = 0;
    int requestCountTotal = 0;
    int hostFailureCutoffSkips = 0;
    boolean aborted = false;
    String abortReason = null;
    Instant deadline = hardTimeoutSeconds > 0 ? runStarted.plusSeconds(hardTimeoutSeconds) : null;
    CareersDiscoveryService.DiscoveryMetrics metrics =
        new CareersDiscoveryService.DiscoveryMetrics();
    CareersDiscoveryService.VendorProbeLimiter vendorProbeLimiter = buildVendorProbeLimiter();

    try {
      List<List<CompanyTarget>> batches = chunk(companies, batchSize);
      for (List<CompanyTarget> batch : batches) {
        for (CompanyTarget company : batch) {
          if (deadline != null && Instant.now().isAfter(deadline)) {
            aborted = true;
            abortReason = "time_budget_exceeded";
            metrics.incrementTimeBudgetExceeded();
            break;
          }
          if (requestBudget > 0 && requestCountTotal >= requestBudget) {
            aborted = true;
            abortReason = "request_budget_exceeded";
            break;
          }
          Instant startedAt = Instant.now();
          String status = "FAILED";
          String reasonCode = ReasonCodeClassifier.UNKNOWN;
          String stage = "PAGE_SCAN";
          int foundEndpoints = 0;
          Integer httpStatus = null;
          String errorDetail = null;
          CareersDiscoveryService.DiscoveryOutcome outcome = null;
          companiesConsidered++;

          try {
            if (company.domain() == null || company.domain().isBlank()) {
              status = "SKIPPED";
              reasonCode = ReasonCodeClassifier.NO_DOMAIN;
              stage = "DOMAIN";
            } else if (isHostFailureCutoffReached(company, hostFailureCutoff)) {
              status = "SKIPPED";
              reasonCode = ReasonCodeClassifier.HOST_COOLDOWN;
              stage = "COOLDOWN";
              errorDetail = "host_failure_cutoff_exceeded";
              cachedSkipCount++;
              hostFailureCutoffSkips++;
            } else {
              companiesAttemptedCount++;
              int beforeCount = repository.countAtsEndpointsForCompany(company.companyId());
              int requestsBefore = metrics.requestsIssuedCount();
              boolean budgetAbort = false;
              CanaryHttpBudget runBudget = buildRunBudget(requestBudget, requestCountTotal, deadline);
              try (CanaryHttpBudgetContext.Scope scope = CanaryHttpBudgetContext.activate(runBudget)) {
                outcome =
                    discoveryService.discoverForCompany(
                        company,
                        deadline,
                        metrics,
                        vendorProbeLimiter,
                        vendorProbeOnly,
                        hostFailureCutoff);
              } catch (CanaryAbortException budgetAbortException) {
                String mappedAbortReason = mapRunAbortReason(budgetAbortException.getMessage());
                if (mappedAbortReason == null) {
                  throw budgetAbortException;
                }
                aborted = true;
                abortReason = mappedAbortReason;
                reasonCode = ReasonCodeClassifier.TIMEOUT;
                stage = "BUDGET";
                errorDetail = mappedAbortReason;
                budgetAbort = true;
              }
              int requestDelta = Math.max(0, runBudget.totalRequests());
              if (requestDelta == 0) {
                requestDelta = Math.max(0, metrics.requestsIssuedCount() - requestsBefore);
              }
              if (requestDelta == 0
                  && outcome != null
                  && outcome.funnel() != null
                  && outcome.funnel().requestCount() > 0) {
                requestDelta = outcome.funnel().requestCount();
              }
              requestCountTotal += Math.max(0, requestDelta);
              if (outcome != null && outcome.funnel() != null) {
                endpointsPromoted += Math.max(0, outcome.funnel().endpointsPromoted());
                endpointsConfirmed += Math.max(0, outcome.funnel().endpointsConfirmed());
              }
              int afterCount = repository.countAtsEndpointsForCompany(company.companyId());
              endpointsAdded += Math.max(0, afterCount - beforeCount);
              if (outcome != null && outcome.countsByType() != null) {
                for (Integer count : outcome.countsByType().values()) {
                  foundEndpoints += count == null ? 0 : count;
                }
              }

              if (budgetAbort) {
                persistBudgetAbortState(company);
                // Keep the explicit budget-abort classification from the catch block.
              } else if (outcome != null && outcome.hasEndpoints()) {
                status = "SUCCEEDED";
                reasonCode = null;
                stage = "ATS_DETECTED";
              } else if (outcome != null && outcome.skipped()) {
                cachedSkipCount++;
                status = "SKIPPED";
                CareersDiscoveryService.DiscoveryFailure failure = outcome.primaryFailure();
                FailureMapping mapping = mapFailure(failure);
                reasonCode = mapping.reasonCode();
                stage = mapping.stage();
                httpStatus = mapping.httpStatus();
                errorDetail = mapping.errorDetail();
                if (isHostFailureCutoffReason(failure)) {
                  hostFailureCutoffSkips++;
                }
              } else {
                CareersDiscoveryService.DiscoveryFailure failure =
                    outcome == null ? null : outcome.primaryFailure();
                FailureMapping mapping = mapFailure(failure);
                reasonCode = mapping.reasonCode();
                stage = mapping.stage();
                httpStatus = mapping.httpStatus();
                errorDetail = mapping.errorDetail();
                if (isHostFailureCutoffReason(failure)) {
                  status = "SKIPPED";
                  cachedSkipCount++;
                  hostFailureCutoffSkips++;
                }
                if (outcome != null && outcome.timeBudgetExceeded()) {
                  aborted = true;
                  abortReason = "time_budget_exceeded";
                }
              }
              if (requestBudget > 0 && requestCountTotal >= requestBudget) {
                aborted = true;
                abortReason = "request_budget_exceeded";
              }
            }
          } catch (Exception e) {
            lastError = e.getMessage();
            reasonCode = ReasonCodeClassifier.UNKNOWN;
            stage = "PAGE_SCAN";
            errorDetail = e.getMessage();
            log.warn(
                "Careers discovery run {} failed for {} ({})",
                runId,
                company.ticker(),
                company.domain(),
                e);
          }

          long durationMs = Duration.between(startedAt, Instant.now()).toMillis();
          repository.upsertCareersDiscoveryCompanyResult(
              runId,
              company.companyId(),
              status,
              reasonCode,
              stage,
              foundEndpoints,
              durationMs,
              httpStatus,
              errorDetail,
              outcome != null && outcome.funnel() != null && outcome.funnel().careersUrlFound(),
              outcome == null || outcome.funnel() == null
                  ? null
                  : outcome.funnel().careersUrlInitial(),
              outcome == null || outcome.funnel() == null
                  ? null
                  : outcome.funnel().careersUrlFinal(),
              outcome == null || outcome.funnel() == null
                  ? null
                  : outcome.funnel().careersDiscoveryMethod(),
              outcome == null || outcome.funnel() == null
                  ? null
                  : outcome.funnel().careersDiscoveryStageFailure(),
              outcome != null && outcome.funnel() != null && outcome.funnel().vendorDetected(),
              outcome == null || outcome.funnel() == null ? null : outcome.funnel().vendorName(),
              outcome != null && outcome.funnel() != null && outcome.funnel().endpointExtracted(),
              outcome == null || outcome.funnel() == null ? null : outcome.funnel().endpointUrl(),
              outcome == null || outcome.funnel() == null ? 0 : outcome.funnel().endpointsPromoted(),
              outcome == null || outcome.funnel() == null ? 0 : outcome.funnel().endpointsConfirmed(),
              outcome == null || outcome.funnel() == null
                  ? null
                  : outcome.funnel().httpStatusFirstFailure(),
              outcome == null || outcome.funnel() == null ? null : outcome.funnel().requestCount());
          processed++;
          if ("SUCCEEDED".equals(status)) {
            succeeded++;
          } else if ("FAILED".equals(status)) {
            failed++;
          }
          repository.updateCareersDiscoveryRunProgress(
              runId,
              processed,
              succeeded,
              failed,
              endpointsAdded,
              endpointsPromoted,
              endpointsConfirmed,
              lastError,
              companiesConsidered,
              companiesAttemptedCount,
              cachedSkipCount,
              metrics.homepageScanned(),
              toAtsTypeMap(metrics.endpointsFoundHomepage()),
              toAtsTypeMap(metrics.endpointsFoundVendorProbe()),
              metrics.sitemapsScanned(),
              metrics.sitemapUrlsChecked(),
              toAtsTypeMap(metrics.endpointsFoundSitemap()),
              metrics.careersPathsChecked(),
              metrics.robotsBlockedCount(),
              metrics.fetchFailedCount(),
              metrics.timeBudgetExceededCount(),
              requestCountTotal,
              hostFailureCutoffSkips);
          if (aborted) {
            break;
          }
        }
        if (aborted) {
          break;
        }
      }

      if (aborted && processed == 0) {
        repository.updateCareersDiscoveryRunProgress(
            runId,
            processed,
            succeeded,
            failed,
            endpointsAdded,
            endpointsPromoted,
            endpointsConfirmed,
            abortReason,
            companiesConsidered,
            companiesAttemptedCount,
            cachedSkipCount,
            metrics.homepageScanned(),
            toAtsTypeMap(metrics.endpointsFoundHomepage()),
            toAtsTypeMap(metrics.endpointsFoundVendorProbe()),
            metrics.sitemapsScanned(),
            metrics.sitemapUrlsChecked(),
            toAtsTypeMap(metrics.endpointsFoundSitemap()),
            metrics.careersPathsChecked(),
            metrics.robotsBlockedCount(),
            metrics.fetchFailedCount(),
            metrics.timeBudgetExceededCount(),
            requestCountTotal,
            hostFailureCutoffSkips);
      }
      if (aborted) {
        repository.completeCareersDiscoveryRun(
            runId, Instant.now(), "ABORTED", abortReason, abortReason);
      } else {
        repository.completeCareersDiscoveryRun(runId, Instant.now(), "SUCCEEDED", lastError);
      }
    } catch (Exception e) {
      lastError = e.getMessage();
      repository.completeCareersDiscoveryRun(
          runId, Instant.now(), "FAILED", lastError, "exception");
      log.warn(
          "Careers discovery run {} failed after {} ms",
          runId,
          Duration.between(runStarted, Instant.now()).toMillis(),
          e);
    }
  }

  private FailureMapping mapFailure(CareersDiscoveryService.DiscoveryFailure failure) {
    if (failure == null) {
      return new FailureMapping(ReasonCodeClassifier.ATS_NOT_FOUND, "PAGE_SCAN", null, null);
    }
    String reason = failure.reasonCode();
    String detail = failure.detail();
    if ("discovery_homepage_blocked_by_robots".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.ROBOTS_BLOCKED, "HOMEPAGE", null, detail);
    }
    if ("discovery_sitemap_blocked_by_robots".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.ROBOTS_BLOCKED, "SITEMAP", null, detail);
    }
    if ("discovery_careers_blocked_by_robots".equals(reason)
        || "discovery_blocked_by_robots".equals(reason)) {
      return new FailureMapping(
          ReasonCodeClassifier.ROBOTS_BLOCKED, "ROBOTS_SITEMAP", null, detail);
    }
    if ("discovery_sitemap_fetch_failed".equals(reason)
        || "discovery_sitemap_no_urls".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.SITEMAP_NOT_FOUND, "SITEMAP", null, detail);
    }
    if ("discovery_homepage_too_large".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.PARSING_FAILED, "HOMEPAGE", null, detail);
    }
    if ("discovery_fetch_failed".equals(reason)) {
      Integer httpStatus = ReasonCodeClassifier.parseHttpStatus(detail);
      String mapped =
          httpStatus != null
              ? ReasonCodeClassifier.fromHttpStatus(httpStatus)
              : ReasonCodeClassifier.fromErrorKey(detail);
      return new FailureMapping(mapped, "PAGE_SCAN", httpStatus, detail);
    }
    if ("discovery_host_cooldown".equals(reason) || "discovery_company_cooldown".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.HOST_COOLDOWN, "COOLDOWN", null, detail);
    }
    if ("discovery_host_failure_cutoff".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.HOST_COOLDOWN, "COOLDOWN", null, detail);
    }
    if ("discovery_time_budget_exceeded".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.TIMEOUT, "BUDGET", null, detail);
    }
    if ("discovery_no_domain".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.NO_DOMAIN, "DOMAIN", null, detail);
    }
    if ("discovery_ats_detected_no_endpoint".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.ATS_NOT_FOUND, "ATS_DETECTED", null, detail);
    }
    if ("discovery_no_match".equals(reason)) {
      return new FailureMapping(ReasonCodeClassifier.ATS_NOT_FOUND, "PAGE_SCAN", null, detail);
    }
    return new FailureMapping(ReasonCodeClassifier.UNKNOWN, "PAGE_SCAN", null, detail);
  }

  private String normalizeStatus(String status) {
    if (status == null || status.isBlank()) {
      return null;
    }
    return status.trim().toUpperCase();
  }

  private List<List<CompanyTarget>> chunk(List<CompanyTarget> companies, int size) {
    List<List<CompanyTarget>> batches = new ArrayList<>();
    if (companies == null || companies.isEmpty()) {
      return batches;
    }
    int idx = 0;
    while (idx < companies.size()) {
      int end = Math.min(companies.size(), idx + size);
      batches.add(companies.subList(idx, end));
      idx = end;
    }
    return batches;
  }

  private CareersDiscoveryRunStatus withFailures(
      CareersDiscoveryRunStatus status,
      Map<String, Long> failuresByReason,
      CrawlJdbcRepository.CareersDiscoveryRunFunnelCounts funnelCounts,
      Map<String, Long> careersStageFailuresByReason) {
    return new CareersDiscoveryRunStatus(
        status.discoveryRunId(),
        status.startedAt(),
        status.finishedAt(),
        status.status(),
        status.companyLimit(),
        status.selectionEligibleCount(),
        status.selectionReturnedCount(),
        status.companiesInputCount(),
        status.companiesAttemptedCount(),
        status.cachedSkipCount(),
        status.processedCount(),
        status.succeededCount(),
        status.failedCount(),
        status.endpointsAdded(),
        status.endpointsPromoted(),
        status.endpointsConfirmed(),
        status.lastError(),
        status.companiesConsidered(),
        status.homepageScanned(),
        status.endpointsFoundHomepageByAtsType(),
        status.endpointsFoundVendorProbeByAtsType(),
        status.sitemapsScanned(),
        status.sitemapUrlsChecked(),
        status.endpointsFoundSitemapByAtsType(),
        status.careersPathsChecked(),
        status.robotsBlockedCount(),
        status.fetchFailedCount(),
        status.timeBudgetExceededCount(),
        failuresByReason == null ? Map.of() : failuresByReason,
        funnelCounts == null ? 0 : funnelCounts.careersUrlFoundCount(),
        funnelCounts == null ? 0 : funnelCounts.vendorDetectedCount(),
        funnelCounts == null ? 0 : funnelCounts.endpointExtractedCount(),
        careersStageFailuresByReason == null ? Map.of() : careersStageFailuresByReason,
        status.requestBudget(),
        status.requestCountTotal(),
        status.hardTimeoutSeconds(),
        status.stopReason(),
        status.hostFailureCutoffCount(),
        status.hostFailureCutoffSkips());
  }

  private CareersDiscoveryService.VendorProbeLimiter buildVendorProbeLimiter() {
    int maxPerHost = properties.getCareersDiscovery().getMaxVendorProbeRequestsPerHost();
    if (maxPerHost <= 0) {
      return null;
    }
    Map<String, java.util.concurrent.atomic.AtomicInteger> counts =
        new java.util.concurrent.ConcurrentHashMap<>();
    return host -> {
      if (host == null || host.isBlank()) {
        return false;
      }
      java.util.concurrent.atomic.AtomicInteger counter =
          counts.computeIfAbsent(host, ignored -> new java.util.concurrent.atomic.AtomicInteger(0));
      return counter.incrementAndGet() <= maxPerHost;
    };
  }

  private Map<String, Integer> toAtsTypeMap(Map<AtsType, Integer> source) {
    Map<String, Integer> mapped = new LinkedHashMap<>();
    if (source == null) {
      return mapped;
    }
    for (Map.Entry<AtsType, Integer> entry : source.entrySet()) {
      if (entry.getKey() != null) {
        mapped.put(entry.getKey().name(), entry.getValue());
      }
    }
    return mapped;
  }

  private CanaryHttpBudget buildRunBudget(
      int requestBudget, int requestCountTotal, Instant deadline) {
    int remainingRequestBudget =
        requestBudget > 0 ? Math.max(0, requestBudget - requestCountTotal) : 0;
    int maxAttemptsPerRequest = Math.max(1, 1 + properties.getRequestMaxRetries());
    return new CanaryHttpBudget(
        0,
        remainingRequestBudget,
        0.0,
        1,
        0,
        maxAttemptsPerRequest,
        properties.getRequestTimeoutSeconds(),
        deadline);
  }

  private String mapRunAbortReason(String budgetAbortReason) {
    if (budgetAbortReason == null || budgetAbortReason.isBlank()) {
      return null;
    }
    String lower = budgetAbortReason.toLowerCase(Locale.ROOT);
    if (lower.contains("total_request_budget_exceeded")) {
      return "request_budget_exceeded";
    }
    if (lower.contains("canary_time_budget_exceeded")) {
      return "time_budget_exceeded";
    }
    return null;
  }

  private void persistBudgetAbortState(CompanyTarget company) {
    if (company == null) {
      return;
    }
    com.delta.jobtracker.crawl.model.CareersDiscoveryState existing =
        repository.findCareersDiscoveryState(company.companyId());
    int nextFailures = (existing == null ? 0 : existing.consecutiveFailures()) + 1;
    repository.upsertCareersDiscoveryState(
        company.companyId(), Instant.now(), "discovery_time_budget_exceeded", null, nextFailures, null);
  }

  private boolean isHostFailureCutoffReached(CompanyTarget company, int cutoff) {
    if (company == null || company.domain() == null || company.domain().isBlank()) {
      return false;
    }
    try {
      com.delta.jobtracker.crawl.model.HostCrawlState hostState =
          repository.findHostCrawlState(company.domain().trim().toLowerCase());
      return hostState != null && hostState.consecutiveFailures() >= Math.max(1, cutoff);
    } catch (Exception ignored) {
      return false;
    }
  }

  private boolean isHostFailureCutoffReason(CareersDiscoveryService.DiscoveryFailure failure) {
    return failure != null && "discovery_host_failure_cutoff".equals(failure.reasonCode());
  }

  private record FailureMapping(
      String reasonCode, String stage, Integer httpStatus, String errorDetail) {}
}
