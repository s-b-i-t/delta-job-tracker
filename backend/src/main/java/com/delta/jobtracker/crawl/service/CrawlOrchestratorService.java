package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryAbortException;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class CrawlOrchestratorService {
    private static final Logger log = LoggerFactory.getLogger(CrawlOrchestratorService.class);

    private final CrawlJdbcRepository repository;
    private final CompanyCrawlerService companyCrawlerService;
    private final ExecutorService crawlExecutor;
    private final ExecutorService crawlRunExecutor;
    private final CrawlerProperties properties;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;

    public CrawlOrchestratorService(
        CrawlJdbcRepository repository,
        CompanyCrawlerService companyCrawlerService,
        @Qualifier("crawlExecutor") ExecutorService crawlExecutor,
        @Qualifier("crawlRunExecutor") ExecutorService crawlRunExecutor,
        CrawlerProperties properties,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService
    ) {
        this.repository = repository;
        this.companyCrawlerService = companyCrawlerService;
        this.crawlExecutor = crawlExecutor;
        this.crawlRunExecutor = crawlRunExecutor;
        this.properties = properties;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
    }

    public CrawlRunSummary run(CrawlRunRequest request) {
        ensureNoActiveRun();
        Instant startedAt = Instant.now();
        long crawlRunId = repository.insertCrawlRun(startedAt, "RUNNING", "crawl started");
        return runWithId(crawlRunId, startedAt, request);
    }

    public long startAsync(CrawlRunRequest request) {
        return startAsync(request, null);
    }

    public long startAsync(CrawlRunRequest request, Runnable preRun) {
        ensureNoActiveRun();
        Instant startedAt = Instant.now();
        long crawlRunId = repository.insertCrawlRun(startedAt, "RUNNING", "crawl started");
        crawlRunExecutor.submit(() -> {
            if (preRun != null) {
                try {
                    preRun.run();
                } catch (Exception e) {
                    log.warn("Pre-run hook failed for crawl {}", crawlRunId, e);
                }
            }
            runWithId(crawlRunId, startedAt, request);
        });
        return crawlRunId;
    }

    private CrawlRunSummary runWithId(long crawlRunId, Instant startedAt, CrawlRunRequest request) {
        Instant finishedAt = null;
        String status = "FAILED";
        String notes = "crawl_failed";
        List<CompanyCrawlSummary> summaries = List.of();
        CanaryHttpBudget budget = buildRunBudget(startedAt);

        ScheduledExecutorService heartbeat = Executors.newSingleThreadScheduledExecutor();
        heartbeat.scheduleAtFixedRate(
            () -> {
                try {
                    repository.updateCrawlRunHeartbeat(crawlRunId, Instant.now());
                } catch (Exception ignored) {
                    // Ignore heartbeat failures to keep crawl running.
                }
            },
            0,
            properties.getCrawlHeartbeatSeconds(),
            TimeUnit.SECONDS
        );

        try (CanaryHttpBudgetContext.Scope scope = budget == null ? null : CanaryHttpBudgetContext.activate(budget)) {
            List<String> tickers = request.normalizedTickers();
            int companyLimit = request.companyLimit() == null
                ? properties.getApi().getDefaultCompanyLimit()
                : Math.max(1, request.companyLimit());
            int resolveLimit = request.resolveLimit() == null
                ? properties.getAutomation().getResolveLimit()
                : Math.max(1, request.resolveLimit());
            int discoverLimit = request.discoverLimit() == null
                ? properties.getAutomation().getDiscoverLimit()
                : Math.max(1, request.discoverLimit());
            Instant deadline = budget == null ? null : startedAt.plusSeconds(properties.getRun().getMaxDurationSeconds());

            if (shouldResolveDomains(request)) {
                DomainResolutionResult resolution = tickers.isEmpty()
                    ? domainResolutionService.resolveMissingDomains(resolveLimit, deadline)
                    : domainResolutionService.resolveMissingDomainsForTickers(tickers, resolveLimit, deadline);
                log.info(
                    "Domain resolver before crawl: resolved={} no_wikipedia_title={} no_item={} no_p856={} wdqs_error={}",
                    resolution.resolvedCount(),
                    resolution.noWikipediaTitleCount(),
                    resolution.noItemCount(),
                    resolution.noP856Count(),
                    resolution.wdqsErrorCount()
                );
                if (budget != null) {
                    budget.checkDeadline();
                }
            }
            if (shouldDiscoverCareers(request)) {
                CareersDiscoveryResult discovery = tickers.isEmpty()
                    ? careersDiscoveryService.discover(discoverLimit, deadline)
                    : careersDiscoveryService.discoverForTickers(tickers, discoverLimit, deadline, false);
                log.info("Careers discovery before crawl: discovered={}, failed={}", discovery.discoveredCountByAtsType(), discovery.failedCount());
                if (budget != null) {
                    budget.checkDeadline();
                }
            }

            List<CompanyTarget> targets = selectTargets(request, tickers, companyLimit);
            int attempted = targets.size();
            int succeeded = 0;
            int failed = 0;
            int jobsExtracted = 0;
            repository.updateCrawlRunProgress(crawlRunId, attempted, succeeded, failed, jobsExtracted, Instant.now());

            if (targets.isEmpty()) {
                status = "NO_TARGETS";
                notes = "No company domains matched";
                summaries = List.of();
            } else {
                List<CompletableFuture<CompanyCrawlSummary>> futures = new ArrayList<>();
                for (CompanyTarget target : targets) {
                    futures.add(CompletableFuture.supplyAsync(
                        () -> {
                            try (CanaryHttpBudgetContext.Scope innerScope = budget == null
                                ? null
                                : CanaryHttpBudgetContext.activate(budget)) {
                                return companyCrawlerService.crawlCompany(crawlRunId, target, request);
                            }
                        },
                        crawlExecutor
                    ));
                }

                List<CompanyCrawlSummary> runSummaries = new ArrayList<>();
                boolean hadErrors = false;
                for (int i = 0; i < futures.size(); i++) {
                    CompanyTarget target = targets.get(i);
                    try {
                        if (budget != null) {
                            budget.checkDeadline();
                        }
                        CompanyCrawlSummary summary = futures.get(i).join();
                        runSummaries.add(summary);
                        jobsExtracted += summary.jobsExtractedCount();
                        if (summary.closeoutSafe()) {
                            succeeded++;
                            repository.markPostingsInactiveNotSeenInRun(target.companyId(), crawlRunId);
                        } else {
                            failed++;
                        }
                    } catch (CompletionException e) {
                        Throwable cause = e.getCause();
                        if (cause instanceof CanaryAbortException abortException) {
                            throw abortException;
                        }
                        hadErrors = true;
                        failed++;
                        log.warn("Company crawl failed for {} ({})", target.ticker(), target.domain(), e);
                        Map<String, Integer> errorMap = new LinkedHashMap<>();
                        errorMap.put("company_crawl_exception", 1);
                        runSummaries.add(new CompanyCrawlSummary(
                            target.companyId(),
                            target.ticker(),
                            target.domain(),
                            0,
                            0,
                            List.of(),
                            0,
                            0,
                            false,
                            errorMap
                        ));
                    } catch (CanaryAbortException e) {
                        throw e;
                    } catch (Exception e) {
                        hadErrors = true;
                        failed++;
                        log.warn("Company crawl failed for {} ({})", target.ticker(), target.domain(), e);
                        Map<String, Integer> errorMap = new LinkedHashMap<>();
                        errorMap.put("company_crawl_exception", 1);
                        runSummaries.add(new CompanyCrawlSummary(
                            target.companyId(),
                            target.ticker(),
                            target.domain(),
                            0,
                            0,
                            List.of(),
                            0,
                            0,
                            false,
                            errorMap
                        ));
                    }
                    repository.updateCrawlRunProgress(crawlRunId, attempted, succeeded, failed, jobsExtracted, Instant.now());
                }

                summaries = runSummaries;
                status = hadErrors ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
                notes = "companies=" + summaries.size();
            }
        } catch (CanaryAbortException e) {
            log.info("Crawl run {} aborted: {}", crawlRunId, e.getMessage());
            status = "ABORTED";
            notes = e.getMessage();
        } catch (Exception e) {
            log.warn("Crawl run {} failed", crawlRunId, e);
            status = "FAILED";
            notes = "exception=" + e.getClass().getSimpleName();
        } finally {
            finishedAt = Instant.now();
            repository.completeCrawlRun(crawlRunId, finishedAt, status, notes);
            heartbeat.shutdownNow();
        }
        return new CrawlRunSummary(crawlRunId, startedAt, finishedAt, status, summaries);
    }

    private CanaryHttpBudget buildRunBudget(Instant startedAt) {
        int maxDurationSeconds = properties.getRun().getMaxDurationSeconds();
        if (maxDurationSeconds <= 0) {
            return null;
        }
        int maxAttempts = Math.max(1, 1 + properties.getRequestMaxRetries());
        return new CanaryHttpBudget(
            0,
            0,
            0.0,
            1,
            0,
            maxAttempts,
            properties.getRequestTimeoutSeconds(),
            startedAt.plusSeconds(maxDurationSeconds)
        );
    }

    public List<CompanyTarget> previewTargets(CrawlRunRequest request) {
        List<String> tickers = request.normalizedTickers();
        int companyLimit = request.companyLimit() == null
            ? properties.getApi().getDefaultCompanyLimit()
            : Math.max(1, request.companyLimit());
        return selectTargets(request, tickers, companyLimit);
    }

    private boolean shouldResolveDomains(CrawlRunRequest request) {
        if (request.resolveDomains() != null) {
            return request.resolveDomains();
        }
        return properties.getAutomation().isResolveMissingDomains();
    }

    private boolean shouldDiscoverCareers(CrawlRunRequest request) {
        if (request.discoverCareers() != null) {
            return request.discoverCareers();
        }
        return properties.getAutomation().isDiscoverCareersEndpoints();
    }

    private void ensureNoActiveRun() {
        int activeMinutes = properties.getActiveRunMinutes();
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(activeMinutes));
        List<CrawlRunMeta> running = repository.findRunningCrawlRuns();
        for (CrawlRunMeta run : running) {
            Instant lastActivity = repository.findLastActivityAtForRun(run.crawlRunId());
            com.delta.jobtracker.crawl.model.CrawlRunStatus status = repository.findCrawlRunStatus(run.crawlRunId());
            Instant lastHeartbeat = status == null ? null : status.lastHeartbeatAt();
            boolean active = run.startedAt().isAfter(cutoff)
                || (lastActivity != null && lastActivity.isAfter(cutoff))
                || (lastHeartbeat != null && lastHeartbeat.isAfter(cutoff));
            if (active) {
                String message = "Active crawl run in progress (id=" + run.crawlRunId()
                    + ", startedAt=" + run.startedAt()
                    + ", lastActivityAt=" + (lastActivity == null ? "none" : lastActivity) + ")";
                throw new ActiveCrawlRunException(message);
            }
        }
    }

    private List<CompanyTarget> selectTargets(CrawlRunRequest request, List<String> tickers, int companyLimit) {
        boolean atsOnly = request.atsOnly() != null ? request.atsOnly() : tickers.isEmpty();
        if (!tickers.isEmpty()) {
            return atsOnly
                ? repository.findCompanyTargetsWithAts(tickers, companyLimit)
                : repository.findCompanyTargets(tickers, companyLimit);
        }
        if (!atsOnly) {
            return repository.findCompanyTargets(tickers, companyLimit);
        }
        List<CompanyTarget> recentTargets = new ArrayList<>();
        if (request.atsDetectedSince() != null) {
            recentTargets = repository.findCompanyTargetsWithAtsDetectedSince(tickers, companyLimit, request.atsDetectedSince());
        }
        if (recentTargets.size() >= companyLimit) {
            return recentTargets;
        }
        List<CompanyTarget> fallbackTargets = repository.findCompanyTargetsWithAts(tickers, companyLimit);
        Map<Long, CompanyTarget> merged = new LinkedHashMap<>();
        for (CompanyTarget target : recentTargets) {
            merged.put(target.companyId(), target);
        }
        for (CompanyTarget target : fallbackTargets) {
            merged.putIfAbsent(target.companyId(), target);
            if (merged.size() >= companyLimit) {
                break;
            }
        }
        return new ArrayList<>(merged.values());
    }
}
