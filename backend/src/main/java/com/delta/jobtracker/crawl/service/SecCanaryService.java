package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryAbortException;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.SecCanarySummary;
import com.delta.jobtracker.crawl.model.SecIngestionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class SecCanaryService {
    private static final Logger log = LoggerFactory.getLogger(SecCanaryService.class);

    private final UniverseIngestionService ingestionService;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;
    private final CompanyCrawlerService companyCrawlerService;
    private final CrawlJdbcRepository repository;
    private final CrawlerProperties properties;

    public SecCanaryService(
        UniverseIngestionService ingestionService,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService,
        CompanyCrawlerService companyCrawlerService,
        CrawlJdbcRepository repository,
        CrawlerProperties properties
    ) {
        this.ingestionService = ingestionService;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
        this.companyCrawlerService = companyCrawlerService;
        this.repository = repository;
        this.properties = properties;
    }

    public SecCanarySummary runSecCanary(Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getCanary().getDefaultLimit()
            : Math.max(1, requestedLimit);
        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(properties.getCanary().getMaxDurationSeconds());
        Map<String, Long> stepDurationsMs = new LinkedHashMap<>();
        Map<String, Integer> topErrors = new LinkedHashMap<>();

        String status = "COMPLETED";
        String abortReason = null;
        Long crawlRunId = null;
        int companiesIngested = 0;
        int jobsExtracted = 0;
        DomainResolutionResult domainResolution = new DomainResolutionResult(0, 0, 0, 0, 0, List.of());
        CareersDiscoveryResult careersDiscovery = new CareersDiscoveryResult(Map.of(), 0, Map.of());
        Map<String, Integer> companiesCrawledByAtsType = Map.of();

        CanaryHttpBudget budget = new CanaryHttpBudget(
            properties.getCanary().getMaxRequestsPerHost(),
            properties.getCanary().getMaxTotalRequests(),
            properties.getCanary().getMax429Rate(),
            properties.getCanary().getMinRequestsFor429Rate(),
            properties.getCanary().getMaxConsecutiveErrors(),
            properties.getCanary().getMaxAttemptsPerRequest(),
            properties.getCanary().getRequestTimeoutSeconds()
        );

        try (CanaryHttpBudgetContext.Scope scope = CanaryHttpBudgetContext.activate(budget)) {
            Instant ingestStart = Instant.now();
            SecIngestionResult ingestionResult = ingestionService.ingestSecCompanies(limit);
            stepDurationsMs.put("ingestSec", Duration.between(ingestStart, Instant.now()).toMillis());
            List<String> tickers = ingestionResult.tickers();
            companiesIngested = tickers.size();
            if (!ingestionResult.sampleErrors().isEmpty()) {
                topErrors.put("ingest_errors", ingestionResult.errorCount());
            }

            if (tickers.isEmpty()) {
                status = "NO_COMPANIES";
            } else if (deadlineExceeded(deadline)) {
                status = "ABORTED";
                abortReason = "time_budget_exceeded";
            } else {
                Instant domainStart = Instant.now();
                domainResolution = domainResolutionService.resolveMissingDomainsForTickers(tickers, tickers.size(), deadline);
                stepDurationsMs.put("resolveDomains", Duration.between(domainStart, Instant.now()).toMillis());
                if (deadlineExceeded(deadline)) {
                    status = "ABORTED";
                    abortReason = "time_budget_exceeded";
                } else {
                    Instant discoveryStart = Instant.now();
                    careersDiscovery = careersDiscoveryService.discoverForTickers(tickers, tickers.size(), deadline);
                    stepDurationsMs.put("discoverCareers", Duration.between(discoveryStart, Instant.now()).toMillis());
                    if (deadlineExceeded(deadline)) {
                        status = "ABORTED";
                        abortReason = "time_budget_exceeded";
                    } else {
                        Instant crawlStart = Instant.now();
                        CrawlOutcome crawlOutcome = runAtsCanaryCrawl(tickers, deadline);
                        stepDurationsMs.put("crawlAts", Duration.between(crawlStart, Instant.now()).toMillis());
                        crawlRunId = crawlOutcome.crawlRunId();
                        jobsExtracted = crawlOutcome.jobsExtracted();
                        if (crawlOutcome.aborted()) {
                            status = "ABORTED";
                            abortReason = crawlOutcome.abortReason();
                        } else if (!"COMPLETED".equals(crawlOutcome.status())) {
                            status = crawlOutcome.status();
                        }
                    }
                }
            }
        } catch (CanaryAbortException e) {
            status = "ABORTED";
            abortReason = e.getMessage();
        } catch (Exception e) {
            log.warn("SEC canary failed", e);
            status = "FAILED";
            abortReason = e.getClass().getSimpleName();
        }

        if (crawlRunId != null) {
            companiesCrawledByAtsType = repository.countCrawlRunCompaniesByAtsType(crawlRunId);
            Map<String, Long> crawlFailures = repository.countCrawlRunCompanyFailures(crawlRunId);
            for (Map.Entry<String, Long> entry : crawlFailures.entrySet()) {
                topErrors.put(entry.getKey(), entry.getValue().intValue());
            }
        }
        mergeDomainErrors(domainResolution, topErrors);
        if (careersDiscovery.failedCount() > 0) {
            topErrors.put("discovery_failed", careersDiscovery.failedCount());
        }

        Instant finishedAt = Instant.now();
        return new SecCanarySummary(
            limit,
            startedAt,
            finishedAt,
            status,
            abortReason,
            Duration.between(startedAt, finishedAt).toMillis(),
            crawlRunId,
            companiesIngested,
            domainResolution,
            careersDiscovery,
            companiesCrawledByAtsType,
            jobsExtracted,
            topErrors,
            stepDurationsMs
        );
    }

    private CrawlOutcome runAtsCanaryCrawl(List<String> tickers, Instant deadline) {
        Instant startedAt = Instant.now();
        long crawlRunId = repository.insertCrawlRun(startedAt, "RUNNING", "sec_canary");
        int attempted = 0;
        int succeeded = 0;
        int failed = 0;
        int jobsExtracted = 0;
        String status = "COMPLETED";
        String abortReason = null;

        CrawlRunRequest request = new CrawlRunRequest(
            tickers,
            tickers.size(),
            null,
            null,
            null,
            null,
            null,
            false,
            false,
            true,
            null
        );

        List<CompanyTarget> targets = repository.findCompanyTargetsWithAts(tickers, tickers.size());
        attempted = targets.size();
        repository.updateCrawlRunProgress(crawlRunId, attempted, succeeded, failed, jobsExtracted, Instant.now());

        for (CompanyTarget target : targets) {
            if (deadlineExceeded(deadline)) {
                status = "ABORTED";
                abortReason = "time_budget_exceeded";
                break;
            }
            try {
                CompanyCrawlSummary summary = companyCrawlerService.crawlCompany(crawlRunId, target, request);
                jobsExtracted += summary.jobsExtractedCount();
                if (summary.closeoutSafe()) {
                    succeeded++;
                    repository.markPostingsInactiveNotSeenInRun(target.companyId(), crawlRunId);
                } else {
                    failed++;
                }
            } catch (CanaryAbortException e) {
                status = "ABORTED";
                abortReason = e.getMessage();
                break;
            } catch (Exception e) {
                failed++;
                log.warn("Canary crawl failed for {} ({})", target.ticker(), target.domain(), e);
            }
            repository.updateCrawlRunProgress(crawlRunId, attempted, succeeded, failed, jobsExtracted, Instant.now());
        }

        if (!"ABORTED".equals(status) && failed > 0) {
            status = "COMPLETED_WITH_ERRORS";
        }
        Instant finishedAt = Instant.now();
        repository.completeCrawlRun(crawlRunId, finishedAt, status, abortReason);
        return new CrawlOutcome(crawlRunId, jobsExtracted, status, abortReason, Duration.between(startedAt, finishedAt).toMillis());
    }

    private void mergeDomainErrors(DomainResolutionResult domainResolution, Map<String, Integer> topErrors) {
        if (domainResolution == null) {
            return;
        }
        if (domainResolution.noWikipediaTitleCount() > 0) {
            topErrors.put("domain_no_wikipedia_title", domainResolution.noWikipediaTitleCount());
        }
        if (domainResolution.noItemCount() > 0) {
            topErrors.put("domain_no_item", domainResolution.noItemCount());
        }
        if (domainResolution.noP856Count() > 0) {
            topErrors.put("domain_no_p856", domainResolution.noP856Count());
        }
        if (domainResolution.wdqsErrorCount() > 0) {
            topErrors.put("domain_wdqs_error", domainResolution.wdqsErrorCount());
        }
    }

    private boolean deadlineExceeded(Instant deadline) {
        return deadline != null && Instant.now().isAfter(deadline);
    }

    private record CrawlOutcome(
        long crawlRunId,
        int jobsExtracted,
        String status,
        String abortReason,
        long durationMs
    ) {
        private boolean aborted() {
            return "ABORTED".equals(status);
        }
    }
}
