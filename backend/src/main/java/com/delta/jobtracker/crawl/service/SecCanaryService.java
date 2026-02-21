package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryAbortException;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CanaryRunResponse;
import com.delta.jobtracker.crawl.model.CanaryRunStatus;
import com.delta.jobtracker.crawl.model.CanaryRunStatusResponse;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.SecCanarySummary;
import com.delta.jobtracker.crawl.model.SecIngestionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

@Service
public class SecCanaryService {
    private static final Logger log = LoggerFactory.getLogger(SecCanaryService.class);

    private final UniverseIngestionService ingestionService;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;
    private final CompanyCrawlerService companyCrawlerService;
    private final CrawlJdbcRepository repository;
    private final CrawlerProperties properties;
    private final ExecutorService canaryExecutor;
    private final ObjectMapper objectMapper;

    public SecCanaryService(
        UniverseIngestionService ingestionService,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService,
        CompanyCrawlerService companyCrawlerService,
        CrawlJdbcRepository repository,
        CrawlerProperties properties,
        @Qualifier("canaryExecutor") ExecutorService canaryExecutor,
        ObjectMapper objectMapper
    ) {
        this.ingestionService = ingestionService;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
        this.companyCrawlerService = companyCrawlerService;
        this.repository = repository;
        this.properties = properties;
        this.canaryExecutor = canaryExecutor;
        this.objectMapper = objectMapper;
    }

    public CanaryRunResponse startSecCanary(Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getCanary().getDefaultLimit()
            : Math.max(1, requestedLimit);
        CanaryRunStatus existing = repository.findRunningCanaryRun("SEC");
        if (existing != null) {
            return new CanaryRunResponse(existing.runId(), existing.status());
        }
        Instant startedAt = Instant.now();
        long runId = repository.insertCanaryRun("SEC", limit, startedAt);
        canaryExecutor.submit(() -> runAndPersistCanary(runId, limit, false, true));
        return new CanaryRunResponse(runId, "RUNNING");
    }

    public CanaryRunResponse startSecFullCycleCanary(
        Integer requestedLimit,
        Boolean vendorProbeOnly,
        Boolean crawl
    ) {
        int limit = requestedLimit == null
            ? properties.getCanary().getDefaultLimit()
            : Math.max(1, requestedLimit);
        boolean vendorProbeOnlyMode = vendorProbeOnly != null && vendorProbeOnly;
        boolean doCrawl = crawl == null || crawl;
        CanaryRunStatus existing = repository.findRunningCanaryRun("SEC_FULL_CYCLE");
        if (existing != null) {
            return new CanaryRunResponse(existing.runId(), existing.status());
        }
        Instant startedAt = Instant.now();
        long runId = repository.insertCanaryRun("SEC_FULL_CYCLE", limit, startedAt);
        canaryExecutor.submit(() -> runAndPersistCanary(runId, limit, vendorProbeOnlyMode, doCrawl));
        return new CanaryRunResponse(runId, "RUNNING");
    }

    public CanaryRunStatusResponse getCanaryRunStatus(long runId) {
        CanaryRunStatus record = repository.findCanaryRun(runId);
        return buildStatusResponse(record);
    }

    public CanaryRunStatusResponse getLatestCanaryRunStatus(String type) {
        CanaryRunStatus record;
        if (type == null || type.isBlank()) {
            record = repository.findLatestCanaryRun();
        } else {
            record = repository.findLatestCanaryRun(type.trim().toUpperCase(Locale.ROOT));
        }
        return buildStatusResponse(record);
    }

    public SecCanarySummary runSecCanary(Integer requestedLimit) {
        return runSecCanary(requestedLimit, false, true);
    }

    public SecCanarySummary runSecCanary(
        Integer requestedLimit,
        boolean vendorProbeOnly,
        boolean crawl
    ) {
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
        int domainsResolved = 0;
        int jobsExtracted = 0;
        long durationIngestMs = 0L;
        long durationDomainResolutionMs = 0L;
        long durationDiscoveryMs = 0L;
        long durationCrawlMs = 0L;
        long durationTotalMs = 0L;
        DomainResolutionResult domainResolution = new DomainResolutionResult(0, 0, 0, 0, 0, 0, List.of());
        CareersDiscoveryResult careersDiscovery = new CareersDiscoveryResult(Map.of(), 0, 0, Map.of());
        Map<String, Integer> endpointsDiscoveredByAtsType = Map.of();
        Map<String, Integer> companiesCrawledByAtsType = Map.of();
        int cooldownSkips = 0;

        CanaryHttpBudget budget = new CanaryHttpBudget(
            properties.getCanary().getMaxRequestsPerHost(),
            properties.getCanary().getMaxTotalRequests(),
            properties.getCanary().getMax429Rate(),
            properties.getCanary().getMinRequestsFor429Rate(),
            properties.getCanary().getMaxConsecutiveErrors(),
            properties.getCanary().getMaxAttemptsPerRequest(),
            properties.getCanary().getRequestTimeoutSeconds(),
            deadline
        );

        try (CanaryHttpBudgetContext.Scope scope = CanaryHttpBudgetContext.activate(budget)) {
            Instant ingestStart = Instant.now();
            SecIngestionResult ingestionResult = ingestionService.ingestSecCompanies(limit);
            durationIngestMs = Duration.between(ingestStart, Instant.now()).toMillis();
            stepDurationsMs.put("ingestSec", durationIngestMs);
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
                durationDomainResolutionMs = Duration.between(domainStart, Instant.now()).toMillis();
                stepDurationsMs.put("resolveDomains", durationDomainResolutionMs);
                domainsResolved = domainResolution.resolvedCount();
                if (deadlineExceeded(deadline)) {
                    status = "ABORTED";
                    abortReason = "time_budget_exceeded";
                } else {
                    Instant discoveryStart = Instant.now();
                    careersDiscovery = careersDiscoveryService.discoverForTickers(
                        tickers,
                        tickers.size(),
                        deadline,
                        vendorProbeOnly
                    );
                    durationDiscoveryMs = Duration.between(discoveryStart, Instant.now()).toMillis();
                    stepDurationsMs.put("discoverCareers", durationDiscoveryMs);
                    endpointsDiscoveredByAtsType = careersDiscovery.discoveredCountByAtsType();
                    if (deadlineExceeded(deadline)) {
                        status = "ABORTED";
                        abortReason = "time_budget_exceeded";
                    } else {
                        if (crawl) {
                            Instant crawlStart = Instant.now();
                            CrawlOutcome crawlOutcome = runAtsCanaryCrawl(tickers, deadline);
                            durationCrawlMs = Duration.between(crawlStart, Instant.now()).toMillis();
                            stepDurationsMs.put("crawlAts", durationCrawlMs);
                            crawlRunId = crawlOutcome.crawlRunId();
                            jobsExtracted = crawlOutcome.jobsExtracted();
                            if (crawlOutcome.aborted()) {
                                status = "ABORTED";
                                abortReason = crawlOutcome.abortReason();
                            } else if (!"COMPLETED".equals(crawlOutcome.status())) {
                                status = crawlOutcome.status();
                            }
                        } else {
                            durationCrawlMs = 0L;
                            stepDurationsMs.put("crawlAts", durationCrawlMs);
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
        cooldownSkips = careersDiscovery.cooldownSkips();
        mergeErrors(topErrors, careersDiscovery.topErrors());
        if (careersDiscovery.failedCount() > 0 && careersDiscovery.topErrors().isEmpty()) {
            topErrors.put("discovery_failed", careersDiscovery.failedCount());
        }
        ensureHostCooldownCategory(topErrors);

        Instant finishedAt = Instant.now();
        durationTotalMs = Duration.between(startedAt, finishedAt).toMillis();
        return new SecCanarySummary(
            limit,
            startedAt,
            finishedAt,
            status,
            abortReason,
            durationTotalMs,
            durationIngestMs,
            durationDomainResolutionMs,
            durationDiscoveryMs,
            durationCrawlMs,
            durationTotalMs,
            crawlRunId,
            companiesIngested,
            domainsResolved,
            endpointsDiscoveredByAtsType,
            domainResolution,
            careersDiscovery,
            companiesCrawledByAtsType,
            jobsExtracted,
            cooldownSkips,
            topErrors,
            stepDurationsMs
        );
    }

    private void runAndPersistCanary(
        long runId,
        int requestedLimit,
        boolean vendorProbeOnly,
        boolean crawl
    ) {
        try {
            SecCanarySummary summary = runSecCanary(requestedLimit, vendorProbeOnly, crawl);
            String runStatus = mapRunStatus(summary.status());
            String summaryJson = null;
            String errorSummaryJson = null;
            try {
                summaryJson = objectMapper.writeValueAsString(summary);
                errorSummaryJson = objectMapper.writeValueAsString(summary.topErrors());
            } catch (Exception e) {
                log.warn("Failed to serialize canary summary for {}", runId, e);
            }
            repository.updateCanaryRun(runId, summary.finishedAt(), runStatus, summaryJson, errorSummaryJson);
        } catch (Exception e) {
            log.warn("SEC canary run {} failed", runId, e);
            repository.updateCanaryRun(runId, Instant.now(), "FAILED", null, null);
        }
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
            topErrors.put("domain_no_identifier", domainResolution.noWikipediaTitleCount());
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
        if (domainResolution.wdqsTimeoutCount() > 0) {
            topErrors.put("domain_wdqs_timeout", domainResolution.wdqsTimeoutCount());
        }
    }

    private void mergeErrors(Map<String, Integer> target, Map<String, Integer> source) {
        if (target == null || source == null || source.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Integer> entry : source.entrySet()) {
            String key = entry.getKey();
            if (key == null) {
                continue;
            }
            int value = entry.getValue() == null ? 0 : entry.getValue();
            target.put(key, target.getOrDefault(key, 0) + value);
        }
    }

    private CanaryRunStatusResponse buildStatusResponse(CanaryRunStatus record) {
        if (record == null) {
            return null;
        }
        SecCanarySummary summary = null;
        Map<String, Integer> errorSummary = null;
        try {
            if (record.summaryJson() != null && !record.summaryJson().isBlank()) {
                summary = objectMapper.readValue(record.summaryJson(), SecCanarySummary.class);
            }
            if (record.errorSummaryJson() != null && !record.errorSummaryJson().isBlank()) {
                errorSummary = objectMapper.readValue(record.errorSummaryJson(), new TypeReference<>() {});
            }
        } catch (Exception e) {
            log.warn("Failed to parse canary run summary for {}", record.runId(), e);
        }
        return new CanaryRunStatusResponse(
            record.runId(),
            record.type(),
            record.requestedLimit(),
            record.startedAt(),
            record.finishedAt(),
            record.status(),
            summary,
            errorSummary
        );
    }

    private void ensureHostCooldownCategory(Map<String, Integer> topErrors) {
        if (topErrors == null || topErrors.isEmpty()) {
            return;
        }
        int cooldownCount = 0;
        for (Map.Entry<String, Integer> entry : topErrors.entrySet()) {
            String key = entry.getKey();
            if (key == null || ReasonCodeClassifier.HOST_COOLDOWN.equals(key)) {
                continue;
            }
            if (key.toLowerCase(Locale.ROOT).contains("host_cooldown")) {
                cooldownCount += entry.getValue() == null ? 0 : entry.getValue();
            }
        }
        if (cooldownCount > 0) {
            topErrors.put(
                ReasonCodeClassifier.HOST_COOLDOWN,
                topErrors.getOrDefault(ReasonCodeClassifier.HOST_COOLDOWN, 0) + cooldownCount
            );
        }
    }

    private boolean deadlineExceeded(Instant deadline) {
        return deadline != null && Instant.now().isAfter(deadline);
    }

    private String mapRunStatus(String summaryStatus) {
        if (summaryStatus == null) {
            return "FAILED";
        }
        if ("ABORTED".equals(summaryStatus)) {
            return "ABORTED";
        }
        if ("FAILED".equals(summaryStatus)) {
            return "FAILED";
        }
        return "SUCCEEDED";
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
