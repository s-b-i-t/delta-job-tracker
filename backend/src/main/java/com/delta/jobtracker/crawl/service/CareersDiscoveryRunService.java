package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyFailureView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyResultView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryFailuresResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunStatus;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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
        CrawlerProperties properties
    ) {
        this.repository = repository;
        this.discoveryService = discoveryService;
        this.discoveryExecutor = discoveryExecutor;
        this.properties = properties;
    }

    public CareersDiscoveryRunResponse startAsync(Integer limit, Integer batchSize) {
        int companyLimit = limit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, limit);
        int safeBatchSize = batchSize == null ? DEFAULT_BATCH_SIZE : Math.max(1, batchSize);

        List<CompanyTarget> companies = repository.findCompaniesWithDomainWithoutAts(companyLimit);
        long runId = repository.insertCareersDiscoveryRun(companyLimit);
        discoveryExecutor.submit(() -> runDiscovery(runId, companies, safeBatchSize));
        return new CareersDiscoveryRunResponse(runId, "RUNNING");
    }

    public CareersDiscoveryRunStatus getRunStatus(long runId) {
        return repository.findCareersDiscoveryRun(runId);
    }

    public List<CareersDiscoveryCompanyResultView> getCompanyResults(long runId, String status, Integer limit) {
        int safeLimit = limit == null ? 200 : Math.max(1, Math.min(limit, 500));
        return repository.findCareersDiscoveryCompanyResults(runId, normalizeStatus(status), safeLimit);
    }

    public CareersDiscoveryFailuresResponse getFailures(long runId) {
        Map<String, Long> counts = repository.countCareersDiscoveryCompanyFailures(runId);
        List<CareersDiscoveryCompanyFailureView> failures = repository.findRecentCareersDiscoveryCompanyFailures(runId, 20);
        return new CareersDiscoveryFailuresResponse(counts, failures);
    }

    private void runDiscovery(long runId, List<CompanyTarget> companies, int batchSize) {
        int processed = 0;
        int succeeded = 0;
        int failed = 0;
        int endpointsAdded = 0;
        String lastError = null;
        Instant runStarted = Instant.now();

        try {
            List<List<CompanyTarget>> batches = chunk(companies, batchSize);
            for (List<CompanyTarget> batch : batches) {
                for (CompanyTarget company : batch) {
                    Instant startedAt = Instant.now();
                    String status = "FAILED";
                    String reasonCode = ReasonCodeClassifier.UNKNOWN;
                    String stage = "PAGE_SCAN";
                    int foundEndpoints = 0;
                    Integer httpStatus = null;
                    String errorDetail = null;

                    try {
                        if (company.domain() == null || company.domain().isBlank()) {
                            status = "SKIPPED";
                            reasonCode = ReasonCodeClassifier.NO_DOMAIN;
                            stage = "DOMAIN";
                        } else {
                            int beforeCount = repository.countAtsEndpointsForCompany(company.companyId());
                            CareersDiscoveryService.DiscoveryOutcome outcome = discoveryService.discoverForCompany(company);
                            int afterCount = repository.countAtsEndpointsForCompany(company.companyId());
                            endpointsAdded += Math.max(0, afterCount - beforeCount);
                            if (outcome != null && outcome.countsByType() != null) {
                                for (Integer count : outcome.countsByType().values()) {
                                    foundEndpoints += count == null ? 0 : count;
                                }
                            }

                            if (outcome != null && outcome.hasEndpoints()) {
                                status = "SUCCEEDED";
                                reasonCode = null;
                                stage = "ATS_DETECTED";
                            } else {
                                CareersDiscoveryService.DiscoveryFailure failure = outcome == null ? null : outcome.primaryFailure();
                                FailureMapping mapping = mapFailure(failure);
                                reasonCode = mapping.reasonCode();
                                stage = mapping.stage();
                                httpStatus = mapping.httpStatus();
                                errorDetail = mapping.errorDetail();
                            }
                        }
                    } catch (Exception e) {
                        lastError = e.getMessage();
                        reasonCode = ReasonCodeClassifier.UNKNOWN;
                        stage = "PAGE_SCAN";
                        errorDetail = e.getMessage();
                        log.warn("Careers discovery run {} failed for {} ({})", runId, company.ticker(), company.domain(), e);
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
                        errorDetail
                    );
                    processed++;
                    if ("SUCCEEDED".equals(status)) {
                        succeeded++;
                    } else if ("FAILED".equals(status)) {
                        failed++;
                    }
                    repository.updateCareersDiscoveryRunProgress(runId, processed, succeeded, failed, endpointsAdded, lastError);
                }
            }

            repository.completeCareersDiscoveryRun(runId, Instant.now(), "SUCCEEDED", lastError);
        } catch (Exception e) {
            lastError = e.getMessage();
            repository.completeCareersDiscoveryRun(runId, Instant.now(), "FAILED", lastError);
            log.warn("Careers discovery run {} failed after {} ms", runId, Duration.between(runStarted, Instant.now()).toMillis(), e);
        }
    }

    private FailureMapping mapFailure(CareersDiscoveryService.DiscoveryFailure failure) {
        if (failure == null) {
            return new FailureMapping(ReasonCodeClassifier.ATS_NOT_FOUND, "PAGE_SCAN", null, null);
        }
        String reason = failure.reasonCode();
        String detail = failure.detail();
        if ("discovery_blocked_by_robots".equals(reason)) {
            return new FailureMapping(ReasonCodeClassifier.ROBOTS_BLOCKED, "ROBOTS_SITEMAP", null, detail);
        }
        if ("discovery_fetch_failed".equals(reason)) {
            Integer httpStatus = ReasonCodeClassifier.parseHttpStatus(detail);
            String mapped = httpStatus != null
                ? ReasonCodeClassifier.fromHttpStatus(httpStatus)
                : ReasonCodeClassifier.fromErrorKey(detail);
            return new FailureMapping(mapped, "PAGE_SCAN", httpStatus, detail);
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

    private record FailureMapping(String reasonCode, String stage, Integer httpStatus, String errorDetail) {
    }
}
