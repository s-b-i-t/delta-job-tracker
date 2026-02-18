package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.ats.AtsEndpointExtractor;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.jobs.JobPostingExtractor;
import com.delta.jobtracker.crawl.model.AtsAdapterResult;
import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.DiscoveredUrlType;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.model.SitemapUrlEntry;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsRules;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.SitemapService;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import com.delta.jobtracker.crawl.util.UrlClassifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class CompanyCrawlerService {
    private static final Logger log = LoggerFactory.getLogger(CompanyCrawlerService.class);
    private static final String HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
    private static final String STAGE_ROBOTS_SITEMAP = "ROBOTS_SITEMAP";
    private static final String STAGE_JSONLD = "JSONLD";

    private final CrawlerProperties properties;
    private final RobotsTxtService robotsTxtService;
    private final SitemapService sitemapService;
    private final CrawlJdbcRepository repository;
    private final AtsDetector atsDetector;
    private final AtsEndpointExtractor atsEndpointExtractor;
    private final PoliteHttpClient httpClient;
    private final JobPostingExtractor jobPostingExtractor;
    private final AtsAdapterIngestionService atsAdapterIngestionService;

    public CompanyCrawlerService(
        CrawlerProperties properties,
        RobotsTxtService robotsTxtService,
        SitemapService sitemapService,
        CrawlJdbcRepository repository,
        AtsDetector atsDetector,
        AtsEndpointExtractor atsEndpointExtractor,
        PoliteHttpClient httpClient,
        JobPostingExtractor jobPostingExtractor,
        AtsAdapterIngestionService atsAdapterIngestionService
    ) {
        this.properties = properties;
        this.robotsTxtService = robotsTxtService;
        this.sitemapService = sitemapService;
        this.repository = repository;
        this.atsDetector = atsDetector;
        this.atsEndpointExtractor = atsEndpointExtractor;
        this.httpClient = httpClient;
        this.jobPostingExtractor = jobPostingExtractor;
        this.atsAdapterIngestionService = atsAdapterIngestionService;
    }

    public CompanyCrawlSummary crawlCompany(long crawlRunId, CompanyTarget company, CrawlRunRequest request) {
        log.info("Crawling {} ({})", company.ticker(), company.domain());
        Map<String, Integer> errors = new LinkedHashMap<>();
        Instant startedAt = Instant.now();
        Instant deadline = startedAt.plusSeconds(properties.getMaxCompanySeconds());
        Instant now = startedAt;
        List<AtsDetectionRecord> atsDetections = new ArrayList<>();
        boolean adapterSuccess = false;
        boolean fallbackSuccess = false;

        List<AtsEndpointRecord> existingEndpoints = repository.findAtsEndpoints(company.companyId());
        for (AtsEndpointRecord endpoint : existingEndpoints) {
            atsDetections.add(new AtsDetectionRecord(endpoint.atsType(), endpoint.endpointUrl()));
        }
        AtsAdapterResult adapterResult = atsAdapterIngestionService.ingestIfSupported(
            crawlRunId,
            company,
            existingEndpoints,
            request.maxJobsPerCompanyWorkday()
        );
        if (adapterResult != null) {
            mergeErrors(errors, adapterResult.errors());
            adapterSuccess = adapterResult.successfulFetch();
            if (adapterResult.jobsExtractedCount() > 0) {
                return new CompanyCrawlSummary(
                    company.companyId(),
                    company.ticker(),
                    company.domain(),
                    0,
                    0,
                    dedupeAtsDetections(atsDetections),
                    adapterResult.jobpostingPagesFoundCount(),
                    adapterResult.jobsExtractedCount(),
                    true,
                    topErrors(errors, 5)
                );
            }
        }
        if (budgetExceeded(deadline)) {
            increment(errors, "company_time_budget_exceeded");
            return new CompanyCrawlSummary(
                company.companyId(),
                company.ticker(),
                company.domain(),
                0,
                0,
                dedupeAtsDetections(atsDetections),
                adapterResult == null ? 0 : adapterResult.jobpostingPagesFoundCount(),
                adapterResult == null ? 0 : adapterResult.jobsExtractedCount(),
                adapterSuccess,
                topErrors(errors, 5)
            );
        }

        String stageEndpoint = stageEndpointUrl(company);
        Instant robotsStartedAt = Instant.now();
        repository.upsertCrawlRunCompanyResultStart(
            crawlRunId,
            company.companyId(),
            "RUNNING",
            STAGE_ROBOTS_SITEMAP,
            null,
            stageEndpoint,
            robotsStartedAt,
            false
        );

        RobotsRules rootRules = robotsTxtService.getRulesForHost(company.domain());
        List<String> seedSitemaps = new ArrayList<>(rootRules.getSitemapUrls());
        if (seedSitemaps.isEmpty()) {
            seedSitemaps.add("https://" + company.domain() + "/sitemap.xml");
            if (!company.domain().startsWith("www.")) {
                seedSitemaps.add("https://www." + company.domain() + "/sitemap.xml");
            }
        }

        int maxSitemapUrls = request.maxSitemapUrls() == null
            ? properties.getSitemap().getMaxUrlsPerDomain()
            : request.maxSitemapUrls();

        SitemapDiscoveryResult sitemapResult = sitemapService.discover(
            seedSitemaps,
            properties.getSitemap().getMaxDepth(),
            properties.getSitemap().getMaxSitemaps(),
            maxSitemapUrls
        );
        mergeErrors(errors, sitemapResult.errors());

        for (var sitemap : sitemapResult.fetchedSitemaps()) {
            repository.insertDiscoveredSitemap(
                crawlRunId,
                company.companyId(),
                sitemap.sitemapUrl(),
                sitemap.fetchedAt(),
                sitemap.urlCount()
            );
        }

        LinkedHashSet<String> candidateUrls = new LinkedHashSet<>();
        LinkedHashSet<String> atsLandingUrls = new LinkedHashSet<>();
        for (SitemapUrlEntry entry : sitemapResult.discoveredUrls()) {
            DiscoveredUrlType type = UrlClassifier.classify(entry.url());
            repository.upsertDiscoveredUrl(crawlRunId, company.companyId(), entry.url(), type, "discovered", null);
            if (type == DiscoveredUrlType.CANDIDATE_JOB) {
                candidateUrls.add(entry.url());
            } else if (type == DiscoveredUrlType.ATS_LANDING) {
                candidateUrls.add(entry.url());
                atsLandingUrls.add(entry.url());
            }
        }

        String robotsErrorKey = null;
        if (candidateUrls.isEmpty()) {
            boolean robotsUnavailable = robotsTxtService.isRobotsUnavailableForUrl("https://" + company.domain() + "/");
            if (sitemapResult.fetchedSitemaps().isEmpty()) {
                if (sitemapResult.errors().containsKey("blocked_by_robots")) {
                    increment(errors, robotsUnavailable ? "robots_fetch_failed" : "sitemap_blocked_by_robots");
                    robotsErrorKey = "blocked_by_robots";
                } else if (!sitemapResult.errors().isEmpty()) {
                    increment(errors, "sitemap_fetch_failed");
                    robotsErrorKey = topErrorKey(sitemapResult.errors());
                    if (robotsErrorKey == null) {
                        robotsErrorKey = "sitemap_fetch_failed";
                    }
                } else {
                    increment(errors, "no_sitemaps_found");
                    robotsErrorKey = "no_sitemaps_found";
                }
            } else if (sitemapResult.discoveredUrls().isEmpty()) {
                increment(errors, "sitemap_no_urls");
                robotsErrorKey = "sitemap_no_urls";
            } else {
                increment(errors, "no_candidate_urls");
                robotsErrorKey = "no_candidate_urls";
            }
        }

        if (candidateUrls.isEmpty()) {
            StageFailure failure = stageFailure(robotsErrorKey);
            recordStageFinish(
                crawlRunId,
                company.companyId(),
                STAGE_ROBOTS_SITEMAP,
                stageEndpoint,
                robotsStartedAt,
                "FAILED",
                0,
                failure.reasonCode(),
                failure.httpStatus(),
                failure.errorDetail(),
                failure.retryable()
            );
        } else {
            recordStageFinish(
                crawlRunId,
                company.companyId(),
                STAGE_ROBOTS_SITEMAP,
                stageEndpoint,
                robotsStartedAt,
                "SUCCEEDED",
                0,
                null,
                null,
                null,
                false
            );
        }

        if (budgetExceeded(deadline)) {
            increment(errors, "company_time_budget_exceeded");
            return new CompanyCrawlSummary(
                company.companyId(),
                company.ticker(),
                company.domain(),
                sitemapResult.fetchedSitemaps().size(),
                candidateUrls.size(),
                dedupeAtsDetections(atsDetections),
                0,
                0,
                adapterSuccess || fallbackSuccess,
                topErrors(errors, 5)
            );
        }

        List<AtsDetectionRecord> discoveredAts = detectAtsEndpoints(crawlRunId, company, candidateUrls, atsLandingUrls, errors, deadline);
        atsDetections.addAll(discoveredAts);

        int maxJobPages = request.maxJobPages() == null
            ? properties.getExtraction().getMaxJobPages()
            : request.maxJobPages();

        int pagesWithJobPosting = 0;
        int jobsExtracted = 0;
        Map<String, Integer> jsonldErrors = new LinkedHashMap<>();

        List<String> pagesToFetch = new ArrayList<>(candidateUrls);
        if (pagesToFetch.size() > maxJobPages) {
            pagesToFetch = pagesToFetch.subList(0, maxJobPages);
        }

        Instant jsonldStartedAt = Instant.now();
        repository.upsertCrawlRunCompanyResultStart(
            crawlRunId,
            company.companyId(),
            "RUNNING",
            STAGE_JSONLD,
            null,
            stageEndpoint,
            jsonldStartedAt,
            false
        );

        if (pagesToFetch.isEmpty()) {
            recordStageFinish(
                crawlRunId,
                company.companyId(),
                STAGE_JSONLD,
                stageEndpoint,
                jsonldStartedAt,
                "SKIPPED",
                0,
                ReasonCodeClassifier.SITEMAP_NOT_FOUND,
                null,
                "no_candidate_urls",
                false
            );
        } else {
            for (String url : pagesToFetch) {
                if (budgetExceeded(deadline)) {
                    increment(errors, "company_time_budget_exceeded");
                    increment(jsonldErrors, "company_time_budget_exceeded");
                    break;
                }
                if (repository.seenNoStructuredData(company.companyId(), url)) {
                    repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "skipped_known_no_structured_data", now);
                    continue;
                }
                if (!robotsTxtService.isAllowed(url)) {
                    repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "blocked_by_robots", now);
                    increment(errors, "blocked_by_robots");
                    increment(jsonldErrors, "blocked_by_robots");
                    continue;
                }

                HttpFetchResult fetch = httpClient.get(url, HTML_ACCEPT);
                Instant fetchedAt = Instant.now();
                if (!fetch.isSuccessful()) {
                    String status = errorKey(fetch);
                    repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, status, fetchedAt);
                    increment(errors, status);
                    increment(jsonldErrors, status);
                    continue;
                }

                if (fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
                    String status = "http_" + fetch.statusCode();
                    repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, status, fetchedAt);
                    increment(errors, status);
                    increment(jsonldErrors, status);
                    continue;
                }

                fallbackSuccess = true;
                List<NormalizedJobPosting> postings = jobPostingExtractor.extract(fetch.body(), fetch.finalUrlOrRequested());
                if (postings.isEmpty()) {
                    repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "no_jobposting_structured_data", fetchedAt);
                    continue;
                }

                pagesWithJobPosting++;
                repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "jobposting_found", fetchedAt);
                repository.upsertJobPostingsBatch(company.companyId(), crawlRunId, postings, fetchedAt);
                jobsExtracted += postings.size();
            }

            if (fallbackSuccess) {
                recordStageFinish(
                    crawlRunId,
                    company.companyId(),
                    STAGE_JSONLD,
                    stageEndpoint,
                    jsonldStartedAt,
                    "SUCCEEDED",
                    jobsExtracted,
                    null,
                    null,
                    null,
                    false
                );
            } else {
                StageFailure failure = stageFailure(topErrorKey(jsonldErrors));
                recordStageFinish(
                    crawlRunId,
                    company.companyId(),
                    STAGE_JSONLD,
                    stageEndpoint,
                    jsonldStartedAt,
                    "FAILED",
                    jobsExtracted,
                    failure.reasonCode(),
                    failure.httpStatus(),
                    failure.errorDetail(),
                    failure.retryable()
                );
            }
        }

        boolean closeoutSafe = adapterSuccess || fallbackSuccess;
        Map<String, Integer> topErrors = topErrors(errors, 5);
        return new CompanyCrawlSummary(
            company.companyId(),
            company.ticker(),
            company.domain(),
            sitemapResult.fetchedSitemaps().size(),
            candidateUrls.size(),
            dedupeAtsDetections(atsDetections),
            pagesWithJobPosting,
            jobsExtracted,
            closeoutSafe,
            topErrors
        );
    }

    private List<AtsDetectionRecord> detectAtsEndpoints(
        long crawlRunId,
        CompanyTarget company,
        LinkedHashSet<String> candidateUrls,
        LinkedHashSet<String> atsLandingUrls,
        Map<String, Integer> errors,
        Instant deadline
    ) {
        LinkedHashSet<String> probes = new LinkedHashSet<>();
        if (company.careersHintUrl() != null && !company.careersHintUrl().isBlank()) {
            probes.add(company.careersHintUrl().trim());
        }
        probes.add("https://" + company.domain() + "/careers");
        probes.add("https://" + company.domain() + "/jobs");
        probes.addAll(atsLandingUrls);
        for (String candidate : candidateUrls) {
            if (probes.size() >= 25) {
                break;
            }
            probes.add(candidate);
        }

        List<AtsDetectionRecord> detections = new ArrayList<>();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        for (String probe : probes) {
            if (budgetExceeded(deadline)) {
                increment(errors, "company_time_budget_exceeded");
                break;
            }
            List<AtsDetectionRecord> directEndpoints = atsEndpointExtractor.extract(probe, null);
            if (!directEndpoints.isEmpty()) {
                for (AtsDetectionRecord endpoint : directEndpoints) {
                    registerAtsDetection(
                        crawlRunId,
                        company.companyId(),
                        endpoint.atsType(),
                        endpoint.atsUrl(),
                        probe,
                        "ats_detected_from_hint",
                        "pattern",
                        false,
                        detections,
                        seen
                    );
                }
            }

            if (!robotsTxtService.isAllowed(probe)) {
                continue;
            }
            HttpFetchResult fetch = httpClient.get(probe, HTML_ACCEPT);
            String resolved = fetch.finalUrlOrRequested();

            List<AtsDetectionRecord> extracted = atsEndpointExtractor.extract(resolved, fetch.body());
            if (!extracted.isEmpty()) {
                for (AtsDetectionRecord endpoint : extracted) {
                    registerAtsDetection(
                        crawlRunId,
                        company.companyId(),
                        endpoint.atsType(),
                        endpoint.atsUrl(),
                        resolved,
                        fetch.isSuccessful() ? "ats_detected" : "ats_detected_probe_failed",
                        fetch.isSuccessful() ? "html" : "pattern",
                        fetch.isSuccessful(),
                        detections,
                        seen
                    );
                }
            } else {
                AtsType type = atsDetector.detect(resolved, fetch.body());
                if (type != AtsType.UNKNOWN) {
                    increment(errors, "ats_detected_no_endpoint");
                } else if (!fetch.isSuccessful()) {
                    increment(errors, errorKey(fetch));
                }
            }
        }
        return detections;
    }

    private void registerAtsDetection(
        long crawlRunId,
        long companyId,
        AtsType atsType,
        String url,
        String discoveredFromUrl,
        String status,
        String detectionMethod,
        boolean verified,
        List<AtsDetectionRecord> detections,
        LinkedHashSet<String> seen
    ) {
        String key = atsType.name() + "|" + url.toLowerCase(Locale.ROOT);
        if (seen.contains(key)) {
            return;
        }
        seen.add(key);
        detections.add(new AtsDetectionRecord(atsType, url));
        repository.upsertAtsEndpoint(companyId, atsType, url, discoveredFromUrl, 0.9, Instant.now(), detectionMethod, verified);
        repository.upsertDiscoveredUrl(
            crawlRunId,
            companyId,
            url,
            DiscoveredUrlType.ATS_LANDING,
            status,
            Instant.now()
        );
    }

    private List<AtsDetectionRecord> dedupeAtsDetections(List<AtsDetectionRecord> input) {
        LinkedHashMap<String, AtsDetectionRecord> unique = new LinkedHashMap<>();
        for (AtsDetectionRecord record : input) {
            String key = record.atsType() + "|" + record.atsUrl().toLowerCase(Locale.ROOT);
            unique.putIfAbsent(key, record);
        }
        return new ArrayList<>(unique.values());
    }

    private void mergeErrors(Map<String, Integer> accumulator, Map<String, Integer> additions) {
        additions.forEach((key, value) -> accumulator.put(key, accumulator.getOrDefault(key, 0) + value));
    }

    private String errorKey(HttpFetchResult fetch) {
        if (fetch.errorCode() != null) {
            return fetch.errorCode();
        }
        if (fetch.statusCode() > 0) {
            return "http_" + fetch.statusCode();
        }
        return "unknown_error";
    }

    private void increment(Map<String, Integer> errors, String key) {
        errors.put(key, errors.getOrDefault(key, 0) + 1);
    }

    private void recordStageFinish(
        long crawlRunId,
        long companyId,
        String stage,
        String endpointUrl,
        Instant startedAt,
        String status,
        int jobsExtracted,
        String reasonCode,
        Integer httpStatus,
        String errorDetail,
        boolean retryable
    ) {
        Instant finishedAt = Instant.now();
        String stopReason = resolveStopReason(status, errorDetail);
        repository.upsertCrawlRunCompanyResultFinish(
            crawlRunId,
            companyId,
            status,
            stage,
            null,
            endpointUrl,
            startedAt,
            finishedAt,
            java.time.Duration.between(startedAt, finishedAt).toMillis(),
            jobsExtracted,
            false,
            null,
            stopReason,
            reasonCode,
            httpStatus,
            errorDetail,
            retryable
        );
    }

    private StageFailure stageFailure(String errorKey) {
        if (errorKey == null || errorKey.isBlank()) {
            return new StageFailure(ReasonCodeClassifier.UNKNOWN, null, null, false);
        }
        String reasonCode = ReasonCodeClassifier.fromErrorKey(errorKey);
        Integer httpStatus = ReasonCodeClassifier.parseHttpStatus(errorKey);
        boolean retryable = ReasonCodeClassifier.isRetryable(reasonCode);
        return new StageFailure(reasonCode, httpStatus, errorKey, retryable);
    }

    private String topErrorKey(Map<String, Integer> errors) {
        if (errors == null || errors.isEmpty()) {
            return null;
        }
        return errors.entrySet().stream()
            .max(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .orElse(null);
    }

    private String resolveStopReason(String status, String errorDetail) {
        if ("FAILED".equalsIgnoreCase(status)) {
            if (errorDetail != null) {
                String lower = errorDetail.toLowerCase(Locale.ROOT);
                if (lower.contains("time_budget") || lower.contains("budget_exceeded")) {
                    return "TIME_BUDGET";
                }
            }
            return "ERROR";
        }
        return "COMPLETE";
    }

    private boolean budgetExceeded(Instant deadline) {
        return deadline != null && Instant.now().isAfter(deadline);
    }

    private String stageEndpointUrl(CompanyTarget company) {
        if (company == null || company.domain() == null || company.domain().isBlank()) {
            return null;
        }
        return "https://" + company.domain();
    }

    private Map<String, Integer> topErrors(Map<String, Integer> errors, int limit) {
        return errors.entrySet().stream()
            .sorted(Map.Entry.<String, Integer>comparingByValue(Comparator.reverseOrder()))
            .limit(limit)
            .collect(
                LinkedHashMap::new,
                (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                LinkedHashMap::putAll
            );
    }

    private record StageFailure(String reasonCode, Integer httpStatus, String errorDetail, boolean retryable) {
    }
}
