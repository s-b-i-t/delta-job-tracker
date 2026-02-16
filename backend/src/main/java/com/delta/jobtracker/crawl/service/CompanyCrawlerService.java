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
        Instant now = Instant.now();
        List<AtsDetectionRecord> atsDetections = new ArrayList<>();
        boolean adapterSuccess = false;
        boolean fallbackSuccess = false;

        List<AtsEndpointRecord> existingEndpoints = repository.findAtsEndpoints(company.companyId());
        for (AtsEndpointRecord endpoint : existingEndpoints) {
            atsDetections.add(new AtsDetectionRecord(endpoint.atsType(), endpoint.endpointUrl()));
        }
        AtsAdapterResult adapterResult = atsAdapterIngestionService.ingestIfSupported(crawlRunId, company, existingEndpoints);
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

        if (candidateUrls.isEmpty()) {
            boolean robotsUnavailable = robotsTxtService.isRobotsUnavailableForUrl("https://" + company.domain() + "/");
            if (sitemapResult.fetchedSitemaps().isEmpty()) {
                if (sitemapResult.errors().containsKey("blocked_by_robots")) {
                    increment(errors, robotsUnavailable ? "robots_fetch_failed" : "sitemap_blocked_by_robots");
                } else if (!sitemapResult.errors().isEmpty()) {
                    increment(errors, "sitemap_fetch_failed");
                } else {
                    increment(errors, "no_sitemaps_found");
                }
            } else if (sitemapResult.discoveredUrls().isEmpty()) {
                increment(errors, "sitemap_no_urls");
            } else {
                increment(errors, "no_candidate_urls");
            }
        }

        List<AtsDetectionRecord> discoveredAts = detectAtsEndpoints(crawlRunId, company, candidateUrls, atsLandingUrls, errors);
        atsDetections.addAll(discoveredAts);

        int maxJobPages = request.maxJobPages() == null
            ? properties.getExtraction().getMaxJobPages()
            : request.maxJobPages();

        int pagesWithJobPosting = 0;
        int jobsExtracted = 0;

        List<String> pagesToFetch = new ArrayList<>(candidateUrls);
        if (pagesToFetch.size() > maxJobPages) {
            pagesToFetch = pagesToFetch.subList(0, maxJobPages);
        }

        for (String url : pagesToFetch) {
            if (repository.seenNoStructuredData(company.companyId(), url)) {
                repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "skipped_known_no_structured_data", now);
                continue;
            }
            if (!robotsTxtService.isAllowed(url)) {
                repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, "blocked_by_robots", now);
                increment(errors, "blocked_by_robots");
                continue;
            }

            HttpFetchResult fetch = httpClient.get(url, HTML_ACCEPT);
            Instant fetchedAt = Instant.now();
            if (!fetch.isSuccessful()) {
                String status = errorKey(fetch);
                repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, status, fetchedAt);
                increment(errors, status);
                continue;
            }

            if (fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
                String status = "http_" + fetch.statusCode();
                repository.updateDiscoveredUrlStatus(crawlRunId, company.companyId(), url, status, fetchedAt);
                increment(errors, status);
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
            for (NormalizedJobPosting posting : postings) {
                repository.upsertJobPosting(company.companyId(), crawlRunId, posting, fetchedAt);
                jobsExtracted++;
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
        Map<String, Integer> errors
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
}
