package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsEndpointExtractor;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class CareersDiscoveryService {
    private static final Logger log = LoggerFactory.getLogger(CareersDiscoveryService.class);
    private static final String HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

    private static final List<String> COMMON_PATHS = List.of(
        "/careers",
        "/jobs",
        "/careers/jobs",
        "/about/careers",
        "/join-us",
        "/careers/search",
        "/careers/opportunities",
        "/careers/openings",
        "/jobs/search"
    );

    private static final List<String> HINT_TOKENS = List.of(
        "careers",
        "jobs",
        "join",
        "talent",
        "opportunities",
        "work-with-us"
    );

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;
    private final PoliteHttpClient httpClient;
    private final RobotsTxtService robotsTxtService;
    private final AtsDetector atsDetector;
    private final AtsEndpointExtractor atsEndpointExtractor;

    public CareersDiscoveryService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        PoliteHttpClient httpClient,
        RobotsTxtService robotsTxtService,
        AtsDetector atsDetector,
        AtsEndpointExtractor atsEndpointExtractor
    ) {
        this.properties = properties;
        this.repository = repository;
        this.httpClient = httpClient;
        this.robotsTxtService = robotsTxtService;
        this.atsDetector = atsDetector;
        this.atsEndpointExtractor = atsEndpointExtractor;
    }

    public CareersDiscoveryResult discover(Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, requestedLimit);

        List<CompanyTarget> companies = repository.findCompaniesWithDomainWithoutAts(limit);
        return discoverForCompanies(companies);
    }

    public CareersDiscoveryResult discoverForTickers(List<String> tickers, Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, requestedLimit);
        List<CompanyTarget> companies = repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, limit);
        return discoverForCompanies(companies);
    }

    private CareersDiscoveryResult discoverForCompanies(List<CompanyTarget> companies) {
        Map<String, Integer> discoveredCountByType = new LinkedHashMap<>();
        int failedCount = 0;
        Map<String, Integer> topErrors = new LinkedHashMap<>();

        for (CompanyTarget company : companies) {
            try {
                DiscoveryOutcome outcome = discoverForCompany(company);
                if (!outcome.hasEndpoints()) {
                    failedCount++;
                    DiscoveryFailure failure = outcome.primaryFailure();
                    String reason = failure == null ? "discovery_no_match" : failure.reasonCode();
                    increment(topErrors, company.ticker() + " (" + company.name() + "): " + reason);
                } else {
                    for (Map.Entry<AtsType, Integer> entry : outcome.countsByType().entrySet()) {
                        increment(discoveredCountByType, entry.getKey().name(), entry.getValue());
                    }
                }
            } catch (Exception e) {
                failedCount++;
                increment(topErrors, company.ticker() + " (" + company.name() + "): exception");
                log.warn("Careers discovery failed for {} ({})", company.ticker(), company.domain(), e);
            }
        }

        return new CareersDiscoveryResult(discoveredCountByType, failedCount, topErrors);
    }

    private DiscoveryOutcome discoverForCompany(CompanyTarget company) {
        LinkedHashSet<String> candidates = buildCandidates(company);
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        Map<AtsType, Integer> countsByType = new LinkedHashMap<>();
        List<DiscoveryFailure> failures = new ArrayList<>();

        int maxCandidates = properties.getCareersDiscovery().getMaxCandidatesPerCompany();
        int inspected = 0;
        for (String candidate : candidates) {
            if (inspected >= maxCandidates) {
                break;
            }
            inspected++;

            List<AtsDetectionRecord> patternEndpoints = atsEndpointExtractor.extract(candidate, null);
            if (!patternEndpoints.isEmpty()) {
                registerEndpoints(company, candidate, patternEndpoints, "pattern", 0.7, false, seen, countsByType);
                continue;
            }

            if (!robotsTxtService.isAllowed(candidate)) {
                failures.add(new DiscoveryFailure("discovery_blocked_by_robots", candidate, null));
                log.debug("Careers discovery blocked by robots: {}", candidate);
                continue;
            }

            HttpFetchResult fetch = httpClient.get(candidate, HTML_ACCEPT);
            if (!fetch.isSuccessful()) {
                String status = fetch.errorCode() == null ? "http_" + fetch.statusCode() : fetch.errorCode();
                failures.add(new DiscoveryFailure("discovery_fetch_failed", candidate, status));
                continue;
            }

            String resolved = normalizeCandidateUrl(fetch.finalUrlOrRequested());
            List<AtsDetectionRecord> extracted = atsEndpointExtractor.extract(resolved, fetch.body());
            if (!extracted.isEmpty()) {
                registerEndpoints(company, candidate, extracted, "html", 0.85, true, seen, countsByType);
                continue;
            }

            List<AtsDetectionRecord> shortLinkEndpoints = resolveGreenhouseShortLinks(fetch.body());
            if (!shortLinkEndpoints.isEmpty()) {
                registerEndpoints(company, candidate, shortLinkEndpoints, "html", 0.8, true, seen, countsByType);
                continue;
            }

            AtsType detected = atsDetector.detect(resolved, fetch.body());
            if (detected != AtsType.UNKNOWN) {
                failures.add(new DiscoveryFailure("discovery_ats_detected_no_endpoint", resolved, detected.name()));
                continue;
            }
        }

        if (countsByType.isEmpty()) {
            DiscoveryFailure failure = selectFailure(failures);
            repository.insertCareersDiscoveryFailure(
                company.companyId(),
                failure.reasonCode(),
                failure.candidateUrl(),
                failure.detail(),
                Instant.now()
            );
            return new DiscoveryOutcome(countsByType, failure);
        }

        return new DiscoveryOutcome(countsByType, null);
    }

    private List<String> discoverLinksFromHomepage(List<String> homepageUrls) {
        List<String> out = new ArrayList<>();
        for (String homepage : homepageUrls) {
            if (homepage == null || homepage.isBlank()) {
                continue;
            }
            if (!robotsTxtService.isAllowed(homepage)) {
                continue;
            }

            HttpFetchResult fetch = httpClient.get(homepage, HTML_ACCEPT);
            if (!fetch.isSuccessful() || fetch.body() == null) {
                continue;
            }

            Document doc = Jsoup.parse(fetch.body(), homepage);
            for (Element anchor : doc.select("a[href]")) {
                String href = anchor.attr("abs:href");
                if (href == null || href.isBlank()) {
                    continue;
                }
                String text = anchor.text() == null ? "" : anchor.text().toLowerCase(Locale.ROOT);
                String hrefLower = href.toLowerCase(Locale.ROOT);

                boolean isHint = containsHint(text) || containsHint(hrefLower);
                if (!isHint) {
                    continue;
                }
                if (hrefLower.startsWith("http://") || hrefLower.startsWith("https://")) {
                    String normalized = normalizeCandidateUrl(href);
                    if (normalized != null) {
                        out.add(normalized);
                    }
                }
            }
        }
        return out;
    }

    private boolean containsHint(String value) {
        for (String token : HINT_TOKENS) {
            if (value.contains(token)) {
                return true;
            }
        }
        return false;
    }

    private void increment(Map<String, Integer> map, String key) {
        map.put(key, map.getOrDefault(key, 0) + 1);
    }

    private void increment(Map<String, Integer> map, String key, int delta) {
        map.put(key, map.getOrDefault(key, 0) + delta);
    }

    private LinkedHashSet<String> buildCandidates(CompanyTarget company) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        if (company.careersHintUrl() != null && !company.careersHintUrl().isBlank()) {
            addCandidate(candidates, company.careersHintUrl());
        }

        List<String> hosts = new ArrayList<>();
        if (company.domain() != null && !company.domain().isBlank()) {
            hosts.add(company.domain());
            if (!company.domain().startsWith("www.")) {
                hosts.add("www." + company.domain());
            }
        }

        for (String host : hosts) {
            for (String path : COMMON_PATHS) {
                addCandidate(candidates, host + path);
            }
        }
        if (company.domain() != null && !company.domain().isBlank()) {
            addCandidate(candidates, "careers." + company.domain());
            addCandidate(candidates, "jobs." + company.domain());
        }

        List<String> homepageUrls = new ArrayList<>();
        for (String host : hosts) {
            homepageUrls.add("https://" + host + "/");
            homepageUrls.add("http://" + host + "/");
        }
        candidates.addAll(discoverLinksFromHomepage(homepageUrls));
        return candidates;
    }

    private void addCandidate(LinkedHashSet<String> candidates, String raw) {
        if (raw == null || raw.isBlank()) {
            return;
        }
        for (String variant : candidateVariants(raw.trim())) {
            String normalized = normalizeCandidateUrl(variant);
            if (normalized != null) {
                candidates.add(normalized);
            }
        }
    }

    private List<String> candidateVariants(String raw) {
        if (raw.startsWith("http://") || raw.startsWith("https://")) {
            return List.of(raw);
        }
        if (raw.startsWith("//")) {
            return List.of("https:" + raw, "http:" + raw);
        }
        return List.of("https://" + raw, "http://" + raw);
    }

    private String normalizeCandidateUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String value = raw.trim();
        if (value.startsWith("//")) {
            value = "https:" + value;
        }
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        try {
            java.net.URI uri = new java.net.URI(value);
            if (uri.getHost() == null) {
                return null;
            }
            String scheme = uri.getScheme() == null ? "https" : uri.getScheme().toLowerCase(Locale.ROOT);
            String host = uri.getHost().toLowerCase(Locale.ROOT);
            String path = uri.getPath() == null ? "" : uri.getPath();
            String filteredQuery = filterTrackingParams(uri.getRawQuery());
            String normalized = scheme + "://" + host + path;
            if (filteredQuery != null && !filteredQuery.isBlank()) {
                normalized = normalized + "?" + filteredQuery;
            }
            if (normalized.endsWith("/") && normalized.length() > (scheme + "://x/").length()) {
                normalized = normalized.substring(0, normalized.length() - 1);
            }
            return normalized;
        } catch (Exception ignored) {
            return null;
        }
    }

    private String filterTrackingParams(String rawQuery) {
        if (rawQuery == null || rawQuery.isBlank()) {
            return null;
        }
        String[] parts = rawQuery.split("&");
        List<String> kept = new ArrayList<>();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String key = part;
            int idx = part.indexOf('=');
            if (idx >= 0) {
                key = part.substring(0, idx);
            }
            String lower = key.toLowerCase(Locale.ROOT);
            if (lower.startsWith("utm_") || lower.equals("ref") || lower.equals("source") || lower.equals("gh_src") || lower.equals("lever-source")) {
                continue;
            }
            kept.add(part);
        }
        if (kept.isEmpty()) {
            return null;
        }
        return String.join("&", kept);
    }

    private void registerEndpoints(
        CompanyTarget company,
        String discoveredFromUrl,
        List<AtsDetectionRecord> endpoints,
        String detectionMethod,
        double confidence,
        boolean verified,
        LinkedHashSet<String> seen,
        Map<AtsType, Integer> countsByType
    ) {
        for (AtsDetectionRecord endpoint : endpoints) {
            String endpointUrl = endpoint.atsUrl();
            if (endpointUrl == null || endpointUrl.isBlank()) {
                continue;
            }
            String key = endpoint.atsType().name() + "|" + endpointUrl.toLowerCase(Locale.ROOT);
            if (seen.contains(key)) {
                continue;
            }
            seen.add(key);
            repository.upsertAtsEndpoint(
                company.companyId(),
                endpoint.atsType(),
                endpointUrl,
                discoveredFromUrl,
                confidence,
                Instant.now(),
                detectionMethod,
                verified
            );
            countsByType.put(endpoint.atsType(), countsByType.getOrDefault(endpoint.atsType(), 0) + 1);
        }
    }

    private List<AtsDetectionRecord> resolveGreenhouseShortLinks(String html) {
        List<String> shortLinks = atsEndpointExtractor.extractGreenhouseShortLinks(html);
        if (shortLinks.isEmpty()) {
            return List.of();
        }
        List<AtsDetectionRecord> resolved = new ArrayList<>();
        int attempts = 0;
        for (String link : shortLinks) {
            if (attempts >= 2) {
                break;
            }
            attempts++;
            if (!robotsTxtService.isAllowed(link)) {
                continue;
            }
            HttpFetchResult fetch = httpClient.get(link, HTML_ACCEPT);
            resolved.addAll(atsEndpointExtractor.extract(fetch.finalUrlOrRequested(), fetch.body()));
            if (!resolved.isEmpty()) {
                break;
            }
        }
        return resolved;
    }

    private DiscoveryFailure selectFailure(List<DiscoveryFailure> failures) {
        if (failures == null || failures.isEmpty()) {
            return new DiscoveryFailure("discovery_no_match", null, null);
        }
        for (DiscoveryFailure failure : failures) {
            if ("discovery_blocked_by_robots".equals(failure.reasonCode())) {
                return failure;
            }
        }
        for (DiscoveryFailure failure : failures) {
            if ("discovery_fetch_failed".equals(failure.reasonCode())) {
                return failure;
            }
        }
        return failures.getFirst();
    }

    private record DiscoveryOutcome(Map<AtsType, Integer> countsByType, DiscoveryFailure failure) {
        boolean hasEndpoints() {
            return countsByType != null && !countsByType.isEmpty();
        }

        DiscoveryFailure primaryFailure() {
            return failure;
        }
    }

    private record DiscoveryFailure(String reasonCode, String candidateUrl, String detail) {
    }
}
