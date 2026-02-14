package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsType;
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
        "/careers/search"
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

    public CareersDiscoveryService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        PoliteHttpClient httpClient,
        RobotsTxtService robotsTxtService,
        AtsDetector atsDetector
    ) {
        this.properties = properties;
        this.repository = repository;
        this.httpClient = httpClient;
        this.robotsTxtService = robotsTxtService;
        this.atsDetector = atsDetector;
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
                AtsType discovered = discoverForCompany(company);
                if (discovered == null || discovered == AtsType.UNKNOWN) {
                    failedCount++;
                    increment(topErrors, company.ticker() + " (" + company.name() + "): no_ats_detected");
                } else {
                    increment(discoveredCountByType, discovered.name());
                }
            } catch (Exception e) {
                failedCount++;
                increment(topErrors, company.ticker() + " (" + company.name() + "): exception");
                log.warn("Careers discovery failed for {} ({})", company.ticker(), company.domain(), e);
            }
        }

        return new CareersDiscoveryResult(discoveredCountByType, failedCount, topErrors);
    }

    private AtsType discoverForCompany(CompanyTarget company) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        String base = "https://" + company.domain();
        LinkedHashSet<String> seen = new LinkedHashSet<>();
        AtsType detectedByPattern = AtsType.UNKNOWN;

        if (company.careersHintUrl() != null && !company.careersHintUrl().isBlank()) {
            candidates.add(company.careersHintUrl().trim());
        }
        for (String path : COMMON_PATHS) {
            candidates.add(base + path);
        }
        candidates.add("https://careers." + company.domain());
        candidates.add("https://jobs." + company.domain());
        candidates.addAll(discoverLinksFromHomepage(company.domain(), base));

        int maxCandidates = properties.getCareersDiscovery().getMaxCandidatesPerCompany();
        int inspected = 0;
        for (String candidate : candidates) {
            if (inspected >= maxCandidates) {
                break;
            }
            inspected++;

            AtsType patternType = atsDetector.detect(candidate);
            if (patternType != AtsType.UNKNOWN) {
                if (registerPatternEndpoint(company, candidate, patternType, seen) && detectedByPattern == AtsType.UNKNOWN) {
                    detectedByPattern = patternType;
                }
            }

            if (!robotsTxtService.isAllowed(candidate)) {
                log.debug("Careers discovery blocked by robots: {}", candidate);
                continue;
            }
            HttpFetchResult fetch = httpClient.get(candidate, HTML_ACCEPT);
            AtsType detected = atsDetector.detect(fetch.finalUrlOrRequested(), fetch.body());
            if (detected == AtsType.UNKNOWN) {
                continue;
            }

            double confidence = atsDetector.detect(fetch.finalUrlOrRequested()) != AtsType.UNKNOWN ? 0.95 : 0.75;
            repository.upsertAtsEndpoint(
                company.companyId(),
                detected,
                fetch.finalUrlOrRequested(),
                candidate,
                confidence,
                Instant.now(),
                "html",
                true
            );
            return detected;
        }
        return detectedByPattern;
    }

    private List<String> discoverLinksFromHomepage(String domain, String baseUrl) {
        List<String> out = new ArrayList<>();
        String homepage = baseUrl + "/";
        if (!robotsTxtService.isAllowed(homepage)) {
            return out;
        }

        HttpFetchResult fetch = httpClient.get(homepage, HTML_ACCEPT);
        if (!fetch.isSuccessful() || fetch.body() == null) {
            return out;
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
                out.add(href);
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

    private boolean registerPatternEndpoint(
        CompanyTarget company,
        String candidate,
        AtsType type,
        LinkedHashSet<String> seen
    ) {
        String key = type.name() + "|" + candidate.toLowerCase(Locale.ROOT);
        if (seen.contains(key)) {
            return false;
        }
        seen.add(key);
        repository.upsertAtsEndpoint(
            company.companyId(),
            type,
            candidate,
            candidate,
            0.6,
            Instant.now(),
            "pattern",
            false
        );
        return true;
    }

    private void increment(Map<String, Integer> map, String key) {
        map.put(key, map.getOrDefault(key, 0) + 1);
    }
}
