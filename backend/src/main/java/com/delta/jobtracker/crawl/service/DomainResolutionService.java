package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class DomainResolutionService {
    private static final Logger log = LoggerFactory.getLogger(DomainResolutionService.class);
    private static final String WDQS_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
    private static final String SPARQL_ACCEPT = "application/sparql-results+json";

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;
    private final PoliteHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Object wdqsThrottleLock = new Object();
    private Instant wdqsNextAllowedAt = Instant.EPOCH;

    public DomainResolutionService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        PoliteHttpClient httpClient,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.repository = repository;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public DomainResolutionResult resolveMissingDomains(Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);

        List<CompanyIdentity> missingDomain = repository.findCompaniesMissingDomain(limit);
        return resolveCompanies(missingDomain, limit);
    }

    public DomainResolutionResult resolveMissingDomainsForTickers(List<String> tickers, Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);
        List<CompanyIdentity> missingDomain = repository.findCompaniesMissingDomainByTickers(tickers, limit);
        return resolveCompanies(missingDomain, limit);
    }

    private DomainResolutionResult resolveCompanies(List<CompanyIdentity> missingDomain, int limit) {
        if (missingDomain.isEmpty()) {
            return new DomainResolutionResult(0, 0, List.of());
        }

        int batchSize = Math.min(properties.getDomainResolution().getBatchSize(), limit);
        int resolvedCount = 0;
        int failedCount = 0;
        Map<String, Integer> topErrors = new LinkedHashMap<>();

        for (int from = 0; from < missingDomain.size(); from += batchSize) {
            int to = Math.min(missingDomain.size(), from + batchSize);
            List<CompanyIdentity> batch = missingDomain.subList(from, to);

            Map<String, String> byTicker = resolveWebsitesByTicker(batch);
            List<CompanyIdentity> unresolvedAfterTicker = new ArrayList<>();

            for (CompanyIdentity company : batch) {
                if (!byTicker.containsKey(company.ticker().toUpperCase(Locale.ROOT))) {
                    unresolvedAfterTicker.add(company);
                }
            }
            Map<String, String> byName = resolveWebsitesByName(unresolvedAfterTicker);

            for (CompanyIdentity company : batch) {
                String website = byTicker.get(company.ticker().toUpperCase(Locale.ROOT));
                if (website == null) {
                    website = byName.get(company.name().toLowerCase(Locale.ROOT));
                }
                if (website == null) {
                    failedCount++;
                    increment(topErrors, company.ticker() + " (" + company.name() + "): no_wikidata_match");
                    continue;
                }

                String domain = normalizeDomain(website);
                if (domain == null) {
                    failedCount++;
                    increment(topErrors, company.ticker() + " (" + company.name() + "): invalid_website_url");
                    continue;
                }

                repository.upsertCompanyDomain(
                    company.companyId(),
                    domain,
                    null,
                    "WIKIDATA",
                    0.9,
                    Instant.now()
                );
                resolvedCount++;
            }
        }

        List<String> top = topErrors.entrySet().stream()
            .limit(10)
            .map(entry -> entry.getKey() + " x" + entry.getValue())
            .toList();

        log.info("Domain resolver finished. resolved={}, failed={}", resolvedCount, failedCount);
        return new DomainResolutionResult(resolvedCount, failedCount, top);
    }

    private Map<String, String> resolveWebsitesByTicker(List<CompanyIdentity> companies) {
        List<String> tickers = companies.stream()
            .map(CompanyIdentity::ticker)
            .filter(ticker -> ticker != null && !ticker.isBlank())
            .map(ticker -> ticker.toUpperCase(Locale.ROOT))
            .distinct()
            .toList();
        if (tickers.isEmpty()) {
            return Map.of();
        }

        String values = tickers.stream()
            .map(this::sparqlLiteral)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                SELECT ?ticker ?website WHERE {
                  VALUES ?ticker { %s }
                  ?company wdt:P249 ?ticker ;
                           wdt:P856 ?website .
                }
                """.formatted(values);

        JsonNode root = executeWdqsQuery(query);
        if (root == null) {
            return Map.of();
        }

        Map<String, String> result = new HashMap<>();
        JsonNode bindings = root.path("results").path("bindings");
        if (!bindings.isArray()) {
            return result;
        }

        for (JsonNode row : bindings) {
            String ticker = row.path("ticker").path("value").asText(null);
            String website = row.path("website").path("value").asText(null);
            if (ticker == null || website == null) {
                continue;
            }
            result.putIfAbsent(ticker.toUpperCase(Locale.ROOT), website);
        }
        return result;
    }

    private Map<String, String> resolveWebsitesByName(List<CompanyIdentity> companies) {
        List<String> names = companies.stream()
            .map(CompanyIdentity::name)
            .filter(name -> name != null && !name.isBlank())
            .distinct()
            .toList();
        if (names.isEmpty()) {
            return Map.of();
        }

        String values = names.stream()
            .map(this::sparqlLiteral)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?name ?website WHERE {
                  VALUES ?name { %s }
                  ?company rdfs:label ?label ;
                           wdt:P856 ?website .
                  FILTER (LANG(?label) = "en")
                  FILTER (LCASE(STR(?label)) = LCASE(?name))
                }
                """.formatted(values);

        JsonNode root = executeWdqsQuery(query);
        if (root == null) {
            return Map.of();
        }

        Map<String, String> result = new HashMap<>();
        JsonNode bindings = root.path("results").path("bindings");
        if (!bindings.isArray()) {
            return result;
        }

        for (JsonNode row : bindings) {
            String name = row.path("name").path("value").asText(null);
            String website = row.path("website").path("value").asText(null);
            if (name == null || website == null) {
                continue;
            }
            result.putIfAbsent(name.toLowerCase(Locale.ROOT), website);
        }
        return result;
    }

    private JsonNode executeWdqsQuery(String sparql) {
        waitForWdqsSlot();
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String url = WDQS_ENDPOINT + "?query=" + encoded;
        HttpFetchResult fetch = httpClient.get(url, SPARQL_ACCEPT);
        if (!fetch.isSuccessful() || fetch.statusCode() < 200 || fetch.statusCode() >= 300 || fetch.body() == null) {
            log.warn("WDQS query failed: status={}, errorCode={}", fetch.statusCode(), fetch.errorCode());
            return null;
        }
        try {
            return objectMapper.readTree(fetch.body());
        } catch (Exception e) {
            log.warn("Failed to parse WDQS response", e);
            return null;
        }
    }

    private void waitForWdqsSlot() {
        synchronized (wdqsThrottleLock) {
            Instant now = Instant.now();
            if (wdqsNextAllowedAt.isAfter(now)) {
                long sleepMs = Duration.between(now, wdqsNextAllowedAt).toMillis();
                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            wdqsNextAllowedAt = Instant.now().plusMillis(properties.getDomainResolution().getWdqsMinDelayMs());
        }
    }

    private String normalizeDomain(String websiteUrl) {
        if (websiteUrl == null || websiteUrl.isBlank()) {
            return null;
        }
        String value = websiteUrl.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        try {
            URI uri = new URI(value);
            String host = uri.getHost();
            if (host == null || host.isBlank()) {
                return null;
            }
            host = host.toLowerCase(Locale.ROOT);
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            return host;
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private String sparqlLiteral(String raw) {
        String escaped = raw
            .replace("\\", "\\\\")
            .replace("\"", "\\\"");
        return "\"" + escaped + "\"";
    }

    private void increment(Map<String, Integer> errors, String key) {
        errors.put(key, errors.getOrDefault(key, 0) + 1);
    }
}
