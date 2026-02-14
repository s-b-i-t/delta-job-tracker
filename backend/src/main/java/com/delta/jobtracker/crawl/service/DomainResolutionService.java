package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

@Service
public class DomainResolutionService {
    private static final Logger log = LoggerFactory.getLogger(DomainResolutionService.class);
    private static final String WDQS_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
    private static final String SPARQL_ACCEPT = "application/sparql-results+json";
    private static final String WIKIPEDIA_DOMAIN = "https://en.wikipedia.org";
    private static final Pattern FOOTNOTE_PATTERN = Pattern.compile("\\[[^\\]]+\\]");

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;
    private final PoliteHttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Sp500WikipediaClient sp500WikipediaClient;
    private final Object wdqsThrottleLock = new Object();
    private Instant wdqsNextAllowedAt = Instant.EPOCH;

    public DomainResolutionService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        PoliteHttpClient httpClient,
        ObjectMapper objectMapper,
        Sp500WikipediaClient sp500WikipediaClient
    ) {
        this.properties = properties;
        this.repository = repository;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.sp500WikipediaClient = sp500WikipediaClient;
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
        Map<String, String> wikiArticlesByTicker = fetchWikipediaArticlesByTicker(topErrors);

        for (int from = 0; from < missingDomain.size(); from += batchSize) {
            int to = Math.min(missingDomain.size(), from + batchSize);
            List<CompanyIdentity> batch = missingDomain.subList(from, to);

            Map<String, String> byWikipedia = resolveWebsitesByWikipediaArticle(batch, wikiArticlesByTicker, topErrors);
            List<CompanyIdentity> unresolvedAfterTicker = new ArrayList<>();

            for (CompanyIdentity company : batch) {
                String ticker = company.ticker();
                if (ticker == null || !byWikipedia.containsKey(ticker.toUpperCase(Locale.ROOT))) {
                    unresolvedAfterTicker.add(company);
                }
            }
            Map<String, String> byTicker = resolveWebsitesByTicker(unresolvedAfterTicker, topErrors);
            List<CompanyIdentity> unresolvedAfterName = new ArrayList<>();
            for (CompanyIdentity company : unresolvedAfterTicker) {
                String ticker = company.ticker();
                if (ticker == null || !byTicker.containsKey(ticker.toUpperCase(Locale.ROOT))) {
                    unresolvedAfterName.add(company);
                }
            }
            Map<String, String> byName = resolveWebsitesByName(unresolvedAfterName, topErrors);

            for (CompanyIdentity company : batch) {
                String key = company.ticker() == null ? null : company.ticker().toUpperCase(Locale.ROOT);
                String website = key == null ? null : byWikipedia.get(key);
                if (website == null) {
                    website = key == null ? null : byTicker.get(key);
                }
                if (website == null) {
                    website = key == null ? null : byName.get(key);
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

    private Map<String, String> resolveWebsitesByWikipediaArticle(
        List<CompanyIdentity> companies,
        Map<String, String> wikiArticlesByTicker,
        Map<String, Integer> topErrors
    ) {
        if (companies.isEmpty() || wikiArticlesByTicker.isEmpty()) {
            return Map.of();
        }

        Map<String, String> articleByTicker = new HashMap<>();
        List<String> articles = new ArrayList<>();
        for (CompanyIdentity company : companies) {
            if (company.ticker() == null) {
                continue;
            }
            String ticker = company.ticker().toUpperCase(Locale.ROOT);
            String article = wikiArticlesByTicker.get(ticker);
            if (article == null || article.isBlank()) {
                continue;
            }
            articleByTicker.put(ticker, article);
            articles.add(article);
        }

        if (articles.isEmpty()) {
            return Map.of();
        }

        String values = articles.stream()
            .distinct()
            .map(this::sparqlIri)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                PREFIX schema: <http://schema.org/>
                SELECT ?article ?website WHERE {
                  VALUES ?article { %s }
                  ?article schema:about ?company ;
                           schema:isPartOf <https://en.wikipedia.org/> .
                  ?company wdt:P856 ?website .
                }
                """.formatted(values);

        JsonNode root = executeWdqsQuery(query, topErrors, "wdqs_wikipedia_article_failed");
        if (root == null) {
            return Map.of();
        }

        Map<String, String> websiteByArticle = new HashMap<>();
        JsonNode bindings = root.path("results").path("bindings");
        if (bindings.isArray()) {
            for (JsonNode row : bindings) {
                String article = row.path("article").path("value").asText(null);
                String website = row.path("website").path("value").asText(null);
                if (article == null || website == null) {
                    continue;
                }
                websiteByArticle.putIfAbsent(article, website);
            }
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : articleByTicker.entrySet()) {
            String website = websiteByArticle.get(entry.getValue());
            if (website != null) {
                result.putIfAbsent(entry.getKey(), website);
            }
        }
        return result;
    }

    private Map<String, String> resolveWebsitesByTicker(
        List<CompanyIdentity> companies,
        Map<String, Integer> topErrors
    ) {
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

        JsonNode root = executeWdqsQuery(query, topErrors, "wdqs_ticker_failed");
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

    private Map<String, String> resolveWebsitesByName(
        List<CompanyIdentity> companies,
        Map<String, Integer> topErrors
    ) {
        Map<String, String> variantToTicker = new HashMap<>();
        Set<String> variants = new LinkedHashSet<>();

        for (CompanyIdentity company : companies) {
            if (company.name() == null || company.name().isBlank() || company.ticker() == null) {
                continue;
            }
            Set<String> generated = generateNameVariants(company.name());
            if (generated.isEmpty()) {
                continue;
            }
            String ticker = company.ticker().toUpperCase(Locale.ROOT);
            for (String variant : generated) {
                String key = variant.toLowerCase(Locale.ROOT);
                if (!variantToTicker.containsKey(key)) {
                    variantToTicker.put(key, ticker);
                    variants.add(variant);
                }
            }
        }

        if (variants.isEmpty()) {
            return Map.of();
        }

        String values = variants.stream()
            .map(this::sparqlLiteral)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                SELECT ?name ?website WHERE {
                  VALUES ?name { %s }
                  ?company wdt:P856 ?website ;
                           rdfs:label ?label .
                  FILTER (LANG(?label) = "en")
                  OPTIONAL {
                    ?company skos:altLabel ?altLabel .
                    FILTER (LANG(?altLabel) = "en")
                  }
                  OPTIONAL {
                    ?company wdt:P1448 ?officialName .
                    FILTER (LANG(?officialName) = "en")
                  }
                  BIND(LCASE(?name) AS ?nameLower)
                  BIND(LCASE(STR(?label)) AS ?labelLower)
                  BIND(LCASE(STR(?altLabel)) AS ?altLower)
                  BIND(LCASE(STR(?officialName)) AS ?officialLower)
                  FILTER (
                    ?labelLower = ?nameLower
                    || ?altLower = ?nameLower
                    || ?officialLower = ?nameLower
                  )
                }
                """.formatted(values);

        JsonNode root = executeWdqsQuery(query, topErrors, "wdqs_name_failed");
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
            String ticker = variantToTicker.get(name.toLowerCase(Locale.ROOT));
            if (ticker != null) {
                result.putIfAbsent(ticker, website);
            }
        }
        return result;
    }

    private JsonNode executeWdqsQuery(String sparql, Map<String, Integer> topErrors, String errorKey) {
        waitForWdqsSlot();
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String url = WDQS_ENDPOINT + "?query=" + encoded;
        HttpFetchResult fetch = httpClient.get(url, SPARQL_ACCEPT);
        if (!fetch.isSuccessful() || fetch.statusCode() < 200 || fetch.statusCode() >= 300 || fetch.body() == null) {
            log.warn("WDQS query failed: status={}, errorCode={}", fetch.statusCode(), fetch.errorCode());
            if (topErrors != null && errorKey != null) {
                increment(topErrors, errorKey);
            }
            return null;
        }
        try {
            return objectMapper.readTree(fetch.body());
        } catch (Exception e) {
            log.warn("Failed to parse WDQS response", e);
            if (topErrors != null && errorKey != null) {
                increment(topErrors, errorKey);
            }
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

    private String sparqlIri(String url) {
        String safe = url.replace(">", "%3E");
        return "<" + safe + ">";
    }

    private Map<String, String> fetchWikipediaArticlesByTicker(Map<String, Integer> topErrors) {
        String wikiUrl = properties.getData().getSp500WikipediaUrl();
        if (wikiUrl == null || wikiUrl.isBlank()) {
            increment(topErrors, "wikipedia_url_missing");
            return Map.of();
        }

        Document document;
        try {
            int timeoutMs = Math.max(1, properties.getRequestTimeoutSeconds()) * 1000;
            document = sp500WikipediaClient.fetchConstituentsPage(wikiUrl, properties.getUserAgent(), timeoutMs);
        } catch (Exception e) {
            log.warn("Failed to fetch Wikipedia constituents page for domain resolution", e);
            increment(topErrors, "wikipedia_fetch_failed");
            return Map.of();
        }

        Element table = findConstituentsTable(document);
        if (table == null) {
            increment(topErrors, "wikipedia_table_missing");
            return Map.of();
        }

        Map<String, Integer> headerIndex = buildHeaderIndex(table);
        Integer tickerIndex = findHeaderIndex(headerIndex, "symbol", "ticker");
        Integer nameIndex = findHeaderIndex(headerIndex, "security", "name");
        if (tickerIndex == null || nameIndex == null) {
            increment(topErrors, "wikipedia_table_headers_missing");
            return Map.of();
        }

        Map<String, String> result = new HashMap<>();
        for (Element row : table.select("tr")) {
            Elements cells = row.select("td");
            if (cells.isEmpty()) {
                continue;
            }
            String ticker = normalizeTicker(readCell(cells, tickerIndex));
            if (ticker == null) {
                continue;
            }
            Element nameCell = cells.get(nameIndex);
            String articleUrl = extractWikipediaArticleUrl(nameCell);
            if (articleUrl != null) {
                result.putIfAbsent(ticker, articleUrl);
            }
        }
        return result;
    }

    private String extractWikipediaArticleUrl(Element nameCell) {
        if (nameCell == null) {
            return null;
        }
        Element link = nameCell.selectFirst("a[href]");
        if (link == null) {
            return null;
        }
        String href = link.attr("href");
        if (href == null || href.isBlank()) {
            return null;
        }
        String normalized = href.trim();
        if (normalized.startsWith("//")) {
            normalized = "https:" + normalized;
        } else if (normalized.startsWith("/wiki/")) {
            normalized = WIKIPEDIA_DOMAIN + normalized;
        } else if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            return null;
        }
        int hashIdx = normalized.indexOf('#');
        if (hashIdx > 0) {
            normalized = normalized.substring(0, hashIdx);
        }
        return normalized;
    }

    private Element findConstituentsTable(Document document) {
        if (document == null) {
            return null;
        }
        Element byId = document.selectFirst("table#constituents");
        if (byId != null) {
            return byId;
        }
        for (Element table : document.select("table")) {
            Map<String, Integer> headerIndex = buildHeaderIndex(table);
            boolean hasSymbol = findHeaderIndex(headerIndex, "symbol", "ticker") != null;
            boolean hasSecurity = findHeaderIndex(headerIndex, "security", "name") != null;
            boolean hasSector = findHeaderIndex(headerIndex, "gics sector", "sector") != null;
            if (hasSymbol && hasSecurity && hasSector) {
                return table;
            }
        }
        return null;
    }

    private Map<String, Integer> buildHeaderIndex(Element table) {
        Map<String, Integer> index = new LinkedHashMap<>();
        Element headerRow = table.selectFirst("tr:has(th)");
        if (headerRow == null) {
            return index;
        }
        int i = 0;
        for (Element th : headerRow.select("th")) {
            String normalizedHeader = normalizeHeader(th.text());
            if (!normalizedHeader.isEmpty()) {
                index.put(normalizedHeader, i);
            }
            i++;
        }
        return index;
    }

    private Integer findHeaderIndex(Map<String, Integer> headerIndex, String... names) {
        for (String name : names) {
            for (Map.Entry<String, Integer> entry : headerIndex.entrySet()) {
                String header = entry.getKey();
                if (header.equals(name) || header.contains(name)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private String normalizeHeader(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace('\u00A0', ' ')
            .trim()
            .replaceAll("\\s+", " ")
            .toLowerCase(Locale.ROOT);
    }

    private String readCell(Elements cells, Integer index) {
        if (index == null || index < 0 || index >= cells.size()) {
            return null;
        }
        return cells.get(index).text();
    }

    private String normalizeTicker(String ticker) {
        String normalized = normalizeText(ticker);
        return normalized == null ? null : normalized.toUpperCase(Locale.ROOT);
    }

    private String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.replace('\u00A0', ' ').trim();
        cleaned = FOOTNOTE_PATTERN.matcher(cleaned).replaceAll("").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private Set<String> generateNameVariants(String name) {
        String normalized = normalizeText(name);
        if (normalized == null) {
            return Set.of();
        }

        Set<String> variants = new LinkedHashSet<>();
        variants.add(normalized);

        String noPrefix = stripPrefix(normalized, "the ");
        if (!noPrefix.equals(normalized)) {
            variants.add(noPrefix);
        }

        String strippedSuffix = stripCorporateSuffix(normalized);
        if (!strippedSuffix.equals(normalized)) {
            variants.add(strippedSuffix);
        }

        String noPunct = stripPunctuation(normalized);
        if (!noPunct.equals(normalized)) {
            variants.add(noPunct);
        }

        String noPunctSuffix = stripCorporateSuffix(noPunct);
        if (!noPunctSuffix.equals(noPunct)) {
            variants.add(noPunctSuffix);
        }

        variants.removeIf(value -> value == null || value.isBlank() || value.length() < 3);
        return variants;
    }

    private String stripPunctuation(String value) {
        return value.replaceAll("[^A-Za-z0-9\\s]", "").replaceAll("\\s+", " ").trim();
    }

    private String stripPrefix(String value, String prefix) {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.startsWith(prefix)) {
            return value.substring(prefix.length()).trim();
        }
        return value;
    }

    private String stripCorporateSuffix(String value) {
        String trimmed = value.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        String[] suffixes = {
            "incorporated", "inc", "corp", "corporation", "company", "co", "ltd", "plc", "holdings", "group", "limited"
        };
        for (String suffix : suffixes) {
            String dotted = suffix + ".";
            if (lower.endsWith(" " + suffix)) {
                return trimmed.substring(0, trimmed.length() - suffix.length()).trim();
            }
            if (lower.endsWith(" " + dotted)) {
                return trimmed.substring(0, trimmed.length() - dotted.length()).trim();
            }
        }
        return trimmed;
    }

    private void increment(Map<String, Integer> errors, String key) {
        errors.put(key, errors.getOrDefault(key, 0) + 1);
    }
}
