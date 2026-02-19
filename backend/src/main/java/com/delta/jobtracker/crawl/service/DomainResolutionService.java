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
import java.net.URLDecoder;
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

@Service
public class DomainResolutionService {
    private static final Logger log = LoggerFactory.getLogger(DomainResolutionService.class);
    private static final String WDQS_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
    private static final String SPARQL_ACCEPT = "application/sparql-results+json";
    private static final int MAX_ERROR_SAMPLES = 10;
    private static final int WDQS_MAX_ATTEMPTS = 3;
    private static final long WDQS_RETRY_BASE_MS = 750L;
    private static final long WDQS_RETRY_MAX_MS = 4000L;
    private static final int WDQS_BODY_SAMPLE_CHARS = 500;

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
        return resolveCompanies(missingDomain, limit, null);
    }

    public DomainResolutionResult resolveMissingDomainsForTickers(List<String> tickers, Integer requestedLimit) {
        int limit = requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);
        List<CompanyIdentity> missingDomain = repository.findCompaniesMissingDomainByTickers(tickers, limit);
        return resolveCompanies(missingDomain, limit, null);
    }

    public DomainResolutionResult resolveMissingDomainsForTickers(
        List<String> tickers,
        Integer requestedLimit,
        Instant deadline
    ) {
        int limit = requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);
        List<CompanyIdentity> missingDomain = repository.findCompaniesMissingDomainByTickers(tickers, limit);
        return resolveCompanies(missingDomain, limit, deadline);
    }

    private DomainResolutionResult resolveCompanies(List<CompanyIdentity> missingDomain, int limit, Instant deadline) {
        if (missingDomain.isEmpty()) {
            return new DomainResolutionResult(0, 0, 0, 0, 0, List.of());
        }

        int batchSize = Math.min(properties.getDomainResolution().getBatchSize(), limit);
        Counts counts = new Counts();
        ErrorCollector errors = new ErrorCollector();
        int totalWithTitle = 0;
        for (CompanyIdentity company : missingDomain) {
            if (company.wikipediaTitle() != null && !company.wikipediaTitle().isBlank()) {
                totalWithTitle++;
            }
        }
        int batchCount = (missingDomain.size() + batchSize - 1) / batchSize;
        log.info("Domain resolver starting: companies={}, with_wikipedia_title={}, batch_size={}",
            missingDomain.size(),
            totalWithTitle,
            batchSize
        );

        for (int from = 0; from < missingDomain.size(); from += batchSize) {
            if (deadline != null && Instant.now().isAfter(deadline)) {
                log.info("Domain resolver stopped early due to time budget (processed {} of {})", from, missingDomain.size());
                break;
            }
            int to = Math.min(missingDomain.size(), from + batchSize);
            List<CompanyIdentity> batch = missingDomain.subList(from, to);
            int batchIndex = (from / batchSize) + 1;

            Map<CompanyIdentity, List<String>> titlesByCompany = new LinkedHashMap<>();
            Map<String, List<CompanyIdentity>> companiesByTitle = new LinkedHashMap<>();

            for (CompanyIdentity company : batch) {
                if (deadline != null && Instant.now().isAfter(deadline)) {
                    log.info("Domain resolver stopped early due to time budget (batch {}/{})", batchIndex, batchCount);
                    break;
                }
                List<String> titles = buildTitleVariants(company.wikipediaTitle());
                if (titles.isEmpty()) {
                    counts.noWikipediaTitle++;
                    errors.add(company, "no_wikipedia_title");
                    continue;
                }
                titlesByCompany.put(company, titles);
                for (String title : titles) {
                    companiesByTitle.computeIfAbsent(title, ignored -> new ArrayList<>()).add(company);
                }
            }

            if (titlesByCompany.isEmpty()) {
                log.info("Domain resolver batch {}/{} skipped (no wikipedia_title)", batchIndex, batchCount);
                continue;
            }

            log.info(
                "Domain resolver batch {}/{} companies={} titles={}",
                batchIndex,
                batchCount,
                batch.size(),
                companiesByTitle.keySet().size()
            );
            WdqsLookup lookup = fetchWdqsMatches(new ArrayList<>(companiesByTitle.keySet()));
            if (lookup == null) {
                counts.wdqsError += titlesByCompany.size();
                for (CompanyIdentity company : titlesByCompany.keySet()) {
                    errors.add(company, "wdqs_error");
                }
                continue;
            }

            for (Map.Entry<CompanyIdentity, List<String>> entry : titlesByCompany.entrySet()) {
                CompanyIdentity company = entry.getKey();
                WdqsMatch match = findMatch(entry.getValue(), lookup.matches());
                if (match == null) {
                    counts.noItem++;
                    errors.add(company, "no_item");
                    continue;
                }
                if (match.website() == null || match.website().isBlank()) {
                    counts.noP856++;
                    errors.add(company, "no_p856");
                    continue;
                }

                String domain = normalizeDomain(match.website());
                if (domain == null) {
                    counts.noP856++;
                    errors.add(company, "invalid_website_url");
                    continue;
                }

                repository.upsertCompanyDomain(
                    company.companyId(),
                    domain,
                    null,
                    "WIKIDATA",
                    0.95,
                    Instant.now(),
                    "enwiki_sitelink",
                    match.qid()
                );
                counts.resolved++;
            }
        }

        log.info(
            "Domain resolver finished. resolved={} no_wikipedia_title={} no_item={} no_p856={} wdqs_error={}",
            counts.resolved,
            counts.noWikipediaTitle,
            counts.noItem,
            counts.noP856,
            counts.wdqsError
        );
        return new DomainResolutionResult(
            counts.resolved,
            counts.noWikipediaTitle,
            counts.noItem,
            counts.noP856,
            counts.wdqsError,
            errors.sampleErrors()
        );
    }

    private WdqsMatch findMatch(List<String> titles, Map<String, WdqsMatch> matches) {
        for (String title : titles) {
            WdqsMatch match = matches.get(title);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private WdqsLookup fetchWdqsMatches(List<String> titles) {
        if (titles.isEmpty()) {
            return new WdqsLookup(Map.of());
        }

        String values = titles.stream()
            .map(title -> sparqlLangLiteral(title, "en"))
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                PREFIX schema: <http://schema.org/>
                SELECT ?candidateTitle ?articleTitle ?item ?officialWebsite WHERE {
                  VALUES ?candidateTitle { %s }
                  ?article schema:isPartOf <https://en.wikipedia.org/> ;
                           schema:name ?articleTitle ;
                           schema:about ?item .
                  FILTER (LCASE(STR(?articleTitle)) = LCASE(STR(?candidateTitle)))
                  OPTIONAL { ?item wdt:P856 ?officialWebsite . }
                }
                """.formatted(values);

        JsonNode root = executeWdqsQuery(query);
        if (root == null) {
            return null;
        }

        Map<String, WdqsMatch> matches = new HashMap<>();
        JsonNode bindings = root.path("results").path("bindings");
        if (bindings.isArray()) {
            for (JsonNode row : bindings) {
                String candidate = row.path("candidateTitle").path("value").asText(null);
                String title = row.path("articleTitle").path("value").asText(null);
                String item = row.path("item").path("value").asText(null);
                String website = row.path("officialWebsite").path("value").asText(null);
                if (candidate == null || title == null || item == null) {
                    continue;
                }
                String qid = extractQid(item);
                WdqsMatch existing = matches.get(candidate);
                if (existing == null) {
                    matches.put(candidate, new WdqsMatch(qid, website));
                } else if ((existing.website() == null || existing.website().isBlank()) && website != null) {
                    matches.put(candidate, new WdqsMatch(existing.qid() != null ? existing.qid() : qid, website));
                }
            }
        }
        return new WdqsLookup(matches);
    }

    private JsonNode executeWdqsQuery(String sparql) {
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String formBody = "query=" + encoded;

        for (int attempt = 1; attempt <= WDQS_MAX_ATTEMPTS; attempt++) {
            waitForWdqsSlot();
            HttpFetchResult fetch = httpClient.postForm(WDQS_ENDPOINT, formBody, SPARQL_ACCEPT);
            if (fetch.isSuccessful() && fetch.statusCode() >= 200 && fetch.statusCode() < 300 && fetch.body() != null) {
                try {
                    return objectMapper.readTree(fetch.body());
                } catch (Exception e) {
                    log.warn("Failed to parse WDQS response on attempt {}", attempt, e);
                    return null;
                }
            }

            log.warn(
                "WDQS query failed attempt={} status={} errorCode={} bodySample={}",
                attempt,
                fetch.statusCode(),
                fetch.errorCode(),
                sampleBody(fetch.body())
            );

            if (!shouldRetry(fetch) || attempt == WDQS_MAX_ATTEMPTS) {
                return null;
            }
            sleepBackoff(attempt);
        }
        return null;
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

    private List<String> buildTitleVariants(String rawTitle) {
        if (rawTitle == null || rawTitle.isBlank()) {
            return List.of();
        }
        String trimmed = stripFragmentAndQuery(rawTitle.trim());
        if (trimmed.isBlank()) {
            return List.of();
        }

        String decoded = decodeTitle(trimmed);
        String spaced = decoded.replace('_', ' ').trim();
        if (spaced.isBlank()) {
            return List.of();
        }
        List<String> variants = new ArrayList<>();
        String cleaned = cleanupTitle(spaced);
        addVariant(variants, cleaned);
        addVariant(variants, stripCorporateSuffixes(cleaned));
        addVariant(variants, cleaned.replace("&", "and"));
        return List.copyOf(variants);
    }

    private String stripFragmentAndQuery(String value) {
        String result = value;
        int hashIdx = result.indexOf('#');
        if (hashIdx > 0) {
            result = result.substring(0, hashIdx);
        }
        int queryIdx = result.indexOf('?');
        if (queryIdx > 0) {
            result = result.substring(0, queryIdx);
        }
        return result;
    }

    private String decodeTitle(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return value;
        }
    }

    private void addVariant(List<String> variants, String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return;
        }
        String normalized = candidate.trim();
        if (!variants.contains(normalized)) {
            variants.add(normalized);
        }
    }

    private String cleanupTitle(String value) {
        String cleaned = value.trim();
        cleaned = cleaned.replaceAll("\\s*/.*$", "");
        cleaned = cleaned.replaceAll("\\s*\\(.*\\)$", "");
        cleaned = cleaned.replaceAll("\\s+", " ").trim();
        return cleaned;
    }

    private String stripCorporateSuffixes(String value) {
        if (value == null || value.isBlank()) {
            return value;
        }
        String cleaned = value.trim();
        String[] tokens = cleaned.split(" ");
        int end = tokens.length;
        while (end > 1) {
            String token = normalizeSuffixToken(tokens[end - 1]);
            if (!isCorporateSuffix(token)) {
                break;
            }
            end--;
        }
        if (end == tokens.length) {
            return cleaned;
        }
        return String.join(" ", java.util.Arrays.copyOf(tokens, end)).trim();
    }

    private String normalizeSuffixToken(String token) {
        if (token == null) {
            return "";
        }
        return token.replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT);
    }

    private boolean isCorporateSuffix(String token) {
        return token.equals("INC")
            || token.equals("CORP")
            || token.equals("CORPORATION")
            || token.equals("CO")
            || token.equals("COMPANY")
            || token.equals("LTD")
            || token.equals("LIMITED")
            || token.equals("PLC")
            || token.equals("LLC")
            || token.equals("LP")
            || token.equals("AG")
            || token.equals("SA")
            || token.equals("NV")
            || token.equals("HOLDING")
            || token.equals("HOLDINGS")
            || token.equals("GROUP")
            || token.equals("TRUST");
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

    private String sparqlLangLiteral(String raw, String lang) {
        String literal = sparqlLiteral(raw);
        if (lang == null || lang.isBlank()) {
            return literal;
        }
        return literal + "@" + lang;
    }

    private String extractQid(String itemUrl) {
        if (itemUrl == null || itemUrl.isBlank()) {
            return null;
        }
        int idx = itemUrl.lastIndexOf('/');
        if (idx < 0 || idx + 1 >= itemUrl.length()) {
            return null;
        }
        String candidate = itemUrl.substring(idx + 1);
        return candidate.isBlank() ? null : candidate;
    }

    private boolean shouldRetry(HttpFetchResult fetch) {
        if (fetch == null) {
            return true;
        }
        if (fetch.errorCode() != null) {
            return true;
        }
        int status = fetch.statusCode();
        return status == 429 || status >= 500;
    }

    private void sleepBackoff(int attempt) {
        long backoffMs = Math.min(WDQS_RETRY_MAX_MS, WDQS_RETRY_BASE_MS * (1L << (attempt - 1)));
        try {
            Thread.sleep(backoffMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String sampleBody(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        String cleaned = body.replaceAll("\\s+", " ").trim();
        if (cleaned.length() <= WDQS_BODY_SAMPLE_CHARS) {
            return cleaned;
        }
        return cleaned.substring(0, WDQS_BODY_SAMPLE_CHARS) + "...";
    }

    private static class Counts {
        private int resolved;
        private int noWikipediaTitle;
        private int noItem;
        private int noP856;
        private int wdqsError;
    }

    private static class ErrorCollector {
        private final List<String> sampleErrors = new ArrayList<>();

        private void add(CompanyIdentity company, String error) {
            if (sampleErrors.size() >= MAX_ERROR_SAMPLES) {
                return;
            }
            String ticker = company.ticker() == null ? "" : company.ticker();
            String name = company.name() == null ? "" : company.name();
            sampleErrors.add(ticker + " (" + name + "): " + error);
        }

        private List<String> sampleErrors() {
            return List.copyOf(sampleErrors);
        }
    }

    private record WdqsLookup(Map<String, WdqsMatch> matches) {
    }

    private record WdqsMatch(String qid, String website) {
    }
}
