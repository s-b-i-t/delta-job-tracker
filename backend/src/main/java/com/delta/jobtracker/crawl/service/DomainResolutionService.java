package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.net.IDN;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
public class DomainResolutionService {
    private static final Logger log = LoggerFactory.getLogger(DomainResolutionService.class);
    private static final String WDQS_ENDPOINT = "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
    private static final String SPARQL_ACCEPT = "application/sparql-results+json";
    private static final String CIK_PROPERTY = "P5531";
    private static final String TICKER_PROPERTY = "P249";
    private static final String METHOD_WIKIPEDIA = "WIKIPEDIA_TITLE";
    private static final String METHOD_CIK = "CIK";
    private static final String METHOD_TICKER = "TICKER";
    private static final String METHOD_OVERRIDE = "OVERRIDE";
    private static final String METHOD_NONE = "NONE";
    private static final String STATUS_RESOLVED = "RESOLVED";
    private static final String STATUS_NO_ITEM = "NO_ITEM";
    private static final String STATUS_NO_P856 = "NO_P856";
    private static final String STATUS_INVALID_WEBSITE = "INVALID_WEBSITE_URL";
    private static final String STATUS_WDQS_ERROR = "WDQS_ERROR";
    private static final String STATUS_WDQS_TIMEOUT = "WDQS_TIMEOUT";
    private static final String STATUS_NO_IDENTIFIER = "NO_IDENTIFIER";
    private static final int MAX_ERROR_SAMPLES = 10;
    private static final int WDQS_MAX_ATTEMPTS = 3;
    private static final long WDQS_RETRY_BASE_MS = 750L;
    private static final long WDQS_RETRY_MAX_MS = 4000L;
    private static final int WDQS_BODY_SAMPLE_CHARS = 500;
    private static final int CACHE_MAX_ENTRIES = 1024;
    private static final Set<String> ATS_HOST_SUFFIXES = Set.of(
        "greenhouse.io",
        "lever.co",
        "myworkdayjobs.com",
        "myworkday.com",
        "ashbyhq.com"
    );

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;
    private final WdqsHttpClient wdqsHttpClient;
    private final ObjectMapper objectMapper;
    private final Object wdqsThrottleLock = new Object();
    private Instant wdqsNextAllowedAt = Instant.EPOCH;
    private final Map<String, WdqsMatch> titleCache = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, WdqsMatch> cikCache = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, WdqsMatch> tickerCache = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, DomainOverride> overridesByTicker = Collections.synchronizedMap(new LinkedHashMap<>());
    private volatile boolean overridesLoaded = false;

    public DomainResolutionService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        WdqsHttpClient wdqsHttpClient,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.repository = repository;
        this.wdqsHttpClient = wdqsHttpClient;
        this.objectMapper = objectMapper;
    }

    public DomainResolutionResult resolveMissingDomains(Integer requestedLimit) {
        return resolveMissingDomains(requestedLimit, null);
    }

    public DomainResolutionResult resolveMissingDomains(Integer requestedLimit, Instant deadline) {
        int limit = requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);

        List<CompanyIdentity> missingDomain = repository.findCompaniesMissingDomain(limit);
        return resolveCompanies(missingDomain, limit, deadline);
    }

    public DomainResolutionResult resolveMissingDomainsForTickers(List<String> tickers, Integer requestedLimit) {
        return resolveMissingDomainsForTickers(tickers, requestedLimit, null);
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
            return new DomainResolutionResult(0, 0, 0, 0, 0, 0, List.of());
        }

        loadOverrides();
        int batchSize = Math.min(properties.getDomainResolution().getBatchSize(), limit);
        Counts counts = new Counts();
        ErrorCollector errors = new ErrorCollector();
        int totalWithTitle = 0;
        int totalWithCik = 0;
        int totalWithTicker = 0;
        for (CompanyIdentity company : missingDomain) {
            if (hasWikipediaTitle(company)) {
                totalWithTitle++;
            } else if (hasCik(company)) {
                totalWithCik++;
            } else if (hasTicker(company)) {
                totalWithTicker++;
            }
        }
        int batchCount = (missingDomain.size() + batchSize - 1) / batchSize;
        log.info("Domain resolver starting: companies={}, with_wikipedia_title={}, with_cik={}, with_ticker={}, batch_size={}",
            missingDomain.size(),
            totalWithTitle,
            totalWithCik,
            totalWithTicker,
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
            Map<CompanyIdentity, List<String>> ciksByCompany = new LinkedHashMap<>();
            Map<String, List<CompanyIdentity>> companiesByCik = new LinkedHashMap<>();
            Map<CompanyIdentity, List<String>> tickersByCompany = new LinkedHashMap<>();
            Map<String, List<CompanyIdentity>> companiesByTicker = new LinkedHashMap<>();

            for (CompanyIdentity company : batch) {
                if (deadline != null && Instant.now().isAfter(deadline)) {
                    log.info("Domain resolver stopped early due to time budget (batch {}/{})", batchIndex, batchCount);
                    break;
                }
                Instant now = Instant.now();
                if (applyOverride(company, counts)) {
                    continue;
                }
                if (hasWikipediaTitle(company)) {
                    if (shouldSkipCached(company, METHOD_WIKIPEDIA, now)) {
                        continue;
                    }
                    List<String> titles = buildTitleVariants(company.wikipediaTitle());
                    if (titles.isEmpty()) {
                        counts.noWikipediaTitle++;
                        errors.add(company, "no_wikipedia_title");
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_WIKIPEDIA,
                            STATUS_NO_IDENTIFIER,
                            "no_wikipedia_title",
                            now
                        );
                        continue;
                    }
                    titlesByCompany.put(company, titles);
                    WdqsMatch cached = findCachedMatch(titles, titleCache);
                    if (cached != null) {
                        applyMatch(company, cached, METHOD_WIKIPEDIA, "enwiki_sitelink", counts, errors);
                        continue;
                    }
                    for (String title : titles) {
                        companiesByTitle.computeIfAbsent(title, ignored -> new ArrayList<>()).add(company);
                    }
                } else if (hasCik(company)) {
                    if (shouldSkipCached(company, METHOD_CIK, now)) {
                        continue;
                    }
                    List<String> ciks = buildCikVariants(company.cik());
                    if (ciks.isEmpty()) {
                        counts.noWikipediaTitle++;
                        errors.add(company, "no_cik");
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_CIK,
                            STATUS_NO_IDENTIFIER,
                            "no_cik",
                            now
                        );
                        continue;
                    }
                    ciksByCompany.put(company, ciks);
                    WdqsMatch cached = findCachedMatch(ciks, cikCache);
                    if (cached != null) {
                        applyMatch(company, cached, METHOD_CIK, "cik", counts, errors);
                        continue;
                    }
                    for (String cik : ciks) {
                        companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
                    }
                } else if (hasTicker(company)) {
                    if (shouldSkipCached(company, METHOD_TICKER, now)) {
                        continue;
                    }
                    List<String> variants = buildTickerVariants(company.ticker());
                    if (variants.isEmpty()) {
                        counts.noWikipediaTitle++;
                        errors.add(company, "no_ticker");
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_TICKER,
                            STATUS_NO_IDENTIFIER,
                            "no_ticker",
                            now
                        );
                        continue;
                    }
                    tickersByCompany.put(company, variants);
                    WdqsMatch cached = findCachedMatch(variants, tickerCache);
                    if (cached != null) {
                        applyMatch(company, cached, METHOD_TICKER, "stock_ticker", counts, errors);
                        continue;
                    }
                    for (String variant : variants) {
                        companiesByTicker.computeIfAbsent(variant, ignored -> new ArrayList<>()).add(company);
                    }
                } else {
                    counts.noWikipediaTitle++;
                    errors.add(company, "no_identifier");
                    repository.updateCompanyDomainResolutionCache(
                        company.companyId(),
                        METHOD_NONE,
                        STATUS_NO_IDENTIFIER,
                        "no_identifier",
                        now
                    );
                }
            }

            if (!titlesByCompany.isEmpty()) {
                log.info(
                    "Domain resolver batch {}/{} title-companies={} titles={}",
                    batchIndex,
                    batchCount,
                    titlesByCompany.size(),
                    companiesByTitle.keySet().size()
                );
                WdqsLookupResult lookupResult = fetchWdqsMatchesByTitle(new ArrayList<>(companiesByTitle.keySet()));
                if (lookupResult.lookup() == null) {
                    String errorCategory = lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
                    for (CompanyIdentity company : titlesByCompany.keySet()) {
                        if ("wdqs_timeout".equals(errorCategory)) {
                            counts.wdqsTimeout++;
                        } else {
                            counts.wdqsError++;
                        }
                        errors.add(company, errorCategory);
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_WIKIPEDIA,
                            "wdqs_timeout".equals(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
                            errorCategory,
                            Instant.now()
                        );
                    }
                } else {
                    cacheMatches(titleCache, lookupResult.lookup().matches());
                    for (Map.Entry<CompanyIdentity, List<String>> entry : titlesByCompany.entrySet()) {
                        CompanyIdentity company = entry.getKey();
                        WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
                        applyMatch(company, match, METHOD_WIKIPEDIA, "enwiki_sitelink", counts, errors);
                    }
                }
            }

            if (!ciksByCompany.isEmpty()) {
                log.info(
                    "Domain resolver batch {}/{} cik-companies={} ciks={}",
                    batchIndex,
                    batchCount,
                    ciksByCompany.size(),
                    companiesByCik.keySet().size()
                );
                WdqsLookupResult lookupResult = fetchWdqsMatchesByCik(new ArrayList<>(companiesByCik.keySet()));
                if (lookupResult.lookup() == null) {
                    String errorCategory = lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
                    for (CompanyIdentity company : ciksByCompany.keySet()) {
                        if ("wdqs_timeout".equals(errorCategory)) {
                            counts.wdqsTimeout++;
                        } else {
                            counts.wdqsError++;
                        }
                        errors.add(company, errorCategory);
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_CIK,
                            "wdqs_timeout".equals(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
                            errorCategory,
                            Instant.now()
                        );
                    }
                } else {
                    cacheMatches(cikCache, lookupResult.lookup().matches());
                    for (Map.Entry<CompanyIdentity, List<String>> entry : ciksByCompany.entrySet()) {
                        CompanyIdentity company = entry.getKey();
                        WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
                        applyMatch(company, match, METHOD_CIK, "cik", counts, errors);
                    }
                }
            }

            if (!tickersByCompany.isEmpty()) {
                log.info(
                    "Domain resolver batch {}/{} ticker-companies={} tickers={}",
                    batchIndex,
                    batchCount,
                    tickersByCompany.size(),
                    companiesByTicker.keySet().size()
                );
                WdqsLookupResult lookupResult = fetchWdqsMatchesByTicker(new ArrayList<>(companiesByTicker.keySet()));
                if (lookupResult.lookup() == null) {
                    String errorCategory = lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
                    for (CompanyIdentity company : tickersByCompany.keySet()) {
                        if ("wdqs_timeout".equals(errorCategory)) {
                            counts.wdqsTimeout++;
                        } else {
                            counts.wdqsError++;
                        }
                        errors.add(company, errorCategory);
                        repository.updateCompanyDomainResolutionCache(
                            company.companyId(),
                            METHOD_TICKER,
                            "wdqs_timeout".equals(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
                            errorCategory,
                            Instant.now()
                        );
                    }
                } else {
                    cacheMatches(tickerCache, lookupResult.lookup().matches());
                    for (Map.Entry<CompanyIdentity, List<String>> entry : tickersByCompany.entrySet()) {
                        CompanyIdentity company = entry.getKey();
                        WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
                        applyMatch(company, match, METHOD_TICKER, "stock_ticker", counts, errors);
                    }
                }
            }
        }

        log.info(
            "Domain resolver finished. resolved={} no_wikipedia_title={} no_item={} no_p856={} wdqs_error={} wdqs_timeout={}",
            counts.resolved,
            counts.noWikipediaTitle,
            counts.noItem,
            counts.noP856,
            counts.wdqsError,
            counts.wdqsTimeout
        );
        return new DomainResolutionResult(
            counts.resolved,
            counts.noWikipediaTitle,
            counts.noItem,
            counts.noP856,
            counts.wdqsError,
            counts.wdqsTimeout,
            errors.sampleErrors()
        );
    }

    private void cacheMatches(Map<String, WdqsMatch> cache, Map<String, WdqsMatch> matches) {
        if (cache == null || matches == null || matches.isEmpty()) {
            return;
        }
        synchronized (cache) {
            for (Map.Entry<String, WdqsMatch> entry : matches.entrySet()) {
                String key = entry.getKey();
                WdqsMatch value = entry.getValue();
                if (key == null || value == null) {
                    continue;
                }
                if (cache.size() >= CACHE_MAX_ENTRIES && !cache.containsKey(key)) {
                    Iterator<String> iterator = cache.keySet().iterator();
                    if (iterator.hasNext()) {
                        iterator.next();
                        iterator.remove();
                    }
                }
                cache.put(key, value);
            }
        }
    }

    private WdqsMatch findCachedMatch(List<String> keys, Map<String, WdqsMatch> cache) {
        if (keys == null || cache == null || cache.isEmpty()) {
            return null;
        }
        synchronized (cache) {
            for (String key : keys) {
                WdqsMatch match = cache.get(key);
                if (match != null) {
                    return match;
                }
            }
        }
        return null;
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

    private void applyMatch(
        CompanyIdentity company,
        WdqsMatch match,
        String method,
        String resolutionMethod,
        Counts counts,
        ErrorCollector errors
    ) {
        if (match == null) {
            counts.noItem++;
            errors.add(company, "no_item");
            repository.updateCompanyDomainResolutionCache(
                company.companyId(),
                method,
                STATUS_NO_ITEM,
                "no_item",
                Instant.now()
            );
            return;
        }
        if (match.website() == null || match.website().isBlank()) {
            counts.noP856++;
            errors.add(company, "no_p856");
            repository.updateCompanyDomainResolutionCache(
                company.companyId(),
                method,
                STATUS_NO_P856,
                "no_p856",
                Instant.now()
            );
            return;
        }

        NormalizedDomain normalized = normalizeDomain(match.website());
        if (normalized == null || normalized.domain() == null || normalized.domain().isBlank()) {
            counts.noP856++;
            errors.add(company, "invalid_website_url");
            repository.updateCompanyDomainResolutionCache(
                company.companyId(),
                method,
                STATUS_INVALID_WEBSITE,
                "invalid_website_url",
                Instant.now()
            );
            return;
        }

        repository.upsertCompanyDomain(
            company.companyId(),
            normalized.domain(),
            normalized.careersHintUrl(),
            "WIKIDATA",
            0.95,
            Instant.now(),
            resolutionMethod,
            match.qid()
        );
        repository.updateCompanyDomainResolutionCache(
            company.companyId(),
            method,
            STATUS_RESOLVED,
            null,
            Instant.now()
        );
        counts.resolved++;
    }

    private boolean hasWikipediaTitle(CompanyIdentity company) {
        return company != null && company.wikipediaTitle() != null && !company.wikipediaTitle().isBlank();
    }

    private boolean hasCik(CompanyIdentity company) {
        return company != null && company.cik() != null && !company.cik().isBlank();
    }

    private boolean hasTicker(CompanyIdentity company) {
        return company != null && company.ticker() != null && !company.ticker().isBlank();
    }

    private boolean applyOverride(CompanyIdentity company, Counts counts) {
        if (company == null || company.ticker() == null) {
            return false;
        }
        DomainOverride override = findOverride(company.ticker());
        if (override == null) {
            return false;
        }
        NormalizedDomain normalized = normalizeDomain(override.domain());
        if (normalized == null || normalized.domain() == null || normalized.domain().isBlank()) {
            log.warn("Override for {} ignored due to invalid domain '{}'", company.ticker(), override.domain());
            return false;
        }
        repository.upsertCompanyDomain(
            company.companyId(),
            normalized.domain(),
            override.careersHintUrl(),
            "MANUAL",
            1.0,
            Instant.now(),
            "override_csv",
            null
        );
        repository.updateCompanyDomainResolutionCache(
            company.companyId(),
            METHOD_OVERRIDE,
            STATUS_RESOLVED,
            null,
            Instant.now()
        );
        counts.resolved++;
        return true;
    }

    private boolean shouldSkipCached(CompanyIdentity company, String method, Instant now) {
        if (company == null || method == null || now == null) {
            return false;
        }
        int ttlMinutes = properties.getDomainResolution().getCacheTtlMinutes();
        if (ttlMinutes <= 0) {
            return false;
        }
        Instant attemptedAt = company.domainResolutionAttemptedAt();
        if (attemptedAt == null) {
            return false;
        }
        String lastMethod = company.domainResolutionMethod();
        if (lastMethod == null || !lastMethod.equalsIgnoreCase(method)) {
            return false;
        }
        return attemptedAt.plus(Duration.ofMinutes(ttlMinutes)).isAfter(now);
    }

    private WdqsLookupResult fetchWdqsMatchesByTitle(List<String> titles) {
        if (titles.isEmpty()) {
            return new WdqsLookupResult(new WdqsLookup(Map.of()), null);
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

        WdqsQueryResult queryResult = executeWdqsQuery(query);
        if (queryResult.root() == null) {
            return new WdqsLookupResult(null, queryResult.errorCategory());
        }

        Map<String, WdqsMatch> matches = new HashMap<>();
        JsonNode bindings = queryResult.root().path("results").path("bindings");
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
        return new WdqsLookupResult(new WdqsLookup(matches), null);
    }

    private WdqsLookupResult fetchWdqsMatchesByCik(List<String> ciks) {
        if (ciks.isEmpty()) {
            return new WdqsLookupResult(new WdqsLookup(Map.of()), null);
        }

        String values = ciks.stream()
            .map(this::sparqlLiteral)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                SELECT ?candidateCik ?item ?officialWebsite WHERE {
                  VALUES ?candidateCik { %s }
                  ?item wdt:%s ?candidateCik .
                  OPTIONAL { ?item wdt:P856 ?officialWebsite . }
                }
                """.formatted(values, CIK_PROPERTY);

        WdqsQueryResult queryResult = executeWdqsQuery(query);
        if (queryResult.root() == null) {
            return new WdqsLookupResult(null, queryResult.errorCategory());
        }

        Map<String, WdqsMatch> matches = new HashMap<>();
        JsonNode bindings = queryResult.root().path("results").path("bindings");
        if (bindings.isArray()) {
            for (JsonNode row : bindings) {
                String candidate = row.path("candidateCik").path("value").asText(null);
                String item = row.path("item").path("value").asText(null);
                String website = row.path("officialWebsite").path("value").asText(null);
                if (candidate == null || item == null) {
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
        return new WdqsLookupResult(new WdqsLookup(matches), null);
    }

    private WdqsLookupResult fetchWdqsMatchesByTicker(List<String> tickers) {
        if (tickers.isEmpty()) {
            return new WdqsLookupResult(new WdqsLookup(Map.of()), null);
        }

        String values = tickers.stream()
            .map(this::sparqlLiteral)
            .reduce((a, b) -> a + " " + b)
            .orElse("");

        String query =
            """
                SELECT ?candidateTicker ?item ?officialWebsite WHERE {
                  VALUES ?candidateTicker { %s }
                  ?item wdt:%s ?candidateTicker .
                  OPTIONAL { ?item wdt:P856 ?officialWebsite . }
                }
                """.formatted(values, TICKER_PROPERTY);

        WdqsQueryResult queryResult = executeWdqsQuery(query);
        if (queryResult.root() == null) {
            return new WdqsLookupResult(null, queryResult.errorCategory());
        }

        Map<String, WdqsMatch> matches = new HashMap<>();
        JsonNode bindings = queryResult.root().path("results").path("bindings");
        if (bindings.isArray()) {
            for (JsonNode row : bindings) {
                String candidate = row.path("candidateTicker").path("value").asText(null);
                String item = row.path("item").path("value").asText(null);
                String website = row.path("officialWebsite").path("value").asText(null);
                if (candidate == null || item == null) {
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
        return new WdqsLookupResult(new WdqsLookup(matches), null);
    }

    private WdqsQueryResult executeWdqsQuery(String sparql) {
        String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
        String formBody = "query=" + encoded;
        String lastCategory = null;

        for (int attempt = 1; attempt <= WDQS_MAX_ATTEMPTS; attempt++) {
            waitForWdqsSlot();
            HttpFetchResult fetch = wdqsHttpClient.postForm(WDQS_ENDPOINT, formBody, SPARQL_ACCEPT);
            if (fetch.isSuccessful() && fetch.statusCode() >= 200 && fetch.statusCode() < 300 && fetch.body() != null) {
                try {
                    return new WdqsQueryResult(objectMapper.readTree(fetch.body()), null);
                } catch (Exception e) {
                    log.warn("Failed to parse WDQS response on attempt {}", attempt, e);
                    return new WdqsQueryResult(null, "wdqs_error");
                }
            }

            lastCategory = isTimeout(fetch) ? "wdqs_timeout" : "wdqs_error";
            log.warn(
                "WDQS query failed attempt={} status={} errorCode={} bodySample={}",
                attempt,
                fetch.statusCode(),
                fetch.errorCode(),
                sampleBody(fetch.body())
            );

            if (!shouldRetry(fetch) || attempt == WDQS_MAX_ATTEMPTS) {
                return new WdqsQueryResult(null, lastCategory);
            }
            sleepBackoff(attempt);
        }
        return new WdqsQueryResult(null, lastCategory == null ? "wdqs_error" : lastCategory);
    }

    private void waitForWdqsSlot() {
        synchronized (wdqsThrottleLock) {
            Instant now = Instant.now();
            if (wdqsNextAllowedAt.isAfter(now)) {
                long sleepMs = Duration.between(now, wdqsNextAllowedAt).toMillis();
                if (sleepMs > 0) {
                    checkCanaryDeadline();
                    try {
                        Thread.sleep(sleepMs);
                        checkCanaryDeadline();
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

    private List<String> buildCikVariants(String rawCik) {
        if (rawCik == null || rawCik.isBlank()) {
            return List.of();
        }
        String digits = rawCik.replaceAll("\\D", "");
        if (digits.isBlank()) {
            return List.of();
        }
        List<String> variants = new ArrayList<>();
        addVariant(variants, digits);
        String noLeading = digits.replaceFirst("^0+(?!$)", "");
        addVariant(variants, noLeading);
        if (digits.length() < 10) {
            String padded = String.format("%1$" + 10 + "s", noLeading).replace(' ', '0');
            addVariant(variants, padded);
        }
        return List.copyOf(variants);
    }

    private List<String> buildTickerVariants(String rawTicker) {
        if (rawTicker == null || rawTicker.isBlank()) {
            return List.of();
        }
        String normalized = rawTicker.trim().toUpperCase(Locale.ROOT);
        List<String> variants = new ArrayList<>();
        addVariant(variants, normalized);
        int colonIdx = normalized.indexOf(':');
        if (colonIdx > 0 && colonIdx + 1 < normalized.length()) {
            addVariant(variants, normalized.substring(colonIdx + 1));
        }
        addVariant(variants, normalized.replace(".", ""));
        addVariant(variants, normalized.replace("-", ""));
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

    private NormalizedDomain normalizeDomain(String websiteUrl) {
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
            host = host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
            host = IDN.toASCII(host);
            host = host.toLowerCase(Locale.ROOT);
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            boolean atsHost = isAtsHost(host);
            String registrable = registrableDomain(host);
            String careersHint = null;
            String resolvedDomain = registrable;
            if (atsHost) {
                careersHint = value;
                String derived = deriveDomainFromAts(host, uri.getPath());
                if (derived != null) {
                    resolvedDomain = registrableDomain(derived);
                }
            }
            if (resolvedDomain == null || resolvedDomain.isBlank()) {
                return null;
            }
            return new NormalizedDomain(resolvedDomain, careersHint);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private boolean isAtsHost(String host) {
        if (host == null) {
            return false;
        }
        for (String suffix : ATS_HOST_SUFFIXES) {
            if (host.equals(suffix) || host.endsWith("." + suffix)) {
                return true;
            }
        }
        return false;
    }

    private String deriveDomainFromAts(String host, String path) {
        if (host == null) {
            return null;
        }
        if (host.endsWith("greenhouse.io") || host.endsWith("lever.co") || host.endsWith("ashbyhq.com")) {
            String slug = firstPathSegment(path);
            if (slug != null) {
                return slug + ".com";
            }
        }
        if (host.endsWith("myworkdayjobs.com") || host.endsWith("myworkday.com")) {
            String[] labels = host.split("\\.");
            if (labels.length >= 3) {
                String prefix = labels[0];
                int dashIdx = prefix.indexOf('-');
                if (dashIdx > 0) {
                    prefix = prefix.substring(0, dashIdx);
                }
                int wdIdx = prefix.indexOf("wd");
                if (wdIdx > 0) {
                    prefix = prefix.substring(0, wdIdx);
                }
                if (!prefix.isBlank()) {
                    return prefix + ".com";
                }
            }
        }
        return null;
    }

    private String registrableDomain(String host) {
        if (host == null || host.isBlank()) {
            return null;
        }
        String[] parts = host.split("\\.");
        if (parts.length <= 2) {
            return host;
        }
        String tld = parts[parts.length - 1];
        String second = parts[parts.length - 2];
        if (tld.length() == 2 && isSecondLevelCc(second) && parts.length >= 3) {
            return (parts[parts.length - 3] + "." + second + "." + tld).toLowerCase(Locale.ROOT);
        }
        return (second + "." + tld).toLowerCase(Locale.ROOT);
    }

    private boolean isSecondLevelCc(String value) {
        return value != null && (
            value.equals("co") || value.equals("com") || value.equals("net") ||
            value.equals("org") || value.equals("gov") || value.equals("ac")
        );
    }

    private String firstPathSegment(String path) {
        if (path == null || path.isBlank()) {
            return null;
        }
        String trimmed = path.startsWith("/") ? path.substring(1) : path;
        if (trimmed.isBlank()) {
            return null;
        }
        String segment = trimmed.split("/")[0];
        segment = segment.replaceAll("[^A-Za-z0-9.-]", "");
        return segment.isBlank() ? null : segment;
    }

    private DomainOverride findOverride(String rawTicker) {
        if (rawTicker == null || rawTicker.isBlank()) {
            return null;
        }
        String normalized = rawTicker.trim().toUpperCase(Locale.ROOT);
        synchronized (overridesByTicker) {
            return overridesByTicker.get(normalized);
        }
    }

    private void loadOverrides() {
        if (overridesLoaded) {
            return;
        }
        synchronized (overridesByTicker) {
            if (overridesLoaded) {
                return;
            }
            Path path = resolvePath(properties.getData().getDomainsCsv());
            if (path == null || !Files.exists(path)) {
                overridesLoaded = true;
                return;
            }
            try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreSurroundingSpaces(true)
                     .build()
                     .parse(reader)
            ) {
                for (CSVRecord record : parser) {
                    String ticker = readColumn(record, "ticker");
                    String domain = readColumn(record, "domain");
                    String careers = readColumn(record, "optional_careers_hint_url");
                    if (ticker == null || domain == null) {
                        continue;
                    }
                    overridesByTicker.put(
                        ticker.trim().toUpperCase(Locale.ROOT),
                        new DomainOverride(domain.trim(), careers == null ? null : careers.trim())
                    );
                }
                log.info("Loaded {} domain overrides from {}", overridesByTicker.size(), path);
            } catch (IOException e) {
                log.warn("Failed to load domain overrides from {}: {}", path, e.getMessage());
            } finally {
                overridesLoaded = true;
            }
        }
    }

    private Path resolvePath(String configuredPath) {
        if (configuredPath == null || configuredPath.isBlank()) {
            return null;
        }
        Path path = Paths.get(configuredPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return Paths.get("").toAbsolutePath().resolve(path).normalize();
    }

    private String readColumn(CSVRecord record, String header) {
        if (record == null || header == null) {
            return null;
        }
        for (String key : record.toMap().keySet()) {
            if (key != null && key.trim().equalsIgnoreCase(header)) {
                String value = record.get(key);
                if (value == null || value.trim().isEmpty()) {
                    return null;
                }
                return value.trim();
            }
        }
        return null;
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

    private boolean isTimeout(HttpFetchResult fetch) {
        if (fetch == null) {
            return false;
        }
        if (fetch.errorCode() != null) {
            return fetch.errorCode().toLowerCase(Locale.ROOT).contains("timeout");
        }
        return fetch.statusCode() == 408;
    }

    private void sleepBackoff(int attempt) {
        long backoffMs = Math.min(WDQS_RETRY_MAX_MS, WDQS_RETRY_BASE_MS * (1L << (attempt - 1)));
        try {
            checkCanaryDeadline();
            Thread.sleep(backoffMs);
            checkCanaryDeadline();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void checkCanaryDeadline() {
        CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
        if (budget != null) {
            budget.checkDeadline();
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
        private int wdqsTimeout;
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

    private record WdqsLookupResult(WdqsLookup lookup, String errorCategory) {
    }

    private record WdqsQueryResult(JsonNode root, String errorCategory) {
    }

    private record WdqsLookup(Map<String, WdqsMatch> matches) {
    }

    private record WdqsMatch(String qid, String website) {
    }

    private record NormalizedDomain(String domain, String careersHintUrl) {
    }

    private record DomainOverride(String domain, String careersHintUrl) {
    }
}
