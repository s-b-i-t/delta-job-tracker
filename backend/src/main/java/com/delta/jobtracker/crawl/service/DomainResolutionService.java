package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionMetrics;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DomainResolutionService {
  private static final Logger log = LoggerFactory.getLogger(DomainResolutionService.class);
  private static final String WDQS_ENDPOINT =
      "https://query.wikidata.org/bigdata/namespace/wdq/sparql";
  private static final String SPARQL_ACCEPT = "application/sparql-results+json";
  private static final String CIK_PROPERTY = "P5531";
  private static final String METHOD_WIKIPEDIA = "WIKIPEDIA_TITLE";
  private static final String METHOD_CIK = "CIK";
  private static final String METHOD_HEURISTIC = "HEURISTIC_NAME";
  private static final String METHOD_WIKIPEDIA_INFOBOX = "WIKIPEDIA_INFOBOX";
  private static final String METHOD_NONE = "NONE";
  private static final String STATUS_RESOLVED = "RESOLVED";
  private static final String STATUS_NO_ITEM = "NO_ITEM";
  private static final String STATUS_NO_P856 = "NO_P856";
  private static final String STATUS_INVALID_WEBSITE = "INVALID_WEBSITE_URL";
  private static final String STATUS_WDQS_ERROR = "WDQS_ERROR";
  private static final String STATUS_WDQS_TIMEOUT = "WDQS_TIMEOUT";
  private static final String STATUS_NO_IDENTIFIER = "NO_IDENTIFIER";
  private static final String STATUS_INFOBOX_ERROR = "INFOBOX_ERROR";
  private static final String SELECTION_MODE_ELIGIBLE_BATCH = "eligible_batch";
  private static final String SELECTION_MODE_ELIGIBLE_BATCH_INCLUDE_NON_EMPLOYER =
      "eligible_batch_include_non_employer";
  private static final String SELECTION_MODE_TICKER_TARGETED = "ticker_targeted";
  private static final int MAX_ERROR_SAMPLES = 10;
  private static final int MAX_NOT_EMPLOYER_SAMPLES = 10;
  private static final int WDQS_MAX_ATTEMPTS = 3;
  private static final long WDQS_RETRY_BASE_MS = 750L;
  private static final long WDQS_RETRY_MAX_MS = 4000L;
  private static final int WDQS_BODY_SAMPLE_CHARS = 500;
  private static final String HTML_ACCEPT =
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
  private static final int HEURISTIC_MAX_BYTES = 256_000;
  private static final int HEURISTIC_MAX_CANDIDATES = 6;
  private static final long HEURISTIC_MAX_TOTAL_DURATION_MS = 120_000L;
  private static final double WIKIPEDIA_INFOBOX_CONFIDENCE = 0.9;
  private static final double HEURISTIC_CONFIDENCE = 0.7;
  private static final int HEURISTIC_NON_COM_TOP_SLUGS = 2;
  private static final List<String> OFFICIAL_SITE_INVESTOR_MARKERS =
      List.of("investor relations", "investors", "shareholders", "annual meeting", "earnings");
  private static final List<String> OFFICIAL_SITE_EXCHANGE_MARKERS =
      List.of("nyse", "nasdaq", "tsx", "asx", "euronext", "lse");
  private static final List<String> PARKED_PAGE_HINTS =
      List.of(
          "domain for sale",
          "buy this domain",
          "this domain may be for sale",
          "sedo domain parking",
          "godaddy",
          "dan.com",
          "domaineasy",
          "afternic",
          "hugedomains",
          "parkingcrew",
          "bodis",
          "parked free");
  private static final List<String> PARKED_HOST_SUFFIXES =
      List.of(
          "domaineasy.com",
          "dan.com",
          "sedo.com",
          "afternic.com",
          "hugedomains.com",
          "parkingcrew.net",
          "bodis.com");
  private static final List<String> INFOBOX_DISALLOWED_HOST_SUFFIXES =
      List.of(
          "boards.greenhouse.io",
          "greenhouse.io",
          "jobs.lever.co",
          "lever.co",
          "myworkdayjobs.com",
          "workdayjobs.com",
          "smartrecruiters.com",
          "icims.com",
          "jobvite.com",
          "breezy.hr",
          "ashbyhq.com");
  private static final Pattern URL_TEXT_PATTERN =
      Pattern.compile("https?://[^\\s<>\"']+", Pattern.CASE_INSENSITIVE);

  private final CrawlerProperties properties;
  private final CrawlJdbcRepository repository;
  private final WdqsHttpClient wdqsHttpClient;
  private final PoliteHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final Object wdqsThrottleLock = new Object();
  private Instant wdqsNextAllowedAt = Instant.EPOCH;

  public DomainResolutionService(
      CrawlerProperties properties,
      CrawlJdbcRepository repository,
      WdqsHttpClient wdqsHttpClient,
      PoliteHttpClient httpClient,
      ObjectMapper objectMapper) {
    this.properties = properties;
    this.repository = repository;
    this.wdqsHttpClient = wdqsHttpClient;
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
  }

  public DomainResolutionResult resolveMissingDomains(Integer requestedLimit) {
    return resolveMissingDomains(requestedLimit, null, null);
  }

  public DomainResolutionResult resolveMissingDomains(Integer requestedLimit, Instant deadline) {
    return resolveMissingDomains(requestedLimit, deadline, null);
  }

  public DomainResolutionResult resolveMissingDomains(
      Integer requestedLimit, Instant deadline, Boolean includeNonEmployer) {
    int limit =
        requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);
    boolean includeNonEmployerCandidates = Boolean.TRUE.equals(includeNonEmployer);

    int selectionEligibleCount =
        repository.countCompaniesMissingDomainEligible(includeNonEmployerCandidates);
    int skippedNotEmployerCount = 0;
    List<String> skippedNotEmployerSample = List.of();
    if (!includeNonEmployerCandidates) {
      skippedNotEmployerCount = repository.countLikelyNonEmployerMissingDomainEligible();
      List<String> sample =
          repository.sampleLikelyNonEmployerMissingDomainEligible(MAX_NOT_EMPLOYER_SAMPLES);
      skippedNotEmployerSample = sample == null ? List.of() : sample;
    }

    List<CompanyIdentity> selected =
        includeNonEmployerCandidates
            ? repository.findCompaniesMissingDomain(limit, true)
            : repository.findCompaniesMissingDomain(limit);
    List<CompanyIdentity> missingDomain = selected == null ? List.of() : selected;
    return resolveCompanies(
        missingDomain,
        limit,
        deadline,
        includeNonEmployerCandidates
            ? SELECTION_MODE_ELIGIBLE_BATCH_INCLUDE_NON_EMPLOYER
            : SELECTION_MODE_ELIGIBLE_BATCH,
        missingDomain.size(),
        selectionEligibleCount,
        skippedNotEmployerCount,
        skippedNotEmployerSample);
  }

  public DomainResolutionResult resolveMissingDomainsForTickers(
      List<String> tickers, Integer requestedLimit) {
    return resolveMissingDomainsForTickers(tickers, requestedLimit, null);
  }

  public DomainResolutionResult resolveMissingDomainsForTickers(
      List<String> tickers, Integer requestedLimit, Instant deadline) {
    int limit =
        requestedLimit == null
            ? properties.getDomainResolution().getDefaultLimit()
            : Math.max(1, requestedLimit);
    List<CompanyIdentity> missingDomain =
        repository.findCompaniesMissingDomainByTickers(tickers, limit);
    List<CompanyIdentity> companies = missingDomain == null ? List.of() : missingDomain;
    return resolveCompanies(
        companies,
        limit,
        deadline,
        SELECTION_MODE_TICKER_TARGETED,
        companies.size(),
        companies.size(),
        0,
        List.of());
  }

  private DomainResolutionResult resolveCompanies(
      List<CompanyIdentity> missingDomain,
      int limit,
      Instant deadline,
      String selectionMode,
      int selectionReturnedCount,
      int selectionEligibleCount,
      int skippedNotEmployerCount,
      List<String> skippedNotEmployerSample) {
    Instant startedAt = Instant.now();
    List<CompanyIdentity> companies = missingDomain == null ? List.of() : missingDomain;
    ResolutionMetricsCollector metrics =
        new ResolutionMetricsCollector(
            companies.size(),
            selectionReturnedCount,
            selectionEligibleCount,
            skippedNotEmployerCount,
            skippedNotEmployerSample);
    if (companies.isEmpty()) {
      return new DomainResolutionResult(0, 0, 0, 0, 0, 0, List.of(), metrics.toModel(0L));
    }

    int batchSize = Math.min(properties.getDomainResolution().getBatchSize(), limit);
    Counts counts = new Counts();
    ErrorCollector errors = new ErrorCollector();
    Map<String, HttpFetchResult> heuristicFetchCache = new HashMap<>();
    HeuristicBudget heuristicBudget = new HeuristicBudget(HEURISTIC_MAX_TOTAL_DURATION_MS);
    int totalWithTitle = 0;
    int totalWithCik = 0;
    for (CompanyIdentity company : companies) {
      if (hasWikipediaTitle(company)) {
        totalWithTitle++;
      } else if (hasCik(company)) {
        totalWithCik++;
      }
    }
    int batchCount = (companies.size() + batchSize - 1) / batchSize;
    log.info(
        "Domain resolver starting: companies={}, with_wikipedia_title={}, with_cik={}, batch_size={}, wdqs_chunk_size={}",
        companies.size(),
        totalWithTitle,
        totalWithCik,
        batchSize,
        properties.getDomainResolution().getWdqsQueryChunkSize());

    for (int from = 0; from < companies.size(); from += batchSize) {
      if (deadline != null && Instant.now().isAfter(deadline)) {
        log.info(
            "Domain resolver stopped early due to time budget (processed {} of {})",
            from,
            companies.size());
        break;
      }
      int to = Math.min(companies.size(), from + batchSize);
      List<CompanyIdentity> batch = companies.subList(from, to);
      int batchIndex = (from / batchSize) + 1;

      Map<CompanyIdentity, List<String>> titlesByCompany = new LinkedHashMap<>();
      Map<String, List<CompanyIdentity>> companiesByTitle = new LinkedHashMap<>();
      Map<CompanyIdentity, List<String>> ciksByCompany = new LinkedHashMap<>();
      Map<String, List<CompanyIdentity>> companiesByCik = new LinkedHashMap<>();
      Map<CompanyIdentity, DomainResolutionAttemptContext> attemptsByCompany =
          new LinkedHashMap<>();

      for (CompanyIdentity company : batch) {
        if (deadline != null && Instant.now().isAfter(deadline)) {
          log.info(
              "Domain resolver stopped early due to time budget (batch {}/{})",
              batchIndex,
              batchCount);
          break;
        }
        Instant now = Instant.now();
        if (hasWikipediaTitle(company)) {
          if (shouldSkipCached(company, METHOD_WIKIPEDIA, now)) {
            if (hasCik(company) && !shouldSkipCached(company, METHOD_CIK, now)) {
              metrics.incrementCompaniesAttempted();
              DomainResolutionAttemptContext attempt =
                  beginAttempt(attemptsByCompany, company, selectionMode, now);
              attempt.addStep("selection", METHOD_CIK, "queued", "cached_wikipedia");
              List<String> ciks = buildCikVariants(company.cik());
              if (ciks.isEmpty()) {
                if (tryHeuristicFallback(
                    company,
                    attempt,
                    "no_cik",
                    counts,
                    errors,
                    metrics,
                    deadline,
                    heuristicFetchCache,
                    heuristicBudget)) {
                  continue;
                }
                counts.noWikipediaTitle++;
                errors.add(company, "no_cik");
                attempt.addStep("identifier_check", METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik");
                finalizeCompanyAttempt(
                    company, attempt, METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik", null);
                continue;
              }
              ciksByCompany.put(company, ciks);
              for (String cik : ciks) {
                companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
              }
              continue;
            }
            metrics.incrementCachedSkip();
            continue;
          }
          metrics.incrementCompaniesAttempted();
          DomainResolutionAttemptContext attempt =
              beginAttempt(attemptsByCompany, company, selectionMode, now);
          attempt.addStep("selection", METHOD_WIKIPEDIA, "queued", "wikipedia_title");
          List<String> titles = buildTitleVariants(company.wikipediaTitle());
          if (titles.isEmpty()) {
            if (hasCik(company) && !shouldSkipCached(company, METHOD_CIK, now)) {
              attempt.addStep("wikipedia_lookup", METHOD_WIKIPEDIA, "skipped", "no_title_variants");
              List<String> ciks = buildCikVariants(company.cik());
              if (!ciks.isEmpty()) {
                attempt.addStep("selection", METHOD_CIK, "queued", "cik_fallback");
                ciksByCompany.put(company, ciks);
                for (String cik : ciks) {
                  companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
                }
                continue;
              }
            }
            if (tryHeuristicFallback(
                company,
                attempt,
                "no_wikipedia_title",
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget)) {
              continue;
            }
            counts.noWikipediaTitle++;
            errors.add(company, "no_wikipedia_title");
            attempt.addStep(
                "identifier_check", METHOD_WIKIPEDIA, STATUS_NO_IDENTIFIER, "no_wikipedia_title");
            finalizeCompanyAttempt(
                company,
                attempt,
                METHOD_WIKIPEDIA,
                STATUS_NO_IDENTIFIER,
                "no_wikipedia_title",
                null);
            continue;
          }
          titlesByCompany.put(company, titles);
          for (String title : titles) {
            companiesByTitle.computeIfAbsent(title, ignored -> new ArrayList<>()).add(company);
          }
        } else if (hasCik(company)) {
          if (shouldSkipCached(company, METHOD_CIK, now)) {
            metrics.incrementCachedSkip();
            continue;
          }
          metrics.incrementCompaniesAttempted();
          DomainResolutionAttemptContext attempt =
              beginAttempt(attemptsByCompany, company, selectionMode, now);
          attempt.addStep("selection", METHOD_CIK, "queued", "cik");
          List<String> ciks = buildCikVariants(company.cik());
          if (ciks.isEmpty()) {
            if (tryHeuristicFallback(
                company,
                attempt,
                "no_cik",
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget)) {
              continue;
            }
            counts.noWikipediaTitle++;
            errors.add(company, "no_cik");
            attempt.addStep("identifier_check", METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik");
            finalizeCompanyAttempt(
                company, attempt, METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik", null);
            continue;
          }
          ciksByCompany.put(company, ciks);
          for (String cik : ciks) {
            companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
          }
        } else {
          metrics.incrementCompaniesAttempted();
          DomainResolutionAttemptContext attempt =
              beginAttempt(attemptsByCompany, company, selectionMode, now);
          if (tryHeuristicFallback(
              company,
              attempt,
              "no_identifier",
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget)) {
            continue;
          }
          counts.noWikipediaTitle++;
          errors.add(company, "no_identifier");
          attempt.addStep("identifier_check", METHOD_NONE, STATUS_NO_IDENTIFIER, "no_identifier");
          finalizeCompanyAttempt(
              company, attempt, METHOD_NONE, STATUS_NO_IDENTIFIER, "no_identifier", null);
        }
      }

      if (!titlesByCompany.isEmpty()) {
        log.info(
            "Domain resolver batch {}/{} title-companies={} titles={}",
            batchIndex,
            batchCount,
            titlesByCompany.size(),
            companiesByTitle.keySet().size());
        metrics.incrementWdqsTitleBatch();
        Instant wdqsStartedAt = Instant.now();
        WdqsLookupBatchResult lookupResult =
            fetchWdqsMatchesByTitle(new ArrayList<>(companiesByTitle.keySet()), metrics);
        metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
        for (Map.Entry<CompanyIdentity, List<String>> entry : titlesByCompany.entrySet()) {
          CompanyIdentity company = entry.getKey();
          WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
          if (match != null) {
            applyMatch(
                company,
                attemptsByCompany.get(company),
                match,
                METHOD_WIKIPEDIA,
                "enwiki_sitelink",
                true,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget);
            continue;
          }
          String errorCategory =
              findFailureCategory(entry.getValue(), lookupResult.failedCategories());
          if (errorCategory != null) {
            applyWdqsLookupFailure(
                company,
                attemptsByCompany.get(company),
                METHOD_WIKIPEDIA,
                errorCategory,
                true,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget);
            continue;
          }
          applyMatch(
              company,
              attemptsByCompany.get(company),
              null,
              METHOD_WIKIPEDIA,
              "enwiki_sitelink",
              true,
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget);
        }
      }

      if (!ciksByCompany.isEmpty()) {
        log.info(
            "Domain resolver batch {}/{} cik-companies={} ciks={}",
            batchIndex,
            batchCount,
            ciksByCompany.size(),
            companiesByCik.keySet().size());
        metrics.incrementWdqsCikBatch();
        Instant wdqsStartedAt = Instant.now();
        WdqsLookupBatchResult lookupResult =
            fetchWdqsMatchesByCik(new ArrayList<>(companiesByCik.keySet()), metrics);
        metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
        for (Map.Entry<CompanyIdentity, List<String>> entry : ciksByCompany.entrySet()) {
          CompanyIdentity company = entry.getKey();
          WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
          if (match != null) {
            applyMatch(
                company,
                attemptsByCompany.get(company),
                match,
                METHOD_CIK,
                "cik",
                false,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget);
            continue;
          }
          String errorCategory =
              findFailureCategory(entry.getValue(), lookupResult.failedCategories());
          if (errorCategory != null) {
            applyWdqsLookupFailure(
                company,
                attemptsByCompany.get(company),
                METHOD_CIK,
                errorCategory,
                false,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget);
            continue;
          }
          applyMatch(
              company,
              attemptsByCompany.get(company),
              null,
              METHOD_CIK,
              "cik",
              false,
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget);
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
        counts.wdqsTimeout);
    return new DomainResolutionResult(
        counts.resolved,
        counts.noWikipediaTitle,
        counts.noItem,
        counts.noP856,
        counts.wdqsError,
        counts.wdqsTimeout,
        errors.sampleErrors(),
        metrics.toModel(Duration.between(startedAt, Instant.now()).toMillis()));
  }

  private DomainResolutionAttemptContext beginAttempt(
      Map<CompanyIdentity, DomainResolutionAttemptContext> attemptsByCompany,
      CompanyIdentity company,
      String selectionMode,
      Instant startedAt) {
    return attemptsByCompany.computeIfAbsent(
        company,
        ignored ->
            new DomainResolutionAttemptContext(
                startedAt == null ? Instant.now() : startedAt, selectionMode));
  }

  private void finalizeCompanyAttempt(
      CompanyIdentity company,
      DomainResolutionAttemptContext attempt,
      String finalMethod,
      String finalStatus,
      String errorCategory,
      ResolvedDomainWrite resolvedWrite) {
    if (company == null || attempt == null) {
      return;
    }
    Instant finishedAt = Instant.now();
    if (resolvedWrite != null
        && resolvedWrite.domain() != null
        && !resolvedWrite.domain().isBlank()) {
      repository.upsertCompanyDomain(
          company.companyId(),
          resolvedWrite.domain(),
          null,
          resolvedWrite.source(),
          resolvedWrite.confidence(),
          finishedAt,
          resolvedWrite.resolutionMethod(),
          resolvedWrite.wikidataQid());
    }
    repository.updateCompanyDomainResolutionCache(
        company.companyId(), finalMethod, finalStatus, errorCategory, finishedAt);
    repository.insertDomainResolutionAttempt(
        company.companyId(),
        attempt.startedAt(),
        finishedAt,
        attempt.selectionMode(),
        finalMethod,
        finalStatus,
        errorCategory,
        resolvedWrite == null ? null : resolvedWrite.domain(),
        resolvedWrite == null ? null : resolvedWrite.source(),
        resolvedWrite == null ? null : resolvedWrite.resolutionMethod(),
        serializeAttemptSteps(attempt.steps()));
  }

  private String serializeAttemptSteps(List<DomainResolutionAttemptStep> steps) {
    try {
      return objectMapper.writeValueAsString(steps == null ? List.of() : steps);
    } catch (Exception e) {
      log.warn("Failed to serialize domain resolution attempt steps", e);
      return "[]";
    }
  }

  private void recordStep(
      DomainResolutionAttemptContext attempt,
      String stage,
      String method,
      String outcome,
      String detail) {
    recordStep(attempt, stage, method, outcome, detail, null);
  }

  private void recordStep(
      DomainResolutionAttemptContext attempt,
      String stage,
      String method,
      String outcome,
      String detail,
      String resolvedDomain) {
    if (attempt == null) {
      return;
    }
    attempt.addStep(stage, method, outcome, detail, resolvedDomain);
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
      DomainResolutionAttemptContext attempt,
      WdqsMatch match,
      String method,
      String resolutionMethod,
      boolean allowCikFallback,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (match == null) {
      recordStep(attempt, "wdqs_lookup", method, STATUS_NO_ITEM, resolutionMethod);
      if (allowCikFallback
          && tryCikFallbackLookup(
              company,
              attempt,
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(company, attempt, "no_item", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
          attempt,
          "no_item",
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return;
      }
      counts.noItem++;
      errors.add(company, "no_item");
      finalizeCompanyAttempt(company, attempt, method, STATUS_NO_ITEM, "no_item", null);
      return;
    }
    if (match.website() == null || match.website().isBlank()) {
      recordStep(attempt, "wdqs_lookup", method, STATUS_NO_P856, resolutionMethod);
      if (allowCikFallback
          && tryCikFallbackLookup(
              company,
              attempt,
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(company, attempt, "no_p856", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
          attempt,
          "no_p856",
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return;
      }
      counts.noP856++;
      errors.add(company, "no_p856");
      finalizeCompanyAttempt(company, attempt, method, STATUS_NO_P856, "no_p856", null);
      return;
    }

    String domain = normalizeDomain(match.website());
    if (domain == null) {
      recordStep(attempt, "wdqs_lookup", method, STATUS_INVALID_WEBSITE, resolutionMethod);
      if (allowCikFallback
          && tryCikFallbackLookup(
              company,
              attempt,
              counts,
              errors,
              metrics,
              deadline,
              heuristicFetchCache,
              heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(
          company, attempt, "invalid_website_url", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
          attempt,
          "invalid_website_url",
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return;
      }
      counts.noP856++;
      errors.add(company, "invalid_website_url");
      finalizeCompanyAttempt(
          company, attempt, method, STATUS_INVALID_WEBSITE, "invalid_website_url", null);
      return;
    }

    recordStep(attempt, "wdqs_lookup", method, STATUS_RESOLVED, resolutionMethod, domain);
    finalizeCompanyAttempt(
        company,
        attempt,
        method,
        STATUS_RESOLVED,
        null,
        new ResolvedDomainWrite(domain, "WIKIDATA", 0.95, resolutionMethod, match.qid()));
    counts.resolved++;
    metrics.recordResolved("WIKIDATA");
  }

  private void applyWdqsLookupFailure(
      CompanyIdentity company,
      DomainResolutionAttemptContext attempt,
      String method,
      String errorCategory,
      boolean allowCikFallback,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    recordStep(attempt, "wdqs_lookup", method, errorCategory, "transport_failure");
    if (allowCikFallback
        && tryCikFallbackLookup(
            company,
            attempt,
            counts,
            errors,
            metrics,
            deadline,
            heuristicFetchCache,
            heuristicBudget)) {
      return;
    }
    if (tryHeuristicFallback(
        company,
        attempt,
        errorCategory,
        counts,
        errors,
        metrics,
        deadline,
        heuristicFetchCache,
        heuristicBudget)) {
      return;
    }
    metrics.recordWdqsFailure(errorCategory);
    if (isWdqsTimeoutCategory(errorCategory)) {
      counts.wdqsTimeout++;
    } else {
      counts.wdqsError++;
    }
    errors.add(company, errorCategory);
    finalizeCompanyAttempt(
        company,
        attempt,
        method,
        isWdqsTimeoutCategory(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
        errorCategory,
        null);
  }

  private String findFailureCategory(List<String> keys, Map<String, String> failedCategories) {
    if (keys == null || keys.isEmpty() || failedCategories == null || failedCategories.isEmpty()) {
      return null;
    }
    for (String key : keys) {
      String category = failedCategories.get(key);
      if (category != null && !category.isBlank()) {
        return category;
      }
    }
    return null;
  }

  private boolean tryWikipediaInfoboxFallback(
      CompanyIdentity company,
      DomainResolutionAttemptContext attemptContext,
      String failureReason,
      Counts counts,
      ResolutionMetricsCollector metrics,
      Instant deadline) {
    if (!hasWikipediaTitle(company)) {
      recordStep(
          attemptContext, "wikipedia_infobox", METHOD_WIKIPEDIA_INFOBOX, "skipped", "no_title");
      return false;
    }
    Instant now = Instant.now();
    if (deadline != null && now.isAfter(deadline)) {
      recordStep(
          attemptContext, "wikipedia_infobox", METHOD_WIKIPEDIA_INFOBOX, "skipped", "deadline");
      return false;
    }
    if (shouldSkipCached(company, METHOD_WIKIPEDIA_INFOBOX, now)) {
      recordStep(
          attemptContext, "wikipedia_infobox", METHOD_WIKIPEDIA_INFOBOX, "skipped", "cached");
      return false;
    }

    metrics.incrementWikipediaInfoboxTried();
    Instant startedAt = Instant.now();
    InfoboxResolutionAttempt attempt = resolveFromWikipediaInfobox(company);
    metrics.addWikipediaInfoboxDuration(Duration.between(startedAt, Instant.now()).toMillis());

    if (attempt.resolvedDomain() != null) {
      recordStep(
          attemptContext,
          "wikipedia_infobox",
          METHOD_WIKIPEDIA_INFOBOX,
          STATUS_RESOLVED,
          failureReason,
          attempt.resolvedDomain());
      finalizeCompanyAttempt(
          company,
          attemptContext,
          METHOD_WIKIPEDIA_INFOBOX,
          STATUS_RESOLVED,
          null,
          new ResolvedDomainWrite(
              attempt.resolvedDomain(),
              "WIKIPEDIA",
              WIKIPEDIA_INFOBOX_CONFIDENCE,
              METHOD_WIKIPEDIA_INFOBOX,
              null));
      counts.resolved++;
      metrics.incrementWikipediaInfoboxResolved();
      metrics.recordResolved("WIKIPEDIA_INFOBOX");
      return true;
    }

    metrics.incrementWikipediaInfoboxRejected();
    recordStep(
        attemptContext,
        "wikipedia_infobox",
        METHOD_WIKIPEDIA_INFOBOX,
        attempt.status() == null ? STATUS_INFOBOX_ERROR : attempt.status(),
        attempt.errorCategory() == null ? failureReason : attempt.errorCategory());
    return false;
  }

  private InfoboxResolutionAttempt resolveFromWikipediaInfobox(CompanyIdentity company) {
    if (company == null || company.wikipediaTitle() == null || company.wikipediaTitle().isBlank()) {
      return InfoboxResolutionAttempt.failure(STATUS_NO_IDENTIFIER, "no_wikipedia_title");
    }
    String pageUrl = wikipediaPageUrl(company.wikipediaTitle());
    if (pageUrl == null) {
      return InfoboxResolutionAttempt.failure(STATUS_INVALID_WEBSITE, "invalid_wikipedia_title");
    }
    HttpFetchResult fetch = httpClient.get(pageUrl, HTML_ACCEPT, HEURISTIC_MAX_BYTES);
    if (fetch == null || !fetch.isSuccessful() || fetch.body() == null || fetch.body().isBlank()) {
      String error = fetch == null ? "infobox_fetch_failed" : classifyInfoBoxFetchError(fetch);
      return InfoboxResolutionAttempt.failure(STATUS_INFOBOX_ERROR, error);
    }
    String infoboxUrl = extractWikipediaInfoboxWebsiteUrl(fetch.body(), pageUrl);
    if (infoboxUrl == null || infoboxUrl.isBlank()) {
      return InfoboxResolutionAttempt.failure(STATUS_NO_P856, "infobox_no_website");
    }
    String domain = normalizeDomain(infoboxUrl);
    if (domain == null || domain.isBlank()) {
      return InfoboxResolutionAttempt.failure(
          STATUS_INVALID_WEBSITE, "infobox_invalid_website_url");
    }
    if (isParkedHost(domain)) {
      return InfoboxResolutionAttempt.failure(STATUS_INVALID_WEBSITE, "infobox_parked_host");
    }
    if (isDisallowedInfoboxHost(domain)) {
      return InfoboxResolutionAttempt.failure(STATUS_INVALID_WEBSITE, "infobox_disallowed_host");
    }
    if (domain.endsWith(".wikipedia.org") || domain.equals("wikipedia.org")) {
      return InfoboxResolutionAttempt.failure(STATUS_INVALID_WEBSITE, "infobox_internal_link");
    }
    return InfoboxResolutionAttempt.success(domain);
  }

  private String classifyInfoBoxFetchError(HttpFetchResult fetch) {
    if (fetch == null) {
      return "infobox_fetch_failed";
    }
    if (fetch.errorCode() != null && !fetch.errorCode().isBlank()) {
      return "infobox_" + fetch.errorCode();
    }
    return fetch.statusCode() > 0 ? "infobox_http_" + fetch.statusCode() : "infobox_fetch_failed";
  }

  private String wikipediaPageUrl(String wikipediaTitle) {
    if (wikipediaTitle == null || wikipediaTitle.isBlank()) {
      return null;
    }
    String normalized = stripFragmentAndQuery(wikipediaTitle.trim());
    if (normalized.isBlank()) {
      return null;
    }
    String decoded = decodeTitle(normalized).replace(' ', '_');
    String encoded = URLEncoder.encode(decoded, StandardCharsets.UTF_8).replace("+", "%20");
    return "https://en.wikipedia.org/wiki/" + encoded;
  }

  private String extractWikipediaInfoboxWebsiteUrl(String html, String baseUrl) {
    if (html == null || html.isBlank()) {
      return null;
    }
    Document doc = Jsoup.parse(html, baseUrl);
    Element infobox = doc.selectFirst("table.infobox");
    if (infobox == null) {
      return null;
    }
    for (Element row : infobox.select("tr")) {
      Element header = row.selectFirst("th");
      Element value = row.selectFirst("td");
      if (header == null || value == null) {
        continue;
      }
      String headerText =
          header.text() == null ? "" : header.text().trim().toLowerCase(Locale.ROOT);
      if (!headerText.equals("website") && !headerText.contains("website")) {
        continue;
      }
      String preferred = firstExternalInfoboxUrl(value, true);
      if (preferred != null) {
        return preferred;
      }
      String fallback = firstExternalInfoboxUrl(value, false);
      if (fallback != null) {
        return fallback;
      }
      return firstUrlFromText(value.text());
    }
    return null;
  }

  private String firstExternalInfoboxUrl(Element valueCell, boolean httpsOnly) {
    if (valueCell == null) {
      return null;
    }
    Elements links = valueCell.select("a[href]");
    for (Element link : links) {
      String href = link.absUrl("href");
      if (href == null || href.isBlank()) {
        href = link.attr("href");
      }
      if (href == null || href.isBlank()) {
        continue;
      }
      String lower = href.toLowerCase(Locale.ROOT);
      if (!lower.startsWith("http://") && !lower.startsWith("https://")) {
        continue;
      }
      if (httpsOnly && !lower.startsWith("https://")) {
        continue;
      }
      return href;
    }
    return null;
  }

  private String firstUrlFromText(String text) {
    if (text == null || text.isBlank()) {
      return null;
    }
    Matcher matcher = URL_TEXT_PATTERN.matcher(text);
    if (!matcher.find()) {
      return null;
    }
    return matcher.group();
  }

  private boolean isDisallowedInfoboxHost(String host) {
    if (host == null || host.isBlank()) {
      return false;
    }
    String normalized = host.toLowerCase(Locale.ROOT);
    for (String suffix : INFOBOX_DISALLOWED_HOST_SUFFIXES) {
      if (suffix == null || suffix.isBlank()) {
        continue;
      }
      String s = suffix.toLowerCase(Locale.ROOT);
      if (normalized.equals(s) || normalized.endsWith("." + s)) {
        return true;
      }
    }
    return false;
  }

  private boolean tryCikFallbackLookup(
      CompanyIdentity company,
      DomainResolutionAttemptContext attempt,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (!hasCik(company)) {
      recordStep(attempt, "cik_lookup", METHOD_CIK, "skipped", "no_cik");
      return false;
    }
    Instant now = Instant.now();
    if (shouldSkipCached(company, METHOD_CIK, now)) {
      recordStep(attempt, "cik_lookup", METHOD_CIK, "skipped", "cached");
      return false;
    }
    if (deadline != null && now.isAfter(deadline)) {
      recordStep(attempt, "cik_lookup", METHOD_CIK, "skipped", "deadline");
      return false;
    }
    List<String> ciks = buildCikVariants(company.cik());
    if (ciks.isEmpty()) {
      recordStep(attempt, "cik_lookup", METHOD_CIK, "skipped", "no_cik_variants");
      return false;
    }

    metrics.incrementWdqsCikBatch();
    Instant wdqsStartedAt = Instant.now();
    WdqsLookupBatchResult lookupResult = fetchWdqsMatchesByCik(ciks, metrics);
    metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
    WdqsMatch match = findMatch(ciks, lookupResult.lookup().matches());
    if (match == null) {
      String errorCategory = findFailureCategory(ciks, lookupResult.failedCategories());
      if (errorCategory == null) {
        recordStep(attempt, "cik_lookup", METHOD_CIK, STATUS_NO_ITEM, "cik");
        return false;
      }
      if (tryHeuristicFallback(
          company,
          attempt,
          errorCategory,
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return true;
      }
      metrics.recordWdqsFailure(errorCategory);
      if (isWdqsTimeoutCategory(errorCategory)) {
        counts.wdqsTimeout++;
      } else {
        counts.wdqsError++;
      }
      errors.add(company, errorCategory);
      recordStep(attempt, "cik_lookup", METHOD_CIK, errorCategory, "transport_failure");
      finalizeCompanyAttempt(
          company,
          attempt,
          METHOD_CIK,
          isWdqsTimeoutCategory(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
          errorCategory,
          null);
      return true;
    }
    applyMatch(
        company,
        attempt,
        match,
        METHOD_CIK,
        "cik",
        false,
        counts,
        errors,
        metrics,
        deadline,
        heuristicFetchCache,
        heuristicBudget);
    return true;
  }

  private boolean hasWikipediaTitle(CompanyIdentity company) {
    return company != null
        && company.wikipediaTitle() != null
        && !company.wikipediaTitle().isBlank();
  }

  private boolean hasCik(CompanyIdentity company) {
    return company != null && company.cik() != null && !company.cik().isBlank();
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

  private WdqsLookupBatchResult fetchWdqsMatchesByTitle(
      List<String> titles, ResolutionMetricsCollector metrics) {
    if (titles.isEmpty()) {
      return new WdqsLookupBatchResult(new WdqsLookup(Map.of()), Map.of());
    }

    Map<String, WdqsMatch> matches = new HashMap<>();
    Map<String, String> failedCategories = new LinkedHashMap<>();
    for (List<String> chunk : partitionWdqsKeys(titles)) {
      metrics.incrementWdqsTitleRequest();
      String values =
          chunk.stream()
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
                  """
              .formatted(values);

      WdqsQueryResult queryResult = executeWdqsQuery(query, metrics);
      if (queryResult.root() == null) {
        String errorCategory =
            queryResult.errorCategory() == null ? "wdqs_error" : queryResult.errorCategory();
        for (String title : chunk) {
          failedCategories.put(title, errorCategory);
        }
        continue;
      }

      JsonNode bindings = queryResult.root().path("results").path("bindings");
      if (!bindings.isArray()) {
        continue;
      }
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
        } else if ((existing.website() == null || existing.website().isBlank())
            && website != null) {
          matches.put(
              candidate, new WdqsMatch(existing.qid() != null ? existing.qid() : qid, website));
        }
      }
    }
    return new WdqsLookupBatchResult(new WdqsLookup(matches), Map.copyOf(failedCategories));
  }

  private WdqsLookupBatchResult fetchWdqsMatchesByCik(
      List<String> ciks, ResolutionMetricsCollector metrics) {
    if (ciks.isEmpty()) {
      return new WdqsLookupBatchResult(new WdqsLookup(Map.of()), Map.of());
    }

    Map<String, WdqsMatch> matches = new HashMap<>();
    Map<String, String> failedCategories = new LinkedHashMap<>();
    for (List<String> chunk : partitionWdqsKeys(ciks)) {
      metrics.incrementWdqsCikRequest();
      String values =
          chunk.stream().map(this::sparqlLiteral).reduce((a, b) -> a + " " + b).orElse("");

      String query =
          """
                  SELECT ?candidateCik ?item ?officialWebsite WHERE {
                    VALUES ?candidateCik { %s }
                    ?item wdt:%s ?candidateCik .
                    OPTIONAL { ?item wdt:P856 ?officialWebsite . }
                  }
                  """
              .formatted(values, CIK_PROPERTY);

      WdqsQueryResult queryResult = executeWdqsQuery(query, metrics);
      if (queryResult.root() == null) {
        String errorCategory =
            queryResult.errorCategory() == null ? "wdqs_error" : queryResult.errorCategory();
        for (String cik : chunk) {
          failedCategories.put(cik, errorCategory);
        }
        continue;
      }

      JsonNode bindings = queryResult.root().path("results").path("bindings");
      if (!bindings.isArray()) {
        continue;
      }
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
        } else if ((existing.website() == null || existing.website().isBlank())
            && website != null) {
          matches.put(
              candidate, new WdqsMatch(existing.qid() != null ? existing.qid() : qid, website));
        }
      }
    }
    return new WdqsLookupBatchResult(new WdqsLookup(matches), Map.copyOf(failedCategories));
  }

  private WdqsQueryResult executeWdqsQuery(String sparql, ResolutionMetricsCollector metrics) {
    String encoded = URLEncoder.encode(sparql, StandardCharsets.UTF_8);
    String formBody = "query=" + encoded;
    String lastCategory = null;

    for (int attempt = 1; attempt <= WDQS_MAX_ATTEMPTS; attempt++) {
      waitForWdqsSlot();
      HttpFetchResult fetch = wdqsHttpClient.postForm(WDQS_ENDPOINT, formBody, SPARQL_ACCEPT);
      if (fetch.isSuccessful()
          && fetch.statusCode() >= 200
          && fetch.statusCode() < 300
          && fetch.body() != null) {
        try {
          return new WdqsQueryResult(objectMapper.readTree(fetch.body()), null);
        } catch (Exception e) {
          log.warn("Failed to parse WDQS response on attempt {}", attempt, e);
          return new WdqsQueryResult(null, "wdqs_error");
        }
      }

      lastCategory = classifyWdqsFailure(fetch);
      log.warn(
          "WDQS query failed attempt={} category={} status={} errorCode={} bodySample={}",
          attempt,
          lastCategory,
          fetch.statusCode(),
          fetch.errorCode(),
          sampleBody(fetch.body()));

      if (!shouldRetry(fetch) || attempt == WDQS_MAX_ATTEMPTS) {
        return new WdqsQueryResult(null, lastCategory);
      }
      metrics.recordWdqsRetry(lastCategory);
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
      wdqsNextAllowedAt =
          Instant.now().plusMillis(properties.getDomainResolution().getWdqsMinDelayMs());
    }
  }

  private List<List<String>> partitionWdqsKeys(List<String> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    int chunkSize = properties.getDomainResolution().getWdqsQueryChunkSize();
    List<List<String>> chunks = new ArrayList<>();
    for (int from = 0; from < values.size(); from += chunkSize) {
      int to = Math.min(values.size(), from + chunkSize);
      chunks.add(List.copyOf(values.subList(from, to)));
    }
    return List.copyOf(chunks);
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

  private boolean tryHeuristicFallback(
      CompanyIdentity company,
      DomainResolutionAttemptContext attempt,
      String failureReason,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (company == null || company.name() == null || company.name().isBlank()) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "no_company_name");
      return false;
    }
    if (deadline != null && Instant.now().isAfter(deadline)) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "deadline");
      return false;
    }
    if (heuristicBudget != null && !heuristicBudget.allowAttempt()) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "budget_exhausted");
      return false;
    }
    NameSignals signals = buildNameSignals(company.name());
    if (signals == null || signals.strongTokens().isEmpty()) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "no_name_signals");
      return false;
    }
    if (!hasWikipediaTitle(company)
        && !hasCik(company)
        && isLowConfidenceHeuristicSignals(signals)) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "low_confidence_signals");
      return false;
    }

    List<String> candidates = heuristicDomainCandidates(company);
    if (candidates.isEmpty()) {
      recordStep(attempt, "heuristic", METHOD_HEURISTIC, "skipped", "no_candidates");
      return false;
    }

    metrics.incrementHeuristicCompaniesTried();
    int attempted = 0;
    for (String candidate : candidates) {
      if (attempted >= HEURISTIC_MAX_CANDIDATES) {
        break;
      }
      if (deadline != null && Instant.now().isAfter(deadline)) {
        break;
      }
      attempted++;
      metrics.incrementHeuristicCandidateTried();
      String candidateTld = topLevelDomain(candidate);
      metrics.recordHeuristicCandidateTld(candidateTld);
      HttpFetchResult fetch =
          heuristicFetchCache == null ? null : heuristicFetchCache.get(candidate);
      boolean cacheHit = fetch != null;
      if (!cacheHit) {
        Instant fetchStartedAt = Instant.now();
        fetch = httpClient.get("https://" + candidate + "/", HTML_ACCEPT, HEURISTIC_MAX_BYTES);
        long fetchDurationMs = Duration.between(fetchStartedAt, Instant.now()).toMillis();
        metrics.addHeuristicDuration(fetchDurationMs);
        if (heuristicBudget != null) {
          heuristicBudget.addDuration(fetchDurationMs);
        }
        if (heuristicFetchCache != null && fetch != null) {
          heuristicFetchCache.put(candidate, fetch);
        }
      }
      if (!cacheHit && fetch != null && fetch.isSuccessful()) {
        metrics.incrementHeuristicFetchSuccess();
      }

      HeuristicVerdict verdict = verifyHeuristicCandidate(company, candidate, fetch, signals);
      if (!verdict.accepted()) {
        metrics.incrementHeuristicRejected();
        continue;
      }

      String resolvedDomain = verdict.resolvedDomain();
      String resolvedMethod = heuristicResolutionMethodForDomain(resolvedDomain);
      recordStep(
          attempt, "heuristic", METHOD_HEURISTIC, STATUS_RESOLVED, failureReason, resolvedDomain);
      finalizeCompanyAttempt(
          company,
          attempt,
          METHOD_HEURISTIC,
          STATUS_RESOLVED,
          null,
          new ResolvedDomainWrite(
              resolvedDomain, "HEURISTIC", HEURISTIC_CONFIDENCE, resolvedMethod, null));
      counts.resolved++;
      metrics.incrementHeuristicResolved();
      metrics.recordHeuristicResolvedTld(topLevelDomain(resolvedDomain));
      metrics.recordResolved("HEURISTIC");
      metrics.recordHeuristicSuccessReason(failureReason);
      log.debug(
          "Domain heuristic resolved ticker={} name={} candidate={} resolved={} reason={}",
          company.ticker(),
          company.name(),
          candidate,
          resolvedDomain,
          failureReason);
      return true;
    }
    recordStep(attempt, "heuristic", METHOD_HEURISTIC, "no_match", failureReason);
    return false;
  }

  private List<String> heuristicDomainCandidates(CompanyIdentity company) {
    if (company == null || company.name() == null || company.name().isBlank()) {
      return List.of();
    }
    LinkedHashSet<String> slugs = new LinkedHashSet<>();
    String cleanedName = cleanupTitle(company.name().replace('&', ' '));
    String strippedName = stripCorporateSuffixes(cleanedName);
    addHeuristicSlugs(slugs, strippedName);
    addHeuristicSlugs(slugs, cleanedName);
    if (company.ticker() != null
        && !company.ticker().isBlank()
        && company.ticker().trim().length() <= 4) {
      addHeuristicSlugs(slugs, company.ticker());
    }

    List<String> rankedTlds = properties.getDomainResolution().getHeuristicTlds();
    LinkedHashSet<String> candidates = new LinkedHashSet<>();
    List<String> orderedSlugs = slugs.stream().filter(s -> s != null && !s.isBlank()).toList();
    if (orderedSlugs.isEmpty()) {
      return List.of();
    }

    // Phase 1: top slugs on .com first (high-signal, preserves prior behavior)
    int nonComTopSlugs = Math.min(HEURISTIC_NON_COM_TOP_SLUGS, orderedSlugs.size());
    for (int i = 0; i < nonComTopSlugs; i++) {
      candidates.add(orderedSlugs.get(i) + ".com");
    }
    // Phase 2: same top slugs on ranked non-.com TLDs
    for (String tld : rankedTlds) {
      if ("com".equalsIgnoreCase(tld)) {
        continue;
      }
      for (int i = 0; i < nonComTopSlugs; i++) {
        candidates.add(orderedSlugs.get(i) + "." + tld.toLowerCase(Locale.ROOT));
      }
    }
    // Phase 3: remaining slugs on .com only
    for (int i = nonComTopSlugs; i < orderedSlugs.size(); i++) {
      candidates.add(orderedSlugs.get(i) + ".com");
    }
    return new ArrayList<>(candidates);
  }

  private String heuristicResolutionMethodForDomain(String domain) {
    String tld = topLevelDomain(domain);
    if (tld == null || tld.isBlank()) {
      return METHOD_HEURISTIC + "_COM";
    }
    return METHOD_HEURISTIC + "_" + tld.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
  }

  private String topLevelDomain(String hostOrDomain) {
    if (hostOrDomain == null || hostOrDomain.isBlank()) {
      return null;
    }
    String normalized = hostOrDomain.toLowerCase(Locale.ROOT);
    int idx = normalized.lastIndexOf('.');
    if (idx < 0 || idx + 1 >= normalized.length()) {
      return null;
    }
    String tld = normalized.substring(idx + 1);
    return tld.isBlank() ? null : tld;
  }

  private void addHeuristicSlugs(LinkedHashSet<String> slugs, String raw) {
    if (raw == null || raw.isBlank()) {
      return;
    }
    String normalized = raw.toLowerCase(Locale.ROOT).replace("&", " and ");
    normalized = normalized.replaceAll("[^a-z0-9]+", " ").trim();
    if (normalized.isBlank()) {
      return;
    }
    String compact = normalized.replace(" ", "");
    if (!compact.isBlank()) {
      slugs.add(compact);
    }
    String hyphenated = normalized.replace(" ", "-");
    if (!hyphenated.isBlank()) {
      slugs.add(hyphenated);
    }
    if (normalized.startsWith("the ")) {
      String withoutThe = normalized.substring(4).trim();
      if (!withoutThe.isBlank()) {
        slugs.add(withoutThe.replace(" ", ""));
        slugs.add(withoutThe.replace(" ", "-"));
      }
    }
  }

  private HeuristicVerdict verifyHeuristicCandidate(
      CompanyIdentity company, String candidateDomain, HttpFetchResult fetch, NameSignals signals) {
    if (fetch == null || !fetch.isSuccessful() || fetch.body() == null || fetch.body().isBlank()) {
      return HeuristicVerdict.reject("fetch_failed");
    }

    String resolvedDomain = normalizeDomain(fetch.finalUrlOrRequested());
    if (resolvedDomain == null || resolvedDomain.isBlank()) {
      return HeuristicVerdict.reject("no_resolved_domain");
    }
    if (isParkedHost(resolvedDomain)) {
      return HeuristicVerdict.reject("parked_host");
    }

    String text = fetch.body().toLowerCase(Locale.ROOT);
    for (String parkedHint : PARKED_PAGE_HINTS) {
      if (text.contains(parkedHint)) {
        return HeuristicVerdict.reject("parked_page");
      }
    }

    Document doc = Jsoup.parse(fetch.body(), fetch.finalUrlOrRequested());
    String pageTitle = doc.title() == null ? "" : doc.title();
    String bodyText = doc.body() == null ? "" : doc.body().text();
    String ogSiteName = extractOgSiteName(doc);
    String verificationText = (pageTitle + " " + bodyText).toLowerCase(Locale.ROOT);

    if (signals == null || signals.strongTokens().isEmpty()) {
      return HeuristicVerdict.reject("no_name_signals");
    }

    int tokenMatches = 0;
    for (String token : signals.strongTokens()) {
      if (verificationText.contains(token.toLowerCase(Locale.ROOT))) {
        tokenMatches++;
      }
    }

    boolean rootMatches =
        hostnameContainsAcceptableRoot(candidateDomain, signals.acceptableRoots())
            || hostnameContainsAcceptableRoot(resolvedDomain, signals.acceptableRoots());

    if (acceptByOfficialSiteSignals(
        company,
        candidateDomain,
        resolvedDomain,
        pageTitle,
        bodyText,
        ogSiteName,
        signals,
        rootMatches)) {
      return HeuristicVerdict.accept(resolvedDomain);
    }

    if (!rootMatches) {
      return HeuristicVerdict.reject("root_mismatch");
    }
    if (tokenMatches == 0
        && !hostnameContainsAcceptableRoot(resolvedDomain, signals.acceptableRoots())) {
      return HeuristicVerdict.reject("name_not_found");
    }

    return HeuristicVerdict.accept(resolvedDomain);
  }

  private boolean acceptByOfficialSiteSignals(
      CompanyIdentity company,
      String candidateDomain,
      String resolvedDomain,
      String pageTitle,
      String bodyText,
      String ogSiteName,
      NameSignals signals,
      boolean rootMatches) {
    if (company == null
        || !hasCik(company)
        || signals == null
        || signals.strongTokens().isEmpty()) {
      return false;
    }
    String normalizedTitle = normalizeEvidenceText(pageTitle);
    String normalizedBody = normalizeEvidenceText(bodyText);
    String normalizedOgSiteName = normalizeEvidenceText(ogSiteName);
    boolean exactTickerInTitle = containsExactTickerToken(pageTitle, company.ticker());
    boolean aliasMatch =
        hostnameContainsAcceptableRoot(candidateDomain, buildAliasRoots(ogSiteName))
            || hostnameContainsAcceptableRoot(resolvedDomain, buildAliasRoots(ogSiteName));
    boolean deterministicAliasMatch =
        hasDeterministicOfficialAliasMatch(
            company, candidateDomain, resolvedDomain, normalizedTitle, normalizedOgSiteName);
    boolean companyOrBaseMatch =
        hasConservativeCompanyOrBaseMatch(
            company, signals, normalizedTitle, normalizedBody, normalizedOgSiteName);
    if (exactTickerInTitle && companyOrBaseMatch) {
      return true;
    }
    boolean investorOrExchangeSignals =
        containsAnyMarker(normalizedTitle, OFFICIAL_SITE_INVESTOR_MARKERS)
            || containsAnyMarker(normalizedTitle, OFFICIAL_SITE_EXCHANGE_MARKERS)
            || containsAnyMarker(normalizedBody, OFFICIAL_SITE_INVESTOR_MARKERS)
            || containsAnyMarker(normalizedBody, OFFICIAL_SITE_EXCHANGE_MARKERS)
            || containsAnyMarker(normalizedOgSiteName, OFFICIAL_SITE_INVESTOR_MARKERS)
            || containsAnyMarker(normalizedOgSiteName, OFFICIAL_SITE_EXCHANGE_MARKERS);
    if (investorOrExchangeSignals && companyOrBaseMatch && (rootMatches || aliasMatch)) {
      return true;
    }
    if (deterministicAliasMatch && exactTickerInTitle) {
      return true;
    }
    return aliasMatch
        && exactTickerInTitle
        && hasBaseTokenMatch(company, signals, normalizedTitle, normalizedOgSiteName);
  }

  private boolean hasDeterministicOfficialAliasMatch(
      CompanyIdentity company,
      String candidateDomain,
      String resolvedDomain,
      String normalizedTitle,
      String normalizedOgSiteName) {
    if (company == null || company.name() == null || company.name().isBlank()) {
      return false;
    }
    List<String> aliasTokens = buildDeterministicAliasTokens(company.name());
    if (aliasTokens.isEmpty()) {
      return false;
    }
    for (String alias : aliasTokens) {
      if (!containsExactToken(normalizedTitle, alias)
          && !containsExactToken(normalizedOgSiteName, alias)) {
        continue;
      }
      if (hostnameContainsAcceptableRoot(candidateDomain, List.of(alias))
          || hostnameContainsAcceptableRoot(resolvedDomain, List.of(alias))) {
        return true;
      }
    }
    return false;
  }

  private boolean hasConservativeCompanyOrBaseMatch(
      CompanyIdentity company,
      NameSignals signals,
      String normalizedTitle,
      String normalizedBody,
      String normalizedOgSiteName) {
    String combined = (normalizedTitle + " " + normalizedOgSiteName + " " + normalizedBody).trim();
    String strippedName =
        stripCorporateSuffixes(cleanupTitle(company.name() == null ? "" : company.name()));
    String normalizedStrippedName = normalizeEvidenceText(strippedName);
    if (!normalizedStrippedName.isBlank() && combined.contains(normalizedStrippedName)) {
      return true;
    }
    int tokenMatches = 0;
    for (String token : signals.strongTokens()) {
      String normalizedToken = normalizeEvidenceText(token);
      if (!normalizedToken.isBlank() && combined.contains(normalizedToken)) {
        tokenMatches++;
      }
    }
    if (tokenMatches >= Math.min(2, signals.strongTokens().size())) {
      return true;
    }
    return hasBaseTokenMatch(company, signals, normalizedTitle, normalizedOgSiteName);
  }

  private boolean hasBaseTokenMatch(
      CompanyIdentity company,
      NameSignals signals,
      String normalizedTitle,
      String normalizedOgSiteName) {
    if (company == null || company.name() == null) {
      return false;
    }
    String baseToken = firstBaseToken(signals, company.name());
    if (baseToken == null || baseToken.isBlank()) {
      return false;
    }
    return normalizedTitle.contains(baseToken) || normalizedOgSiteName.contains(baseToken);
  }

  private String firstBaseToken(NameSignals signals, String companyName) {
    if (signals != null) {
      for (String token : signals.strongTokens()) {
        if (token != null && token.length() >= 4) {
          return normalizeEvidenceText(token);
        }
      }
    }
    String stripped = normalizeEvidenceText(stripCorporateSuffixes(cleanupTitle(companyName)));
    if (stripped.isBlank()) {
      return null;
    }
    String[] tokens = stripped.split("\\s+");
    for (String token : tokens) {
      if (!token.isBlank() && token.length() >= 4) {
        return token;
      }
    }
    return null;
  }

  private List<String> buildDeterministicAliasTokens(String companyName) {
    if (companyName == null || companyName.isBlank()) {
      return List.of();
    }
    String cleaned = cleanupTitle(companyName);
    String normalized = normalizeEvidenceText(cleaned);
    if (normalized.isBlank()) {
      return List.of();
    }
    LinkedHashSet<String> aliases = new LinkedHashSet<>();
    String[] tokens = normalized.split("\\s+");
    StringBuilder acronym = new StringBuilder();
    for (String token : tokens) {
      if (token == null || token.isBlank() || isAliasStopword(token)) {
        continue;
      }
      acronym.append(token.charAt(0));
    }
    if (acronym.length() >= 2 && acronym.length() <= 6) {
      aliases.add(acronym.toString());
    }
    return List.copyOf(aliases);
  }

  private boolean isAliasStopword(String token) {
    if (token == null || token.isBlank()) {
      return true;
    }
    return token.equals("the")
        || token.equals("and")
        || token.equals("of")
        || token.equals("de")
        || token.equals("du")
        || token.equals("la")
        || token.equals("le");
  }

  private boolean containsExactToken(String text, String token) {
    if (text == null || text.isBlank() || token == null || token.isBlank()) {
      return false;
    }
    Pattern pattern =
        Pattern.compile(
            "(^|[^a-z0-9])" + Pattern.quote(token.toLowerCase(Locale.ROOT)) + "([^a-z0-9]|$)");
    return pattern.matcher(text.toLowerCase(Locale.ROOT)).find();
  }

  private boolean containsExactTickerToken(String text, String ticker) {
    if (text == null || text.isBlank() || ticker == null || ticker.isBlank()) {
      return false;
    }
    Pattern pattern =
        Pattern.compile(
            "(^|[^A-Z0-9])" + Pattern.quote(ticker.toUpperCase(Locale.ROOT)) + "([^A-Z0-9]|$)");
    return pattern.matcher(text.toUpperCase(Locale.ROOT)).find();
  }

  private List<String> buildAliasRoots(String ogSiteName) {
    if (ogSiteName == null || ogSiteName.isBlank()) {
      return List.of();
    }
    String normalized = normalizeEvidenceText(ogSiteName);
    if (normalized.isBlank()) {
      return List.of();
    }
    LinkedHashSet<String> aliases = new LinkedHashSet<>();
    aliases.add(normalized.replace(" ", ""));
    aliases.add(normalized.replace(" ", "-"));
    return aliases.stream().filter(alias -> alias != null && !alias.isBlank()).toList();
  }

  private boolean containsAnyMarker(String text, List<String> markers) {
    if (text == null || text.isBlank() || markers == null || markers.isEmpty()) {
      return false;
    }
    for (String marker : markers) {
      if (marker != null && !marker.isBlank() && text.contains(marker.toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private String extractOgSiteName(Document doc) {
    if (doc == null) {
      return "";
    }
    Element element = doc.selectFirst("meta[property=og:site_name], meta[name=og:site_name]");
    if (element == null) {
      return "";
    }
    String content = element.attr("content");
    return content == null ? "" : content.trim();
  }

  private String normalizeEvidenceText(String value) {
    if (value == null || value.isBlank()) {
      return "";
    }
    return value
        .toLowerCase(Locale.ROOT)
        .replace('&', ' ')
        .replaceAll("[^a-z0-9]+", " ")
        .replaceAll("\\s+", " ")
        .trim();
  }

  private boolean isLowConfidenceHeuristicSignals(NameSignals signals) {
    if (signals == null) {
      return true;
    }
    boolean hasStrongToken =
        signals.strongTokens().stream().anyMatch(token -> token != null && token.length() >= 4);
    boolean hasAcceptableRoot =
        signals.acceptableRoots().stream().anyMatch(root -> root != null && root.length() >= 4);
    return !hasStrongToken || !hasAcceptableRoot;
  }

  private NameSignals buildNameSignals(String name) {
    if (name == null || name.isBlank()) {
      return null;
    }
    String cleaned = cleanupTitle(name.replace('&', ' '));
    String stripped = stripCorporateSuffixes(cleaned);
    LinkedHashSet<String> roots = new LinkedHashSet<>();
    addHeuristicSlugs(roots, cleaned);
    addHeuristicSlugs(roots, stripped);

    LinkedHashSet<String> tokens = new LinkedHashSet<>();
    String normalized = stripped.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", " ").trim();
    if (!normalized.isBlank()) {
      for (String token : normalized.split("\\s+")) {
        if (token.length() >= 3 && !isWeakNameToken(token)) {
          tokens.add(token);
        }
      }
    }
    if (tokens.isEmpty()) {
      String fallback = normalized.replace(" ", "");
      if (!fallback.isBlank()) {
        tokens.add(fallback);
      }
    }
    return new NameSignals(List.copyOf(tokens), List.copyOf(roots));
  }

  private boolean isWeakNameToken(String token) {
    if (token == null || token.isBlank()) {
      return true;
    }
    return token.equals("the")
        || token.equals("and")
        || token.equals("company")
        || token.equals("group")
        || token.equals("holdings")
        || token.equals("holding")
        || token.equals("inc")
        || token.equals("corp")
        || token.equals("co")
        || token.equals("plc")
        || token.equals("ltd")
        || token.equals("llc");
  }

  private boolean hostnameContainsAcceptableRoot(String host, List<String> acceptableRoots) {
    if (host == null || host.isBlank() || acceptableRoots == null || acceptableRoots.isEmpty()) {
      return false;
    }
    String[] labels = host.toLowerCase(Locale.ROOT).split("\\.");
    for (String label : labels) {
      if (label == null || label.isBlank()) {
        continue;
      }
      for (String root : acceptableRoots) {
        if (root != null && !root.isBlank() && label.equals(root.toLowerCase(Locale.ROOT))) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isParkedHost(String host) {
    if (host == null || host.isBlank()) {
      return false;
    }
    String normalized = host.toLowerCase(Locale.ROOT);
    for (String suffix : PARKED_HOST_SUFFIXES) {
      if (suffix == null || suffix.isBlank()) {
        continue;
      }
      String s = suffix.toLowerCase(Locale.ROOT);
      if (normalized.equals(s) || normalized.endsWith("." + s)) {
        return true;
      }
    }
    return false;
  }

  private String sparqlLiteral(String raw) {
    String escaped = raw.replace("\\", "\\\\").replace("\"", "\\\"");
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
    return isRetryableWdqsCategory(classifyWdqsFailure(fetch));
  }

  private String classifyWdqsFailure(HttpFetchResult fetch) {
    if (fetch == null) {
      return "wdqs_error";
    }
    if (fetch.errorCode() != null) {
      String code = fetch.errorCode().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_");
      if (code.startsWith("wdqs_")) {
        return code;
      }
      return "wdqs_" + code;
    }
    int status = fetch.statusCode();
    if (status == 408) {
      return "wdqs_http_408";
    }
    if (status == 429) {
      return "wdqs_http_429";
    }
    if (status >= 500) {
      return "wdqs_http_5xx";
    }
    if (status > 0) {
      return "wdqs_http_" + status;
    }
    return "wdqs_error";
  }

  private boolean isRetryableWdqsCategory(String category) {
    if (category == null || category.isBlank()) {
      return false;
    }
    return isWdqsTimeoutCategory(category)
        || "wdqs_http_429".equals(category)
        || "wdqs_http_5xx".equals(category)
        || "wdqs_dns_error".equals(category)
        || "wdqs_connect_error".equals(category)
        || "wdqs_io_error".equals(category);
  }

  private boolean isWdqsTimeoutCategory(String category) {
    if (category == null || category.isBlank()) {
      return false;
    }
    return category.contains("timeout") || "wdqs_http_408".equals(category);
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

  private static final class HeuristicBudget {
    private final long maxTotalDurationMs;
    private long usedDurationMs;
    private boolean exhaustedLogged;

    private HeuristicBudget(long maxTotalDurationMs) {
      this.maxTotalDurationMs = Math.max(0L, maxTotalDurationMs);
    }

    private boolean allowAttempt() {
      if (maxTotalDurationMs <= 0L) {
        return true;
      }
      boolean allowed = usedDurationMs < maxTotalDurationMs;
      if (!allowed && !exhaustedLogged) {
        exhaustedLogged = true;
        log.info(
            "Domain heuristic budget exhausted (used={}ms, max={}ms); skipping remaining heuristic fallback attempts",
            usedDurationMs,
            maxTotalDurationMs);
      }
      return allowed;
    }

    private void addDuration(long durationMs) {
      usedDurationMs += Math.max(0L, durationMs);
    }
  }

  private static final class ResolutionMetricsCollector {
    private final int companiesInputCount;
    private final int selectionReturnedCount;
    private final int selectionEligibleCount;
    private final int skippedNotEmployerCount;
    private final List<String> skippedNotEmployerSample;
    private int companiesAttemptedCount;
    private int cachedSkipCount;
    private int wdqsTitleBatchCount;
    private int wdqsCikBatchCount;
    private int wdqsTitleRequestCount;
    private int wdqsCikRequestCount;
    private int wdqsRetryCount;
    private int wikipediaInfoboxTriedCount;
    private int wikipediaInfoboxResolvedCount;
    private int wikipediaInfoboxRejectedCount;
    private int heuristicCompaniesTriedCount;
    private int heuristicCandidatesTriedCount;
    private int heuristicFetchSuccessCount;
    private int heuristicResolvedCount;
    private int heuristicRejectedCount;
    private long wdqsDurationMs;
    private long wikipediaInfoboxDurationMs;
    private long heuristicDurationMs;
    private final Map<String, Integer> wdqsFailureByCategory = new LinkedHashMap<>();
    private final Map<String, Integer> wdqsRetryByCategory = new LinkedHashMap<>();
    private final Map<String, Integer> heuristicCandidatesTriedByTld = new LinkedHashMap<>();
    private final Map<String, Integer> heuristicResolvedByTld = new LinkedHashMap<>();
    private final Map<String, Integer> resolvedByMethod = new LinkedHashMap<>();
    private final Map<String, Integer> heuristicSuccessByReason = new LinkedHashMap<>();

    private ResolutionMetricsCollector(
        int companiesInputCount,
        int selectionReturnedCount,
        int selectionEligibleCount,
        int skippedNotEmployerCount,
        List<String> skippedNotEmployerSample) {
      this.companiesInputCount = Math.max(0, companiesInputCount);
      this.selectionReturnedCount = Math.max(0, selectionReturnedCount);
      this.selectionEligibleCount = Math.max(0, selectionEligibleCount);
      this.skippedNotEmployerCount = Math.max(0, skippedNotEmployerCount);
      this.skippedNotEmployerSample =
          skippedNotEmployerSample == null ? List.of() : List.copyOf(skippedNotEmployerSample);
    }

    private void incrementCompaniesAttempted() {
      companiesAttemptedCount++;
    }

    private void incrementCachedSkip() {
      cachedSkipCount++;
    }

    private void incrementWdqsTitleBatch() {
      wdqsTitleBatchCount++;
    }

    private void incrementWdqsCikBatch() {
      wdqsCikBatchCount++;
    }

    private void incrementWdqsTitleRequest() {
      wdqsTitleRequestCount++;
    }

    private void incrementWdqsCikRequest() {
      wdqsCikRequestCount++;
    }

    private void recordWdqsRetry(String category) {
      wdqsRetryCount++;
      String key = (category == null || category.isBlank()) ? "wdqs_error" : category;
      wdqsRetryByCategory.put(key, wdqsRetryByCategory.getOrDefault(key, 0) + 1);
    }

    private void recordWdqsFailure(String category) {
      String key = (category == null || category.isBlank()) ? "wdqs_error" : category;
      wdqsFailureByCategory.put(key, wdqsFailureByCategory.getOrDefault(key, 0) + 1);
    }

    private void incrementHeuristicCompaniesTried() {
      heuristicCompaniesTriedCount++;
    }

    private void incrementWikipediaInfoboxTried() {
      wikipediaInfoboxTriedCount++;
    }

    private void incrementWikipediaInfoboxResolved() {
      wikipediaInfoboxResolvedCount++;
    }

    private void incrementWikipediaInfoboxRejected() {
      wikipediaInfoboxRejectedCount++;
    }

    private void incrementHeuristicCandidateTried() {
      heuristicCandidatesTriedCount++;
    }

    private void incrementHeuristicFetchSuccess() {
      heuristicFetchSuccessCount++;
    }

    private void incrementHeuristicResolved() {
      heuristicResolvedCount++;
    }

    private void incrementHeuristicRejected() {
      heuristicRejectedCount++;
    }

    private void recordHeuristicCandidateTld(String tld) {
      String key = (tld == null || tld.isBlank()) ? "unknown" : tld.toLowerCase(Locale.ROOT);
      heuristicCandidatesTriedByTld.put(
          key, heuristicCandidatesTriedByTld.getOrDefault(key, 0) + 1);
    }

    private void recordHeuristicResolvedTld(String tld) {
      String key = (tld == null || tld.isBlank()) ? "unknown" : tld.toLowerCase(Locale.ROOT);
      heuristicResolvedByTld.put(key, heuristicResolvedByTld.getOrDefault(key, 0) + 1);
    }

    private void addWdqsDuration(long durationMs) {
      wdqsDurationMs += Math.max(0L, durationMs);
    }

    private void addHeuristicDuration(long durationMs) {
      heuristicDurationMs += Math.max(0L, durationMs);
    }

    private void addWikipediaInfoboxDuration(long durationMs) {
      wikipediaInfoboxDurationMs += Math.max(0L, durationMs);
    }

    private void recordResolved(String method) {
      if (method == null || method.isBlank()) {
        return;
      }
      resolvedByMethod.put(method, resolvedByMethod.getOrDefault(method, 0) + 1);
    }

    private void recordHeuristicSuccessReason(String reason) {
      if (reason == null || reason.isBlank()) {
        return;
      }
      heuristicSuccessByReason.put(reason, heuristicSuccessByReason.getOrDefault(reason, 0) + 1);
    }

    private DomainResolutionMetrics toModel(long totalDurationMs) {
      return new DomainResolutionMetrics(
          companiesInputCount,
          selectionReturnedCount,
          selectionEligibleCount,
          skippedNotEmployerCount,
          skippedNotEmployerSample,
          companiesAttemptedCount,
          cachedSkipCount,
          wdqsTitleBatchCount,
          wdqsCikBatchCount,
          wdqsTitleRequestCount,
          wdqsCikRequestCount,
          wdqsRetryCount,
          wikipediaInfoboxTriedCount,
          wikipediaInfoboxResolvedCount,
          wikipediaInfoboxRejectedCount,
          heuristicCompaniesTriedCount,
          heuristicCandidatesTriedCount,
          heuristicFetchSuccessCount,
          heuristicResolvedCount,
          heuristicRejectedCount,
          Math.max(0L, totalDurationMs),
          wdqsDurationMs,
          wikipediaInfoboxDurationMs,
          heuristicDurationMs,
          Map.copyOf(wdqsFailureByCategory),
          Map.copyOf(wdqsRetryByCategory),
          Map.copyOf(heuristicCandidatesTriedByTld),
          Map.copyOf(heuristicResolvedByTld),
          Map.copyOf(resolvedByMethod),
          Map.copyOf(heuristicSuccessByReason));
    }
  }

  private static final class DomainResolutionAttemptContext {
    private final Instant startedAt;
    private final String selectionMode;
    private final List<DomainResolutionAttemptStep> steps = new ArrayList<>();

    private DomainResolutionAttemptContext(Instant startedAt, String selectionMode) {
      this.startedAt = startedAt;
      this.selectionMode = selectionMode;
    }

    private Instant startedAt() {
      return startedAt;
    }

    private String selectionMode() {
      return selectionMode;
    }

    private List<DomainResolutionAttemptStep> steps() {
      return List.copyOf(steps);
    }

    private void addStep(String stage, String method, String outcome, String detail) {
      addStep(stage, method, outcome, detail, null);
    }

    private void addStep(
        String stage, String method, String outcome, String detail, String resolvedDomain) {
      steps.add(
          new DomainResolutionAttemptStep(
              Instant.now(), stage, method, outcome, detail, resolvedDomain));
    }
  }

  private record DomainResolutionAttemptStep(
      Instant recordedAt,
      String stage,
      String method,
      String outcome,
      String detail,
      String resolvedDomain) {}

  private record ResolvedDomainWrite(
      String domain,
      String source,
      double confidence,
      String resolutionMethod,
      String wikidataQid) {}

  private record HeuristicVerdict(boolean accepted, String resolvedDomain, String rejectionReason) {
    private static HeuristicVerdict accept(String resolvedDomain) {
      return new HeuristicVerdict(true, resolvedDomain, null);
    }

    private static HeuristicVerdict reject(String reason) {
      return new HeuristicVerdict(false, null, reason);
    }
  }

  private record InfoboxResolutionAttempt(
      String resolvedDomain, String status, String errorCategory) {
    private static InfoboxResolutionAttempt success(String domain) {
      return new InfoboxResolutionAttempt(domain, STATUS_RESOLVED, null);
    }

    private static InfoboxResolutionAttempt failure(String status, String errorCategory) {
      return new InfoboxResolutionAttempt(null, status, errorCategory);
    }
  }

  private record NameSignals(List<String> strongTokens, List<String> acceptableRoots) {}

  private record WdqsLookupBatchResult(WdqsLookup lookup, Map<String, String> failedCategories) {}

  private record WdqsQueryResult(JsonNode root, String errorCategory) {}

  private record WdqsLookup(Map<String, WdqsMatch> matches) {}

  private record WdqsMatch(String qid, String website) {}
}
