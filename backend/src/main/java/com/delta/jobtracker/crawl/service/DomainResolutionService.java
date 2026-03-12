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
import java.util.stream.Collectors;
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
  private static final String METHOD_EQUIVALENT_CIK = "EQUIVALENT_CIK";
  private static final String METHOD_NORMALIZED_TITLE = "NORMALIZED_COMPANY_TITLE";
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
        companies, limit, deadline, companies.size(), companies.size(), 0, List.of());
  }

  private DomainResolutionResult resolveCompanies(
      List<CompanyIdentity> missingDomain,
      int limit,
      Instant deadline,
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
    Map<Long, CrawlJdbcRepository.EquivalentResolvedDomainEvidence> equivalentDomainsByCompanyId =
        repository.findEquivalentResolvedDomainsByCompanyIds(
            companies.stream().map(CompanyIdentity::companyId).collect(Collectors.toList()));
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
        "Domain resolver starting: companies={}, with_wikipedia_title={}, with_cik={}, batch_size={}",
        companies.size(),
        totalWithTitle,
        totalWithCik,
        batchSize);

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

      for (CompanyIdentity company : batch) {
        if (deadline != null && Instant.now().isAfter(deadline)) {
          log.info(
              "Domain resolver stopped early due to time budget (batch {}/{})",
              batchIndex,
              batchCount);
          break;
        }
        Instant now = Instant.now();
        if (tryEquivalentCikDomainFallback(
            company, equivalentDomainsByCompanyId.get(company.companyId()), counts, metrics, now)) {
          continue;
        }
        if (hasWikipediaTitle(company)) {
          if (shouldSkipCached(company, METHOD_WIKIPEDIA, now)) {
            if (hasCik(company) && !shouldSkipCached(company, METHOD_CIK, now)) {
              metrics.incrementCompaniesAttempted();
              List<String> ciks = buildCikVariants(company.cik());
              if (ciks.isEmpty()) {
                if (tryHeuristicFallback(
                    company,
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
                repository.updateCompanyDomainResolutionCache(
                    company.companyId(), METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik", now);
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
          List<String> titles = buildTitleVariants(company.wikipediaTitle());
          if (titles.isEmpty()) {
            if (hasCik(company) && !shouldSkipCached(company, METHOD_CIK, now)) {
              List<String> ciks = buildCikVariants(company.cik());
              if (!ciks.isEmpty()) {
                ciksByCompany.put(company, ciks);
                for (String cik : ciks) {
                  companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
                }
                continue;
              }
            }
            if (tryHeuristicFallback(
                company,
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
            repository.updateCompanyDomainResolutionCache(
                company.companyId(),
                METHOD_WIKIPEDIA,
                STATUS_NO_IDENTIFIER,
                "no_wikipedia_title",
                now);
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
          List<String> ciks = buildCikVariants(company.cik());
          if (ciks.isEmpty()) {
            if (tryHeuristicFallback(
                company,
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
            repository.updateCompanyDomainResolutionCache(
                company.companyId(), METHOD_CIK, STATUS_NO_IDENTIFIER, "no_cik", now);
            continue;
          }
          ciksByCompany.put(company, ciks);
          for (String cik : ciks) {
            companiesByCik.computeIfAbsent(cik, ignored -> new ArrayList<>()).add(company);
          }
        } else {
          metrics.incrementCompaniesAttempted();
          if (tryHeuristicFallback(
              company,
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
          repository.updateCompanyDomainResolutionCache(
              company.companyId(), METHOD_NONE, STATUS_NO_IDENTIFIER, "no_identifier", now);
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
        WdqsLookupResult lookupResult =
            fetchWdqsMatchesByTitle(new ArrayList<>(companiesByTitle.keySet()));
        metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
        if (lookupResult.lookup() == null) {
          String errorCategory =
              lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
          for (CompanyIdentity company : titlesByCompany.keySet()) {
            if (tryCikFallbackLookup(
                company, counts, errors, metrics, deadline, heuristicFetchCache, heuristicBudget)) {
              continue;
            }
            if (tryHeuristicFallback(
                company,
                errorCategory,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget)) {
              continue;
            }
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
                Instant.now());
          }
        } else {
          for (Map.Entry<CompanyIdentity, List<String>> entry : titlesByCompany.entrySet()) {
            CompanyIdentity company = entry.getKey();
            WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
            applyMatch(
                company,
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
          }
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
        WdqsLookupResult lookupResult =
            fetchWdqsMatchesByCik(new ArrayList<>(companiesByCik.keySet()));
        metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
        if (lookupResult.lookup() == null) {
          String errorCategory =
              lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
          for (CompanyIdentity company : ciksByCompany.keySet()) {
            if (tryHeuristicFallback(
                company,
                errorCategory,
                counts,
                errors,
                metrics,
                deadline,
                heuristicFetchCache,
                heuristicBudget)) {
              continue;
            }
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
                Instant.now());
          }
        } else {
          for (Map.Entry<CompanyIdentity, List<String>> entry : ciksByCompany.entrySet()) {
            CompanyIdentity company = entry.getKey();
            WdqsMatch match = findMatch(entry.getValue(), lookupResult.lookup().matches());
            applyMatch(
                company,
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

  private WdqsMatch findMatch(List<String> titles, Map<String, WdqsMatch> matches) {
    for (String title : titles) {
      WdqsMatch match = matches.get(title);
      if (match != null) {
        return match;
      }
    }
    return null;
  }

  private boolean tryEquivalentCikDomainFallback(
      CompanyIdentity company,
      CrawlJdbcRepository.EquivalentResolvedDomainEvidence evidence,
      Counts counts,
      ResolutionMetricsCollector metrics,
      Instant now) {
    if (company == null || evidence == null) {
      return false;
    }
    String domain = evidence.domain();
    if (domain == null || domain.isBlank()) {
      return false;
    }
    repository.upsertCompanyDomain(
        company.companyId(),
        domain,
        evidence.careersHintUrl(),
        "CIK_EQUIVALENT",
        evidence.confidence(),
        evidence.resolvedAt() == null ? now : evidence.resolvedAt(),
        evidence.resolutionMethod() == null ? METHOD_EQUIVALENT_CIK : evidence.resolutionMethod(),
        evidence.wikidataQid());
    repository.updateCompanyDomainResolutionCache(
        company.companyId(), METHOD_EQUIVALENT_CIK, STATUS_RESOLVED, null, now);
    counts.resolved++;
    metrics.recordResolved("EQUIVALENT_CIK");
    return true;
  }

  private void applyMatch(
      CompanyIdentity company,
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
      if (allowCikFallback
          && tryCikFallbackLookup(
              company, counts, errors, metrics, deadline, heuristicFetchCache, heuristicBudget)) {
        return;
      }
      if (!METHOD_NORMALIZED_TITLE.equals(method)
          && tryNormalizedCompanyTitleFallbackLookup(
          company, counts, errors, metrics, deadline, heuristicFetchCache, heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(company, "no_item", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
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
      repository.updateCompanyDomainResolutionCache(
          company.companyId(), method, STATUS_NO_ITEM, "no_item", Instant.now());
      return;
    }
    if (match.website() == null || match.website().isBlank()) {
      if (allowCikFallback
          && tryCikFallbackLookup(
              company, counts, errors, metrics, deadline, heuristicFetchCache, heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(company, "no_p856", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
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
      repository.updateCompanyDomainResolutionCache(
          company.companyId(), method, STATUS_NO_P856, "no_p856", Instant.now());
      return;
    }

    String domain = normalizeDomain(match.website());
    if (domain == null) {
      if (allowCikFallback
          && tryCikFallbackLookup(
              company, counts, errors, metrics, deadline, heuristicFetchCache, heuristicBudget)) {
        return;
      }
      if (tryWikipediaInfoboxFallback(company, "invalid_website_url", counts, metrics, deadline)) {
        return;
      }
      if (tryHeuristicFallback(
          company,
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
      repository.updateCompanyDomainResolutionCache(
          company.companyId(),
          method,
          STATUS_INVALID_WEBSITE,
          "invalid_website_url",
          Instant.now());
      return;
    }

    repository.upsertCompanyDomain(
        company.companyId(),
        domain,
        null,
        "WIKIDATA",
        0.95,
        Instant.now(),
        resolutionMethod,
        match.qid());
    repository.updateCompanyDomainResolutionCache(
        company.companyId(), method, STATUS_RESOLVED, null, Instant.now());
    counts.resolved++;
    metrics.recordResolved("WIKIDATA");
  }

  private boolean tryWikipediaInfoboxFallback(
      CompanyIdentity company,
      String failureReason,
      Counts counts,
      ResolutionMetricsCollector metrics,
      Instant deadline) {
    if (!hasWikipediaTitle(company)) {
      return false;
    }
    Instant now = Instant.now();
    if (deadline != null && now.isAfter(deadline)) {
      return false;
    }
    if (shouldSkipCached(company, METHOD_WIKIPEDIA_INFOBOX, now)) {
      return false;
    }

    metrics.incrementWikipediaInfoboxTried();
    Instant startedAt = Instant.now();
    InfoboxResolutionAttempt attempt = resolveFromWikipediaInfobox(company);
    metrics.addWikipediaInfoboxDuration(Duration.between(startedAt, Instant.now()).toMillis());

    if (attempt.resolvedDomain() != null) {
      repository.upsertCompanyDomain(
          company.companyId(),
          attempt.resolvedDomain(),
          null,
          "WIKIPEDIA",
          WIKIPEDIA_INFOBOX_CONFIDENCE,
          Instant.now(),
          METHOD_WIKIPEDIA_INFOBOX,
          null);
      repository.updateCompanyDomainResolutionCache(
          company.companyId(), METHOD_WIKIPEDIA_INFOBOX, STATUS_RESOLVED, null, Instant.now());
      counts.resolved++;
      metrics.incrementWikipediaInfoboxResolved();
      metrics.recordResolved("WIKIPEDIA_INFOBOX");
      return true;
    }

    metrics.incrementWikipediaInfoboxRejected();
    repository.updateCompanyDomainResolutionCache(
        company.companyId(),
        METHOD_WIKIPEDIA_INFOBOX,
        attempt.status() == null ? STATUS_INFOBOX_ERROR : attempt.status(),
        attempt.errorCategory() == null ? failureReason : attempt.errorCategory(),
        Instant.now());
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
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (!hasCik(company)) {
      return false;
    }
    Instant now = Instant.now();
    if (shouldSkipCached(company, METHOD_CIK, now)) {
      return false;
    }
    if (deadline != null && now.isAfter(deadline)) {
      return false;
    }
    List<String> ciks = buildCikVariants(company.cik());
    if (ciks.isEmpty()) {
      return false;
    }

    metrics.incrementWdqsCikBatch();
    Instant wdqsStartedAt = Instant.now();
    WdqsLookupResult lookupResult = fetchWdqsMatchesByCik(ciks);
    metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
    if (lookupResult.lookup() == null) {
      String errorCategory =
          lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
      if (tryHeuristicFallback(
          company,
          errorCategory,
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return true;
      }
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
          Instant.now());
      return true;
    }

    WdqsMatch match = findMatch(ciks, lookupResult.lookup().matches());
    applyMatch(
        company,
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

  private boolean tryNormalizedCompanyTitleFallbackLookup(
      CompanyIdentity company,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (company == null || hasWikipediaTitle(company) || !hasCik(company) || company.name() == null) {
      return false;
    }
    Instant now = Instant.now();
    if (shouldSkipCached(company, METHOD_NORMALIZED_TITLE, now)) {
      return false;
    }
    if (deadline != null && now.isAfter(deadline)) {
      return false;
    }
    List<String> titles = buildTitleVariants(company.name());
    if (titles.isEmpty()) {
      return false;
    }

    metrics.incrementWdqsTitleBatch();
    Instant wdqsStartedAt = Instant.now();
    WdqsLookupResult lookupResult = fetchWdqsMatchesByTitle(titles);
    metrics.addWdqsDuration(Duration.between(wdqsStartedAt, Instant.now()).toMillis());
    if (lookupResult.lookup() == null) {
      String errorCategory =
          lookupResult.errorCategory() == null ? "wdqs_error" : lookupResult.errorCategory();
      if (tryHeuristicFallback(
          company,
          errorCategory,
          counts,
          errors,
          metrics,
          deadline,
          heuristicFetchCache,
          heuristicBudget)) {
        return true;
      }
      if ("wdqs_timeout".equals(errorCategory)) {
        counts.wdqsTimeout++;
      } else {
        counts.wdqsError++;
      }
      errors.add(company, errorCategory);
      repository.updateCompanyDomainResolutionCache(
          company.companyId(),
          METHOD_NORMALIZED_TITLE,
          "wdqs_timeout".equals(errorCategory) ? STATUS_WDQS_TIMEOUT : STATUS_WDQS_ERROR,
          errorCategory,
          now);
      return true;
    }

    WdqsMatch match = findMatch(titles, lookupResult.lookup().matches());
    applyMatch(
        company,
        match,
        METHOD_NORMALIZED_TITLE,
        "normalized_company_name",
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

  private WdqsLookupResult fetchWdqsMatchesByTitle(List<String> titles) {
    if (titles.isEmpty()) {
      return new WdqsLookupResult(new WdqsLookup(Map.of()), null);
    }

    String values =
        titles.stream()
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
        } else if ((existing.website() == null || existing.website().isBlank())
            && website != null) {
          matches.put(
              candidate, new WdqsMatch(existing.qid() != null ? existing.qid() : qid, website));
        }
      }
    }
    return new WdqsLookupResult(new WdqsLookup(matches), null);
  }

  private WdqsLookupResult fetchWdqsMatchesByCik(List<String> ciks) {
    if (ciks.isEmpty()) {
      return new WdqsLookupResult(new WdqsLookup(Map.of()), null);
    }

    String values = ciks.stream().map(this::sparqlLiteral).reduce((a, b) -> a + " " + b).orElse("");

    String query =
        """
                SELECT ?candidateCik ?item ?officialWebsite WHERE {
                  VALUES ?candidateCik { %s }
                  ?item wdt:%s ?candidateCik .
                  OPTIONAL { ?item wdt:P856 ?officialWebsite . }
                }
                """
            .formatted(values, CIK_PROPERTY);

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
        } else if ((existing.website() == null || existing.website().isBlank())
            && website != null) {
          matches.put(
              candidate, new WdqsMatch(existing.qid() != null ? existing.qid() : qid, website));
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

      lastCategory = isTimeout(fetch) ? "wdqs_timeout" : "wdqs_error";
      log.warn(
          "WDQS query failed attempt={} status={} errorCode={} bodySample={}",
          attempt,
          fetch.statusCode(),
          fetch.errorCode(),
          sampleBody(fetch.body()));

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
      wdqsNextAllowedAt =
          Instant.now().plusMillis(properties.getDomainResolution().getWdqsMinDelayMs());
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
    cleaned = cleaned.replaceAll("\\s*,\\s*", ", ");
    cleaned = cleaned.replaceAll("^[,;:\\s]+", "");
    cleaned = cleaned.replaceAll("[,;:\\s]+$", "");
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
    return cleanupTitle(String.join(" ", java.util.Arrays.copyOf(tokens, end)).trim());
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
      String failureReason,
      Counts counts,
      ErrorCollector errors,
      ResolutionMetricsCollector metrics,
      Instant deadline,
      Map<String, HttpFetchResult> heuristicFetchCache,
      HeuristicBudget heuristicBudget) {
    if (company == null || company.name() == null || company.name().isBlank()) {
      return false;
    }
    if (deadline != null && Instant.now().isAfter(deadline)) {
      return false;
    }
    if (heuristicBudget != null && !heuristicBudget.allowAttempt()) {
      return false;
    }
    NameSignals signals = buildNameSignals(company.name());
    if (signals == null || signals.strongTokens().isEmpty()) {
      return false;
    }
    if (!hasWikipediaTitle(company)
        && !hasCik(company)
        && isLowConfidenceHeuristicSignals(signals)) {
      return false;
    }

    List<String> candidates = heuristicDomainCandidates(company);
    if (candidates.isEmpty()) {
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
      repository.upsertCompanyDomain(
          company.companyId(),
          resolvedDomain,
          null,
          "HEURISTIC",
          HEURISTIC_CONFIDENCE,
          Instant.now(),
          heuristicResolutionMethodForDomain(resolvedDomain),
          null);
      repository.updateCompanyDomainResolutionCache(
          company.companyId(), METHOD_HEURISTIC, STATUS_RESOLVED, null, Instant.now());
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

    if (!rootMatches) {
      return HeuristicVerdict.reject("root_mismatch");
    }
    if (tokenMatches == 0
        && !hostnameContainsAcceptableRoot(resolvedDomain, signals.acceptableRoots())) {
      return HeuristicVerdict.reject("name_not_found");
    }

    return HeuristicVerdict.accept(resolvedDomain);
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
          Map.copyOf(heuristicCandidatesTriedByTld),
          Map.copyOf(heuristicResolvedByTld),
          Map.copyOf(resolvedByMethod),
          Map.copyOf(heuristicSuccessByReason));
    }
  }

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

  private record WdqsLookupResult(WdqsLookup lookup, String errorCategory) {}

  private record WdqsQueryResult(JsonNode root, String errorCategory) {}

  private record WdqsLookup(Map<String, WdqsMatch> matches) {}

  private record WdqsMatch(String qid, String website) {}
}
