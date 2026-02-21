package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.ats.AtsEndpointExtractor;
import com.delta.jobtracker.crawl.ats.AtsFingerprintFromHtmlLinksDetector;
import com.delta.jobtracker.crawl.ats.AtsFingerprintFromSitemapsDetector;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CareersDiscoveryMethodMetrics;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CareersDiscoveryState;
import com.delta.jobtracker.crawl.model.CareersDiscoveryWithMetrics;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.model.SitemapUrlEntry;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsRules;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.SitemapService;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CareersDiscoveryService {
  private static final Logger log = LoggerFactory.getLogger(CareersDiscoveryService.class);
  private static final String HTML_ACCEPT =
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
  private static final int MAX_HOMEPAGE_BYTES = 512_000;
  private static final int HOMEPAGE_TIMEOUT_SECONDS = 10;
  private static final int MAX_SITEMAP_URLS_DISCOVERY = 120;

  private static final List<String> COMMON_PATHS =
      List.of(
          "/careers",
          "/jobs",
          "/careers/jobs",
          "/about/careers",
          "/join-us",
          "/careers/search",
          "/careers/opportunities",
          "/careers/openings",
          "/jobs/search");

  private static final List<String> LINK_SCAN_PATHS =
      List.of("/", "/careers", "/jobs", "/about/careers");
  private static final int MAX_LINK_SCAN_PAGES = 6;
  private static final int MAX_LINK_SCAN_ENDPOINTS = 10;

  private static final List<String> HINT_TOKENS =
      List.of("careers", "jobs", "join", "talent", "opportunities", "work-with-us");

  private final CrawlerProperties properties;
  private final CrawlJdbcRepository repository;
  private final PoliteHttpClient httpClient;
  private final RobotsTxtService robotsTxtService;
  private final AtsDetector atsDetector;
  private final AtsEndpointExtractor atsEndpointExtractor;
  private final SitemapService sitemapService;
  private final AtsFingerprintFromHtmlLinksDetector homepageLinksDetector;
  private final AtsFingerprintFromSitemapsDetector sitemapDetector;
  private final HostCrawlStateService hostCrawlStateService;

  public CareersDiscoveryService(
      CrawlerProperties properties,
      CrawlJdbcRepository repository,
      PoliteHttpClient httpClient,
      RobotsTxtService robotsTxtService,
      AtsDetector atsDetector,
      AtsEndpointExtractor atsEndpointExtractor,
      SitemapService sitemapService,
      AtsFingerprintFromHtmlLinksDetector homepageLinksDetector,
      AtsFingerprintFromSitemapsDetector sitemapDetector,
      HostCrawlStateService hostCrawlStateService) {
    this.properties = properties;
    this.repository = repository;
    this.httpClient = httpClient;
    this.robotsTxtService = robotsTxtService;
    this.atsDetector = atsDetector;
    this.atsEndpointExtractor = atsEndpointExtractor;
    this.sitemapService = sitemapService;
    this.homepageLinksDetector = homepageLinksDetector;
    this.sitemapDetector = sitemapDetector;
    this.hostCrawlStateService = hostCrawlStateService;
  }

  public CareersDiscoveryResult discover(Integer requestedLimit) {
    return discover(requestedLimit, null, false);
  }

  public CareersDiscoveryResult discover(Integer requestedLimit, Instant deadline) {
    return discover(requestedLimit, deadline, false);
  }

  public CareersDiscoveryResult discover(
      Integer requestedLimit, Instant deadline, boolean vendorProbeOnly) {
    int limit =
        requestedLimit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, requestedLimit);

    List<CompanyTarget> companies = repository.findCompaniesWithDomainWithoutAts(limit);
    return discoverForCompanies(companies, deadline, vendorProbeOnly);
  }

  public CareersDiscoveryResult discoverForTickers(List<String> tickers, Integer requestedLimit) {
    return discoverForTickers(tickers, requestedLimit, null, false);
  }

  public CareersDiscoveryResult discoverForTickers(
      List<String> tickers, Integer requestedLimit, Instant deadline) {
    return discoverForTickers(tickers, requestedLimit, deadline, false);
  }

  public CareersDiscoveryResult discoverForTickers(
      List<String> tickers, Integer requestedLimit, Instant deadline, boolean vendorProbeOnly) {
    int limit =
        requestedLimit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, requestedLimit);
    List<CompanyTarget> companies =
        repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, limit);
    return discoverForCompanies(companies, deadline, vendorProbeOnly);
  }

  public CareersDiscoveryWithMetrics discoverForTickersWithMetrics(
      List<String> tickers, Integer requestedLimit, Instant deadline, boolean vendorProbeOnly) {
    int limit =
        requestedLimit == null
            ? properties.getCareersDiscovery().getDefaultLimit()
            : Math.max(1, requestedLimit);
    List<CompanyTarget> companies =
        repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, limit);
    DiscoveryMetrics metrics = new DiscoveryMetrics();
    CareersDiscoveryResult result =
        discoverForCompanies(companies, deadline, vendorProbeOnly, metrics);
    return new CareersDiscoveryWithMetrics(result, toMethodMetrics(metrics));
  }

  private CareersDiscoveryResult discoverForCompanies(
      List<CompanyTarget> companies, Instant deadline, boolean vendorProbeOnly) {
    return discoverForCompanies(companies, deadline, vendorProbeOnly, null);
  }

  private CareersDiscoveryResult discoverForCompanies(
      List<CompanyTarget> companies,
      Instant deadline,
      boolean vendorProbeOnly,
      DiscoveryMetrics metrics) {
    Map<String, Integer> discoveredCountByType = new LinkedHashMap<>();
    int failedCount = 0;
    int cooldownSkips = 0;
    Map<String, Integer> topErrors = new LinkedHashMap<>();
    VendorProbeLimiter vendorProbeLimiter =
        vendorProbeLimiter(properties.getCareersDiscovery().getMaxVendorProbeRequestsPerHost());

    for (CompanyTarget company : companies) {
      if (deadline != null && Instant.now().isAfter(deadline)) {
        log.info("Careers discovery stopped early due to time budget");
        break;
      }
      try {
        DiscoveryOutcome outcome =
            discoverForCompany(company, deadline, metrics, vendorProbeLimiter, vendorProbeOnly);
        cooldownSkips += outcome.cooldownSkips();
        if (outcome.skipped()) {
          continue;
        }
        if (!outcome.hasEndpoints()) {
          failedCount++;
          DiscoveryFailure failure = outcome.primaryFailure();
          String reason = failure == null ? "discovery_no_match" : failure.reasonCode();
          increment(topErrors, reason);
        } else {
          for (Map.Entry<AtsType, Integer> entry : outcome.countsByType().entrySet()) {
            increment(discoveredCountByType, entry.getKey().name(), entry.getValue());
          }
        }
      } catch (Exception e) {
        failedCount++;
        increment(topErrors, "discovery_exception");
        log.warn("Careers discovery failed for {} ({})", company.ticker(), company.domain(), e);
      }
    }

    return new CareersDiscoveryResult(discoveredCountByType, failedCount, cooldownSkips, topErrors);
  }

  private CareersDiscoveryMethodMetrics toMethodMetrics(DiscoveryMetrics metrics) {
    if (metrics == null) {
      return new CareersDiscoveryMethodMetrics(0, 0, 0, 0, 0, 0, 0, Map.of(), Map.of(), Map.of());
    }
    return new CareersDiscoveryMethodMetrics(
        metrics.homepageScanned(),
        metrics.careersPathsChecked(),
        metrics.robotsBlockedCount(),
        metrics.fetchFailedCount(),
        metrics.timeBudgetExceededCount(),
        metrics.sitemapsScanned(),
        metrics.sitemapUrlsChecked(),
        toStringKeyMap(metrics.endpointsFoundHomepage()),
        toStringKeyMap(metrics.endpointsFoundVendorProbe()),
        toStringKeyMap(metrics.endpointsFoundSitemap()));
  }

  private Map<String, Integer> toStringKeyMap(Map<AtsType, Integer> source) {
    if (source == null || source.isEmpty()) {
      return Map.of();
    }
    Map<String, Integer> out = new LinkedHashMap<>();
    for (Map.Entry<AtsType, Integer> entry : source.entrySet()) {
      AtsType type = entry.getKey();
      if (type == null) {
        continue;
      }
      int value = entry.getValue() == null ? 0 : entry.getValue();
      if (value <= 0) {
        continue;
      }
      out.put(type.name(), value);
    }
    return out;
  }

  DiscoveryOutcome discoverForCompany(CompanyTarget company, Instant deadline) {
    return discoverForCompany(company, deadline, null, null, false);
  }

  DiscoveryOutcome discoverForCompany(
      CompanyTarget company,
      Instant deadline,
      DiscoveryMetrics metrics,
      VendorProbeLimiter vendorProbeLimiter,
      boolean vendorProbeOnly) {
    LinkedHashSet<String> seen = new LinkedHashSet<>();
    Map<AtsType, Integer> countsByType = new LinkedHashMap<>();
    List<DiscoveryFailure> failures = new ArrayList<>();
    AtomicInteger cooldownSkips = new AtomicInteger(0);

    if (company == null) {
      DiscoveryFailure failure = new DiscoveryFailure("discovery_no_domain", null, null);
      return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, false);
    }

    boolean hasDomain = company.domain() != null && !company.domain().isBlank();
    if (!hasDomain && !vendorProbeOnly) {
      DiscoveryFailure failure = new DiscoveryFailure("discovery_no_domain", null, null);
      return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, false);
    }

    if (isCompanyCoolingDown(company.companyId())) {
      cooldownSkips.incrementAndGet();
      DiscoveryFailure failure = new DiscoveryFailure("discovery_company_cooldown", null, null);
      return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), true, false);
    }

    if (deadlineExceeded(deadline)) {
      DiscoveryFailure failure = new DiscoveryFailure("discovery_time_budget_exceeded", null, null);
      if (metrics != null) {
        metrics.incrementTimeBudgetExceeded();
      }
      updateDiscoveryState(company, failure);
      return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, true);
    }

    if (!vendorProbeOnly && hasDomain) {
      String homepageUrl = "https://" + company.domain().trim().toLowerCase(Locale.ROOT) + "/";
      if (!robotsTxtService.isAllowed(homepageUrl)) {
        DiscoveryFailure failure =
            new DiscoveryFailure("discovery_homepage_blocked_by_robots", homepageUrl, null);
        failures.add(failure);
        if (metrics != null) {
          metrics.incrementRobotsBlocked();
        }
        recordCooldownFailure(homepageUrl, ReasonCodeClassifier.ROBOTS_BLOCKED);
      } else {
        if (metrics != null) {
          metrics.incrementHomepageScanned();
        }
        HttpFetchResult fetch = fetchHomepage(homepageUrl);
        if ("body_too_large".equals(fetch.errorCode())) {
          failures.add(
              new DiscoveryFailure(
                  "discovery_homepage_too_large", homepageUrl, fetch.errorMessage()));
          recordCooldownSuccess(homepageUrl);
        } else if (!fetch.isSuccessful() || fetch.body() == null) {
          failures.add(failureFromFetch("discovery_fetch_failed", homepageUrl, fetch));
          if (metrics != null) {
            metrics.incrementFetchFailed();
          }
          recordCooldownFromFetch(homepageUrl, fetch);
        } else if (fetch.bodyBytes() != null && fetch.bodyBytes().length > MAX_HOMEPAGE_BYTES) {
          failures.add(
              new DiscoveryFailure(
                  "discovery_homepage_too_large",
                  homepageUrl,
                  "bytes=" + fetch.bodyBytes().length));
          recordCooldownSuccess(homepageUrl);
        } else {
          recordCooldownSuccess(homepageUrl);
          Map<AtsDetectionRecord, String> endpoints =
              homepageLinksDetector.detect(fetch.body(), homepageUrl);
          if (!endpoints.isEmpty()) {
            registerEndpoints(
                company,
                homepageUrl,
                new ArrayList<>(endpoints.keySet()),
                "homepage_link",
                0.95,
                true,
                seen,
                countsByType);
            if (metrics != null) {
              metrics.incrementHomepageFound(endpoints.keySet());
            }
            updateDiscoveryStateSuccess(company);
            return new DiscoveryOutcome(countsByType, null, cooldownSkips.get(), false, false);
          }
        }
      }
    }

    if (!vendorProbeOnly && hasDomain) {
      if (deadlineExceeded(deadline)) {
        DiscoveryFailure failure =
            new DiscoveryFailure("discovery_time_budget_exceeded", null, null);
        if (metrics != null) {
          metrics.incrementTimeBudgetExceeded();
        }
        updateDiscoveryState(company, failure);
        return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, true);
      }
      SitemapDetectionOutcome sitemapResult = discoverAtsFromSitemaps(company, metrics, failures);
      if (sitemapResult != null && sitemapResult.hasEndpoints()) {
        registerEndpoints(
            company, sitemapResult.endpoints(), "sitemap", 0.92, true, seen, countsByType);
        if (metrics != null) {
          metrics.incrementSitemapFound(sitemapResult.endpoints().keySet());
        }
        updateDiscoveryStateSuccess(company);
        return new DiscoveryOutcome(countsByType, null, cooldownSkips.get(), false, false);
      }
    }

    List<String> slugCandidates = buildSlugCandidates(company);
    int maxSlugs = properties.getCareersDiscovery().getMaxSlugCandidates();
    int slugIndex = 0;
    for (String slug : slugCandidates) {
      if (slugIndex >= maxSlugs) {
        break;
      }
      slugIndex++;
      if (deadlineExceeded(deadline)) {
        DiscoveryFailure failure =
            new DiscoveryFailure("discovery_time_budget_exceeded", slug, null);
        if (metrics != null) {
          metrics.incrementTimeBudgetExceeded();
        }
        updateDiscoveryState(company, failure);
        return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, true);
      }
      AtsDetectionRecord vendorEndpoint =
          probeVendorSlug(slug, metrics, vendorProbeLimiter, failures);
      if (vendorEndpoint != null) {
        registerEndpoints(
            company,
            vendorEndpoint.atsUrl(),
            List.of(vendorEndpoint),
            "vendor_probe",
            0.9,
            true,
            seen,
            countsByType);
        if (metrics != null) {
          metrics.incrementVendorFound(vendorEndpoint);
        }
        updateDiscoveryStateSuccess(company);
        return new DiscoveryOutcome(countsByType, null, cooldownSkips.get(), false, false);
      }
    }

    if (!vendorProbeOnly && hasDomain) {
      int pathsChecked = 0;
      int maxPaths = properties.getCareersDiscovery().getMaxCareersPaths();
      for (String path : COMMON_PATHS) {
        if (pathsChecked >= maxPaths) {
          break;
        }
        if (deadlineExceeded(deadline)) {
          DiscoveryFailure failure =
              new DiscoveryFailure("discovery_time_budget_exceeded", path, null);
          if (metrics != null) {
            metrics.incrementTimeBudgetExceeded();
          }
          updateDiscoveryState(company, failure);
          return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, true);
        }

        String candidate = "https://" + company.domain().trim().toLowerCase(Locale.ROOT) + path;
        pathsChecked++;
        if (metrics != null) {
          metrics.incrementCareersPathsChecked();
        }

        if (!robotsTxtService.isAllowed(candidate)) {
          failures.add(
              new DiscoveryFailure("discovery_careers_blocked_by_robots", candidate, null));
          if (metrics != null) {
            metrics.incrementRobotsBlocked();
          }
          recordCooldownFailure(candidate, ReasonCodeClassifier.ROBOTS_BLOCKED);
          break;
        }

        HttpFetchResult fetch = httpClient.get(candidate, HTML_ACCEPT);
        if (!fetch.isSuccessful() || fetch.body() == null) {
          failures.add(failureFromFetch("discovery_fetch_failed", candidate, fetch));
          if (metrics != null) {
            metrics.incrementFetchFailed();
          }
          recordCooldownFromFetch(candidate, fetch);
          continue;
        }
        recordCooldownSuccess(candidate);

        String resolved = normalizeCandidateUrl(fetch.finalUrlOrRequested());
        List<AtsDetectionRecord> extracted = atsEndpointExtractor.extract(resolved, fetch.body());
        if (!extracted.isEmpty()) {
          registerEndpoints(
              company, candidate, extracted, "careers_path", 0.85, true, seen, countsByType);
          updateDiscoveryStateSuccess(company);
          return new DiscoveryOutcome(countsByType, null, cooldownSkips.get(), false, false);
        }
      }
    }

    DiscoveryFailure failure = selectFailure(failures);
    if (failure != null) {
      repository.insertCareersDiscoveryFailure(
          company.companyId(),
          failure.reasonCode(),
          failure.candidateUrl(),
          failure.detail(),
          Instant.now());
      updateDiscoveryState(company, failure);
    }
    return new DiscoveryOutcome(countsByType, failure, cooldownSkips.get(), false, false);
  }

  private List<String> discoverLinksFromHomepage(
      List<String> homepageUrls, AtomicInteger cooldownSkips) {
    List<String> out = new ArrayList<>();
    for (String homepage : homepageUrls) {
      if (homepage == null || homepage.isBlank()) {
        continue;
      }
      if (isHostCoolingDown(homepage)) {
        if (cooldownSkips != null) {
          cooldownSkips.incrementAndGet();
        }
        continue;
      }
      if (!robotsTxtService.isAllowed(homepage)) {
        recordCooldownFailure(homepage, ReasonCodeClassifier.ROBOTS_BLOCKED);
        continue;
      }

      HttpFetchResult fetch = httpClient.get(homepage, HTML_ACCEPT, MAX_HOMEPAGE_BYTES);
      if ("body_too_large".equals(fetch.errorCode())) {
        recordCooldownSuccess(homepage);
        continue;
      }
      if (!fetch.isSuccessful() || fetch.body() == null) {
        recordCooldownFromFetch(homepage, fetch);
        continue;
      }
      recordCooldownSuccess(homepage);

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

  private void scanForAtsLinks(
      CompanyTarget company,
      LinkedHashSet<String> seen,
      Map<AtsType, Integer> countsByType,
      AtomicInteger cooldownSkips) {
    if (company == null || company.domain() == null || company.domain().isBlank()) {
      return;
    }
    List<String> hosts = new ArrayList<>();
    hosts.add(company.domain());
    if (!company.domain().startsWith("www.")) {
      hosts.add("www." + company.domain());
    }

    List<String> pages = new ArrayList<>();
    for (String host : hosts) {
      for (String path : LINK_SCAN_PATHS) {
        pages.add("https://" + host + path);
      }
    }

    int pagesScanned = 0;
    int endpointsFound = 0;
    for (String page : pages) {
      if (pagesScanned >= MAX_LINK_SCAN_PAGES || endpointsFound >= MAX_LINK_SCAN_ENDPOINTS) {
        break;
      }
      pagesScanned++;
      if (isHostCoolingDown(page)) {
        if (cooldownSkips != null) {
          cooldownSkips.incrementAndGet();
        }
        continue;
      }
      if (!robotsTxtService.isAllowed(page)) {
        recordCooldownFailure(page, ReasonCodeClassifier.ROBOTS_BLOCKED);
        continue;
      }
      HttpFetchResult fetch = httpClient.get(page, HTML_ACCEPT);
      if (!fetch.isSuccessful() || fetch.body() == null) {
        recordCooldownFromFetch(page, fetch);
        continue;
      }
      recordCooldownSuccess(page);
      Document doc = Jsoup.parse(fetch.body(), page);
      for (Element anchor : doc.select("a[href]")) {
        if (endpointsFound >= MAX_LINK_SCAN_ENDPOINTS) {
          break;
        }
        String href = anchor.attr("abs:href");
        if (href == null || href.isBlank()) {
          continue;
        }
        if (!isAtsLink(href)) {
          continue;
        }
        List<AtsDetectionRecord> endpoints = atsEndpointExtractor.extract(href, null);
        if (endpoints.isEmpty()) {
          continue;
        }
        registerEndpoints(company, page, endpoints, "link", 0.85, true, seen, countsByType);
        endpointsFound += endpoints.size();
      }
    }
  }

  private boolean isAtsLink(String href) {
    String lower = href.toLowerCase(Locale.ROOT);
    return lower.contains("myworkdayjobs.com")
        || lower.contains("boards.greenhouse.io")
        || lower.contains("job-boards.greenhouse.io")
        || lower.contains("jobs.lever.co");
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

  private VendorProbeLimiter vendorProbeLimiter(int maxPerHost) {
    if (maxPerHost <= 0) {
      return null;
    }
    Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    return host -> {
      if (host == null || host.isBlank()) {
        return false;
      }
      AtomicInteger counter = counters.computeIfAbsent(host, ignored -> new AtomicInteger(0));
      return counter.incrementAndGet() <= maxPerHost;
    };
  }

  private LinkedHashSet<String> buildCandidates(
      CompanyTarget company, AtomicInteger cooldownSkips) {
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
    candidates.addAll(discoverLinksFromHomepage(homepageUrls, cooldownSkips));
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
      if (lower.startsWith("utm_")
          || lower.equals("ref")
          || lower.equals("source")
          || lower.equals("gh_src")
          || lower.equals("lever-source")) {
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
      Map<AtsDetectionRecord, String> endpointsWithSource,
      String detectionMethod,
      double confidence,
      boolean verified,
      LinkedHashSet<String> seen,
      Map<AtsType, Integer> countsByType) {
    if (endpointsWithSource == null || endpointsWithSource.isEmpty()) {
      return;
    }
    for (Map.Entry<AtsDetectionRecord, String> entry : endpointsWithSource.entrySet()) {
      registerEndpoint(
          company,
          entry.getKey(),
          entry.getValue(),
          detectionMethod,
          confidence,
          verified,
          seen,
          countsByType);
    }
  }

  private void registerEndpoints(
      CompanyTarget company,
      String discoveredFromUrl,
      List<AtsDetectionRecord> endpoints,
      String detectionMethod,
      double confidence,
      boolean verified,
      LinkedHashSet<String> seen,
      Map<AtsType, Integer> countsByType) {
    if (endpoints == null || endpoints.isEmpty()) {
      return;
    }
    for (AtsDetectionRecord endpoint : endpoints) {
      registerEndpoint(
          company,
          endpoint,
          discoveredFromUrl,
          detectionMethod,
          confidence,
          verified,
          seen,
          countsByType);
    }
  }

  private void registerEndpoint(
      CompanyTarget company,
      AtsDetectionRecord endpoint,
      String discoveredFromUrl,
      String detectionMethod,
      double confidence,
      boolean verified,
      LinkedHashSet<String> seen,
      Map<AtsType, Integer> countsByType) {
    String endpointUrl = endpoint.atsUrl();
    if (endpointUrl == null || endpointUrl.isBlank()) {
      return;
    }
    String key = endpoint.atsType().name() + "|" + endpointUrl.toLowerCase(Locale.ROOT);
    if (seen.contains(key)) {
      return;
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
        verified);
    countsByType.put(endpoint.atsType(), countsByType.getOrDefault(endpoint.atsType(), 0) + 1);
  }

  private HttpFetchResult fetchHomepage(String homepageUrl) {
    CanaryHttpBudget budget =
        new CanaryHttpBudget(
            2,
            4,
            1.0,
            1,
            3,
            1,
            HOMEPAGE_TIMEOUT_SECONDS,
            Duration.ofSeconds(HOMEPAGE_TIMEOUT_SECONDS * 2L));
    try (CanaryHttpBudgetContext.Scope scope = CanaryHttpBudgetContext.activate(budget)) {
      return httpClient.get(homepageUrl, HTML_ACCEPT, MAX_HOMEPAGE_BYTES);
    }
  }

  private List<String> buildSlugCandidates(CompanyTarget company) {
    LinkedHashSet<String> slugs = new LinkedHashSet<>();
    if (company != null && company.domain() != null) {
      String root = domainRoot(company.domain());
      if (root != null) {
        slugs.add(root);
      }
    }
    if (company != null && company.name() != null) {
      slugs.addAll(nameSlugCandidates(company.name()));
    }
    if (company != null && company.ticker() != null && !company.ticker().isBlank()) {
      slugs.add(company.ticker().trim().toLowerCase(Locale.ROOT));
    }
    return new ArrayList<>(slugs);
  }

  private AtsDetectionRecord probeVendorSlug(
      String slug,
      DiscoveryMetrics metrics,
      VendorProbeLimiter vendorProbeLimiter,
      List<DiscoveryFailure> failures) {
    if (slug == null || slug.isBlank()) {
      return null;
    }
    AtsDetectionRecord greenhouse =
        probeVendor(
            "https://boards.greenhouse.io/" + slug,
            AtsType.GREENHOUSE,
            "boards.greenhouse.io",
            this::isGreenhouseSignature,
            metrics,
            vendorProbeLimiter,
            failures);
    if (greenhouse != null) {
      return greenhouse;
    }
    AtsDetectionRecord lever =
        probeVendor(
            "https://jobs.lever.co/" + slug,
            AtsType.LEVER,
            "jobs.lever.co",
            this::isLeverSignature,
            metrics,
            vendorProbeLimiter,
            failures);
    if (lever != null) {
      return lever;
    }
    return probeVendor(
        "https://jobs.smartrecruiters.com/" + slug,
        AtsType.SMARTRECRUITERS,
        "jobs.smartrecruiters.com",
        this::isSmartRecruitersSignature,
        metrics,
        vendorProbeLimiter,
        failures);
  }

  private SitemapDetectionOutcome discoverAtsFromSitemaps(
      CompanyTarget company, DiscoveryMetrics metrics, List<DiscoveryFailure> failures) {
    if (company == null || company.domain() == null || company.domain().isBlank()) {
      return null;
    }
    String domain = company.domain().trim().toLowerCase(Locale.ROOT);
    RobotsRules rules = robotsTxtService.getRulesForHost(domain);
    List<String> seeds = new ArrayList<>(rules.getSitemapUrls());
    if (seeds.isEmpty()) {
      seeds.add("https://" + domain + "/sitemap.xml");
      if (!domain.startsWith("www.")) {
        seeds.add("https://www." + domain + "/sitemap.xml");
      }
    }
    int maxUrls =
        Math.max(
            1, Math.min(MAX_SITEMAP_URLS_DISCOVERY, properties.getSitemap().getMaxUrlsPerDomain()));
    SitemapDiscoveryResult result =
        sitemapService.discover(
            seeds,
            properties.getSitemap().getMaxDepth(),
            properties.getSitemap().getMaxSitemaps(),
            maxUrls);

    if (metrics != null) {
      metrics.incrementSitemapsScanned(result.fetchedSitemaps().size());
      metrics.incrementSitemapUrlsChecked(result.discoveredUrls().size());
    }

    List<String> urls = result.discoveredUrls().stream().map(SitemapUrlEntry::url).toList();
    Map<AtsDetectionRecord, String> endpoints = sitemapDetector.detect(urls);
    if (!endpoints.isEmpty()) {
      return new SitemapDetectionOutcome(endpoints);
    }

    if (result.errors().containsKey("blocked_by_robots")) {
      DiscoveryFailure failure =
          new DiscoveryFailure(
              "discovery_sitemap_blocked_by_robots",
              seeds.isEmpty() ? null : seeds.get(0),
              "blocked_by_robots");
      failures.add(failure);
      if (metrics != null) {
        metrics.incrementRobotsBlocked();
      }
      for (String seed : seeds) {
        recordCooldownFailure(seed, ReasonCodeClassifier.ROBOTS_BLOCKED);
      }
    } else if (!result.errors().isEmpty()) {
      String detail = result.errors().keySet().iterator().next();
      failures.add(
          new DiscoveryFailure(
              "discovery_sitemap_fetch_failed", seeds.isEmpty() ? null : seeds.get(0), detail));
    } else if (result.discoveredUrls().isEmpty()) {
      failures.add(
          new DiscoveryFailure(
              "discovery_sitemap_no_urls", seeds.isEmpty() ? null : seeds.get(0), null));
    }

    return new SitemapDetectionOutcome(Map.of());
  }

  private AtsDetectionRecord probeVendor(
      String url,
      AtsType atsType,
      String host,
      Predicate<String> signatureCheck,
      DiscoveryMetrics metrics,
      VendorProbeLimiter vendorProbeLimiter,
      List<DiscoveryFailure> failures) {
    if (vendorProbeLimiter != null && !vendorProbeLimiter.tryAcquire(host)) {
      return null;
    }
    HttpFetchResult fetch = httpClient.get(url, HTML_ACCEPT);
    if (!fetch.isSuccessful() || fetch.body() == null) {
      if (metrics != null) {
        metrics.incrementFetchFailed();
      }
      if (failures != null) {
        failures.add(failureFromFetch("discovery_fetch_failed", url, fetch));
      }
      recordCooldownFromFetch(url, fetch);
      return null;
    }
    recordCooldownSuccess(url);
    String body = fetch.body();
    if (!signatureCheck.test(body)) {
      return null;
    }
    for (AtsDetectionRecord record :
        atsEndpointExtractor.extract(fetch.finalUrlOrRequested(), body)) {
      if (record.atsType() == atsType) {
        return record;
      }
    }
    return null;
  }

  private boolean isGreenhouseSignature(String body) {
    String lower = body == null ? "" : body.toLowerCase(Locale.ROOT);
    return lower.contains("greenhouse") || lower.contains("boards.greenhouse.io");
  }

  private boolean isLeverSignature(String body) {
    String lower = body == null ? "" : body.toLowerCase(Locale.ROOT);
    return lower.contains("lever") && lower.contains("jobs.lever.co");
  }

  private boolean isSmartRecruitersSignature(String body) {
    String lower = body == null ? "" : body.toLowerCase(Locale.ROOT);
    return lower.contains("smartrecruiters");
  }

  private String domainRoot(String domain) {
    if (domain == null || domain.isBlank()) {
      return null;
    }
    String host = domain.trim().toLowerCase(Locale.ROOT);
    if (!host.startsWith("http://") && !host.startsWith("https://")) {
      host = "https://" + host;
    }
    try {
      java.net.URI uri = new java.net.URI(host);
      String parsedHost = uri.getHost();
      if (parsedHost == null || parsedHost.isBlank()) {
        return null;
      }
      String normalized = parsedHost.toLowerCase(Locale.ROOT);
      if (normalized.startsWith("www.")) {
        normalized = normalized.substring(4);
      }
      int dot = normalized.indexOf('.');
      String root = dot > 0 ? normalized.substring(0, dot) : normalized;
      return normalizeSlugToken(root);
    } catch (Exception e) {
      return null;
    }
  }

  private List<String> nameSlugCandidates(String name) {
    if (name == null || name.isBlank()) {
      return List.of();
    }
    String normalized = name.toLowerCase(Locale.ROOT).replace("&", "and");
    normalized = normalized.replaceAll("[^a-z0-9]+", " ").trim();
    if (normalized.isBlank()) {
      return List.of();
    }
    String compact = normalized.replace(" ", "");
    String hyphenated = normalized.replace(" ", "-");
    LinkedHashSet<String> candidates = new LinkedHashSet<>();
    if (!compact.isBlank()) {
      candidates.add(compact);
    }
    if (!hyphenated.isBlank()) {
      candidates.add(hyphenated);
    }
    return new ArrayList<>(candidates);
  }

  private String normalizeSlugToken(String token) {
    if (token == null) {
      return null;
    }
    String cleaned = token.trim().toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9-]", "");
    return cleaned.isBlank() ? null : cleaned;
  }

  private DiscoveryFailure failureFromFetch(
      String reasonCode, String candidate, HttpFetchResult fetch) {
    if (fetch == null) {
      return new DiscoveryFailure(reasonCode, candidate, null);
    }
    String detail = fetch.errorCode() == null ? "http_" + fetch.statusCode() : fetch.errorCode();
    String reason =
        detail != null && detail.contains("host_cooldown") ? "discovery_host_cooldown" : reasonCode;
    return new DiscoveryFailure(reason, candidate, detail);
  }

  private boolean isCompanyCoolingDown(long companyId) {
    CareersDiscoveryState state = repository.findCareersDiscoveryState(companyId);
    if (state == null || state.nextAttemptAt() == null) {
      return false;
    }
    return state.nextAttemptAt().isAfter(Instant.now());
  }

  private void updateDiscoveryStateSuccess(CompanyTarget company) {
    if (company == null) {
      return;
    }
    repository.upsertCareersDiscoveryState(company.companyId(), Instant.now(), null, null, 0, null);
  }

  private void updateDiscoveryState(CompanyTarget company, DiscoveryFailure failure) {
    if (company == null || failure == null) {
      return;
    }
    String reason = failure.reasonCode();
    if ("discovery_company_cooldown".equals(reason) || "discovery_no_domain".equals(reason)) {
      return;
    }
    CareersDiscoveryState existing = repository.findCareersDiscoveryState(company.companyId());
    int failures = existing == null ? 0 : existing.consecutiveFailures();
    int nextFailures = failures + 1;
    Instant now = Instant.now();
    Instant nextAttemptAt = null;
    if (isRobotsBlockedReason(reason)) {
      int days = properties.getCareersDiscovery().getRobotsCooldownDays();
      nextAttemptAt = now.plus(Duration.ofDays(Math.max(1, days)));
    } else if (isTransientFailure(failure)) {
      Duration backoff = backoffForFailures(nextFailures);
      if (backoff != null) {
        nextAttemptAt = now.plus(backoff);
      }
    }

    repository.upsertCareersDiscoveryState(
        company.companyId(), now, reason, failure.candidateUrl(), nextFailures, nextAttemptAt);
  }

  private boolean isRobotsBlockedReason(String reason) {
    return "discovery_blocked_by_robots".equals(reason)
        || "discovery_homepage_blocked_by_robots".equals(reason)
        || "discovery_sitemap_blocked_by_robots".equals(reason)
        || "discovery_careers_blocked_by_robots".equals(reason);
  }

  private boolean isTransientFailure(DiscoveryFailure failure) {
    if (failure == null) {
      return false;
    }
    String reason = failure.reasonCode();
    if ("discovery_host_cooldown".equals(reason)) {
      return true;
    }
    if (!"discovery_fetch_failed".equals(reason)) {
      return false;
    }
    String detail = failure.detail() == null ? "" : failure.detail().toLowerCase(Locale.ROOT);
    return detail.contains("timeout")
        || detail.contains("http_5")
        || detail.contains("http_429")
        || detail.contains("io_error");
  }

  private Duration backoffForFailures(int failures) {
    List<Integer> steps = properties.getCareersDiscovery().getFailureBackoffMinutes();
    if (steps.isEmpty()) {
      return null;
    }
    int index = Math.max(0, Math.min(failures, steps.size()) - 1);
    int minutes = steps.get(index);
    return Duration.ofMinutes(Math.max(1, minutes));
  }

  private boolean deadlineExceeded(Instant deadline) {
    return deadline != null && Instant.now().isAfter(deadline);
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
        recordCooldownFailure(link, ReasonCodeClassifier.ROBOTS_BLOCKED);
        continue;
      }
      HttpFetchResult fetch = httpClient.get(link, HTML_ACCEPT);
      if (!fetch.isSuccessful()) {
        recordCooldownFromFetch(link, fetch);
      } else {
        recordCooldownSuccess(link);
      }
      resolved.addAll(atsEndpointExtractor.extract(fetch.finalUrlOrRequested(), fetch.body()));
      if (!resolved.isEmpty()) {
        break;
      }
    }
    return resolved;
  }

  private boolean isHostCoolingDown(String url) {
    if (hostCrawlStateService == null) {
      return false;
    }
    String host = hostFromUrl(url);
    if (host == null) {
      return false;
    }
    return hostCrawlStateService.nextAllowedAt(host) != null;
  }

  private void recordCooldownFailure(String url, String category) {
    if (hostCrawlStateService == null) {
      return;
    }
    String host = hostFromUrl(url);
    if (host == null) {
      return;
    }
    hostCrawlStateService.recordFailure(host, category);
  }

  private void recordCooldownFromFetch(String url, HttpFetchResult fetch) {
    if (hostCrawlStateService == null) {
      return;
    }
    String host = hostFromUrl(url);
    if (host == null || fetch == null) {
      return;
    }
    if (fetch.isSuccessful()) {
      hostCrawlStateService.recordSuccess(host);
      return;
    }
    if (fetch.statusCode() == 429) {
      hostCrawlStateService.recordFailure(host, ReasonCodeClassifier.HTTP_429_RATE_LIMIT);
      return;
    }
    if (fetch.statusCode() == 408
        || (fetch.errorCode() != null
            && fetch.errorCode().toLowerCase(Locale.ROOT).contains("timeout"))) {
      hostCrawlStateService.recordFailure(host, ReasonCodeClassifier.TIMEOUT);
    }
  }

  private void recordCooldownSuccess(String url) {
    if (hostCrawlStateService == null) {
      return;
    }
    String host = hostFromUrl(url);
    if (host == null) {
      return;
    }
    hostCrawlStateService.recordSuccess(host);
  }

  private String hostFromUrl(String url) {
    if (url == null || url.isBlank()) {
      return null;
    }
    try {
      java.net.URI uri = new java.net.URI(url);
      return uri.getHost();
    } catch (Exception e) {
      return null;
    }
  }

  private DiscoveryFailure selectFailure(List<DiscoveryFailure> failures) {
    if (failures == null || failures.isEmpty()) {
      return new DiscoveryFailure("discovery_no_match", null, null);
    }
    for (DiscoveryFailure failure : failures) {
      if ("discovery_time_budget_exceeded".equals(failure.reasonCode())) {
        return failure;
      }
    }
    for (DiscoveryFailure failure : failures) {
      String reason = failure.reasonCode();
      if ("discovery_homepage_blocked_by_robots".equals(reason)
          || "discovery_sitemap_blocked_by_robots".equals(reason)
          || "discovery_careers_blocked_by_robots".equals(reason)
          || "discovery_blocked_by_robots".equals(reason)) {
        return failure;
      }
    }
    for (DiscoveryFailure failure : failures) {
      if ("discovery_host_cooldown".equals(failure.reasonCode())) {
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

  record SitemapDetectionOutcome(Map<AtsDetectionRecord, String> endpoints) {
    boolean hasEndpoints() {
      return endpoints != null && !endpoints.isEmpty();
    }
  }

  record DiscoveryOutcome(
      Map<AtsType, Integer> countsByType,
      DiscoveryFailure failure,
      int cooldownSkips,
      boolean skipped,
      boolean timeBudgetExceeded) {
    boolean hasEndpoints() {
      return countsByType != null && !countsByType.isEmpty();
    }

    DiscoveryFailure primaryFailure() {
      return failure;
    }
  }

  static final class DiscoveryMetrics {
    private int homepageScanned;
    private int careersPathsChecked;
    private int robotsBlockedCount;
    private int fetchFailedCount;
    private int timeBudgetExceededCount;
    private int sitemapsScanned;
    private int sitemapUrlsChecked;
    private final Map<AtsType, Integer> endpointsFoundHomepage = new LinkedHashMap<>();
    private final Map<AtsType, Integer> endpointsFoundVendorProbe = new LinkedHashMap<>();
    private final Map<AtsType, Integer> endpointsFoundSitemap = new LinkedHashMap<>();

    void incrementHomepageScanned() {
      homepageScanned++;
    }

    void incrementCareersPathsChecked() {
      careersPathsChecked++;
    }

    void incrementRobotsBlocked() {
      robotsBlockedCount++;
    }

    void incrementFetchFailed() {
      fetchFailedCount++;
    }

    void incrementTimeBudgetExceeded() {
      timeBudgetExceededCount++;
    }

    void incrementHomepageFound(Iterable<AtsDetectionRecord> endpoints) {
      for (AtsDetectionRecord record : endpoints) {
        endpointsFoundHomepage.put(
            record.atsType(), endpointsFoundHomepage.getOrDefault(record.atsType(), 0) + 1);
      }
    }

    void incrementSitemapsScanned(int delta) {
      sitemapsScanned += Math.max(0, delta);
    }

    void incrementSitemapUrlsChecked(int delta) {
      sitemapUrlsChecked += Math.max(0, delta);
    }

    void incrementSitemapFound(Iterable<AtsDetectionRecord> endpoints) {
      for (AtsDetectionRecord record : endpoints) {
        endpointsFoundSitemap.put(
            record.atsType(), endpointsFoundSitemap.getOrDefault(record.atsType(), 0) + 1);
      }
    }

    void incrementVendorFound(AtsDetectionRecord record) {
      if (record == null) {
        return;
      }
      endpointsFoundVendorProbe.put(
          record.atsType(), endpointsFoundVendorProbe.getOrDefault(record.atsType(), 0) + 1);
    }

    int homepageScanned() {
      return homepageScanned;
    }

    int careersPathsChecked() {
      return careersPathsChecked;
    }

    int robotsBlockedCount() {
      return robotsBlockedCount;
    }

    int fetchFailedCount() {
      return fetchFailedCount;
    }

    int timeBudgetExceededCount() {
      return timeBudgetExceededCount;
    }

    Map<AtsType, Integer> endpointsFoundHomepage() {
      return endpointsFoundHomepage;
    }

    Map<AtsType, Integer> endpointsFoundVendorProbe() {
      return endpointsFoundVendorProbe;
    }

    int sitemapsScanned() {
      return sitemapsScanned;
    }

    int sitemapUrlsChecked() {
      return sitemapUrlsChecked;
    }

    Map<AtsType, Integer> endpointsFoundSitemap() {
      return endpointsFoundSitemap;
    }
  }

  interface VendorProbeLimiter {
    boolean tryAcquire(String host);
  }

  record DiscoveryFailure(String reasonCode, String candidateUrl, String detail) {}
}
