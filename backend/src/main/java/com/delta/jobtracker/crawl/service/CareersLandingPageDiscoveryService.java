package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Service;

@Service
public class CareersLandingPageDiscoveryService {
  private static final String HTML_ACCEPT = "text/html,application/xhtml+xml";
  private static final int MAX_BYTES = 512 * 1024;
  private static final int MAX_REDIRECT_HOPS = 5; // best-effort, inferred from initial->final only

  private final PoliteHttpClient httpClient;
  private final RobotsTxtService robotsTxtService;
  private final CareersLandingLinkExtractor linkExtractor;
  private final AtsDetector atsDetector;

  public CareersLandingPageDiscoveryService(
      PoliteHttpClient httpClient,
      RobotsTxtService robotsTxtService,
      CareersLandingLinkExtractor linkExtractor,
      AtsDetector atsDetector) {
    this.httpClient = httpClient;
    this.robotsTxtService = robotsTxtService;
    this.linkExtractor = linkExtractor;
    this.atsDetector = atsDetector;
  }

  public DiscoveryResult discover(String domain) {
    return discover(domain, 10, 5);
  }

  public DiscoveryResult discover(String domain, int requestBudget, int topHomepageCandidates) {
    if (domain == null || domain.isBlank()) {
      return DiscoveryResult.failure(
          FailureReason.NETWORK_ERROR, null, null, null, 0, 0, null, null);
    }
    String normalizedDomain = domain.trim().toLowerCase(Locale.ROOT);
    int budget = Math.max(1, requestBudget);
    int requests = 0;
    List<String> redirects = new ArrayList<>();
    List<ProbeRecord> probes = new ArrayList<>();
    String homepageUrl = "https://" + normalizedDomain + "/";

    if (!robotsTxtService.isAllowed(homepageUrl)) {
      return DiscoveryResult.failure(
          FailureReason.BOT_PROTECTION_403,
          homepageUrl,
          null,
          Method.HOMEPAGE_LINK,
          0,
          403,
          redirects,
          probes);
    }

    requests++;
    HttpFetchResult homepage = httpClient.get(homepageUrl, HTML_ACCEPT, MAX_BYTES);
    probes.add(ProbeRecord.fromFetch(Method.HOMEPAGE_LINK, homepageUrl, homepage));
    if (isBlocked(homepage, 403)) {
      return DiscoveryResult.failure(
          FailureReason.BOT_PROTECTION_403,
          homepageUrl,
          finalUrl(homepage),
          Method.HOMEPAGE_LINK,
          requests,
          403,
          redirectChain(homepageUrl, homepage),
          probes);
    }
    if (isBlocked(homepage, 429)) {
      return DiscoveryResult.failure(
          FailureReason.RATE_LIMIT_429,
          homepageUrl,
          finalUrl(homepage),
          Method.HOMEPAGE_LINK,
          requests,
          429,
          redirectChain(homepageUrl, homepage),
          probes);
    }
    if (hasNetworkLikeError(homepage)) {
      return DiscoveryResult.failure(
          mapNetworkFailure(homepage),
          homepageUrl,
          finalUrl(homepage),
          Method.HOMEPAGE_LINK,
          requests,
          null,
          redirectChain(homepageUrl, homepage),
          probes);
    }

    List<String> homepageCandidates = List.of();
    if (homepage.isSuccessful() && homepage.body() != null) {
      String homeFinal = finalUrl(homepage) == null ? homepageUrl : finalUrl(homepage);
      homepageCandidates =
          linkExtractor.extractRanked(
              homepage.body(), homeFinal, Math.max(1, topHomepageCandidates));
      AtsType vendorFromHome = atsDetector.detect(homeFinal, homepage.body());
      if (vendorFromHome != null && vendorFromHome != AtsType.UNKNOWN) {
        return DiscoveryResult.success(
            homeFinal,
            homeFinal,
            Method.HOMEPAGE_LINK,
            requests,
            homepage.statusCode(),
            redirectChain(homepageUrl, homepage),
            vendorFromHome.name(),
            probes,
            homepage.body());
      }
    }

    for (String candidate : homepageCandidates) {
      if (requests >= budget) {
        break;
      }
      ProbeAttemptResult attempted =
          attemptCandidate(candidate, Method.HOMEPAGE_LINK, requests, probes);
      requests = attempted.requestsUsed();
      if (attempted.result() != null) {
        return attempted.result();
      }
    }

    if (!homepageCandidates.isEmpty()) {
      // We had candidates but none worked; continue to path/subdomain fallbacks, but preserve stage
      // if all fail.
    }

    FailureReason pathFailure = null;
    FailureReason subdomainFailure = null;
    for (String candidate : linkExtractor.defaultFallbackCandidates(normalizedDomain)) {
      if (requests >= budget) {
        break;
      }
      Method method =
          candidate.contains("://careers.") || candidate.contains("://jobs.")
              ? Method.SUBDOMAIN_GUESS
              : Method.PATH_GUESS;
      ProbeAttemptResult attempted = attemptCandidate(candidate, method, requests, probes);
      requests = attempted.requestsUsed();
      if (attempted.result() != null) {
        return attempted.result();
      }
      if (attempted.failureReason() != null) {
        if (method == Method.SUBDOMAIN_GUESS) {
          subdomainFailure = attempted.failureReason();
        } else {
          pathFailure = attempted.failureReason();
        }
      }
    }

    FailureReason finalFailure;
    Method failureMethod = null;
    if (homepageCandidates.isEmpty()) {
      finalFailure = FailureReason.NO_CAREERS_LINK_ON_HOMEPAGE;
      failureMethod = Method.HOMEPAGE_LINK;
    } else if (pathFailure == FailureReason.ALL_COMMON_PATHS_404) {
      finalFailure = pathFailure;
      failureMethod = Method.PATH_GUESS;
    } else if (subdomainFailure == FailureReason.SUBDOMAIN_PROBES_404) {
      finalFailure = subdomainFailure;
      failureMethod = Method.SUBDOMAIN_GUESS;
    } else {
      finalFailure =
          pathFailure != null
              ? pathFailure
              : (subdomainFailure != null ? subdomainFailure : FailureReason.ALL_COMMON_PATHS_404);
      failureMethod = pathFailure != null ? Method.PATH_GUESS : Method.SUBDOMAIN_GUESS;
    }

    return DiscoveryResult.failure(
        finalFailure,
        homepageUrl,
        finalUrl(homepage),
        failureMethod,
        requests,
        null,
        redirectChain(homepageUrl, homepage),
        probes);
  }

  private ProbeAttemptResult attemptCandidate(
      String candidate, Method method, int requestsUsed, List<ProbeRecord> probes) {
    if (!robotsTxtService.isAllowed(candidate)) {
      return new ProbeAttemptResult(
          null,
          requestsUsed,
          FailureReason.BOT_PROTECTION_403,
          method == Method.SUBDOMAIN_GUESS
              ? FailureReason.SUBDOMAIN_PROBES_404
              : FailureReason.ALL_COMMON_PATHS_404);
    }
    int nextRequests = requestsUsed + 1;
    HttpFetchResult fetch = httpClient.get(candidate, HTML_ACCEPT, MAX_BYTES);
    probes.add(ProbeRecord.fromFetch(method, candidate, fetch));
    String finalUrl = finalUrl(fetch);
    List<String> chain = redirectChain(candidate, fetch);
    if (chain.size() > MAX_REDIRECT_HOPS + 1) {
      return new ProbeAttemptResult(
          DiscoveryResult.failure(
              FailureReason.REDIRECT_CHAIN_TOO_LONG,
              candidate,
              finalUrl,
              method,
              nextRequests,
              fetch == null ? null : fetch.statusCode(),
              chain,
              probes,
              fetch == null ? null : fetch.body()),
          nextRequests,
          FailureReason.REDIRECT_CHAIN_TOO_LONG,
          null);
    }
    if (isBlocked(fetch, 403)) {
      return new ProbeAttemptResult(
          DiscoveryResult.failure(
              FailureReason.BOT_PROTECTION_403,
              candidate,
              finalUrl,
              method,
              nextRequests,
              403,
              chain,
              probes,
              fetch.body()),
          nextRequests,
          FailureReason.BOT_PROTECTION_403,
          null);
    }
    if (isBlocked(fetch, 429)) {
      return new ProbeAttemptResult(
          DiscoveryResult.failure(
              FailureReason.RATE_LIMIT_429,
              candidate,
              finalUrl,
              method,
              nextRequests,
              429,
              chain,
              probes,
              fetch.body()),
          nextRequests,
          FailureReason.RATE_LIMIT_429,
          null);
    }
    if (hasNetworkLikeError(fetch)) {
      return new ProbeAttemptResult(
          DiscoveryResult.failure(
              mapNetworkFailure(fetch),
              candidate,
              finalUrl,
              method,
              nextRequests,
              null,
              chain,
              probes,
              null),
          nextRequests,
          mapNetworkFailure(fetch),
          null);
    }
    if (fetch == null) {
      return new ProbeAttemptResult(null, nextRequests, null, null);
    }
    AtsType detected = atsDetector.detect(finalUrl, fetch.body());
    boolean vendorUrl = detected != null && detected != AtsType.UNKNOWN;
    if (vendorUrl || fetch.statusCode() == 200) {
      return new ProbeAttemptResult(
          DiscoveryResult.success(
              candidate,
              finalUrl,
              method,
              nextRequests,
              fetch.statusCode(),
              chain,
              vendorUrl ? detected.name() : null,
              probes,
              fetch.body()),
          nextRequests,
          null,
          null);
    }
    if (fetch.statusCode() == 404) {
      FailureReason aggregate =
          method == Method.SUBDOMAIN_GUESS
              ? FailureReason.SUBDOMAIN_PROBES_404
              : FailureReason.ALL_COMMON_PATHS_404;
      return new ProbeAttemptResult(null, nextRequests, aggregate, aggregate);
    }
    return new ProbeAttemptResult(null, nextRequests, null, null);
  }

  private boolean isBlocked(HttpFetchResult fetch, int status) {
    return fetch != null && fetch.statusCode() == status;
  }

  private boolean hasNetworkLikeError(HttpFetchResult fetch) {
    return fetch != null && fetch.errorCode() != null && !fetch.errorCode().isBlank();
  }

  private FailureReason mapNetworkFailure(HttpFetchResult fetch) {
    if (fetch == null) {
      return FailureReason.NETWORK_ERROR;
    }
    String code = fetch.errorCode() == null ? "" : fetch.errorCode().toLowerCase(Locale.ROOT);
    if (code.contains("timeout")) {
      return FailureReason.NETWORK_ERROR;
    }
    if (code.contains("ssl") || code.contains("tls")) {
      return FailureReason.TLS_ERROR;
    }
    if (code.contains("dns") || code.contains("unknownhost")) {
      return FailureReason.DNS_ERROR;
    }
    if (code.contains("invalid_url")) {
      return FailureReason.NETWORK_ERROR;
    }
    return FailureReason.NETWORK_ERROR;
  }

  private String finalUrl(HttpFetchResult fetch) {
    return fetch == null ? null : fetch.finalUrlOrRequested();
  }

  private List<String> redirectChain(String requested, HttpFetchResult fetch) {
    LinkedHashSet<String> out = new LinkedHashSet<>();
    if (requested != null && !requested.isBlank()) {
      out.add(requested);
    }
    if (fetch != null) {
      String finalUrl = fetch.finalUrlOrRequested();
      if (finalUrl != null && !finalUrl.isBlank()) {
        out.add(finalUrl);
      }
    }
    return new ArrayList<>(out);
  }

  public enum Method {
    HOMEPAGE_LINK,
    PATH_GUESS,
    SUBDOMAIN_GUESS
  }

  public enum FailureReason {
    NO_CAREERS_LINK_ON_HOMEPAGE,
    ALL_COMMON_PATHS_404,
    SUBDOMAIN_PROBES_404,
    REDIRECT_CHAIN_TOO_LONG,
    NETWORK_ERROR,
    TLS_ERROR,
    DNS_ERROR,
    BOT_PROTECTION_403,
    RATE_LIMIT_429,
    CAREERS_PAGE_200_NO_VENDOR_SIGNATURE
  }

  public record ProbeRecord(
      Method method, String requestedUrl, String finalUrl, Integer httpStatus, String errorCode) {
    static ProbeRecord fromFetch(Method method, String requestedUrl, HttpFetchResult fetch) {
      return new ProbeRecord(
          method,
          requestedUrl,
          fetch == null ? null : fetch.finalUrlOrRequested(),
          fetch == null ? null : fetch.statusCode(),
          fetch == null ? null : fetch.errorCode());
    }
  }

  public record DiscoveryResult(
      boolean careersUrlFound,
      String careersUrlInitial,
      String careersUrlFinal,
      Method method,
      FailureReason failureReason,
      int requestCount,
      Integer httpStatusFirstFailure,
      List<String> redirectChain,
      String vendorName,
      List<ProbeRecord> probes,
      String responseBody) {
    static DiscoveryResult success(
        String initial,
        String fin,
        Method method,
        int requestCount,
        Integer httpStatus,
        List<String> redirectChain,
        String vendorName,
        List<ProbeRecord> probes,
        String responseBody) {
      return new DiscoveryResult(
          true,
          initial,
          fin,
          method,
          null,
          requestCount,
          httpStatus,
          redirectChain == null ? List.of() : List.copyOf(redirectChain),
          vendorName,
          probes == null ? List.of() : List.copyOf(probes),
          responseBody);
    }

    static DiscoveryResult failure(
        FailureReason reason,
        String initial,
        String fin,
        Method method,
        int requestCount,
        Integer httpStatus,
        List<String> redirectChain,
        List<ProbeRecord> probes) {
      return failure(
          reason, initial, fin, method, requestCount, httpStatus, redirectChain, probes, null);
    }

    static DiscoveryResult failure(
        FailureReason reason,
        String initial,
        String fin,
        Method method,
        int requestCount,
        Integer httpStatus,
        List<String> redirectChain,
        List<ProbeRecord> probes,
        String responseBody) {
      return new DiscoveryResult(
          false,
          initial,
          fin,
          method,
          reason,
          requestCount,
          httpStatus,
          redirectChain == null ? List.of() : List.copyOf(redirectChain),
          null,
          probes == null ? List.of() : List.copyOf(probes),
          responseBody);
    }
  }

  private record ProbeAttemptResult(
      DiscoveryResult result,
      int requestsUsed,
      FailureReason failureReason,
      FailureReason aggregateFailureReason) {}
}
