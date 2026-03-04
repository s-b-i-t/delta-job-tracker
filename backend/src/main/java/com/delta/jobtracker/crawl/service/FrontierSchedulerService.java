package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.FrontierBackoffDecision;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierFetchOutcome;
import com.delta.jobtracker.crawl.model.FrontierHostState;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierSchedulerResult;
import com.delta.jobtracker.crawl.model.FrontierSitemapParseResult;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.FrontierRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.FrontierSitemapParser;
import com.delta.jobtracker.crawl.util.FrontierJobSignalHeuristics;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

@Service
public class FrontierSchedulerService {
  private static final int MAX_SITEMAP_BYTES = 2_000_000;

  private final FrontierRepository frontierRepository;
  private final PoliteHttpClient httpClient;
  private final FrontierBackoffPolicy backoffPolicy;
  private final FrontierSitemapParser sitemapParser;
  private final FrontierJobSignalHeuristics jobSignalHeuristics;
  private final RobotsTxtService robotsTxtService;
  private final CrawlerProperties properties;

  public FrontierSchedulerService(
      FrontierRepository frontierRepository,
      PoliteHttpClient httpClient,
      FrontierBackoffPolicy backoffPolicy,
      FrontierSitemapParser sitemapParser,
      FrontierJobSignalHeuristics jobSignalHeuristics,
      RobotsTxtService robotsTxtService,
      CrawlerProperties properties) {
    this.frontierRepository = frontierRepository;
    this.httpClient = httpClient;
    this.backoffPolicy = backoffPolicy;
    this.sitemapParser = sitemapParser;
    this.jobSignalHeuristics = jobSignalHeuristics;
    this.robotsTxtService = robotsTxtService;
    this.properties = properties;
  }

  public FrontierSchedulerResult fetchDueSitemaps(int maxFetches) {
    int safeMaxFetches = Math.max(1, maxFetches);
    int urlsFetched = 0;
    int blockedByBackoff = 0;
    int urlsEnqueued = 0;
    int sitemapUrlsEnqueued = 0;
    int candidateUrlsEnqueued = 0;
    int httpRequestCount = 0;
    int http429Count = 0;
    Map<String, Integer> statusBucketCounts = new LinkedHashMap<>();

    while (urlsFetched < safeMaxFetches) {
      FrontierQueueUrl claimed =
          frontierRepository.claimNextDueUrl(
              "frontier-seeder",
              properties.getFrontier().getUrlLeaseSeconds(),
              FrontierUrlKind.SITEMAP);
      if (claimed == null) {
        blockedByBackoff +=
            frontierRepository.countDueUrlsBlockedByBackoff(FrontierUrlKind.SITEMAP);
        break;
      }

      urlsFetched++;
      FrontierProcessingResult processingResult = processClaimedSitemap(claimed);
      frontierRepository.completeFetch(claimed, processingResult.outcome());

      urlsEnqueued += processingResult.urlsEnqueued();
      sitemapUrlsEnqueued += processingResult.sitemapUrlsEnqueued();
      candidateUrlsEnqueued += processingResult.candidateUrlsEnqueued();
      httpRequestCount += processingResult.httpRequestCount();
      http429Count += processingResult.http429Count();
      increment(statusBucketCounts, processingResult.outcome().hostStatusBucket());
    }

    return new FrontierSchedulerResult(
        urlsFetched,
        blockedByBackoff,
        urlsEnqueued,
        sitemapUrlsEnqueued,
        candidateUrlsEnqueued,
        httpRequestCount,
        http429Count,
        statusBucketCounts);
  }

  private FrontierProcessingResult processClaimedSitemap(FrontierQueueUrl claimed) {
    FrontierHostState hostState = frontierRepository.findHostState(claimed.host());
    int currentBackoffState = hostState == null ? 0 : hostState.backoffState();
    Instant now = Instant.now();

    int urlsEnqueued = 0;
    int sitemapUrlsEnqueued = 0;
    int candidateUrlsEnqueued = 0;
    int httpRequestCount = 0;
    int http429Count = 0;

    try {
      if (properties.getFrontier().isRespectRobotsForSitemaps()
          && !robotsTxtService.isAllowed(claimed.url())) {
        FrontierFetchOutcome outcome =
            new FrontierFetchOutcome(
                "BLOCKED",
                now,
                null,
                0L,
                "blocked_by_robots",
                "blocked_by_robots",
                "ROBOTS_BLOCKED",
                now.plusMillis(properties.getPerHostDelayMs()),
                Math.max(0, currentBackoffState));
        return new FrontierProcessingResult(
            outcome,
            urlsEnqueued,
            sitemapUrlsEnqueued,
            candidateUrlsEnqueued,
            httpRequestCount,
            http429Count);
      }

      HttpFetchResult fetch =
          httpClient.get(
              claimed.url(), "application/xml,text/xml;q=0.9,*/*;q=0.1", MAX_SITEMAP_BYTES);
      httpRequestCount++;
      if (fetch.statusCode() == 429) {
        http429Count++;
      }

      String errorBucket = fetchErrorBucket(fetch);
      FrontierBackoffDecision decision =
          backoffPolicy.onResult(currentBackoffState, fetch.statusCode(), fetch.errorCode());

      if (!fetch.isSuccessful()) {
        FrontierFetchOutcome outcome =
            new FrontierFetchOutcome(
                "FAILED",
                fetch.fetchedAt() == null ? now : fetch.fetchedAt(),
                fetch.statusCode() > 0 ? fetch.statusCode() : null,
                fetch.duration() == null ? null : fetch.duration().toMillis(),
                errorBucket,
                fetch.errorCode() == null ? "status=" + fetch.statusCode() : fetch.errorCode(),
                decision.statusBucket(),
                decision.nextAllowedAt(),
                decision.nextBackoffState());
        return new FrontierProcessingResult(
            outcome,
            urlsEnqueued,
            sitemapUrlsEnqueued,
            candidateUrlsEnqueued,
            httpRequestCount,
            http429Count);
      }

      String xmlPayload;
      try {
        xmlPayload = sitemapParser.extractXmlPayload(claimed.url(), fetch);
      } catch (IOException e) {
        FrontierBackoffDecision parseDecision =
            backoffPolicy.onResult(currentBackoffState, 0, "gzip_decode_error");
        FrontierFetchOutcome outcome =
            new FrontierFetchOutcome(
                "FAILED",
                fetch.fetchedAt() == null ? now : fetch.fetchedAt(),
                fetch.statusCode() > 0 ? fetch.statusCode() : null,
                fetch.duration() == null ? null : fetch.duration().toMillis(),
                "gzip_decode_error",
                e.getMessage(),
                parseDecision.statusBucket(),
                parseDecision.nextAllowedAt(),
                parseDecision.nextBackoffState());
        return new FrontierProcessingResult(
            outcome,
            urlsEnqueued,
            sitemapUrlsEnqueued,
            candidateUrlsEnqueued,
            httpRequestCount,
            http429Count);
      }

      if (xmlPayload == null || xmlPayload.isBlank()) {
        FrontierBackoffDecision parseDecision =
            backoffPolicy.onResult(currentBackoffState, 0, "empty_sitemap_payload");
        FrontierFetchOutcome outcome =
            new FrontierFetchOutcome(
                "FAILED",
                fetch.fetchedAt() == null ? now : fetch.fetchedAt(),
                fetch.statusCode() > 0 ? fetch.statusCode() : null,
                fetch.duration() == null ? null : fetch.duration().toMillis(),
                "empty_sitemap_payload",
                "empty_sitemap_payload",
                parseDecision.statusBucket(),
                parseDecision.nextAllowedAt(),
                parseDecision.nextBackoffState());
        return new FrontierProcessingResult(
            outcome,
            urlsEnqueued,
            sitemapUrlsEnqueued,
            candidateUrlsEnqueued,
            httpRequestCount,
            http429Count);
      }

      FrontierSitemapParseResult parsed =
          sitemapParser.parse(xmlPayload, properties.getFrontier().getMaxUrlsParsedPerSitemap());

      for (String childSitemap : parsed.childSitemaps()) {
        FrontierEnqueueResult enqueue =
            frontierRepository.enqueueUrl(childSitemap, FrontierUrlKind.SITEMAP, 90, Instant.now());
        if (enqueue.inserted()) {
          urlsEnqueued++;
          sitemapUrlsEnqueued++;
        }
      }

      int candidateBudget = properties.getFrontier().getMaxJobCandidatesPerSitemap();
      for (String candidate : parsed.urls()) {
        if (candidateBudget <= 0) {
          break;
        }
        if (!jobSignalHeuristics.isJobLike(candidate)) {
          continue;
        }
        FrontierEnqueueResult enqueue =
            frontierRepository.enqueueUrl(candidate, FrontierUrlKind.CANDIDATE, 50, Instant.now());
        if (enqueue.inserted()) {
          urlsEnqueued++;
          candidateUrlsEnqueued++;
        }
        candidateBudget--;
      }

      FrontierFetchOutcome outcome =
          new FrontierFetchOutcome(
              "FETCHED",
              fetch.fetchedAt() == null ? now : fetch.fetchedAt(),
              fetch.statusCode() > 0 ? fetch.statusCode() : null,
              fetch.duration() == null ? null : fetch.duration().toMillis(),
              errorBucket,
              null,
              decision.statusBucket(),
              decision.nextAllowedAt(),
              decision.nextBackoffState());
      return new FrontierProcessingResult(
          outcome,
          urlsEnqueued,
          sitemapUrlsEnqueued,
          candidateUrlsEnqueued,
          httpRequestCount,
          http429Count);

    } catch (Exception e) {
      FrontierBackoffDecision decision =
          backoffPolicy.onResult(currentBackoffState, 0, "exception");
      FrontierFetchOutcome outcome =
          new FrontierFetchOutcome(
              "FAILED",
              now,
              null,
              null,
              "exception",
              e.getClass().getSimpleName() + ":" + e.getMessage(),
              decision.statusBucket(),
              decision.nextAllowedAt(),
              decision.nextBackoffState());
      return new FrontierProcessingResult(
          outcome,
          urlsEnqueued,
          sitemapUrlsEnqueued,
          candidateUrlsEnqueued,
          httpRequestCount,
          http429Count);
    }
  }

  private String fetchErrorBucket(HttpFetchResult fetch) {
    if (fetch == null) {
      return "missing_fetch";
    }
    if (fetch.errorCode() != null && !fetch.errorCode().isBlank()) {
      return fetch.errorCode();
    }
    if (fetch.statusCode() > 0) {
      return "http_" + fetch.statusCode();
    }
    return "unknown_error";
  }

  private void increment(Map<String, Integer> target, String key) {
    if (key == null || key.isBlank()) {
      return;
    }
    target.put(key, target.getOrDefault(key, 0) + 1);
  }

  private record FrontierProcessingResult(
      FrontierFetchOutcome outcome,
      int urlsEnqueued,
      int sitemapUrlsEnqueued,
      int candidateUrlsEnqueued,
      int httpRequestCount,
      int http429Count) {}
}
