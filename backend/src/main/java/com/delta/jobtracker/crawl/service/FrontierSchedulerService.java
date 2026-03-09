package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.FrontierBackoffDecision;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierFetchOutcome;
import com.delta.jobtracker.crawl.model.FrontierHostState;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierSchedulerDiagnostics;
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
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FrontierSchedulerService {
  private static final int MAX_SITEMAP_BYTES = 2_000_000;
  private static final Logger log = LoggerFactory.getLogger(FrontierSchedulerService.class);

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
    return fetchDueSitemaps(maxFetches, "scheduler-" + UUID.randomUUID());
  }

  public FrontierSchedulerResult fetchDueSitemaps(int maxFetches, String runId) {
    int safeMaxFetches = Math.max(1, maxFetches);
    String effectiveRunId =
        (runId == null || runId.isBlank()) ? "scheduler-" + UUID.randomUUID() : runId;
    long schedulerStartedAtNanos = System.nanoTime();
    int urlsFetched = 0;
    int blockedByBackoff = 0;
    int urlsEnqueued = 0;
    int sitemapUrlsEnqueued = 0;
    int candidateUrlsEnqueued = 0;
    int httpRequestCount = 0;
    int http429Count = 0;
    Map<String, Integer> statusBucketCounts = new LinkedHashMap<>();
    long claimNextDueUrlDbMillis = 0L;
    long findHostStateDbMillis = 0L;
    long countDueBlockedDbMillis = 0L;
    long enqueueDbMillis = 0L;
    long completeFetchDbMillis = 0L;
    long robotsCheckMillis = 0L;
    long httpFetchMillis = 0L;
    long parseMillis = 0L;

    log.info(
        "frontier.seed.scheduler.start runId={} maxFetches={}", effectiveRunId, safeMaxFetches);

    while (urlsFetched < safeMaxFetches) {
      long claimStartedAtNanos = System.nanoTime();
      FrontierQueueUrl claimed =
          frontierRepository.claimNextDueUrl(
              "frontier-seeder",
              properties.getFrontier().getUrlLeaseSeconds(),
              FrontierUrlKind.SITEMAP);
      long claimMillis = elapsedMillis(claimStartedAtNanos);
      claimNextDueUrlDbMillis += claimMillis;
      if (claimed == null) {
        long blockedCountStartedAtNanos = System.nanoTime();
        blockedByBackoff +=
            frontierRepository.countDueUrlsBlockedByBackoff(FrontierUrlKind.SITEMAP);
        long blockedCountMillis = elapsedMillis(blockedCountStartedAtNanos);
        countDueBlockedDbMillis += blockedCountMillis;
        log.info(
            "frontier.seed.scheduler.idle runId={} urlsFetched={} claimNextDueUrlDbMs={} countDueBlockedDbMs={} blockedByBackoff={}",
            effectiveRunId,
            urlsFetched,
            claimMillis,
            blockedCountMillis,
            blockedByBackoff);
        break;
      }

      urlsFetched++;
      log.info(
          "frontier.seed.scheduler.fetch.begin runId={} fetchIndex={} urlId={} host={} url={} claimNextDueUrlDbMs={}",
          effectiveRunId,
          urlsFetched,
          claimed.id(),
          claimed.host(),
          claimed.url(),
          claimMillis);
      long fetchStartedAtNanos = System.nanoTime();
      FrontierProcessingResult processingResult =
          processClaimedSitemap(effectiveRunId, urlsFetched, claimed);
      findHostStateDbMillis += processingResult.findHostStateDbMillis();
      enqueueDbMillis += processingResult.enqueueDbMillis();
      robotsCheckMillis += processingResult.robotsCheckMillis();
      httpFetchMillis += processingResult.httpFetchMillis();
      parseMillis += processingResult.parseMillis();
      long completeFetchStartedAtNanos = System.nanoTime();
      frontierRepository.completeFetch(claimed, processingResult.outcome());
      long completeFetchMillisForClaim = elapsedMillis(completeFetchStartedAtNanos);
      completeFetchDbMillis += completeFetchMillisForClaim;

      urlsEnqueued += processingResult.urlsEnqueued();
      sitemapUrlsEnqueued += processingResult.sitemapUrlsEnqueued();
      candidateUrlsEnqueued += processingResult.candidateUrlsEnqueued();
      httpRequestCount += processingResult.httpRequestCount();
      http429Count += processingResult.http429Count();
      increment(statusBucketCounts, processingResult.outcome().hostStatusBucket());
      long fetchTotalMillis = elapsedMillis(fetchStartedAtNanos);
      long fetchOtherMillis =
          Math.max(
              0L,
              fetchTotalMillis
                  - processingResult.findHostStateDbMillis()
                  - processingResult.enqueueDbMillis()
                  - processingResult.robotsCheckMillis()
                  - processingResult.httpFetchMillis()
                  - processingResult.parseMillis()
                  - completeFetchMillisForClaim);
      log.info(
          "frontier.seed.scheduler.fetch.finish runId={} fetchIndex={} urlId={} host={} outcome={} hostStatusBucket={} httpStatus={} errorBucket={} totalMs={} findHostStateDbMs={} robotsCheckMs={} httpFetchMs={} parseMs={} enqueueDbMs={} completeFetchDbMs={} otherMs={} urlsEnqueued={} sitemapUrlsEnqueued={} candidateUrlsEnqueued={}",
          effectiveRunId,
          urlsFetched,
          claimed.id(),
          claimed.host(),
          processingResult.outcome().urlStatus(),
          processingResult.outcome().hostStatusBucket(),
          processingResult.outcome().httpStatus(),
          processingResult.outcome().errorBucket(),
          fetchTotalMillis,
          processingResult.findHostStateDbMillis(),
          processingResult.robotsCheckMillis(),
          processingResult.httpFetchMillis(),
          processingResult.parseMillis(),
          processingResult.enqueueDbMillis(),
          completeFetchMillisForClaim,
          fetchOtherMillis,
          processingResult.urlsEnqueued(),
          processingResult.sitemapUrlsEnqueued(),
          processingResult.candidateUrlsEnqueued());
    }

    FrontierSchedulerDiagnostics diagnostics =
        new FrontierSchedulerDiagnostics(
            claimNextDueUrlDbMillis,
            findHostStateDbMillis,
            countDueBlockedDbMillis,
            enqueueDbMillis,
            completeFetchDbMillis,
            robotsCheckMillis,
            httpFetchMillis,
            parseMillis,
            elapsedMillis(schedulerStartedAtNanos));
    log.info(
        "frontier.seed.scheduler.summary runId={} urlsFetched={} blockedByBackoff={} urlsEnqueued={} sitemapUrlsEnqueued={} candidateUrlsEnqueued={} httpRequestCount={} http429Count={} totalMs={} claimNextDueUrlDbMs={} findHostStateDbMs={} countDueBlockedDbMs={} enqueueDbMs={} completeFetchDbMs={} robotsCheckMs={} httpFetchMs={} parseMs={} otherMs={}",
        effectiveRunId,
        urlsFetched,
        blockedByBackoff,
        urlsEnqueued,
        sitemapUrlsEnqueued,
        candidateUrlsEnqueued,
        httpRequestCount,
        http429Count,
        diagnostics.totalMillis(),
        diagnostics.claimNextDueUrlDbMillis(),
        diagnostics.findHostStateDbMillis(),
        diagnostics.countDueBlockedDbMillis(),
        diagnostics.enqueueDbMillis(),
        diagnostics.completeFetchDbMillis(),
        diagnostics.robotsCheckMillis(),
        diagnostics.httpFetchMillis(),
        diagnostics.parseMillis(),
        diagnostics.otherMillis());
    return new FrontierSchedulerResult(
        urlsFetched,
        blockedByBackoff,
        urlsEnqueued,
        sitemapUrlsEnqueued,
        candidateUrlsEnqueued,
        httpRequestCount,
        http429Count,
        statusBucketCounts,
        diagnostics);
  }

  private FrontierProcessingResult processClaimedSitemap(
      String runId, int fetchIndex, FrontierQueueUrl claimed) {
    long findHostStateStartedAtNanos = System.nanoTime();
    FrontierHostState hostState = frontierRepository.findHostState(claimed.host());
    long findHostStateDbMillis = elapsedMillis(findHostStateStartedAtNanos);
    int currentBackoffState = hostState == null ? 0 : hostState.backoffState();
    Instant now = Instant.now();

    int urlsEnqueued = 0;
    int sitemapUrlsEnqueued = 0;
    int candidateUrlsEnqueued = 0;
    int httpRequestCount = 0;
    int http429Count = 0;
    long enqueueDbMillis = 0L;
    long robotsCheckMillis = 0L;
    long httpFetchMillis = 0L;
    long parseMillis = 0L;

    try {
      RobotsCheckResult robotsCheckResult = null;
      if (properties.getFrontier().isRespectRobotsForSitemaps()) {
        robotsCheckResult = checkSitemapAllowed(runId, fetchIndex, claimed);
        robotsCheckMillis += robotsCheckResult.durationMillis();
      }
      if (robotsCheckResult != null && !robotsCheckResult.allowed()) {
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
            http429Count,
            findHostStateDbMillis,
            enqueueDbMillis,
            robotsCheckMillis,
            httpFetchMillis,
            parseMillis);
      }

      log.info(
          "frontier.seed.scheduler.http.begin runId={} fetchIndex={} urlId={} host={} url={}",
          runId,
          fetchIndex,
          claimed.id(),
          claimed.host(),
          claimed.url());
      long httpFetchStartedAtNanos = System.nanoTime();
      HttpFetchResult fetch =
          httpClient.get(
              claimed.url(), "application/xml,text/xml;q=0.9,*/*;q=0.1", MAX_SITEMAP_BYTES);
      httpFetchMillis += elapsedMillis(httpFetchStartedAtNanos);
      log.info(
          "frontier.seed.scheduler.http.finish runId={} fetchIndex={} urlId={} host={} status={} errorCode={} durationMs={}",
          runId,
          fetchIndex,
          claimed.id(),
          claimed.host(),
          fetch.statusCode(),
          fetch.errorCode(),
          httpFetchMillis);
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
            http429Count,
            findHostStateDbMillis,
            enqueueDbMillis,
            robotsCheckMillis,
            httpFetchMillis,
            parseMillis);
      }

      String xmlPayload;
      try {
        long parseStartedAtNanos = System.nanoTime();
        xmlPayload = sitemapParser.extractXmlPayload(claimed.url(), fetch);
        parseMillis += elapsedMillis(parseStartedAtNanos);
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
            http429Count,
            findHostStateDbMillis,
            enqueueDbMillis,
            robotsCheckMillis,
            httpFetchMillis,
            parseMillis);
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
            http429Count,
            findHostStateDbMillis,
            enqueueDbMillis,
            robotsCheckMillis,
            httpFetchMillis,
            parseMillis);
      }

      long parseStartedAtNanos = System.nanoTime();
      FrontierSitemapParseResult parsed =
          sitemapParser.parse(xmlPayload, properties.getFrontier().getMaxUrlsParsedPerSitemap());
      parseMillis += elapsedMillis(parseStartedAtNanos);

      long enqueueStartedAtNanos = System.nanoTime();
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
      enqueueDbMillis += elapsedMillis(enqueueStartedAtNanos);

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
          http429Count,
          findHostStateDbMillis,
          enqueueDbMillis,
          robotsCheckMillis,
          httpFetchMillis,
          parseMillis);

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
          http429Count,
          findHostStateDbMillis,
          enqueueDbMillis,
          robotsCheckMillis,
          httpFetchMillis,
          parseMillis);
    }
  }

  private RobotsCheckResult checkSitemapAllowed(
      String runId, int fetchIndex, FrontierQueueUrl claimed) {
    log.info(
        "frontier.seed.scheduler.robots.begin runId={} fetchIndex={} urlId={} host={} url={}",
        runId,
        fetchIndex,
        claimed.id(),
        claimed.host(),
        claimed.url());
    long robotsStartedAtNanos = System.nanoTime();
    boolean allowed = robotsTxtService.isAllowed(claimed.url());
    long durationMillis = elapsedMillis(robotsStartedAtNanos);
    log.info(
        "frontier.seed.scheduler.robots.finish runId={} fetchIndex={} urlId={} host={} allowed={} durationMs={}",
        runId,
        fetchIndex,
        claimed.id(),
        claimed.host(),
        allowed,
        durationMillis);
    return new RobotsCheckResult(allowed, durationMillis);
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
      int http429Count,
      long findHostStateDbMillis,
      long enqueueDbMillis,
      long robotsCheckMillis,
      long httpFetchMillis,
      long parseMillis) {}

  private record RobotsCheckResult(boolean allowed, long durationMillis) {}

  private long elapsedMillis(long startedAtNanos) {
    return (System.nanoTime() - startedAtNanos) / 1_000_000L;
  }
}
