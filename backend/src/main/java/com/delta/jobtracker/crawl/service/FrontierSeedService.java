package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierSchedulerDiagnostics;
import com.delta.jobtracker.crawl.model.FrontierSchedulerResult;
import com.delta.jobtracker.crawl.model.FrontierSeedResponse;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.persistence.FrontierRepository;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class FrontierSeedService {
  private static final Logger log = LoggerFactory.getLogger(FrontierSeedService.class);

  private final FrontierRepository frontierRepository;
  private final FrontierSchedulerService frontierSchedulerService;
  private final CrawlerProperties properties;

  public FrontierSeedService(
      FrontierRepository frontierRepository,
      FrontierSchedulerService frontierSchedulerService,
      CrawlerProperties properties) {
    this.frontierRepository = frontierRepository;
    this.frontierSchedulerService = frontierSchedulerService;
    this.properties = properties;
  }

  public FrontierSeedResponse seedFromCompanyDomains(
      Integer domainLimit, Integer maxSitemapFetches) {
    Instant startedAt = Instant.now();
    long seedStartedAtNanos = System.nanoTime();
    String runId = "seed-" + UUID.randomUUID();
    int requestedDomains =
        domainLimit == null
            ? properties.getFrontier().getSeedDomainLimit()
            : Math.max(1, domainLimit);
    int requestedFetches =
        maxSitemapFetches == null
            ? properties.getFrontier().getMaxSitemapFetchesPerRun()
            : Math.max(1, maxSitemapFetches);
    long preDbMillis = elapsedMillis(seedStartedAtNanos);

    log.info(
        "frontier.seed.start runId={} requestedDomainLimit={} requestedFetchLimit={}",
        runId,
        requestedDomains,
        requestedFetches);

    long findSeedDomainsStartedAtNanos = System.nanoTime();
    List<String> seedDomains = frontierRepository.findSeedDomains(requestedDomains);
    long findSeedDomainsDbMillis = elapsedMillis(findSeedDomainsStartedAtNanos);
    log.info(
        "frontier.seed.findSeedDomains runId={} requestedDomains={} returnedDomains={} durationMs={}",
        runId,
        requestedDomains,
        seedDomains.size(),
        findSeedDomainsDbMillis);
    Set<String> hostsSeen = new LinkedHashSet<>();

    int seededUrlsEnqueued = 0;
    int seededSitemapUrlsEnqueued = 0;
    long ensureHostDbMillis = 0L;
    long robotsLookupMillis = 0L;
    long seedEnqueueDbMillis = 0L;

    for (int domainIndex = 0; domainIndex < seedDomains.size(); domainIndex++) {
      String domain = seedDomains.get(domainIndex);
      if (domain == null || domain.isBlank()) {
        continue;
      }
      String host = domain.trim().toLowerCase(Locale.ROOT);
      int humanIndex = domainIndex + 1;
      hostsSeen.add(host);
      log.info("frontier.seed.domain.start runId={} index={} host={}", runId, humanIndex, host);
      long ensureHostStartedAtNanos = System.nanoTime();
      frontierRepository.ensureHost(host);
      long ensureHostMillisForHost = elapsedMillis(ensureHostStartedAtNanos);
      ensureHostDbMillis += ensureHostMillisForHost;
      log.info(
          "frontier.seed.domain.ensureHost runId={} index={} host={} durationMs={}",
          runId,
          humanIndex,
          host,
          ensureHostMillisForHost);

      long enqueueStartedAtNanos = System.nanoTime();
      int insertedSitemapUrlsForHost = 0;
      FrontierEnqueueResult enqueueResult =
          frontierRepository.enqueueUrl(
              "https://" + host + "/sitemap.xml", FrontierUrlKind.SITEMAP, 100, Instant.now());
      if (enqueueResult.inserted()) {
        seededUrlsEnqueued++;
        seededSitemapUrlsEnqueued++;
        insertedSitemapUrlsForHost++;
      }
      long enqueueMillisForHost = elapsedMillis(enqueueStartedAtNanos);
      seedEnqueueDbMillis += enqueueMillisForHost;
      log.info(
          "frontier.seed.domain.finish runId={} index={} host={} ensureHostDbMs={} robotsMs={} enqueueDbMs={} sitemapSeedCount={} insertedSitemapUrls={}",
          runId,
          humanIndex,
          host,
          ensureHostMillisForHost,
          0L,
          enqueueMillisForHost,
          1,
          insertedSitemapUrlsForHost);
    }

    log.info(
        "frontier.seed.scheduler.begin runId={} requestedFetchLimit={}", runId, requestedFetches);
    long schedulerStartedAtNanos = System.nanoTime();
    FrontierSchedulerResult schedulerResult =
        frontierSchedulerService.fetchDueSitemaps(requestedFetches, runId);
    long schedulerElapsedMillis = elapsedMillis(schedulerStartedAtNanos);

    int urlsEnqueued = seededUrlsEnqueued + schedulerResult.urlsEnqueued();
    int sitemapUrlsEnqueued = seededSitemapUrlsEnqueued + schedulerResult.sitemapUrlsEnqueued();
    int candidateUrlsEnqueued = schedulerResult.candidateUrlsEnqueued();

    int httpRequestCount = schedulerResult.httpRequestCount();
    int http429Count = schedulerResult.http429Count();
    double http429Rate =
        httpRequestCount <= 0 ? 0.0 : ((double) http429Count) / (double) httpRequestCount;

    Map<String, Integer> statusBucketCounts =
        new LinkedHashMap<>(schedulerResult.statusBucketCounts());
    long countQueueStatusesStartedAtNanos = System.nanoTime();
    Map<String, Integer> queueStatusCounts = frontierRepository.countQueueStatuses();
    long countQueueStatusesDbMillis = elapsedMillis(countQueueStatusesStartedAtNanos);

    FrontierSchedulerDiagnostics schedulerDiagnostics = schedulerResult.diagnostics();
    long dbMillis =
        findSeedDomainsDbMillis
            + ensureHostDbMillis
            + seedEnqueueDbMillis
            + countQueueStatusesDbMillis
            + (schedulerDiagnostics == null ? 0L : schedulerDiagnostics.dbMillis());
    long downstreamMillis =
        robotsLookupMillis
            + (schedulerDiagnostics == null ? 0L : schedulerDiagnostics.downstreamMillis());
    long parseMillis = schedulerDiagnostics == null ? 0L : schedulerDiagnostics.parseMillis();
    long totalMillis = elapsedMillis(seedStartedAtNanos);
    long otherMillis =
        Math.max(0L, totalMillis - preDbMillis - dbMillis - downstreamMillis - parseMillis);
    log.info(
        "frontier.seed.summary runId={} totalMs={} preDbMs={} dbMs={} downstreamMs={} parseMs={} otherMs={} findSeedDomainsDbMs={} ensureHostDbMs={} seedEnqueueDbMs={} countQueueStatusesDbMs={} robotsLookupMs={} schedulerTotalMs={} schedulerDbMs={} schedulerDownstreamMs={} schedulerParseMs={} schedulerOtherMs={} seedDomainsRequested={} seedDomainsUsed={} hostsSeen={} urlsEnqueued={} sitemapUrlsEnqueued={} candidateUrlsEnqueued={} urlsFetched={} blockedByBackoff={} httpRequestCount={} http429Count={} http429Rate={}",
        runId,
        totalMillis,
        preDbMillis,
        dbMillis,
        downstreamMillis,
        parseMillis,
        otherMillis,
        findSeedDomainsDbMillis,
        ensureHostDbMillis,
        seedEnqueueDbMillis,
        countQueueStatusesDbMillis,
        robotsLookupMillis,
        schedulerElapsedMillis,
        schedulerDiagnostics == null ? 0L : schedulerDiagnostics.dbMillis(),
        schedulerDiagnostics == null ? 0L : schedulerDiagnostics.downstreamMillis(),
        schedulerDiagnostics == null ? 0L : schedulerDiagnostics.parseMillis(),
        schedulerDiagnostics == null ? 0L : schedulerDiagnostics.otherMillis(),
        requestedDomains,
        seedDomains.size(),
        hostsSeen.size(),
        urlsEnqueued,
        sitemapUrlsEnqueued,
        candidateUrlsEnqueued,
        schedulerResult.urlsFetched(),
        schedulerResult.blockedByBackoff(),
        httpRequestCount,
        http429Count,
        http429Rate);

    return new FrontierSeedResponse(
        startedAt,
        Instant.now(),
        requestedDomains,
        seedDomains.size(),
        hostsSeen.size(),
        urlsEnqueued,
        sitemapUrlsEnqueued,
        candidateUrlsEnqueued,
        schedulerResult.urlsFetched(),
        schedulerResult.blockedByBackoff(),
        httpRequestCount,
        http429Count,
        http429Rate,
        statusBucketCounts,
        queueStatusCounts);
  }

  private long elapsedMillis(long startedAtNanos) {
    return (System.nanoTime() - startedAtNanos) / 1_000_000L;
  }
}
