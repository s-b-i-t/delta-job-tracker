package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierSchedulerResult;
import com.delta.jobtracker.crawl.model.FrontierSeedResponse;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.persistence.FrontierRepository;
import com.delta.jobtracker.crawl.robots.RobotsRules;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Service;

@Service
public class FrontierSeedService {
  private final FrontierRepository frontierRepository;
  private final FrontierSchedulerService frontierSchedulerService;
  private final RobotsTxtService robotsTxtService;
  private final CrawlerProperties properties;

  public FrontierSeedService(
      FrontierRepository frontierRepository,
      FrontierSchedulerService frontierSchedulerService,
      RobotsTxtService robotsTxtService,
      CrawlerProperties properties) {
    this.frontierRepository = frontierRepository;
    this.frontierSchedulerService = frontierSchedulerService;
    this.robotsTxtService = robotsTxtService;
    this.properties = properties;
  }

  public FrontierSeedResponse seedFromCompanyDomains(
      Integer domainLimit, Integer maxSitemapFetches) {
    Instant startedAt = Instant.now();
    int requestedDomains =
        domainLimit == null
            ? properties.getFrontier().getSeedDomainLimit()
            : Math.max(1, domainLimit);
    int requestedFetches =
        maxSitemapFetches == null
            ? properties.getFrontier().getMaxSitemapFetchesPerRun()
            : Math.max(1, maxSitemapFetches);

    List<String> seedDomains = frontierRepository.findSeedDomains(requestedDomains);
    Set<String> hostsSeen = new LinkedHashSet<>();

    int seededUrlsEnqueued = 0;
    int seededSitemapUrlsEnqueued = 0;

    for (String domain : seedDomains) {
      if (domain == null || domain.isBlank()) {
        continue;
      }
      String host = domain.trim().toLowerCase(Locale.ROOT);
      hostsSeen.add(host);
      frontierRepository.ensureHost(host);

      RobotsRules rules = robotsTxtService.getRulesForHost(host);
      Set<String> sitemapSeeds = new LinkedHashSet<>();
      if (rules != null && rules.getSitemapUrls() != null) {
        sitemapSeeds.addAll(rules.getSitemapUrls());
      }
      if (sitemapSeeds.isEmpty()) {
        sitemapSeeds.add("https://" + host + "/sitemap.xml");
      }

      for (String sitemapUrl : sitemapSeeds) {
        FrontierEnqueueResult enqueueResult =
            frontierRepository.enqueueUrl(sitemapUrl, FrontierUrlKind.SITEMAP, 100, Instant.now());
        if (enqueueResult.inserted()) {
          seededUrlsEnqueued++;
          seededSitemapUrlsEnqueued++;
        }
      }
    }

    FrontierSchedulerResult schedulerResult =
        frontierSchedulerService.fetchDueSitemaps(requestedFetches);

    int urlsEnqueued = seededUrlsEnqueued + schedulerResult.urlsEnqueued();
    int sitemapUrlsEnqueued = seededSitemapUrlsEnqueued + schedulerResult.sitemapUrlsEnqueued();
    int candidateUrlsEnqueued = schedulerResult.candidateUrlsEnqueued();

    int httpRequestCount = schedulerResult.httpRequestCount();
    int http429Count = schedulerResult.http429Count();
    double http429Rate =
        httpRequestCount <= 0 ? 0.0 : ((double) http429Count) / (double) httpRequestCount;

    Map<String, Integer> statusBucketCounts =
        new LinkedHashMap<>(schedulerResult.statusBucketCounts());
    Map<String, Integer> queueStatusCounts = frontierRepository.countQueueStatuses();

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
}
