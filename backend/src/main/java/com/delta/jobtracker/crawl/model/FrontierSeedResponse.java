package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.Map;

public record FrontierSeedResponse(
    Instant startedAt,
    Instant finishedAt,
    int seedDomainsRequested,
    int seedDomainsUsed,
    int hostsSeen,
    int urlsEnqueued,
    int sitemapUrlsEnqueued,
    int candidateUrlsEnqueued,
    int urlsFetched,
    int blockedByBackoff,
    int httpRequestCount,
    int http429Count,
    double http429Rate,
    Map<String, Integer> statusBucketCounts,
    Map<String, Integer> queueStatusCounts) {}
