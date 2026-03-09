package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record FrontierSchedulerResult(
    int urlsFetched,
    int blockedByBackoff,
    int urlsEnqueued,
    int sitemapUrlsEnqueued,
    int candidateUrlsEnqueued,
    int httpRequestCount,
    int http429Count,
    Map<String, Integer> statusBucketCounts,
    FrontierSchedulerDiagnostics diagnostics) {}
