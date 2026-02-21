package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.Map;

public record RecentCrawlStatus(
    long crawlRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    long jobsExtractedCount,
    Map<String, Integer> topErrors,
    Long secondsRunning,
    Instant lastActivityAt) {}
