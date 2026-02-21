package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlRunStatusResponse(
    long crawlRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    int companiesAttempted,
    int companiesSucceeded,
    int companiesFailed,
    int jobsExtractedCount,
    Instant lastHeartbeatAt,
    Instant lastActivityAt) {}
