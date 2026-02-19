package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record HostCrawlState(
    String host,
    int consecutiveFailures,
    String lastErrorCategory,
    Instant lastAttemptAt,
    Instant nextAllowedAt
) {
}
