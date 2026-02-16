package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record AtsAttemptSample(
    String ticker,
    String atsType,
    String url,
    String fetchStatus,
    Instant lastFetchedAt
) {
}
