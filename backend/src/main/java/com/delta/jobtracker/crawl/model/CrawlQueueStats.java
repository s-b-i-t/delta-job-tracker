package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.List;

public record CrawlQueueStats(
    long dueCount,
    long lockedCount,
    Instant nextDueAt,
    List<CrawlQueueErrorSample> lastErrors
) {
}
