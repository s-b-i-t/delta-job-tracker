package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlQueueErrorSample(
    long companyId, String lastError, Instant lastFinishedAt, int consecutiveFailures) {}
