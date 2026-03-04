package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record FrontierFetchOutcome(
    String urlStatus,
    Instant fetchedAt,
    Integer httpStatus,
    Long elapsedMs,
    String errorBucket,
    String lastError,
    String hostStatusBucket,
    Instant hostNextAllowedAt,
    int hostBackoffState) {}
