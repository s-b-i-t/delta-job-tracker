package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CanaryRunStatus(
    long runId,
    String type,
    Integer requestedLimit,
    Instant startedAt,
    Instant finishedAt,
    String status,
    String summaryJson,
    String errorSummaryJson
) {
}
