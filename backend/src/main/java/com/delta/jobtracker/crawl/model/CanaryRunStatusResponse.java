package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.Map;

public record CanaryRunStatusResponse(
    long runId,
    String type,
    Integer requestedLimit,
    Instant startedAt,
    Instant finishedAt,
    String status,
    SecCanarySummary summary,
    Map<String, Integer> errorSummary) {}
