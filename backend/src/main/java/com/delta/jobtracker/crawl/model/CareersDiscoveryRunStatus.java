package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CareersDiscoveryRunStatus(
    long discoveryRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    int companyLimit,
    int processedCount,
    int succeededCount,
    int failedCount,
    int endpointsAdded,
    String lastError
) {
}
