package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.Map;

public record SecCanarySummary(
    int requestedLimit,
    Instant startedAt,
    Instant finishedAt,
    String status,
    String abortReason,
    long durationMs,
    Long crawlRunId,
    int companiesIngested,
    DomainResolutionResult domainResolution,
    CareersDiscoveryResult careersDiscovery,
    Map<String, Integer> companiesCrawledByAtsType,
    int jobsExtracted,
    int cooldownSkips,
    Map<String, Integer> topErrors,
    Map<String, Long> stepDurationsMs
) {
}
