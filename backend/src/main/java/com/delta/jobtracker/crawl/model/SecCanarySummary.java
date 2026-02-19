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
    long durationIngestMs,
    long durationDomainResolutionMs,
    long durationDiscoveryMs,
    long durationCrawlMs,
    long durationTotalMs,
    Long crawlRunId,
    int companiesIngested,
    int domainsResolved,
    Map<String, Integer> endpointsDiscoveredByAtsType,
    DomainResolutionResult domainResolution,
    CareersDiscoveryResult careersDiscovery,
    Map<String, Integer> companiesCrawledByAtsType,
    int jobsExtracted,
    int cooldownSkips,
    Map<String, Integer> topErrors,
    Map<String, Long> stepDurationsMs
) {
}
