package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record FullCycleSummary(
    int resolveLimit,
    int discoverLimit,
    int crawlLimit,
    DomainResolutionResult domainResolution,
    CareersDiscoveryResult careersDiscovery,
    CrawlRunSummary crawlRun,
    int jobsExtracted,
    Map<String, Integer> topErrors
) {
}
