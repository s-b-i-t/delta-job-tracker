package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record CareersDiscoveryResult(
    Map<String, Integer> discoveredCountByAtsType,
    int failedCount,
    int cooldownSkips,
    Map<String, Integer> topErrors) {}
