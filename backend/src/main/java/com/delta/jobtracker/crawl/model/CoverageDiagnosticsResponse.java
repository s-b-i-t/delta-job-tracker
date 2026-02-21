package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record CoverageDiagnosticsResponse(
    Map<String, Long> counts,
    Map<String, Long> atsEndpointsByType,
    Map<String, Long> atsEndpointsByMethod) {}
