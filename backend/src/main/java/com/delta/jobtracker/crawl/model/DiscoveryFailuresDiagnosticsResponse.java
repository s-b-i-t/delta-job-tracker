package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record DiscoveryFailuresDiagnosticsResponse(
    Map<String, Long> countsByReason, List<DiscoveryFailureEntry> recentFailures) {}
