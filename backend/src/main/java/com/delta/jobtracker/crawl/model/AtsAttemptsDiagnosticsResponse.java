package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record AtsAttemptsDiagnosticsResponse(
    Map<String, Long> countsByStatus, List<AtsAttemptSample> recentFailures) {}
