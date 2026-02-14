package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record AtsAdapterResult(
    int jobsExtractedCount,
    int jobpostingPagesFoundCount,
    Map<String, Integer> errors
) {
}
