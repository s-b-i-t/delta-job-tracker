package com.delta.jobtracker.crawl.model;

import java.util.List;

public record DomainResolutionResult(
    int resolvedCount,
    int failedCount,
    List<String> topErrors
) {
}
