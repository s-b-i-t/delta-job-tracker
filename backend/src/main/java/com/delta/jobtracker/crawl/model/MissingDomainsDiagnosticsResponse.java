package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record MissingDomainsDiagnosticsResponse(
    long missingDomainCount,
    Map<String, Long> reasons,
    List<MissingDomainEntry> samples
) {
}
