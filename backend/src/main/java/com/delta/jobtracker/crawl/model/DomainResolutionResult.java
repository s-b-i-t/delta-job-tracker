package com.delta.jobtracker.crawl.model;

import java.util.List;

public record DomainResolutionResult(
    int resolvedCount,
    int noWikipediaTitleCount,
    int noItemCount,
    int noP856Count,
    int wdqsErrorCount,
    List<String> sampleErrors
) {
}
