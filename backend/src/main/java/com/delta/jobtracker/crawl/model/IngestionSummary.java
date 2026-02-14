package com.delta.jobtracker.crawl.model;

import java.util.List;

public record IngestionSummary(
    int companiesUpserted,
    int domainsSeeded,
    int errorsCount,
    List<String> sampleErrors
) {
}
