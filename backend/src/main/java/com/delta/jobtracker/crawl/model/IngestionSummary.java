package com.delta.jobtracker.crawl.model;

public record IngestionSummary(
    int companiesUpserted,
    int domainsUpserted
) {
}
