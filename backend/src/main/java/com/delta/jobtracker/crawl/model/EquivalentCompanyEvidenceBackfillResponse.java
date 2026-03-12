package com.delta.jobtracker.crawl.model;

public record EquivalentCompanyEvidenceBackfillResponse(
    int domainRowsInserted, int atsRowsInserted, int totalRowsInserted) {}
