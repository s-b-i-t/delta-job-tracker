package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record AtsEndpointRecord(
    long companyId,
    AtsType atsType,
    String endpointUrl,
    String discoveredFromUrl,
    double confidence,
    Instant detectedAt
) {
}
