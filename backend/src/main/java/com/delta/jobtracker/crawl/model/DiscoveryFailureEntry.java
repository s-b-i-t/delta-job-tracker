package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record DiscoveryFailureEntry(
    String ticker,
    String companyName,
    String candidateUrl,
    String detail,
    Instant observedAt,
    String reasonCode
) {
}
