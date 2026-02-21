package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record MissingDomainEntry(
    String ticker,
    String companyName,
    String cik,
    String wikipediaTitle,
    String domainResolutionMethod,
    String domainResolutionStatus,
    String domainResolutionError,
    Instant domainResolutionAttemptedAt
) {
    public String reason() {
        if (domainResolutionError != null && !domainResolutionError.isBlank()) {
            return domainResolutionError;
        }
        if (domainResolutionStatus != null && !domainResolutionStatus.isBlank()) {
            return domainResolutionStatus;
        }
        return "not_attempted";
    }
}
