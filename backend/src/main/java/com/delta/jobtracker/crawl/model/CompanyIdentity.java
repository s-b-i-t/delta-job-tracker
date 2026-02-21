package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CompanyIdentity(
    long companyId,
    String ticker,
    String name,
    String sector,
    String wikipediaTitle,
    String cik,
    String domainResolutionMethod,
    String domainResolutionStatus,
    String domainResolutionError,
    Instant domainResolutionAttemptedAt) {}
