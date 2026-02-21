package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CareersDiscoveryState(
    long companyId,
    Instant lastAttemptAt,
    String lastReasonCode,
    String lastCandidateUrl,
    int consecutiveFailures,
    Instant nextAttemptAt) {}
