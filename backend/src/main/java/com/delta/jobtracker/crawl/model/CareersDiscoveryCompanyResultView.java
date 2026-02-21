package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CareersDiscoveryCompanyResultView(
    long companyId,
    String ticker,
    String companyName,
    String status,
    String reasonCode,
    String stage,
    int foundEndpointsCount,
    Long durationMs,
    Integer httpStatus,
    String errorDetail,
    Instant createdAt) {}
