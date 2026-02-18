package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CareersDiscoveryCompanyFailureView(
    String ticker,
    String companyName,
    String stage,
    String reasonCode,
    Integer httpStatus,
    String errorDetail,
    Instant createdAt
) {
}
