package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlRunCompanyFailureView(
    String ticker,
    String companyName,
    String stage,
    String reasonCode,
    Integer httpStatus,
    String errorDetail,
    Instant finishedAt
) {
}
