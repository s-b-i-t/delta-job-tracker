package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlRunCompanyResultView(
    long companyId,
    String ticker,
    String companyName,
    String status,
    String stage,
    String atsType,
    String endpointUrl,
    Instant startedAt,
    Instant finishedAt,
    Long durationMs,
    int jobsExtracted,
    boolean truncated,
    String reasonCode,
    Integer httpStatus,
    String errorDetail,
    boolean retryable
) {
}
