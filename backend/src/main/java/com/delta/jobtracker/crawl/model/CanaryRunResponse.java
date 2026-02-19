package com.delta.jobtracker.crawl.model;

public record CanaryRunResponse(
    long runId,
    String status
) {
}
