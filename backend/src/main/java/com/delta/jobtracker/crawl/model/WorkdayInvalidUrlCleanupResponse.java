package com.delta.jobtracker.crawl.model;

public record WorkdayInvalidUrlCleanupResponse(
    int scanned,
    int invalid,
    int deleted,
    int errors,
    boolean dryRun,
    long lastId,
    boolean limitReached
) {
}
