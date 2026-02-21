package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlRunDiagnosticsEntry(
    long crawlRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    long discoveredUrls,
    long discoveredSitemaps,
    long jobPostings) {}
