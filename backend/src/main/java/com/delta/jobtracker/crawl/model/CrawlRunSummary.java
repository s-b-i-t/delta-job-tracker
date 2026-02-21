package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.List;

public record CrawlRunSummary(
    long crawlRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    List<CompanyCrawlSummary> companies) {}
