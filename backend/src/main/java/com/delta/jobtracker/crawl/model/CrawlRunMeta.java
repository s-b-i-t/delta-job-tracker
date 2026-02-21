package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CrawlRunMeta(long crawlRunId, Instant startedAt, Instant finishedAt, String status) {}
