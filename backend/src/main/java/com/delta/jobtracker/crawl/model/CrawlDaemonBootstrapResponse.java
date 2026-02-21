package com.delta.jobtracker.crawl.model;

public record CrawlDaemonBootstrapResponse(
    String source, IngestionSummary ingestion, int queuedCount) {}
