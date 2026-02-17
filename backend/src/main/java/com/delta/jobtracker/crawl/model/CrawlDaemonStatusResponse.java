package com.delta.jobtracker.crawl.model;

public record CrawlDaemonStatusResponse(
    boolean running,
    int workerCount,
    CrawlQueueStats queueStats
) {
}
