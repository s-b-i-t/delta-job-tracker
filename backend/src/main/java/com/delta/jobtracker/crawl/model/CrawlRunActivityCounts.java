package com.delta.jobtracker.crawl.model;

public record CrawlRunActivityCounts(
    long discoveredUrls,
    long discoveredSitemaps,
    long jobPostings
) {
    public long totalActivity() {
        return discoveredUrls + discoveredSitemaps + jobPostings;
    }
}
