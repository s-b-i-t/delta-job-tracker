package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record SitemapFetchRecord(
    String sitemapUrl,
    Instant fetchedAt,
    int urlCount
) {
}
