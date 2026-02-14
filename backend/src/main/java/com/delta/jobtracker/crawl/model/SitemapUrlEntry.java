package com.delta.jobtracker.crawl.model;

public record SitemapUrlEntry(
    String url,
    String lastmod
) {
}
