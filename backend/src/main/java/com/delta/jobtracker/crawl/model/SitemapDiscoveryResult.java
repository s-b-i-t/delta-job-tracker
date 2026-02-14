package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record SitemapDiscoveryResult(
    List<SitemapFetchRecord> fetchedSitemaps,
    List<SitemapUrlEntry> discoveredUrls,
    Map<String, Integer> errors
) {
}
