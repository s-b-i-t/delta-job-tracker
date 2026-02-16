package com.delta.jobtracker.crawl.api;

import java.util.List;

public record CrawlApiRunRequest(
    List<String> tickers,
    Integer companyLimit,
    Integer resolveLimit,
    Integer discoverLimit,
    Integer maxJobPages,
    Integer maxSitemapUrls,
    Boolean ingestBeforeCrawl,
    Boolean resolveDomains,
    Boolean discoverCareers,
    Boolean atsOnly
) {
}
