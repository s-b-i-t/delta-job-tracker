package com.delta.jobtracker.crawl.model;

import java.util.ArrayList;
import java.util.List;

public record CrawlRunRequest(
    List<String> tickers,
    Integer companyLimit,
    Integer resolveLimit,
    Integer discoverLimit,
    Integer maxJobPages,
    Integer maxSitemapUrls,
    Boolean resolveDomains,
    Boolean discoverCareers,
    Boolean atsOnly,
    java.time.Instant atsDetectedSince
) {
    public List<String> normalizedTickers() {
        if (tickers == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String ticker : tickers) {
            if (ticker == null) {
                continue;
            }
            String normalized = ticker.trim().toUpperCase();
            if (!normalized.isEmpty()) {
                out.add(normalized);
            }
        }
        return out;
    }
}
