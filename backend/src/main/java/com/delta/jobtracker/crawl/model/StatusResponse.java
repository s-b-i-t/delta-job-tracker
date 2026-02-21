package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record StatusResponse(
    boolean dbConnectivity, Map<String, Long> counts, RecentCrawlStatus mostRecentCrawlRun) {}
