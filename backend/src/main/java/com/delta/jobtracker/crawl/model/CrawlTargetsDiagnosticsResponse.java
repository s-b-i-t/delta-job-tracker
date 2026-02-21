package com.delta.jobtracker.crawl.model;

import java.util.List;

public record CrawlTargetsDiagnosticsResponse(
    boolean atsOnly, int limit, int total, List<String> tickers) {}
