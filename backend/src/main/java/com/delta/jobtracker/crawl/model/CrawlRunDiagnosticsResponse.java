package com.delta.jobtracker.crawl.model;

import java.util.List;

public record CrawlRunDiagnosticsResponse(
    int limit, int totalRuns, List<CrawlRunDiagnosticsEntry> runs) {}
