package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record CompanyCrawlSummary(
    long companyId,
    String ticker,
    String domain,
    int sitemapsFoundCount,
    int candidateUrlsCount,
    List<AtsDetectionRecord> atsDetected,
    int jobpostingPagesFoundCount,
    int jobsExtractedCount,
    boolean closeoutSafe,
    Map<String, Integer> topErrors) {}
