package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.util.Map;

public record CareersDiscoveryRunStatus(
    long discoveryRunId,
    Instant startedAt,
    Instant finishedAt,
    String status,
    int companyLimit,
    int processedCount,
    int succeededCount,
    int failedCount,
    int endpointsAdded,
    String lastError,
    int companiesConsidered,
    int homepageScanned,
    Map<String, Integer> endpointsFoundHomepageByAtsType,
    Map<String, Integer> endpointsFoundVendorProbeByAtsType,
    int sitemapsScanned,
    int sitemapUrlsChecked,
    Map<String, Integer> endpointsFoundSitemapByAtsType,
    int careersPathsChecked,
    int robotsBlockedCount,
    int fetchFailedCount,
    int timeBudgetExceededCount,
    Map<String, Long> failuresByReason) {}
