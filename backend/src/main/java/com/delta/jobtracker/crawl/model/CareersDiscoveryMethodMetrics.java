package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record CareersDiscoveryMethodMetrics(
    int homepageScanned,
    int careersPathsChecked,
    int robotsBlockedCount,
    int fetchFailedCount,
    int timeBudgetExceededCount,
    int sitemapsScanned,
    int sitemapUrlsChecked,
    Map<String, Integer> endpointsFoundHomepageByAtsType,
    Map<String, Integer> endpointsFoundVendorProbeByAtsType,
    Map<String, Integer> endpointsFoundSitemapByAtsType) {}
