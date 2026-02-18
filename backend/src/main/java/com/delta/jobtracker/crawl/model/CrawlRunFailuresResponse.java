package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record CrawlRunFailuresResponse(
    Map<String, Long> countsByReason,
    List<CrawlRunCompanyFailureView> failures
) {
}
