package com.delta.jobtracker.crawl.model;

public record CompanyTarget(
    long companyId,
    String ticker,
    String name,
    String sector,
    String domain,
    String careersHintUrl
) {
}
