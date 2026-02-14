package com.delta.jobtracker.crawl.model;

public record CompanyIdentity(
    long companyId,
    String ticker,
    String name,
    String sector,
    String wikipediaTitle
) {
}
