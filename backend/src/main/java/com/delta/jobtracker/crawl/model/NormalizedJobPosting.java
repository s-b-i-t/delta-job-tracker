package com.delta.jobtracker.crawl.model;

import java.time.LocalDate;

public record NormalizedJobPosting(
    String sourceUrl,
    String canonicalUrl,
    String title,
    String orgName,
    String locationText,
    String employmentType,
    LocalDate datePosted,
    String descriptionText,
    String externalIdentifier,
    String contentHash
) {
}
