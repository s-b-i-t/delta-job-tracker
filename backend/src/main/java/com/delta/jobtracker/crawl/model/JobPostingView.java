package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.time.LocalDate;

public record JobPostingView(
    long id,
    long companyId,
    String ticker,
    String companyName,
    AtsType atsType,
    String sourceUrl,
    String title,
    String orgName,
    String locationText,
    String employmentType,
    LocalDate datePosted,
    String descriptionText,
    String contentHash,
    Instant firstSeenAt,
    Instant lastSeenAt
) {
}
