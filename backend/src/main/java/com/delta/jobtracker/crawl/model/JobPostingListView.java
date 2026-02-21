package com.delta.jobtracker.crawl.model;

import java.time.Instant;
import java.time.LocalDate;

public record JobPostingListView(
    long id,
    long companyId,
    String ticker,
    String companyName,
    AtsType atsType,
    String sourceUrl,
    String canonicalUrl,
    String title,
    String orgName,
    String locationText,
    String employmentType,
    LocalDate datePosted,
    Instant firstSeenAt,
    Instant lastSeenAt,
    boolean isActive) {}
