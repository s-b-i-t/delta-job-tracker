package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record JobDeltaItem(
    long id,
    String title,
    String locationText,
    String sourceUrl,
    AtsType atsType,
    Instant firstSeenAt,
    Instant lastSeenAt
) {
}
