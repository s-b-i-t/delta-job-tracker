package com.delta.jobtracker.crawl.model;

import java.util.List;

public record SecUniverseIngestionSummary(
    int inserted,
    int updated,
    int total,
    int errorCount,
    List<String> sampleErrors
) {
}
