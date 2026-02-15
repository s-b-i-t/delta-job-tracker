package com.delta.jobtracker.crawl.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record JobDeltaResponse(
    long companyId,
    long fromRunId,
    long toRunId,
    int newCount,
    int removedCount,
    int updatedCount,
    @JsonProperty("new") List<JobDeltaItem> newJobs,
    List<JobDeltaItem> removed,
    List<JobDeltaItem> updated
) {
}
