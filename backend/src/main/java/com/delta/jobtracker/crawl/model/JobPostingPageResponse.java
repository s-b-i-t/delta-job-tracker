package com.delta.jobtracker.crawl.model;

import java.util.List;

public record JobPostingPageResponse(
    long total,
    int page,
    int pageSize,
    List<JobPostingListView> items
) {
}
