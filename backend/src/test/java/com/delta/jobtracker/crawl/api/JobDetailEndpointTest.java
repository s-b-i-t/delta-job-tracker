package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobDetailEndpointTest {

    @Autowired
    private CrawlController controller;

    @Autowired
    private CrawlJdbcRepository repository;

    @Test
    void jobDetailReturnsDescription() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("JD" + suffix, "Jobs Detail Co " + suffix, "Technology");

        Instant runStart = Instant.now().minusSeconds(120);
        long runId = repository.insertCrawlRun(runStart, "RUNNING", "run1");

        NormalizedJobPosting posting = new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Software Engineer",
            "Jobs Detail Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "<p>Detail HTML</p>",
            "req-a",
            "hash-a-" + UUID.randomUUID()
        );

        repository.upsertJobPosting(companyId, runId, posting, runStart.plusSeconds(5));
        repository.completeCrawlRun(runId, runStart.plusSeconds(30), "COMPLETED", "run1 done");

        JobPostingListView listItem = repository.findNewestJobs(10, companyId, null, true, null).getFirst();
        var detail = controller.getJobDetail(listItem.id());

        assertNotNull(detail);
        assertEquals("https://example.com/jobs/a", detail.canonicalUrl());
        assertEquals("<p>Detail HTML</p>", detail.descriptionText());
    }
}
