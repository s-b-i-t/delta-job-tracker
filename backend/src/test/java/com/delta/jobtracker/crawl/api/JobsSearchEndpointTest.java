package com.delta.jobtracker.crawl.api;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobsSearchEndpointTest {

    @Autowired
    private CrawlController controller;

    @Autowired
    private CrawlJdbcRepository repository;

    @Test
    void jobsSearchFiltersByQuery() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("JS" + suffix, "Jobs Search Co " + suffix, "Technology");

        Instant runStart = Instant.now().minusSeconds(120);
        long runId = repository.insertCrawlRun(runStart, "RUNNING", "run1");

        NormalizedJobPosting postingA = new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Software Engineer",
            "Jobs Search Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "desc A",
            "req-a",
            "hash-a-" + UUID.randomUUID()
        );
        NormalizedJobPosting postingB = new NormalizedJobPosting(
            "https://example.com/jobs/b",
            "https://example.com/jobs/b",
            "Account Executive",
            "Jobs Search Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-02"),
            "desc B",
            "req-b",
            "hash-b-" + UUID.randomUUID()
        );

        repository.upsertJobPosting(companyId, runId, postingA, runStart.plusSeconds(5));
        repository.upsertJobPosting(companyId, runId, postingB, runStart.plusSeconds(6));
        repository.completeCrawlRun(runId, runStart.plusSeconds(30), "COMPLETED", "run1 done");

        assertEquals(2L, repository.countTable("job_postings"));

        var allJobs = controller.getJobs(50, companyId, null, true, null);
        assertEquals(2, allJobs.size());

        var payload = controller.getJobs(50, companyId, null, true, "software");
        assertEquals(1, payload.size());
        assertTrue(payload.getFirst().title().contains("Software"));
    }
}
