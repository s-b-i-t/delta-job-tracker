package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
class CrawlJdbcRepositoryJobPostingRunAttributionTest {

    @Autowired
    private CrawlJdbcRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbc;

    @Test
    void upsertJobPostingUsesLastSeenCrawlRunAttribution() {
        String suffix = UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        long companyId = repository.upsertCompany("ZZ" + suffix, "Run Attribution Co " + suffix, "Technology");

        long firstRunId = repository.insertCrawlRun(Instant.now().minusSeconds(120), "RUNNING", "first run");
        long secondRunId = repository.insertCrawlRun(Instant.now().minusSeconds(60), "RUNNING", "second run");

        String contentHash = "hash-" + UUID.randomUUID();
        NormalizedJobPosting posting = new NormalizedJobPosting(
            "https://example.com/jobs/123",
            "https://example.com/jobs/123",
            "Senior Engineer",
            "Run Attribution Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "example description",
            "req-123",
            contentHash
        );

        repository.upsertJobPosting(companyId, firstRunId, posting, Instant.now().minusSeconds(30));
        repository.upsertJobPosting(companyId, secondRunId, posting, Instant.now());

        Long storedRunId = jdbc.queryForObject(
            """
                SELECT crawl_run_id
                FROM job_postings
                WHERE company_id = :companyId
                  AND content_hash = :contentHash
                """,
            new MapSqlParameterSource()
                .addValue("companyId", companyId)
                .addValue("contentHash", contentHash),
            Long.class
        );

        assertEquals(secondRunId, storedRunId);
    }
}
