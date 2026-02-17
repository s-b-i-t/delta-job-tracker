package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobPostingInvalidCanonicalUrlRejectedTest {

    @Autowired
    private CrawlJdbcRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbc;

    @Test
    void skipsInvalidWorkdayCanonicalUrl() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("IW" + suffix, "Invalid Workday Co " + suffix, "Technology");
        long runId = repository.insertCrawlRun(Instant.now(), "RUNNING", "invalid-url");

        NormalizedJobPosting posting = new NormalizedJobPosting(
            "https://community.workday.com/invalid-url/foo",
            "https://community.workday.com/invalid-url/foo",
            "Senior Engineer",
            "Invalid Workday Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-02-01"),
            "desc",
            "req-123",
            "hash-" + UUID.randomUUID()
        );

        repository.upsertJobPosting(companyId, runId, posting, Instant.now());

        Long count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM job_postings
                WHERE company_id = :companyId
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            Long.class
        );

        assertEquals(0L, count == null ? 0L : count);
    }
}
