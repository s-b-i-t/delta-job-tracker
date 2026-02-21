package com.delta.jobtracker.crawl.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobPostingCanonicalUrlUpdateTest {

  @Autowired private CrawlJdbcRepository repository;

  @Autowired private NamedParameterJdbcTemplate jdbc;

  @Test
  void updatesCanonicalUrlWhenExternalIdentifierMatches() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("CU" + suffix, "Canonical Update Co " + suffix, "Technology");
    long runId = repository.insertCrawlRun(Instant.now().minusSeconds(120), "RUNNING", "canonical");

    NormalizedJobPosting initial =
        new NormalizedJobPosting(
            "https://boards-api.greenhouse.io/v1/boards/acme/jobs?content=true",
            "https://boards.greenhouse.io/acme/jobs/old",
            "Senior Engineer",
            "Canonical Update Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-05"),
            "desc",
            "req-123",
            "hash-" + UUID.randomUUID());
    repository.upsertJobPosting(companyId, runId, initial, Instant.now().minusSeconds(30));

    NormalizedJobPosting updated =
        new NormalizedJobPosting(
            "https://boards.greenhouse.io/acme/jobs/123",
            "https://boards.greenhouse.io/acme/jobs/123",
            "Senior Engineer",
            "Canonical Update Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-05"),
            "desc",
            "req-123",
            "hash-" + UUID.randomUUID());
    repository.upsertJobPosting(companyId, runId, updated, Instant.now());

    String canonical =
        jdbc.queryForObject(
            """
                SELECT canonical_url
                FROM job_postings
                WHERE company_id = :companyId
                  AND external_identifier = :externalIdentifier
                """,
            new MapSqlParameterSource()
                .addValue("companyId", companyId)
                .addValue("externalIdentifier", "req-123"),
            String.class);

    assertEquals("https://boards.greenhouse.io/acme/jobs/123", canonical);
  }
}
