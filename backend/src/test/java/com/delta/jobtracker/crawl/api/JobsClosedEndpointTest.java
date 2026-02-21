package com.delta.jobtracker.crawl.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobsClosedEndpointTest {

  @Autowired private CrawlController controller;

  @Autowired private CrawlJdbcRepository repository;

  @Test
  void closedJobsEndpointReturnsInactivePostings() throws Exception {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("JC" + suffix, "Jobs Closed Co " + suffix, "Technology");

    Instant run1Start = Instant.now().minusSeconds(120);
    long run1Id = repository.insertCrawlRun(run1Start, "RUNNING", "run1");
    NormalizedJobPosting postingA =
        new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Engineer A",
            "Jobs Closed Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "desc A",
            "req-a",
            "hash-a-" + UUID.randomUUID());
    NormalizedJobPosting postingB =
        new NormalizedJobPosting(
            "https://example.com/jobs/b",
            "https://example.com/jobs/b",
            "Engineer B",
            "Jobs Closed Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-02"),
            "desc B",
            "req-b",
            "hash-b-" + UUID.randomUUID());
    repository.upsertJobPosting(companyId, run1Id, postingA, run1Start.plusSeconds(5));
    repository.upsertJobPosting(companyId, run1Id, postingB, run1Start.plusSeconds(6));
    repository.completeCrawlRun(run1Id, run1Start.plusSeconds(30), "COMPLETED", "run1 done");

    Instant run2Start = run1Start.plusSeconds(60);
    long run2Id = repository.insertCrawlRun(run2Start, "RUNNING", "run2");
    repository.upsertJobPosting(companyId, run2Id, postingA, run2Start.plusSeconds(5));
    repository.markPostingsInactiveNotSeenInRun(companyId, run2Id);
    repository.completeCrawlRun(run2Id, run2Start.plusSeconds(30), "COMPLETED", "run2 done");

    String since = run1Start.minusSeconds(1).toString();
    var payload = controller.getClosedJobs(since, companyId, 50, null);
    assertEquals(1, payload.size());
    assertTrue(payload.getFirst().title().contains("Engineer B"));
    assertEquals(false, payload.getFirst().isActive());
  }
}
