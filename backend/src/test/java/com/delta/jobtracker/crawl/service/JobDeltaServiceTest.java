package com.delta.jobtracker.crawl.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.delta.jobtracker.crawl.model.JobDeltaResponse;
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
class JobDeltaServiceTest {

  @Autowired private CrawlJdbcRepository repository;

  @Autowired private CrawlStatusService crawlStatusService;

  @Test
  void reportsRemovedJobsBetweenRuns() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("JD" + suffix, "Job Delta Co " + suffix, "Technology");

    Instant run1Start = Instant.now().minusSeconds(120);
    long run1Id = repository.insertCrawlRun(run1Start, "RUNNING", "run1");
    Instant run1Finish = run1Start.plusSeconds(30);

    NormalizedJobPosting postingA =
        new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Engineer A",
            "Job Delta Co",
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
            "Job Delta Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-02"),
            "desc B",
            "req-b",
            "hash-b-" + UUID.randomUUID());

    repository.upsertJobPosting(companyId, run1Id, postingA, run1Start.plusSeconds(5));
    repository.upsertJobPosting(companyId, run1Id, postingB, run1Start.plusSeconds(6));
    repository.completeCrawlRun(run1Id, run1Finish, "COMPLETED", "run1 done");

    Instant run2Start = run1Finish.plusSeconds(10);
    long run2Id = repository.insertCrawlRun(run2Start, "RUNNING", "run2");
    Instant run2Finish = run2Start.plusSeconds(30);

    repository.upsertJobPosting(companyId, run2Id, postingA, run2Start.plusSeconds(5));
    repository.markPostingsInactiveNotSeenInRun(companyId, run2Id);
    repository.completeCrawlRun(run2Id, run2Finish, "COMPLETED", "run2 done");

    JobDeltaResponse delta = crawlStatusService.getJobDelta(companyId, run1Id, run2Id, 50);
    assertNotNull(delta);
    assertEquals(0, delta.newCount());
    assertEquals(1, delta.removedCount());
    assertEquals(0, delta.updatedCount());
    assertEquals(1, delta.removed().size());
    assertEquals("Engineer B", delta.removed().getFirst().title());
  }
}
