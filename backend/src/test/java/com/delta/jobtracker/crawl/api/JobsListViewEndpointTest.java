package com.delta.jobtracker.crawl.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobsListViewEndpointTest {

  @Autowired private WebApplicationContext context;

  @Autowired private CrawlJdbcRepository repository;

  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
  }

  @Test
  void listEndpointsDoNotReturnDescription() throws Exception {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("JL" + suffix, "Jobs List Co " + suffix, "Technology");

    Instant runStart = Instant.now().minusSeconds(120);
    long runId = repository.insertCrawlRun(runStart, "RUNNING", "run1");
    NormalizedJobPosting postingA =
        new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Software Engineer",
            "Jobs List Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "<p>Detail HTML</p>",
            "req-a",
            "hash-a-" + UUID.randomUUID());
    NormalizedJobPosting postingB =
        new NormalizedJobPosting(
            "https://example.com/jobs/b",
            "https://example.com/jobs/b",
            "Data Analyst",
            "Jobs List Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-02"),
            "<p>Detail HTML</p>",
            "req-b",
            "hash-b-" + UUID.randomUUID());
    repository.upsertJobPosting(companyId, runId, postingA, runStart.plusSeconds(5));
    repository.upsertJobPosting(companyId, runId, postingB, runStart.plusSeconds(6));
    repository.completeCrawlRun(runId, runStart.plusSeconds(30), "COMPLETED", "run1 done");

    Instant run2Start = runStart.plusSeconds(60);
    long run2Id = repository.insertCrawlRun(run2Start, "RUNNING", "run2");
    repository.upsertJobPosting(companyId, run2Id, postingA, run2Start.plusSeconds(5));
    repository.markPostingsInactiveNotSeenInRun(companyId, run2Id);
    repository.completeCrawlRun(run2Id, run2Start.plusSeconds(30), "COMPLETED", "run2 done");

    String since = runStart.minusSeconds(1).toString();

    mockMvc
        .perform(
            get("/api/jobs").param("companyId", String.valueOf(companyId)).param("active", "true"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].canonicalUrl").value("https://example.com/jobs/a"))
        .andExpect(jsonPath("$[0].descriptionText").doesNotExist())
        .andExpect(jsonPath("$[0].contentHash").doesNotExist());

    mockMvc
        .perform(
            get("/api/jobs/new")
                .param("companyId", String.valueOf(companyId))
                .param("since", since))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].canonicalUrl").exists())
        .andExpect(jsonPath("$[0].descriptionText").doesNotExist())
        .andExpect(jsonPath("$[0].contentHash").doesNotExist());

    mockMvc
        .perform(
            get("/api/jobs/closed")
                .param("companyId", String.valueOf(companyId))
                .param("since", since))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].canonicalUrl").exists())
        .andExpect(jsonPath("$[0].descriptionText").doesNotExist())
        .andExpect(jsonPath("$[0].contentHash").doesNotExist());
  }
}
