package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.context.WebApplicationContext;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class JobsListViewEndpointTest {

    @Autowired
    private WebApplicationContext context;

    @Autowired
    private CrawlJdbcRepository repository;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void listEndpointsDoNotReturnDescription() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("JL" + suffix, "Jobs List Co " + suffix, "Technology");

        Instant runStart = Instant.now().minusSeconds(120);
        long runId = repository.insertCrawlRun(runStart, "RUNNING", "run1");
        NormalizedJobPosting posting = new NormalizedJobPosting(
            "https://example.com/jobs/a",
            "https://example.com/jobs/a",
            "Software Engineer",
            "Jobs List Co",
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-01"),
            "<p>Detail HTML</p>",
            "req-a",
            "hash-a-" + UUID.randomUUID()
        );
        repository.upsertJobPosting(companyId, runId, posting, runStart.plusSeconds(5));
        repository.completeCrawlRun(runId, runStart.plusSeconds(30), "COMPLETED", "run1 done");

        mockMvc.perform(get("/api/jobs").param("companyId", String.valueOf(companyId)).param("active", "true"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].descriptionText").doesNotExist())
            .andExpect(jsonPath("$[0].contentHash").doesNotExist());
    }
}
