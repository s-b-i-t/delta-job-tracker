package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.DiscoveredUrlType;
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
class CoverageDiagnosticsEndpointTest {

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
    void returnsCoverageCountsAndAtsBreakdown() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("DC" + suffix, "Diagnostics Co " + suffix, "Technology");
        repository.upsertCompanyDomain(companyId, "diag" + suffix.toLowerCase() + ".example.com", null);

        Instant runStart = Instant.now().minusSeconds(30);
        long runId = repository.insertCrawlRun(runStart, "RUNNING", "diag");
        repository.upsertDiscoveredUrl(
            runId,
            companyId,
            "https://diag" + suffix.toLowerCase() + ".example.com/careers",
            DiscoveredUrlType.OTHER,
            "discovered",
            runStart
        );

        repository.upsertAtsEndpoint(
            companyId,
            AtsType.GREENHOUSE,
            "https://boards.greenhouse.io/diagnostics" + suffix.toLowerCase(),
            "https://diag" + suffix.toLowerCase() + ".example.com/careers",
            0.9,
            Instant.now(),
            "test",
            true
        );

        NormalizedJobPosting posting = new NormalizedJobPosting(
            "https://boards.greenhouse.io/diagnostics" + suffix.toLowerCase() + "/jobs/123",
            "https://boards.greenhouse.io/diagnostics" + suffix.toLowerCase() + "/jobs/123",
            "Test Role",
            "Diagnostics Co " + suffix,
            "Remote",
            "FULL_TIME",
            LocalDate.parse("2026-01-10"),
            "<p>Testing</p>",
            "req-" + suffix,
            "hash-" + UUID.randomUUID()
        );
        repository.upsertJobPosting(companyId, runId, posting, runStart.plusSeconds(5));

        mockMvc.perform(get("/api/diagnostics/coverage"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.counts.company_domains").value(1))
            .andExpect(jsonPath("$.counts.discovered_urls").value(1))
            .andExpect(jsonPath("$.counts.ats_endpoints").value(1))
            .andExpect(jsonPath("$.counts.job_postings").value(1))
            .andExpect(jsonPath("$.atsEndpointsByType.GREENHOUSE").value(1));
    }
}
