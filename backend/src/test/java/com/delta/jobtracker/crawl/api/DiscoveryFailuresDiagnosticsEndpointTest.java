package com.delta.jobtracker.crawl.api;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
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
class DiscoveryFailuresDiagnosticsEndpointTest {

  @Autowired private WebApplicationContext context;

  @Autowired private CrawlJdbcRepository repository;

  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
  }

  @Test
  void returnsCountsAndRecentFailures() throws Exception {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("DF" + suffix, "Discovery Fail Co " + suffix, "Technology");
    String candidateUrl = "https://example.com/careers/" + suffix.toLowerCase();
    String detail = "http_403";
    repository.insertCareersDiscoveryFailure(
        companyId, "discovery_fetch_failed", candidateUrl, detail, Instant.now());

    mockMvc
        .perform(get("/api/diagnostics/discovery-failures"))
        .andExpect(status().isOk())
        .andExpect(
            jsonPath("$.countsByReason.discovery_fetch_failed").value(greaterThanOrEqualTo(1)))
        .andExpect(
            jsonPath("$.recentFailures[?(@.ticker=='DF" + suffix + "')].candidateUrl")
                .value(hasItem(candidateUrl)))
        .andExpect(
            jsonPath("$.recentFailures[?(@.ticker=='DF" + suffix + "')].detail")
                .value(hasItem(detail)));
  }
}
