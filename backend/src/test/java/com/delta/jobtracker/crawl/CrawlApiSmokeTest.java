package com.delta.jobtracker.crawl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@ActiveProfiles("test")
class CrawlApiSmokeTest {

    @Autowired
    private WebApplicationContext context;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void ingestEndpointIsPostOnly() throws Exception {
        mockMvc.perform(get("/api/ingest"))
            .andExpect(status().isMethodNotAllowed());
    }

    @Test
    void ingestThenStatusShowsCompanyCounts() throws Exception {
        mockMvc.perform(post("/api/ingest").param("source", "file"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.companiesUpserted").value(greaterThanOrEqualTo(500)))
            .andExpect(jsonPath("$.domainsSeeded").value(greaterThanOrEqualTo(20)))
            .andExpect(jsonPath("$.errorsCount").value(greaterThanOrEqualTo(0)));

        mockMvc.perform(get("/api/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.dbConnectivity").value(true))
            .andExpect(jsonPath("$.counts.companies").value(greaterThan(0)))
            .andExpect(jsonPath("$.counts.company_domains").value(greaterThan(0)))
            .andExpect(jsonPath("$.counts.job_postings").value(0));
    }

    @Test
    void jobsEndpointReturnsEmptyListWhenNoJobs() throws Exception {
        mockMvc.perform(get("/api/jobs"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$").isArray())
            .andExpect(jsonPath("$.length()").value(0));
    }
}
