package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class AtsAdapterWorkdayParserTest {

    @Mock
    private PoliteHttpClient httpClient;
    @Mock
    private RobotsTxtService robotsTxtService;
    @Mock
    private CrawlJdbcRepository repository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void parsesWorkdayCxsPayloadIntoNormalizedJobs() throws Exception {
        AtsAdapterIngestionService service = new AtsAdapterIngestionService(httpClient, robotsTxtService, repository, objectMapper);

        String fixture = Files.readString(Path.of("src/test/resources/fixtures/workday-cxs-response.json"));
        JsonNode root = objectMapper.readTree(fixture);
        AtsAdapterIngestionService.WorkdayEndpoint endpoint =
            new AtsAdapterIngestionService.WorkdayEndpoint("acme.wd5.myworkdayjobs.com", "acme", "External");
        List<NormalizedJobPosting> postings = service.parseWorkdayJobPostings(
            endpoint,
            root,
            "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/jobs",
            new java.util.LinkedHashMap<>(),
            new java.util.concurrent.atomic.AtomicInteger(0)
        );

        assertEquals(2, postings.size());
        assertEquals("Senior Software Engineer", postings.get(0).title());
        assertEquals("https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote-USA/Senior-Software-Engineer_R12345", postings.get(0).canonicalUrl());
        assertEquals("R12345", postings.get(0).externalIdentifier());
        assertTrue(postings.get(1).locationText().contains("New York, NY"));
    }
}
