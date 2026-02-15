package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsAdapterResult;
import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AtsAdapterIngestionServiceTest {

    @Mock
    private PoliteHttpClient httpClient;
    @Mock
    private RobotsTxtService robotsTxtService;
    @Mock
    private CrawlJdbcRepository repository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private AtsAdapterIngestionService service;

    @BeforeEach
    void setUp() {
        service = new AtsAdapterIngestionService(httpClient, robotsTxtService, repository, objectMapper);
    }

    @Test
    void ingestsGreenhouseJobs() {
        CompanyTarget company = new CompanyTarget(1L, "UBER", "Uber", null, "uber.com", null);
        AtsEndpointRecord endpoint = new AtsEndpointRecord(
            1L,
            AtsType.GREENHOUSE,
            "https://boards.greenhouse.io/uber",
            null,
            0.9,
            Instant.now()
        );
        String feedUrl = "https://boards-api.greenhouse.io/v1/boards/uber/jobs?content=true";
        String fallbackUrl = "https://api.greenhouse.io/v1/boards/uber/jobs?content=true";
        when(robotsTxtService.isAllowedForAtsAdapter(anyString())).thenReturn(true);
        when(httpClient.get(eq(feedUrl), anyString())).thenReturn(successFetch(feedUrl, greenhousePayload()));

        AtsAdapterResult result = service.ingestIfSupported(10L, company, List.of(endpoint));

        assertThat(result).isNotNull();
        assertThat(result.jobsExtractedCount()).isEqualTo(1);
        verify(repository, atLeastOnce()).upsertJobPosting(eq(1L), eq(10L), any(NormalizedJobPosting.class), any(Instant.class));
        verify(httpClient, org.mockito.Mockito.never()).get(eq(fallbackUrl), anyString());
    }

    @Test
    void ingestsLeverJobs() {
        CompanyTarget company = new CompanyTarget(2L, "BKNG", "Booking Holdings", null, "bookingholdings.com", null);
        AtsEndpointRecord endpoint = new AtsEndpointRecord(
            2L,
            AtsType.LEVER,
            "https://jobs.lever.co/booking",
            null,
            0.9,
            Instant.now()
        );
        String feedUrl = "https://api.lever.co/v0/postings/booking?mode=json";
        when(robotsTxtService.isAllowedForAtsAdapter(anyString())).thenReturn(true);
        when(httpClient.get(eq(feedUrl), anyString())).thenReturn(successFetch(feedUrl, leverPayload()));

        AtsAdapterResult result = service.ingestIfSupported(11L, company, List.of(endpoint));

        assertThat(result).isNotNull();
        assertThat(result.jobsExtractedCount()).isEqualTo(1);
        verify(repository, atLeastOnce()).upsertJobPosting(eq(2L), eq(11L), any(NormalizedJobPosting.class), any(Instant.class));
    }

    @Test
    void ingestsWorkdayJobs() throws Exception {
        CompanyTarget company = new CompanyTarget(3L, "WMT", "Walmart", null, "walmart.com", null);
        AtsEndpointRecord endpoint = new AtsEndpointRecord(
            3L,
            AtsType.WORKDAY,
            "https://walmart.wd5.myworkdayjobs.com/WalmartExternal",
            null,
            0.9,
            Instant.now()
        );
        String cxsUrl = "https://walmart.wd5.myworkdayjobs.com/wday/cxs/walmart/WalmartExternal/jobs";
        String payload = Files.readString(Path.of("src/test/resources/fixtures/workday-cxs-response.json"));
        when(robotsTxtService.isAllowedForAtsAdapter(anyString())).thenReturn(true);
        when(httpClient.postJson(eq(cxsUrl), anyString(), anyString())).thenReturn(successFetch(cxsUrl, payload));

        AtsAdapterResult result = service.ingestIfSupported(12L, company, List.of(endpoint));

        assertThat(result).isNotNull();
        assertThat(result.jobsExtractedCount()).isEqualTo(2);
        verify(repository, atLeastOnce()).upsertJobPosting(eq(3L), eq(12L), any(NormalizedJobPosting.class), any(Instant.class));
    }

    private HttpFetchResult successFetch(String url, String body) {
        return new HttpFetchResult(
            url,
            null,
            200,
            body,
            body == null ? null : body.getBytes(),
            "application/json",
            null,
            Instant.now(),
            Duration.ZERO,
            null,
            null
        );
    }

    private String greenhousePayload() {
        return """
            {"jobs":[
              {"id":123,"title":"Software Engineer","absolute_url":"https://boards.greenhouse.io/uber/jobs/123",
               "content":"<p>Build things</p>","location":{"name":"San Francisco, CA"},"updated_at":"2024-01-01"}
            ]}
            """;
    }

    private String leverPayload() {
        return """
            [
              {"id":"abc123","text":"Data Engineer","hostedUrl":"https://jobs.lever.co/booking/abc123",
               "descriptionPlain":"Build pipelines","categories":{"location":"New York, NY","commitment":"Full-time"},
               "createdAt":1700000000000}
            ]
            """;
    }
}
