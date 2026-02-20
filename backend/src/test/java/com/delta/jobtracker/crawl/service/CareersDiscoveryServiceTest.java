package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsEndpointExtractor;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CareersDiscoveryServiceTest {

    @Mock
    private CrawlJdbcRepository repository;
    @Mock
    private PoliteHttpClient httpClient;
    @Mock
    private RobotsTxtService robotsTxtService;
    @Mock
    private AtsDetector atsDetector;
    @Mock
    private HostCrawlStateService hostCrawlStateService;

    private CareersDiscoveryService service;

    @BeforeEach
    void setUp() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.getCareersDiscovery().setMaxSlugCandidates(6);
        properties.getCareersDiscovery().setMaxCareersPaths(2);
        properties.getCareersDiscovery().setRobotsCooldownDays(14);
        properties.getCareersDiscovery().setFailureBackoffMinutes(List.of(5, 15));
        service = new CareersDiscoveryService(
            properties,
            repository,
            httpClient,
            robotsTxtService,
            atsDetector,
            new AtsEndpointExtractor(),
            hostCrawlStateService
        );
    }

    @Test
    void homepageScanFindsAtsLinkWithoutCareersProbe() {
        CompanyTarget company = new CompanyTarget(1L, "ACME", "Acme Corp", null, "acme.com", null);
        String homepage = "https://acme.com/";
        when(repository.findCareersDiscoveryState(1L)).thenReturn(null);
        when(robotsTxtService.isAllowed(homepage)).thenReturn(true);
        when(httpClient.get(eq(homepage), anyString()))
            .thenReturn(successHtml(homepage, "<a href=\"https://boards.greenhouse.io/acme\">Careers</a>"));

        CareersDiscoveryService.DiscoveryOutcome outcome = service.discoverForCompany(company, null, null, null, false);

        assertThat(outcome.hasEndpoints()).isTrue();
        verify(repository).upsertAtsEndpoint(
            eq(1L),
            eq(AtsType.GREENHOUSE),
            eq("https://boards.greenhouse.io/acme"),
            eq(homepage),
            eq(0.95),
            any(Instant.class),
            eq("homepage_link"),
            eq(true)
        );
        verify(httpClient, never()).get(eq("https://acme.com/careers"), anyString());
    }

    @Test
    void vendorProbeOnlyPersistsEndpoint() {
        CompanyTarget company = new CompanyTarget(2L, "ACME", "Acme Corp", null, "acme.com", null);
        String vendorUrl = "https://boards.greenhouse.io/acme";
        when(repository.findCareersDiscoveryState(2L)).thenReturn(null);
        when(httpClient.get(eq(vendorUrl), anyString()))
            .thenReturn(successHtml(vendorUrl, "<html>Greenhouse Jobs</html>"));

        CareersDiscoveryService.DiscoveryOutcome outcome = service.discoverForCompany(company, null, null, null, true);

        assertThat(outcome.hasEndpoints()).isTrue();
        verify(repository).upsertAtsEndpoint(
            eq(2L),
            eq(AtsType.GREENHOUSE),
            eq(vendorUrl),
            eq(vendorUrl),
            eq(0.9),
            any(Instant.class),
            eq("vendor_probe"),
            eq(true)
        );
        verify(httpClient, never()).get(eq("https://acme.com/"), anyString());
    }

    @Test
    void robotsBlockedCareersPathsAreSkippedAndCached() {
        CompanyTarget company = new CompanyTarget(3L, "ACME", "Acme Corp", null, "acme.com", null);
        String homepage = "https://acme.com/";
        when(repository.findCareersDiscoveryState(3L)).thenReturn(null);
        when(robotsTxtService.isAllowed(homepage)).thenReturn(true);
        when(robotsTxtService.isAllowed("https://acme.com/careers")).thenReturn(false);
        when(httpClient.get(anyString(), anyString()))
            .thenReturn(failureFetch(homepage));
        when(httpClient.get(eq(homepage), anyString()))
            .thenReturn(successHtml(homepage, "<html>No ATS</html>"));

        service.discoverForCompany(company, null, null, null, false);

        verify(httpClient, never()).get(eq("https://acme.com/careers"), anyString());
        verify(repository).insertCareersDiscoveryFailure(
            eq(3L),
            eq("discovery_blocked_by_robots"),
            eq("https://acme.com/careers"),
            any(),
            any(Instant.class)
        );
        ArgumentCaptor<Instant> nextAttemptCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(repository).upsertCareersDiscoveryState(
            eq(3L),
            any(Instant.class),
            eq("discovery_blocked_by_robots"),
            eq("https://acme.com/careers"),
            eq(1),
            nextAttemptCaptor.capture()
        );
        assertThat(nextAttemptCaptor.getValue()).isAfter(Instant.now());
    }

    private HttpFetchResult successHtml(String url, String body) {
        return new HttpFetchResult(
            url,
            URI.create(url),
            200,
            body,
            body == null ? null : body.getBytes(),
            "text/html",
            null,
            Instant.now(),
            Duration.ofMillis(5),
            null,
            null
        );
    }

    private HttpFetchResult failureFetch(String url) {
        return new HttpFetchResult(
            url,
            URI.create(url),
            503,
            null,
            null,
            null,
            null,
            Instant.now(),
            Duration.ofMillis(5),
            "http_503",
            "service unavailable"
        );
    }
}
