package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.ats.AtsEndpointExtractor;
import com.delta.jobtracker.crawl.ats.AtsFingerprintFromHtmlLinksDetector;
import com.delta.jobtracker.crawl.ats.AtsFingerprintFromSitemapsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.model.SitemapUrlEntry;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsRules;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.SitemapService;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CareersDiscoveryServiceTest {

  @Mock private CrawlJdbcRepository repository;
  @Mock private PoliteHttpClient httpClient;
  @Mock private RobotsTxtService robotsTxtService;
  @Mock private AtsDetector atsDetector;
  @Mock private SitemapService sitemapService;
  @Mock private HostCrawlStateService hostCrawlStateService;

  private CareersDiscoveryService service;
  private AtsFingerprintFromHtmlLinksDetector homepageDetector;
  private AtsFingerprintFromSitemapsDetector sitemapDetector;

  @BeforeEach
  void setUp() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setMaxSlugCandidates(6);
    properties.getCareersDiscovery().setMaxCareersPaths(2);
    properties.getCareersDiscovery().setRobotsCooldownDays(14);
    properties.getCareersDiscovery().setFailureBackoffMinutes(List.of(5, 15));
    homepageDetector = new AtsFingerprintFromHtmlLinksDetector(new AtsEndpointExtractor());
    sitemapDetector = new AtsFingerprintFromSitemapsDetector(new AtsEndpointExtractor());
    lenient()
        .when(robotsTxtService.getRulesForHost(anyString()))
        .thenReturn(RobotsRules.allowAll());
    lenient().when(robotsTxtService.isAllowed(anyString())).thenReturn(true);
    lenient()
        .when(sitemapService.discover(any(), anyInt(), anyInt(), anyInt()))
        .thenReturn(new SitemapDiscoveryResult(List.of(), List.of(), Map.of()));
    service =
        new CareersDiscoveryService(
            properties,
            repository,
            httpClient,
            robotsTxtService,
            atsDetector,
            new AtsEndpointExtractor(),
            sitemapService,
            homepageDetector,
            sitemapDetector,
            hostCrawlStateService);
  }

  @Test
  void homepageScanFindsAtsLinkWithoutCareersProbe() {
    CompanyTarget company = new CompanyTarget(1L, "ACME", "Acme Corp", null, "acme.com", null);
    String homepage = "https://acme.com/";
    when(repository.findCareersDiscoveryState(1L)).thenReturn(null);
    when(robotsTxtService.isAllowed(homepage)).thenReturn(true);
    when(httpClient.get(eq(homepage), anyString(), anyInt()))
        .thenReturn(
            successHtml(homepage, "<a href=\"https://boards.greenhouse.io/acme\">Careers</a>"));

    CareersDiscoveryService.DiscoveryOutcome outcome =
        service.discoverForCompany(company, null, null, null, false);

    assertThat(outcome.hasEndpoints()).isTrue();
    verify(repository)
        .upsertAtsEndpoint(
            eq(1L),
            eq(AtsType.GREENHOUSE),
            eq("https://boards.greenhouse.io/acme"),
            eq(homepage),
            eq(0.95),
            any(Instant.class),
            eq("homepage_link"),
            eq(true));
    verify(httpClient, never()).get(eq("https://acme.com/careers"), anyString());
  }

  @Test
  void vendorProbeOnlyPersistsEndpoint() {
    CompanyTarget company = new CompanyTarget(2L, "ACME", "Acme Corp", null, "acme.com", null);
    String vendorUrl = "https://boards.greenhouse.io/acme";
    when(repository.findCareersDiscoveryState(2L)).thenReturn(null);
    when(httpClient.get(eq(vendorUrl), anyString()))
        .thenReturn(successHtml(vendorUrl, "<html>Greenhouse Jobs</html>"));

    CareersDiscoveryService.DiscoveryOutcome outcome =
        service.discoverForCompany(company, null, null, null, true);

    assertThat(outcome.hasEndpoints()).isTrue();
    verify(repository)
        .upsertAtsEndpoint(
            eq(2L),
            eq(AtsType.GREENHOUSE),
            eq(vendorUrl),
            eq(vendorUrl),
            eq(0.9),
            any(Instant.class),
            eq("vendor_probe"),
            eq(true));
    verify(httpClient, never()).get(eq("https://acme.com/"), anyString(), anyInt());
  }

  @Test
  void sitemapDetectionFindsAtsEndpoint() {
    CompanyTarget company = new CompanyTarget(4L, "SITE", "Site Corp", null, "sitemapco.com", null);
    String homepage = "https://sitemapco.com/";
    when(repository.findCareersDiscoveryState(4L)).thenReturn(null);
    when(httpClient.get(eq(homepage), anyString(), anyInt()))
        .thenReturn(successHtml(homepage, "<html><body>No links</body></html>"));
    SitemapDiscoveryResult sitemapResult =
        new SitemapDiscoveryResult(
            List.of(),
            List.of(
                new SitemapUrlEntry(
                    "https://sitemapco.wd5.myworkdayjobs.com/wday/cxs/sitemapco/External/job/123",
                    null)),
            Map.of());
    when(sitemapService.discover(any(), anyInt(), anyInt(), anyInt())).thenReturn(sitemapResult);

    CareersDiscoveryService.DiscoveryOutcome outcome =
        service.discoverForCompany(company, null, null, null, false);

    assertThat(outcome.hasEndpoints()).isTrue();
    verify(repository)
        .upsertAtsEndpoint(
            eq(4L),
            eq(AtsType.WORKDAY),
            eq("https://sitemapco.wd5.myworkdayjobs.com/External"),
            eq("https://sitemapco.wd5.myworkdayjobs.com/wday/cxs/sitemapco/External/job/123"),
            eq(0.92),
            any(Instant.class),
            eq("sitemap"),
            eq(true));
  }

  @Test
  void robotsBlockedCareersPathsAreSkippedAndCached() {
    CompanyTarget company = new CompanyTarget(3L, "ACME", "Acme Corp", null, "acme.com", null);
    String homepage = "https://acme.com/";
    when(repository.findCareersDiscoveryState(3L)).thenReturn(null);
    when(robotsTxtService.isAllowed(homepage)).thenReturn(true);
    when(robotsTxtService.isAllowed("https://acme.com/careers")).thenReturn(false);
    when(httpClient.get(anyString(), anyString())).thenReturn(failureFetch(homepage));
    when(httpClient.get(eq(homepage), anyString(), anyInt()))
        .thenReturn(successHtml(homepage, "<html>No ATS</html>"));

    service.discoverForCompany(company, null, null, null, false);

    verify(httpClient, never()).get(eq("https://acme.com/careers"), anyString());
    verify(repository)
        .insertCareersDiscoveryFailure(
            eq(3L),
            eq("discovery_careers_blocked_by_robots"),
            eq("https://acme.com/careers"),
            any(),
            any(Instant.class));
    ArgumentCaptor<Instant> nextAttemptCaptor = ArgumentCaptor.forClass(Instant.class);
    verify(repository)
        .upsertCareersDiscoveryState(
            eq(3L),
            any(Instant.class),
            eq("discovery_careers_blocked_by_robots"),
            eq("https://acme.com/careers"),
            eq(1),
            nextAttemptCaptor.capture());
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
        null);
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
        "service unavailable");
  }
}
