package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CareersLandingPageDiscoveryServiceTest {
  @Mock private PoliteHttpClient httpClient;
  @Mock private RobotsTxtService robotsTxtService;

  private CareersLandingPageDiscoveryService service;

  @BeforeEach
  void setUp() {
    lenient().when(robotsTxtService.isAllowed(anyString())).thenReturn(true);
    service =
        new CareersLandingPageDiscoveryService(
            httpClient, robotsTxtService, new CareersLandingLinkExtractor(), new AtsDetector());
  }

  @Test
  void homepageLinkIsReturnedWhenPresent() {
    String home = "https://example.com/";
    when(httpClient.get(eq(home), anyString(), anyInt()))
        .thenReturn(success(home, "<a href=\"/careers\">Careers</a>"));
    when(httpClient.get(eq("https://example.com/careers"), anyString(), anyInt()))
        .thenReturn(success("https://example.com/careers", "<html>Join us</html>"));

    CareersLandingPageDiscoveryService.DiscoveryResult result = service.discover("example.com");

    assertThat(result.careersUrlFound()).isTrue();
    assertThat(result.careersUrlFinal()).isEqualTo("https://example.com/careers");
    assertThat(result.method()).isEqualTo(CareersLandingPageDiscoveryService.Method.HOMEPAGE_LINK);
  }

  @Test
  void fallsBackToCommonPathWhenHomepageHasNoCandidate() {
    String home = "https://example.com/";
    when(httpClient.get(eq(home), anyString(), anyInt()))
        .thenReturn(success(home, "<html><body>No jobs link</body></html>"));
    when(httpClient.get(eq("https://example.com/careers"), anyString(), anyInt()))
        .thenReturn(success("https://example.com/careers", "<html>Careers page</html>"));

    CareersLandingPageDiscoveryService.DiscoveryResult result = service.discover("example.com");

    assertThat(result.careersUrlFound()).isTrue();
    assertThat(result.method()).isEqualTo(CareersLandingPageDiscoveryService.Method.PATH_GUESS);
    assertThat(result.requestCount()).isGreaterThanOrEqualTo(2);
  }

  @Test
  void recordsAllCommonPaths404WhenHomepageMissingHints() {
    String home = "https://example.com/";
    when(httpClient.get(anyString(), anyString(), anyInt()))
        .thenAnswer(invocation -> {
          String url = invocation.getArgument(0, String.class);
          if (home.equals(url)) {
            return success(home, "<html><body>No jobs link</body></html>");
          }
          return failure(url, 404, null, null);
        });

    CareersLandingPageDiscoveryService.DiscoveryResult result = service.discover("example.com");

    assertThat(result.careersUrlFound()).isFalse();
    assertThat(result.failureReason())
        .isIn(
            CareersLandingPageDiscoveryService.FailureReason.NO_CAREERS_LINK_ON_HOMEPAGE,
            CareersLandingPageDiscoveryService.FailureReason.ALL_COMMON_PATHS_404,
            CareersLandingPageDiscoveryService.FailureReason.SUBDOMAIN_PROBES_404);
    assertThat(result.requestCount()).isLessThanOrEqualTo(10);
  }

  @Test
  void vendorRedirectOnCandidateIsAccepted() {
    String home = "https://example.com/";
    when(httpClient.get(eq(home), anyString(), anyInt()))
        .thenReturn(success(home, "<a href=\"/jobs\">Jobs</a>"));
    when(httpClient.get(eq("https://example.com/jobs"), anyString(), anyInt()))
        .thenReturn(
            success(
                "https://example.com/jobs",
                "https://boards.greenhouse.io/acme",
                "<html>Greenhouse</html>"));

    CareersLandingPageDiscoveryService.DiscoveryResult result = service.discover("example.com");

    assertThat(result.careersUrlFound()).isTrue();
    assertThat(result.vendorName()).isEqualTo("GREENHOUSE");
    assertThat(result.careersUrlFinal()).isEqualTo("https://boards.greenhouse.io/acme");
  }

  private HttpFetchResult success(String url, String body) {
    return success(url, url, body);
  }

  private HttpFetchResult success(String requestedUrl, String finalUrl, String body) {
    return new HttpFetchResult(
        requestedUrl,
        URI.create(finalUrl),
        200,
        body,
        body.getBytes(),
        "text/html",
        null,
        Instant.now(),
        Duration.ofMillis(10),
        null,
        null);
  }

  private HttpFetchResult failure(String url, int status, String errorCode, String errorMessage) {
    return new HttpFetchResult(
        url,
        URI.create(url),
        status,
        null,
        null,
        null,
        null,
        Instant.now(),
        Duration.ofMillis(10),
        errorCode,
        errorMessage);
  }
}
