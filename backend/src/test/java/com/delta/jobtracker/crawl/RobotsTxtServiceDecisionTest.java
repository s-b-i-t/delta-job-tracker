package com.delta.jobtracker.crawl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RobotsTxtServiceDecisionTest {

  @Mock private PoliteHttpClient httpClient;

  @Test
  void failClosedDisallowsCrawlButAllowsAtsAdapterWhenRobotsUnavailable() {
    when(httpClient.get(anyString(), anyString())).thenReturn(errorFetch());

    CrawlerProperties properties = new CrawlerProperties();
    properties.getRobots().setFailOpen(false);
    properties.getRobots().setAllowAtsAdapterWhenUnavailable(true);
    RobotsTxtService service = new RobotsTxtService(properties, httpClient);

    assertFalse(service.isAllowed("https://example.com/careers"));
    assertTrue(service.isRobotsUnavailableForUrl("https://example.com/careers"));
    assertTrue(
        service.isAllowedForAtsAdapter("https://api.lever.co/v0/postings/example?mode=json"));
  }

  @Test
  void failOpenAllowsCrawlWhenRobotsUnavailable() {
    when(httpClient.get(anyString(), anyString())).thenReturn(errorFetch());

    CrawlerProperties properties = new CrawlerProperties();
    properties.getRobots().setFailOpen(true);
    RobotsTxtService service = new RobotsTxtService(properties, httpClient);

    assertTrue(service.isAllowed("https://example.com/careers"));
    assertTrue(service.isRobotsUnavailableForUrl("https://example.com/careers"));
  }

  @Test
  void failClosedCanDisableAtsAdapterBypassWhenRobotsUnavailable() {
    when(httpClient.get(anyString(), anyString())).thenReturn(errorFetch());

    CrawlerProperties properties = new CrawlerProperties();
    properties.getRobots().setFailOpen(false);
    properties.getRobots().setAllowAtsAdapterWhenUnavailable(false);
    RobotsTxtService service = new RobotsTxtService(properties, httpClient);

    assertFalse(service.isAllowed("https://example.com/careers"));
    assertTrue(service.isRobotsUnavailableForUrl("https://example.com/careers"));
    assertFalse(
        service.isAllowedForAtsAdapter("https://api.lever.co/v0/postings/example?mode=json"));
  }

  private HttpFetchResult errorFetch() {
    return new HttpFetchResult(
        "https://example.com/robots.txt",
        null,
        0,
        null,
        null,
        null,
        null,
        Instant.now(),
        Duration.ofMillis(5),
        "io_error",
        "connection failed");
  }
}
