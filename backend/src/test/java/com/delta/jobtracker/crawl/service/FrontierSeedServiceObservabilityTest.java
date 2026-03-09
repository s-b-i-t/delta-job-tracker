package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierHostState;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierSeedResponse;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.FrontierRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.FrontierSitemapParser;
import com.delta.jobtracker.crawl.util.FrontierJobSignalHeuristics;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith({MockitoExtension.class, OutputCaptureExtension.class})
class FrontierSeedServiceObservabilityTest {

  @Mock private FrontierRepository frontierRepository;
  @Mock private PoliteHttpClient httpClient;
  @Mock private RobotsTxtService robotsTxtService;
  @Mock private FrontierJobSignalHeuristics jobSignalHeuristics;

  private FrontierSeedService service;

  @BeforeEach
  void setUp() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getFrontier().setSeedDomainLimit(2);
    properties.getFrontier().setMaxSitemapFetchesPerRun(1);
    properties.getFrontier().setRespectRobotsForSitemaps(true);
    properties.getFrontier().setMaxJobCandidatesPerSitemap(5);
    properties.getFrontier().setMaxUrlsParsedPerSitemap(10);
    properties.getFrontier().setUrlLeaseSeconds(30);
    FrontierSchedulerService frontierSchedulerService =
        new FrontierSchedulerService(
            frontierRepository,
            httpClient,
            new FrontierBackoffPolicy(properties),
            new FrontierSitemapParser(),
            jobSignalHeuristics,
            robotsTxtService,
            properties);
    service = new FrontierSeedService(frontierRepository, frontierSchedulerService, properties);
  }

  @Test
  void logsSeedBreakdownAcrossDbAndDownstreamStages(CapturedOutput output) {
    when(frontierRepository.findSeedDomains(2))
        .thenAnswer(
            invocation -> {
              sleepForMillis(20);
              return List.of("Example.com", "jobs.example.org");
            });
    doAnswer(
            invocation -> {
              sleepForMillis(12);
              return null;
            })
        .when(frontierRepository)
        .ensureHost(anyString());

    FrontierQueueUrl claimed =
        new FrontierQueueUrl(
            42L,
            "https://example.com/sitemap.xml",
            "example.com",
            "https://example.com/sitemap.xml",
            FrontierUrlKind.SITEMAP,
            100,
            Instant.now(),
            "QUEUED");
    AtomicInteger claimCalls = new AtomicInteger();
    when(frontierRepository.claimNextDueUrl(anyString(), anyLong(), eq(FrontierUrlKind.SITEMAP)))
        .thenAnswer(
            invocation -> {
              sleepForMillis(claimCalls.get() == 0 ? 9 : 4);
              return claimCalls.getAndIncrement() == 0 ? claimed : null;
            });
    when(frontierRepository.findHostState("example.com"))
        .thenAnswer(
            invocation -> {
              sleepForMillis(8);
              return new FrontierHostState("example.com", 0, "HTTP_2XX");
            });
    when(robotsTxtService.isAllowed(claimed.url()))
        .thenAnswer(
            invocation -> {
              sleepForMillis(14);
              return true;
            });
    when(httpClient.get(eq(claimed.url()), anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              sleepForMillis(16);
              String body =
                  """
                  <urlset>
                    <url><loc>https://example.com/jobs/1</loc></url>
                    <url><loc>https://example.com/careers/2</loc></url>
                    <sitemap><loc>https://example.com/child-sitemap.xml</loc></sitemap>
                  </urlset>
                  """;
              return new HttpFetchResult(
                  claimed.url(),
                  null,
                  200,
                  body,
                  body.getBytes(),
                  "application/xml",
                  null,
                  Instant.now(),
                  Duration.ofMillis(16),
                  null,
                  null);
            });
    when(jobSignalHeuristics.isJobLike(anyString())).thenReturn(true);
    AtomicLong urlIds = new AtomicLong(100L);
    when(frontierRepository.enqueueUrl(anyString(), any(), anyInt(), any()))
        .thenAnswer(
            invocation -> {
              sleepForMillis(15);
              String url = invocation.getArgument(0, String.class);
              return new FrontierEnqueueResult(true, urlIds.incrementAndGet(), url, "example.com");
            });
    doAnswer(
            invocation -> {
              sleepForMillis(7);
              return null;
            })
        .when(frontierRepository)
        .completeFetch(eq(claimed), any());
    when(frontierRepository.countQueueStatuses())
        .thenAnswer(
            invocation -> {
              sleepForMillis(10);
              return Map.of("FETCHED", 1, "QUEUED", 5);
            });

    FrontierSeedResponse response = service.seedFromCompanyDomains(2, 1);

    assertEquals(5, response.urlsEnqueued());
    assertThat(output)
        .contains("frontier.seed.start")
        .contains("frontier.seed.findSeedDomains")
        .contains("frontier.seed.domain.ensureHost")
        .contains("frontier.seed.domain.finish")
        .contains("frontier.seed.scheduler.summary")
        .contains("frontier.seed.summary")
        .contains("dbMs=")
        .contains("downstreamMs=")
        .contains("schedulerDbMs=")
        .contains("schedulerDownstreamMs=");
  }

  private static void sleepForMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("sleep interrupted", e);
    }
  }
}
