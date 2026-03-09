package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierHostState;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierSchedulerResult;
import com.delta.jobtracker.crawl.model.FrontierSeedResponse;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.FrontierRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.FrontierSitemapParser;
import com.delta.jobtracker.crawl.util.FrontierJobSignalHeuristics;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class FrontierSeedServiceTest {

  @Test
  void seedFromCompanyDomainsEnqueuesDefaultSitemapsBeforeRunningScheduler() {
    FrontierRepository frontierRepository = Mockito.mock(FrontierRepository.class);
    FrontierSchedulerService frontierSchedulerService =
        Mockito.mock(FrontierSchedulerService.class);
    CrawlerProperties properties = new CrawlerProperties();
    FrontierSeedService service =
        new FrontierSeedService(frontierRepository, frontierSchedulerService, properties);

    when(frontierRepository.findSeedDomains(2)).thenReturn(List.of("Alpha.test", "Beta.test"));
    when(frontierRepository.enqueueUrl(anyString(), eq(FrontierUrlKind.SITEMAP), eq(100), any()))
        .thenAnswer(
            invocation ->
                new FrontierEnqueueResult(
                    true,
                    1L,
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(0, String.class)));
    when(frontierRepository.countQueueStatuses()).thenReturn(Map.of("QUEUED", 2));
    when(frontierSchedulerService.fetchDueSitemaps(eq(1), anyString()))
        .thenReturn(new FrontierSchedulerResult(0, 0, 0, 0, 0, 0, 0, Map.of(), null));

    FrontierSeedResponse response = service.seedFromCompanyDomains(2, 1);

    verify(frontierRepository).ensureHost("alpha.test");
    verify(frontierRepository).ensureHost("beta.test");
    verify(frontierRepository)
        .enqueueUrl(
            eq("https://alpha.test/sitemap.xml"), eq(FrontierUrlKind.SITEMAP), eq(100), any());
    verify(frontierRepository)
        .enqueueUrl(
            eq("https://beta.test/sitemap.xml"), eq(FrontierUrlKind.SITEMAP), eq(100), any());
    verify(frontierSchedulerService).fetchDueSitemaps(eq(1), anyString());
    assertThat(response.seedDomainsUsed()).isEqualTo(2);
    assertThat(response.sitemapUrlsEnqueued()).isEqualTo(2);
    assertThat(response.urlsFetched()).isEqualTo(0);
  }

  @Test
  void seedFromCompanyDomainsNoLongerPaysRobotsCostForEverySeedDomain() throws Exception {
    FrontierRepository frontierRepository = Mockito.mock(FrontierRepository.class);
    PoliteHttpClient httpClient = Mockito.mock(PoliteHttpClient.class);
    RobotsTxtService robotsTxtService = Mockito.mock(RobotsTxtService.class);
    CrawlerProperties properties = new CrawlerProperties();
    properties.getFrontier().setRespectRobotsForSitemaps(true);

    FrontierSchedulerService frontierSchedulerService =
        new FrontierSchedulerService(
            frontierRepository,
            httpClient,
            new FrontierBackoffPolicy(properties),
            new FrontierSitemapParser(),
            new FrontierJobSignalHeuristics(),
            robotsTxtService,
            properties);
    FrontierSeedService service =
        new FrontierSeedService(frontierRepository, frontierSchedulerService, properties);

    when(frontierRepository.findSeedDomains(3))
        .thenReturn(List.of("alpha.test", "beta.test", "gamma.test"));
    when(frontierRepository.enqueueUrl(anyString(), eq(FrontierUrlKind.SITEMAP), eq(100), any()))
        .thenAnswer(
            invocation ->
                new FrontierEnqueueResult(
                    true,
                    1L,
                    invocation.getArgument(0, String.class),
                    invocation.getArgument(0, String.class)));
    when(frontierRepository.claimNextDueUrl(anyString(), anyLong(), eq(FrontierUrlKind.SITEMAP)))
        .thenReturn(claimedSitemap("alpha.test"))
        .thenReturn(null);
    when(frontierRepository.findHostState("alpha.test"))
        .thenReturn(new FrontierHostState("alpha.test", 0, null));
    doNothing().when(frontierRepository).completeFetch(any(), any());
    when(frontierRepository.countDueUrlsBlockedByBackoff(FrontierUrlKind.SITEMAP)).thenReturn(0);
    when(frontierRepository.countQueueStatuses()).thenReturn(Map.of("BLOCKED", 1, "QUEUED", 3));
    when(robotsTxtService.isAllowed("https://alpha.test/sitemap.xml"))
        .thenAnswer(
            invocation -> {
              Thread.sleep(250L);
              return false;
            });

    long started = System.nanoTime();
    FrontierSeedResponse response = service.seedFromCompanyDomains(3, 1);
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    verify(robotsTxtService, times(1)).isAllowed("https://alpha.test/sitemap.xml");
    verify(robotsTxtService, never()).isAllowed("https://beta.test/sitemap.xml");
    verify(robotsTxtService, never()).isAllowed("https://gamma.test/sitemap.xml");
    verifyNoInteractions(httpClient);
    assertThat(elapsedMs).isLessThan(700L);
    assertThat(response.hostsSeen()).isEqualTo(3);
    assertThat(response.httpRequestCount()).isEqualTo(0);
    assertThat(response.urlsFetched()).isEqualTo(1);
  }

  @Test
  void schedulerStillBlocksClaimedSitemapWhenRobotsDenies() {
    FrontierRepository frontierRepository = Mockito.mock(FrontierRepository.class);
    PoliteHttpClient httpClient = Mockito.mock(PoliteHttpClient.class);
    RobotsTxtService robotsTxtService = Mockito.mock(RobotsTxtService.class);
    CrawlerProperties properties = new CrawlerProperties();

    FrontierSchedulerService scheduler =
        new FrontierSchedulerService(
            frontierRepository,
            httpClient,
            new FrontierBackoffPolicy(properties),
            new FrontierSitemapParser(),
            new FrontierJobSignalHeuristics(),
            robotsTxtService,
            properties);

    when(frontierRepository.claimNextDueUrl(anyString(), anyLong(), eq(FrontierUrlKind.SITEMAP)))
        .thenReturn(claimedSitemap("alpha.test"))
        .thenReturn(null);
    when(frontierRepository.findHostState("alpha.test"))
        .thenReturn(new FrontierHostState("alpha.test", 0, null));
    when(frontierRepository.countDueUrlsBlockedByBackoff(FrontierUrlKind.SITEMAP)).thenReturn(0);
    doNothing().when(frontierRepository).completeFetch(any(), any());
    when(robotsTxtService.isAllowed("https://alpha.test/sitemap.xml")).thenReturn(false);

    FrontierSchedulerResult result = scheduler.fetchDueSitemaps(1);

    verify(robotsTxtService).isAllowed("https://alpha.test/sitemap.xml");
    verifyNoInteractions(httpClient);
    assertThat(result.urlsFetched()).isEqualTo(1);
    assertThat(result.httpRequestCount()).isEqualTo(0);
    assertThat(result.statusBucketCounts()).containsEntry("ROBOTS_BLOCKED", 1);
  }

  @Test
  void schedulerStillFetchesClaimedSitemapWhenRobotsAllows() {
    FrontierRepository frontierRepository = Mockito.mock(FrontierRepository.class);
    PoliteHttpClient httpClient = Mockito.mock(PoliteHttpClient.class);
    RobotsTxtService robotsTxtService = Mockito.mock(RobotsTxtService.class);
    CrawlerProperties properties = new CrawlerProperties();

    FrontierSchedulerService scheduler =
        new FrontierSchedulerService(
            frontierRepository,
            httpClient,
            new FrontierBackoffPolicy(properties),
            new FrontierSitemapParser(),
            new FrontierJobSignalHeuristics(),
            robotsTxtService,
            properties);

    when(frontierRepository.claimNextDueUrl(anyString(), anyLong(), eq(FrontierUrlKind.SITEMAP)))
        .thenReturn(claimedSitemap("alpha.test"))
        .thenReturn(null);
    when(frontierRepository.findHostState("alpha.test"))
        .thenReturn(new FrontierHostState("alpha.test", 0, null));
    when(frontierRepository.countDueUrlsBlockedByBackoff(FrontierUrlKind.SITEMAP)).thenReturn(0);
    doNothing().when(frontierRepository).completeFetch(any(), any());
    when(robotsTxtService.isAllowed("https://alpha.test/sitemap.xml")).thenReturn(true);
    when(httpClient.get(eq("https://alpha.test/sitemap.xml"), anyString(), anyInt()))
        .thenReturn(
            new HttpFetchResult(
                "https://alpha.test/sitemap.xml",
                URI.create("https://alpha.test/sitemap.xml"),
                200,
                """
                <?xml version="1.0" encoding="UTF-8"?>
                <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>
                """,
                null,
                "application/xml",
                null,
                Instant.now(),
                Duration.ofMillis(10),
                null,
                null));

    FrontierSchedulerResult result = scheduler.fetchDueSitemaps(1);

    verify(robotsTxtService).isAllowed("https://alpha.test/sitemap.xml");
    verify(httpClient).get(eq("https://alpha.test/sitemap.xml"), anyString(), anyInt());
    assertThat(result.urlsFetched()).isEqualTo(1);
    assertThat(result.httpRequestCount()).isEqualTo(1);
    assertThat(result.statusBucketCounts()).containsEntry("HTTP_2XX", 1);
  }

  private FrontierQueueUrl claimedSitemap(String host) {
    return new FrontierQueueUrl(
        1L,
        "https://" + host + "/sitemap.xml",
        host,
        "https://" + host + "/sitemap.xml",
        FrontierUrlKind.SITEMAP,
        100,
        Instant.now(),
        "QUEUED");
  }
}
