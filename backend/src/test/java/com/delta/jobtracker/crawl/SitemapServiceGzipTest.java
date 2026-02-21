package com.delta.jobtracker.crawl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.sitemap.SitemapService;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SitemapServiceGzipTest {

  @Mock private PoliteHttpClient httpClient;
  @Mock private RobotsTxtService robotsTxtService;

  @Test
  void extractsUrlsFromGzippedSitemapWhenUrlEndsWithGz() throws Exception {
    SitemapDiscoveryResult result =
        discoverWithResponse(
            "https://example.com/sitemap.xml.gz", "https://example.com/sitemap.xml.gz", null);
    assertExtracted(result);
  }

  @Test
  void extractsUrlsFromGzippedSitemapWhenContentEncodingIsGzip() throws Exception {
    SitemapDiscoveryResult result =
        discoverWithResponse(
            "https://example.com/sitemap.xml", "https://example.com/sitemap.xml", "gzip");
    assertExtracted(result);
  }

  @Test
  void extractsUrlsFromGzippedSitemapWhenMagicBytesPresent() throws Exception {
    SitemapDiscoveryResult result =
        discoverWithResponse(
            "https://example.com/sitemap.xml", "https://example.com/sitemap.xml", null);
    assertExtracted(result);
  }

  private SitemapDiscoveryResult discoverWithResponse(
      String seedUrl, String finalUrl, String contentEncoding) throws Exception {
    byte[] gzipBytes =
        Files.readAllBytes(Path.of("src/test/resources/fixtures/sitemap-urlset.xml.gz"));
    HttpFetchResult fetchResult =
        new HttpFetchResult(
            seedUrl,
            URI.create(finalUrl),
            200,
            null,
            gzipBytes,
            "application/xml",
            contentEncoding,
            Instant.now(),
            Duration.ofMillis(20),
            null,
            null);

    when(robotsTxtService.isAllowed(anyString())).thenReturn(true);
    when(httpClient.get(eq(seedUrl), eq("application/xml,text/xml;q=0.9,*/*;q=0.1"), anyInt()))
        .thenReturn(fetchResult);

    SitemapService service = new SitemapService(httpClient, robotsTxtService);
    return service.discover(List.of(seedUrl), 1, 10, 10);
  }

  private void assertExtracted(SitemapDiscoveryResult result) {
    assertEquals(2, result.discoveredUrls().size());
    assertTrue(
        result.discoveredUrls().stream()
            .anyMatch(entry -> "https://example.com/jobs/alpha".equals(entry.url())));
    assertTrue(
        result.discoveredUrls().stream()
            .anyMatch(entry -> "https://example.com/careers/beta".equals(entry.url())));
  }
}
