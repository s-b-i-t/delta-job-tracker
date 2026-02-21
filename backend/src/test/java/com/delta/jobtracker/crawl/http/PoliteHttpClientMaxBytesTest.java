package com.delta.jobtracker.crawl.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PoliteHttpClientMaxBytesTest {
  private MockWebServer server;
  private ExecutorService executor;

  @AfterEach
  void tearDown() throws Exception {
    if (server != null) {
      server.shutdown();
    }
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Test
  void returnsBodyTooLargeWhenResponseExceedsMaxBytes() throws Exception {
    server = new MockWebServer();
    server.enqueue(new MockResponse().setResponseCode(200).setBody("a".repeat(5000)));
    server.start();

    CrawlerProperties properties = new CrawlerProperties();
    properties.setGlobalConcurrency(1);
    properties.setPerHostDelayMs(1);
    properties.setRequestTimeoutSeconds(5);
    properties.setRequestMaxRetries(0);

    executor = Executors.newFixedThreadPool(1);
    HostCrawlStateService hostCrawlStateService = Mockito.mock(HostCrawlStateService.class);
    PoliteHttpClient client = new PoliteHttpClient(properties, executor, hostCrawlStateService);

    String url = server.url("/big").toString();
    HttpFetchResult result = client.get(url, "text/plain", 1024);

    assertThat(result.errorCode()).isEqualTo("body_too_large");
    assertThat(result.body()).isNull();
    assertThat(server.getRequestCount()).isEqualTo(1);
  }
}
