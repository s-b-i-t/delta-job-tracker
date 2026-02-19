package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class PoliteHttpClientCooldownTest {
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
    void skipsRequestWhenHostInCooldown() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));
        server.start();

        CrawlerProperties properties = new CrawlerProperties();
        properties.setGlobalConcurrency(1);
        properties.setPerHostDelayMs(1);
        properties.setRequestTimeoutSeconds(5);
        properties.setRequestMaxRetries(0);
        properties.setRequestRetryBaseDelayMs(1);
        properties.setRequestRetryMaxDelayMs(5);

        HostCrawlStateService hostCrawlStateService = Mockito.mock(HostCrawlStateService.class);
        when(hostCrawlStateService.nextAllowedAt(anyString()))
            .thenReturn(Instant.now().plusSeconds(60));

        executor = Executors.newFixedThreadPool(1);
        PoliteHttpClient client = new PoliteHttpClient(properties, executor, hostCrawlStateService);

        String url = server.url("/cooldown").toString();
        HttpFetchResult result = client.get(url, "text/plain");

        assertThat(result.errorCode()).isEqualTo("host_cooldown");
        assertThat(result.errorMessage()).contains("cooldown_until=");
        assertThat(server.getRequestCount()).isEqualTo(0);
    }
}
