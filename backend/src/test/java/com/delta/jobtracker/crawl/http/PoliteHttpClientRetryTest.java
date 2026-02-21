package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PoliteHttpClientRetryTest {
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
    void failsFastOn429AndStopsHostInSameRun() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(429).setBody("rate limit"));
        server.start();

        CrawlerProperties properties = new CrawlerProperties();
        properties.setGlobalConcurrency(1);
        properties.setPerHostDelayMs(1);
        properties.setRequestTimeoutSeconds(5);
        properties.setRequestMaxRetries(2);
        properties.setRequestRetryBaseDelayMs(1);
        properties.setRequestRetryMaxDelayMs(5);

        executor = Executors.newFixedThreadPool(1);
        HostCrawlStateService hostCrawlStateService = Mockito.mock(HostCrawlStateService.class);
        AtomicReference<Instant> cooldownUntil = new AtomicReference<>(null);
        when(hostCrawlStateService.nextAllowedAt(anyString())).thenAnswer(invocation -> cooldownUntil.get());
        Mockito.doAnswer(invocation -> {
            cooldownUntil.set(Instant.now().plusSeconds(60));
            return null;
        }).when(hostCrawlStateService).recordFailure(anyString(), eq(ReasonCodeClassifier.HTTP_429_RATE_LIMIT));
        PoliteHttpClient client = new PoliteHttpClient(properties, executor, hostCrawlStateService);

        String url = server.url("/retry").toString();
        HttpFetchResult result = client.get(url, "text/plain");

        assertThat(result.statusCode()).isEqualTo(429);
        assertThat(server.getRequestCount()).isEqualTo(1);

        HttpFetchResult blocked = client.get(url, "text/plain");
        assertThat(blocked.errorCode()).isEqualTo("host_cooldown");
        assertThat(server.getRequestCount()).isEqualTo(1);
        verify(hostCrawlStateService).recordFailure(anyString(), eq(ReasonCodeClassifier.HTTP_429_RATE_LIMIT));
    }
}
