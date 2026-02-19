package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

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
    void retriesOn429ThenSucceeds() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(429).setBody("rate limit"));
        server.enqueue(new MockResponse().setResponseCode(200).setBody("ok"));
        server.start();

        CrawlerProperties properties = new CrawlerProperties();
        properties.setGlobalConcurrency(1);
        properties.setPerHostDelayMs(1);
        properties.setRequestTimeoutSeconds(5);
        properties.setRequestMaxRetries(2);
        properties.setRequestRetryBaseDelayMs(1);
        properties.setRequestRetryMaxDelayMs(5);

        executor = Executors.newFixedThreadPool(1);
        HostCrawlStateService hostCrawlStateService = new HostCrawlStateService(Mockito.mock(CrawlJdbcRepository.class));
        PoliteHttpClient client = new PoliteHttpClient(properties, executor, hostCrawlStateService);

        String url = server.url("/retry").toString();
        HttpFetchResult result = client.get(url, "text/plain");

        assertThat(result.statusCode()).isEqualTo(200);
        assertThat(server.getRequestCount()).isEqualTo(2);
    }
}
