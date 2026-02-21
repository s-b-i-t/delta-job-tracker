package com.delta.jobtracker.crawl.sitemap;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class SitemapServiceMaxBytesTest {
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
    void recordsBodyTooLargeWhenSitemapExceedsClientCap() throws Exception {
        server = new MockWebServer();
        // Default sitemap cap is 2,000,000 bytes; exceed it slightly.
        server.enqueue(new MockResponse().setResponseCode(200).setBody("a".repeat(2_000_100)));
        server.start();

        RobotsTxtService robotsTxtService = Mockito.mock(RobotsTxtService.class);
        when(robotsTxtService.isAllowed(anyString())).thenReturn(true);

        CrawlerProperties properties = new CrawlerProperties();
        properties.setGlobalConcurrency(1);
        properties.setPerHostDelayMs(1);
        properties.setRequestTimeoutSeconds(5);
        properties.setRequestMaxRetries(0);

        executor = Executors.newFixedThreadPool(1);
        HostCrawlStateService hostCrawlStateService = Mockito.mock(HostCrawlStateService.class);
        PoliteHttpClient httpClient = new PoliteHttpClient(properties, executor, hostCrawlStateService);
        SitemapService sitemapService = new SitemapService(httpClient, robotsTxtService);

        String sitemapUrl = server.url("/sitemap.xml").toString();
        SitemapDiscoveryResult result = sitemapService.discover(List.of(sitemapUrl), 0, 1, 50);

        assertThat(result.errors()).containsEntry("body_too_large", 1);
        assertThat(result.discoveredUrls()).isEmpty();
    }
}

