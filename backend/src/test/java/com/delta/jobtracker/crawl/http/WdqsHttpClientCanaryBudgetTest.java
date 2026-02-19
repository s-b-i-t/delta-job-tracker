package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WdqsHttpClientCanaryBudgetTest {
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
    void abortsCanaryOnWdqs429WhenFailFastConfigured() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().setResponseCode(429).setBody("rate limit"));
        server.start();

        CrawlerProperties properties = new CrawlerProperties();
        properties.getDomainResolution().setWdqsTimeoutSeconds(2);

        executor = Executors.newFixedThreadPool(1);
        WdqsHttpClient client = new WdqsHttpClient(properties, executor);

        CanaryHttpBudget budget = new CanaryHttpBudget(
            10,
            100,
            0.01,
            1,
            5,
            1,
            5,
            java.time.Instant.now().plusSeconds(30)
        );

        try (CanaryHttpBudgetContext.Scope ignored = CanaryHttpBudgetContext.activate(budget)) {
            assertThatThrownBy(() -> client.postForm(server.url("/sparql").toString(), "query=1", "application/sparql-results+json"))
                .isInstanceOf(CanaryAbortException.class);
        }
    }
}
