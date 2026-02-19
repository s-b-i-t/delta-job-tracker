package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;

@Service
public class WdqsHttpClient {
    private final CrawlerProperties properties;
    private final HttpClient client;

    public WdqsHttpClient(
        CrawlerProperties properties,
        @Qualifier("httpExecutor") ExecutorService httpExecutor
    ) {
        this.properties = properties;
        int timeoutSeconds = properties.getDomainResolution().getWdqsTimeoutSeconds();
        this.client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(timeoutSeconds))
            .version(HttpClient.Version.HTTP_1_1)
            .executor(httpExecutor)
            .build();
    }

    public HttpFetchResult postForm(String url, String formBody, String acceptHeader) {
        Instant startedAt = Instant.now();
        URI uri = uriFor(url);
        if (uri == null) {
            return errorResult(url, startedAt, "invalid_url", "URL missing host or malformed");
        }

        String safeAccept = (acceptHeader == null || acceptHeader.isBlank()) ? "*/*" : acceptHeader;
        int timeoutSeconds = properties.getDomainResolution().getWdqsTimeoutSeconds();
        HttpRequest request = HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(timeoutSeconds))
            .header("User-Agent", CrawlerProperties.normalizeUserAgent(properties.getUserAgent()))
            .header("Accept", safeAccept)
            .header("Accept-Language", "en-US,en;q=0.8")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(formBody == null ? "" : formBody, StandardCharsets.UTF_8))
            .build();

        try {
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            byte[] responseBytes = response.body();
            String responseBody = responseBytes == null ? null : new String(responseBytes, StandardCharsets.UTF_8);
            return new HttpFetchResult(
                url,
                response.uri(),
                response.statusCode(),
                responseBody,
                responseBytes,
                response.headers().firstValue("Content-Type").orElse(null),
                response.headers().firstValue("Content-Encoding").orElse(null),
                Instant.now(),
                Duration.between(startedAt, Instant.now()),
                null,
                null
            );
        } catch (HttpTimeoutException e) {
            return errorResult(url, startedAt, "timeout", e.getMessage());
        } catch (IOException e) {
            return errorResult(url, startedAt, "io_error", e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return errorResult(url, startedAt, "interrupted", e.getMessage());
        } catch (Exception e) {
            return errorResult(url, startedAt, "http_error", e.getMessage());
        }
    }

    private HttpFetchResult errorResult(String url, Instant startedAt, String code, String message) {
        return new HttpFetchResult(
            url,
            null,
            0,
            null,
            null,
            null,
            null,
            Instant.now(),
            Duration.between(startedAt, Instant.now()),
            code,
            message
        );
    }

    private URI uriFor(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        try {
            return new URI(url);
        } catch (Exception e) {
            return null;
        }
    }
}
