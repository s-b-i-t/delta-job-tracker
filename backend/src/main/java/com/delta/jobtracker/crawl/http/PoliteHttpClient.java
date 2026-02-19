package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PoliteHttpClient {
    private static final Duration BACKOFF_DURATION = Duration.ofSeconds(30);

    private final CrawlerProperties properties;
    private final HttpClient client;
    private final Semaphore globalLimiter;
    private final Map<String, Semaphore> hostLimiters = new ConcurrentHashMap<>();
    private final Map<String, Object> hostLocks = new ConcurrentHashMap<>();
    private final Map<String, Instant> hostNextAllowed = new ConcurrentHashMap<>();
    private final HostCrawlStateService hostCrawlStateService;

    public PoliteHttpClient(
        CrawlerProperties properties,
        @Qualifier("httpExecutor") ExecutorService httpExecutor,
        HostCrawlStateService hostCrawlStateService
    ) {
        this.properties = properties;
        this.client = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(properties.getRequestTimeoutSeconds()))
            .version(HttpClient.Version.HTTP_1_1)
            .executor(httpExecutor)
            .build();
        this.globalLimiter = new Semaphore(Math.max(1, properties.getGlobalConcurrency()));
        this.hostCrawlStateService = hostCrawlStateService;
    }

    public HttpFetchResult get(String url, String acceptHeader) {
        return send(url, "GET", acceptHeader, null);
    }

    public HttpFetchResult postJson(String url, String jsonBody, String acceptHeader) {
        return send(url, "POST", acceptHeader, jsonBody == null ? "" : jsonBody, "application/json");
    }

    public HttpFetchResult postForm(String url, String formBody, String acceptHeader) {
        return send(url, "POST", acceptHeader, formBody == null ? "" : formBody, "application/x-www-form-urlencoded");
    }

    private HttpFetchResult send(String url, String method, String acceptHeader, String body) {
        return send(url, method, acceptHeader, body, "application/json");
    }

    private HttpFetchResult send(String url, String method, String acceptHeader, String body, String contentType) {
        CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
        int maxAttempts = Math.max(1, 1 + properties.getRequestMaxRetries());
        if (budget != null) {
            maxAttempts = Math.max(1, budget.maxAttemptsPerRequest());
        }
        HttpFetchResult lastResult = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            lastResult = executeOnce(url, method, acceptHeader, body, contentType);
            if (lastResult == null || !shouldRetry(lastResult) || attempt >= maxAttempts) {
                return lastResult;
            }
            if (!sleepBackoff(attempt)) {
                return lastResult;
            }
        }
        return lastResult;
    }

    private HttpFetchResult executeOnce(String url, String method, String acceptHeader, String body, String contentType) {
        Instant startedAt = Instant.now();
        URI uri = normalizeUri(url);
        if (uri == null || uri.getHost() == null) {
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
                "invalid_url",
                "URL missing host or malformed"
            );
        }

        String host = uri.getHost().toLowerCase(Locale.ROOT);
        CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
        if (budget != null) {
            budget.beforeRequest(host);
        }
        boolean acquired = false;
        boolean hostAcquired = false;
        try {
            globalLimiter.acquire();
            acquired = true;
            Semaphore hostLimiter = hostLimiters.computeIfAbsent(
                host,
                ignored -> new Semaphore(Math.max(1, properties.getPerHostConcurrency()))
            );
            hostLimiter.acquire();
            hostAcquired = true;
            enforcePerHostDelay(host);

            String safeUserAgent = CrawlerProperties.normalizeUserAgent(properties.getUserAgent());
            String safeAccept = (acceptHeader == null || acceptHeader.isBlank()) ? "*/*" : acceptHeader;
            int timeoutSeconds = properties.getRequestTimeoutSeconds();
            if (budget != null) {
                timeoutSeconds = budget.requestTimeoutSeconds();
            }
            HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .header("User-Agent", safeUserAgent)
                .header("Accept", safeAccept)
                .header("Accept-Language", "en-US,en;q=0.8");
            HttpRequest request;
            if ("POST".equalsIgnoreCase(method)) {
                request = builder
                    .header("Content-Type", contentType == null || contentType.isBlank() ? "application/json" : contentType)
                    .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body, StandardCharsets.UTF_8))
                    .build();
            } else {
                request = builder.GET().build();
            }

            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() == 403 || response.statusCode() == 429) {
                extendBackoff(host, BACKOFF_DURATION);
            }
            byte[] responseBytes = response.body();
            String responseBody = responseBytes == null ? null : new String(responseBytes, StandardCharsets.UTF_8);
            HttpFetchResult result = new HttpFetchResult(
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
            if (budget != null) {
                budget.recordResult(result);
                recordHostCooldownIfNeeded(host, result);
            }
            return result;
        } catch (HttpTimeoutException e) {
            HttpFetchResult result = errorResult(url, startedAt, "timeout", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
                recordHostCooldownIfNeeded(host, result);
            }
            return result;
        } catch (IOException e) {
            HttpFetchResult result = errorResult(url, startedAt, "io_error", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
                recordHostCooldownIfNeeded(host, result);
            }
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            HttpFetchResult result = errorResult(url, startedAt, "interrupted", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
                recordHostCooldownIfNeeded(host, result);
            }
            return result;
        } catch (Exception e) {
            HttpFetchResult result = errorResult(url, startedAt, "http_error", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
                recordHostCooldownIfNeeded(host, result);
            }
            return result;
        } finally {
            if (hostAcquired) {
                Semaphore hostLimiter = hostLimiters.get(host);
                if (hostLimiter != null) {
                    hostLimiter.release();
                }
            }
            if (acquired) {
                globalLimiter.release();
            }
        }
    }

    private boolean shouldRetry(HttpFetchResult result) {
        if (result == null) {
            return false;
        }
        String errorCode = result.errorCode();
        if (errorCode != null && !errorCode.isBlank()) {
            return !errorCode.equals("invalid_url") && !errorCode.equals("interrupted");
        }
        int status = result.statusCode();
        return status == 408 || status == 429 || status >= 500;
    }

    private void recordHostCooldownIfNeeded(String host, HttpFetchResult result) {
        if (hostCrawlStateService == null || host == null || host.isBlank() || result == null) {
            return;
        }
        if (result.isSuccessful()) {
            hostCrawlStateService.recordSuccess(host);
            return;
        }
        String category = cooldownCategory(result);
        if (category != null) {
            hostCrawlStateService.recordFailure(host, category);
        }
    }

    private String cooldownCategory(HttpFetchResult result) {
        if (result == null) {
            return null;
        }
        if (result.errorCode() != null) {
            String code = result.errorCode().toLowerCase(Locale.ROOT);
            if (code.contains("timeout")) {
                return ReasonCodeClassifier.TIMEOUT;
            }
            return null;
        }
        int status = result.statusCode();
        if (status == 429) {
            return ReasonCodeClassifier.HTTP_429_RATE_LIMIT;
        }
        if (status == 408) {
            return ReasonCodeClassifier.TIMEOUT;
        }
        return null;
    }

    private boolean sleepBackoff(int attempt) {
        int baseDelayMs = properties.getRequestRetryBaseDelayMs();
        if (baseDelayMs <= 0) {
            return true;
        }
        int maxDelayMs = properties.getRequestRetryMaxDelayMs();
        long delay = (long) baseDelayMs * (1L << Math.max(0, attempt - 1));
        if (maxDelayMs > 0) {
            delay = Math.min(delay, maxDelayMs);
        }
        if (delay <= 0) {
            return true;
        }
        long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, delay / 2));
        long sleepMs = (delay / 2) + jitter;
        try {
            Thread.sleep(sleepMs);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void enforcePerHostDelay(String host) throws InterruptedException {
        Object lock = hostLocks.computeIfAbsent(host, ignored -> new Object());
        synchronized (lock) {
            Instant now = Instant.now();
            Instant allowedAt = hostNextAllowed.getOrDefault(host, now);
            if (allowedAt.isAfter(now)) {
                long sleepMs = Duration.between(now, allowedAt).toMillis();
                if (sleepMs > 0) {
                    Thread.sleep(sleepMs);
                }
            }
            hostNextAllowed.put(host, Instant.now().plusMillis(Math.max(1, properties.getPerHostDelayMs())));
        }
    }

    private void extendBackoff(String host, Duration duration) {
        Object lock = hostLocks.computeIfAbsent(host, ignored -> new Object());
        synchronized (lock) {
            Instant candidate = Instant.now().plus(duration);
            Instant current = hostNextAllowed.getOrDefault(host, Instant.now());
            if (candidate.isAfter(current)) {
                hostNextAllowed.put(host, candidate);
            }
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

    private URI normalizeUri(String input) {
        if (input == null || input.isBlank()) {
            return null;
        }
        String value = input.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
