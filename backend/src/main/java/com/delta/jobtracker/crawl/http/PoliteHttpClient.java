package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryAbortException;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
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
    private static final int DEFAULT_MAX_BYTES_READ_BUFFER = 8192;
    private static final String BODY_TOO_LARGE_ERROR = "body_too_large";

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
        return send(url, "GET", acceptHeader, null, null, null, null);
    }

    public HttpFetchResult get(String url, String acceptHeader, String userAgentOverride) {
        return send(url, "GET", acceptHeader, null, null, userAgentOverride, null);
    }

    public HttpFetchResult get(String url, String acceptHeader, int maxBytes) {
        return send(url, "GET", acceptHeader, null, null, null, maxBytes);
    }

    public HttpFetchResult get(String url, String acceptHeader, String userAgentOverride, int maxBytes) {
        return send(url, "GET", acceptHeader, null, null, userAgentOverride, maxBytes);
    }

    public HttpFetchResult postJson(String url, String jsonBody, String acceptHeader) {
        return send(url, "POST", acceptHeader, jsonBody == null ? "" : jsonBody, "application/json", null, null);
    }

    public HttpFetchResult postForm(String url, String formBody, String acceptHeader) {
        return send(url, "POST", acceptHeader, formBody == null ? "" : formBody, "application/x-www-form-urlencoded", null, null);
    }

    private HttpFetchResult send(String url, String method, String acceptHeader, String body) {
        return send(url, method, acceptHeader, body, "application/json", null, null);
    }

    private HttpFetchResult send(
        String url,
        String method,
        String acceptHeader,
        String body,
        String contentType,
        String userAgentOverride,
        Integer maxBytes
    ) {
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

    private HttpFetchResult executeOnce(
        String url,
        String method,
        String acceptHeader,
        String body,
        String contentType,
        String userAgentOverride
    ) {
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
        if (hostCrawlStateService != null) {
            Instant nextAllowedAt = hostCrawlStateService.nextAllowedAt(host);
            if (nextAllowedAt != null && nextAllowedAt.isAfter(Instant.now())) {
                return errorResult(
                    url,
                    startedAt,
                    "host_cooldown",
                    "cooldown_until=" + nextAllowedAt
                );
            }
        }
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
            enforcePerHostDelay(host, budget);

            String userAgent = userAgentOverride == null || userAgentOverride.isBlank()
                ? properties.getUserAgent()
                : userAgentOverride;
            String safeUserAgent = CrawlerProperties.normalizeUserAgent(userAgent);
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

            HttpFetchResult result = maxBytes != null && maxBytes > 0
                ? executeWithMaxBytes(url, host, request, startedAt, maxBytes)
                : executeWithoutMaxBytes(url, host, request, startedAt);
            if (budget != null) {
                budget.recordResult(result);
            }
            recordHostCooldownIfNeeded(host, result);
            return result;
        } catch (HttpTimeoutException e) {
            HttpFetchResult result = errorResult(url, startedAt, "timeout", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
            }
            recordHostCooldownIfNeeded(host, result);
            return result;
        } catch (IOException e) {
            HttpFetchResult result = errorResult(url, startedAt, "io_error", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
            }
            recordHostCooldownIfNeeded(host, result);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            HttpFetchResult result = errorResult(url, startedAt, "interrupted", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
            }
            recordHostCooldownIfNeeded(host, result);
            return result;
        } catch (CanaryAbortException e) {
            throw e;
        } catch (Exception e) {
            HttpFetchResult result = errorResult(url, startedAt, "http_error", e.getMessage());
            if (budget != null) {
                budget.recordResult(result);
            }
            recordHostCooldownIfNeeded(host, result);
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
            if (errorCode.equals("host_cooldown") || errorCode.equals(BODY_TOO_LARGE_ERROR)) {
                return false;
            }
            return !errorCode.equals("invalid_url") && !errorCode.equals("interrupted");
        }
        int status = result.statusCode();
        // Fail-fast on 429: record cooldown and let callers skip the host for the rest of the run.
        return status == 408 || status >= 500;
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

    private HttpFetchResult executeWithoutMaxBytes(
        String url,
        String host,
        HttpRequest request,
        Instant startedAt
    ) throws IOException, InterruptedException {
        HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 403 || response.statusCode() == 429) {
            extendBackoff(host, BACKOFF_DURATION);
        }
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
    }

    private HttpFetchResult executeWithMaxBytes(
        String url,
        String host,
        HttpRequest request,
        Instant startedAt,
        int maxBytes
    ) throws IOException, InterruptedException {
        int safeMaxBytes = Math.max(1, maxBytes);
        HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() == 403 || response.statusCode() == 429) {
            extendBackoff(host, BACKOFF_DURATION);
        }

        Long contentLength = parseContentLength(response);
        if (contentLength != null && contentLength > safeMaxBytes) {
            try (InputStream ignored = response.body()) {
                // Close early to avoid downloading the full body.
            }
            return new HttpFetchResult(
                url,
                response.uri(),
                response.statusCode(),
                null,
                null,
                response.headers().firstValue("Content-Type").orElse(null),
                response.headers().firstValue("Content-Encoding").orElse(null),
                Instant.now(),
                Duration.between(startedAt, Instant.now()),
                BODY_TOO_LARGE_ERROR,
                "max_bytes=" + safeMaxBytes + " content_length=" + contentLength
            );
        }

        LimitedBody body = readBodyUpTo(response.body(), safeMaxBytes);
        boolean tooLarge = body.reachedLimit() && contentLength == null;
        if (tooLarge) {
            return new HttpFetchResult(
                url,
                response.uri(),
                response.statusCode(),
                null,
                null,
                response.headers().firstValue("Content-Type").orElse(null),
                response.headers().firstValue("Content-Encoding").orElse(null),
                Instant.now(),
                Duration.between(startedAt, Instant.now()),
                BODY_TOO_LARGE_ERROR,
                "max_bytes=" + safeMaxBytes + " bytes_read=" + body.bytesRead()
            );
        }

        byte[] responseBytes = body.bytes();
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
    }

    private Long parseContentLength(HttpResponse<?> response) {
        if (response == null) {
            return null;
        }
        try {
            String value = response.headers().firstValue("Content-Length").orElse(null);
            if (value == null || value.isBlank()) {
                return null;
            }
            long parsed = Long.parseLong(value.trim());
            return parsed < 0 ? null : parsed;
        } catch (Exception e) {
            return null;
        }
    }

    private LimitedBody readBodyUpTo(InputStream stream, int maxBytes) throws IOException {
        if (stream == null) {
            return new LimitedBody(null, 0, false);
        }
        int safeMaxBytes = Math.max(1, maxBytes);
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.min(safeMaxBytes, 16 * 1024));
        byte[] buffer = new byte[Math.min(DEFAULT_MAX_BYTES_READ_BUFFER, safeMaxBytes)];
        int total = 0;
        boolean reachedLimit = false;
        try (InputStream input = stream) {
            while (total < safeMaxBytes) {
                int remaining = safeMaxBytes - total;
                int read = input.read(buffer, 0, Math.min(buffer.length, remaining));
                if (read < 0) {
                    break;
                }
                if (read > 0) {
                    out.write(buffer, 0, read);
                    total += read;
                }
            }
            reachedLimit = total >= safeMaxBytes;
        }
        return new LimitedBody(out.toByteArray(), total, reachedLimit);
    }

    private record LimitedBody(byte[] bytes, int bytesRead, boolean reachedLimit) {
    }

    private boolean sleepBackoff(int attempt) {
        CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
        if (budget != null) {
            budget.checkDeadline();
        }
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
            if (budget != null) {
                budget.checkDeadline();
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private void enforcePerHostDelay(String host, CanaryHttpBudget budget) throws InterruptedException {
        Object lock = hostLocks.computeIfAbsent(host, ignored -> new Object());
        synchronized (lock) {
            Instant now = Instant.now();
            Instant allowedAt = hostNextAllowed.getOrDefault(host, now);
            if (allowedAt.isAfter(now)) {
                long sleepMs = Duration.between(now, allowedAt).toMillis();
                if (sleepMs > 0) {
                    if (budget != null) {
                        budget.checkDeadline();
                    }
                    Thread.sleep(sleepMs);
                    if (budget != null) {
                        budget.checkDeadline();
                    }
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
