package com.delta.jobtracker.crawl.http;

import com.delta.jobtracker.crawl.model.HttpFetchResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CanaryHttpBudget {
    private final int maxRequestsPerHost;
    private final int maxTotalRequests;
    private final double max429Rate;
    private final int minRequestsFor429Rate;
    private final int maxConsecutiveErrors;
    private final int maxAttemptsPerRequest;
    private final int requestTimeoutSeconds;

    private final Map<String, Integer> hostCounts = new ConcurrentHashMap<>();
    private int totalRequests;
    private int total429;
    private int consecutiveErrors;
    private boolean aborted;
    private String abortReason;

    public CanaryHttpBudget(
        int maxRequestsPerHost,
        int maxTotalRequests,
        double max429Rate,
        int minRequestsFor429Rate,
        int maxConsecutiveErrors,
        int maxAttemptsPerRequest,
        int requestTimeoutSeconds
    ) {
        this.maxRequestsPerHost = Math.max(0, maxRequestsPerHost);
        this.maxTotalRequests = Math.max(0, maxTotalRequests);
        this.max429Rate = Math.max(0.0, max429Rate);
        this.minRequestsFor429Rate = Math.max(1, minRequestsFor429Rate);
        this.maxConsecutiveErrors = Math.max(0, maxConsecutiveErrors);
        this.maxAttemptsPerRequest = Math.max(1, maxAttemptsPerRequest);
        this.requestTimeoutSeconds = Math.max(1, requestTimeoutSeconds);
    }

    public int maxAttemptsPerRequest() {
        return maxAttemptsPerRequest;
    }

    public int requestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public synchronized void beforeRequest(String host) {
        if (aborted) {
            throw new CanaryAbortException(abortReason == null ? "canary_aborted" : abortReason);
        }
        if (maxTotalRequests > 0 && totalRequests >= maxTotalRequests) {
            abort("total_request_budget_exceeded");
        }
        if (host != null && maxRequestsPerHost > 0) {
            int hostCount = hostCounts.getOrDefault(host, 0);
            if (hostCount >= maxRequestsPerHost) {
                abort("per_host_request_budget_exceeded");
            }
            hostCounts.put(host, hostCount + 1);
        }
        totalRequests++;
    }

    public synchronized void recordResult(HttpFetchResult result) {
        if (aborted) {
            throw new CanaryAbortException(abortReason == null ? "canary_aborted" : abortReason);
        }
        if (result == null) {
            return;
        }
        if (result.statusCode() == 429) {
            total429++;
        }
        if (isError(result)) {
            consecutiveErrors++;
        } else {
            consecutiveErrors = 0;
        }

        if (maxConsecutiveErrors > 0 && consecutiveErrors >= maxConsecutiveErrors) {
            abort("consecutive_error_threshold_exceeded");
        }
        if (max429Rate > 0 && totalRequests >= minRequestsFor429Rate) {
            double rate = total429 / Math.max(1.0, totalRequests);
            if (rate >= max429Rate) {
                abort("rate_limit_threshold_exceeded");
            }
        }
    }

    public synchronized boolean isAborted() {
        return aborted;
    }

    public synchronized String abortReason() {
        return abortReason;
    }

    private boolean isError(HttpFetchResult result) {
        if (result.errorCode() != null && !result.errorCode().isBlank()) {
            return true;
        }
        int status = result.statusCode();
        return status == 429 || status >= 500;
    }

    private void abort(String reason) {
        aborted = true;
        abortReason = reason;
        throw new CanaryAbortException(reason);
    }
}
