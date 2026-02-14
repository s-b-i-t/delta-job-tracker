package com.delta.jobtracker.crawl.model;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

public record HttpFetchResult(
    String requestedUrl,
    URI finalUri,
    int statusCode,
    String body,
    byte[] bodyBytes,
    String contentType,
    String contentEncoding,
    Instant fetchedAt,
    Duration duration,
    String errorCode,
    String errorMessage
) {
    public boolean isSuccessful() {
        return statusCode >= 200 && statusCode < 300 && errorCode == null;
    }

    public String finalUrlOrRequested() {
        return finalUri != null ? finalUri.toString() : requestedUrl;
    }
}
