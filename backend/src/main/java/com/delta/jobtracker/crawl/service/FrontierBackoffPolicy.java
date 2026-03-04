package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.FrontierBackoffDecision;
import java.time.Duration;
import java.time.Instant;
import java.util.Locale;
import org.springframework.stereotype.Component;

@Component
public class FrontierBackoffPolicy {
  private static final int MAX_RATE_LIMIT_BACKOFF_STATE = 6;
  private static final int MAX_TRANSIENT_BACKOFF_STATE = 4;

  private final CrawlerProperties properties;

  public FrontierBackoffPolicy(CrawlerProperties properties) {
    this.properties = properties;
  }

  public FrontierBackoffDecision onResult(
      int currentBackoffState, int httpStatus, String errorCode) {
    Instant now = Instant.now();
    String bucket = classifyBucket(httpStatus, errorCode);

    if ("HTTP_429".equals(bucket) || "HTTP_403".equals(bucket)) {
      int nextState = Math.min(MAX_RATE_LIMIT_BACKOFF_STATE, Math.max(0, currentBackoffState) + 1);
      long delaySeconds = Math.min(3600L, 30L * (1L << Math.max(0, nextState - 1)));
      return new FrontierBackoffDecision(bucket, nextState, now.plusSeconds(delaySeconds));
    }

    if ("HTTP_5XX".equals(bucket) || "ERROR_TIMEOUT".equals(bucket)) {
      int nextState = Math.min(MAX_TRANSIENT_BACKOFF_STATE, Math.max(0, currentBackoffState) + 1);
      long delaySeconds = Math.min(600L, 15L * (1L << Math.max(0, nextState - 1)));
      return new FrontierBackoffDecision(bucket, nextState, now.plusSeconds(delaySeconds));
    }

    if ("HTTP_2XX".equals(bucket)) {
      return new FrontierBackoffDecision(bucket, 0, now.plusMillis(properties.getPerHostDelayMs()));
    }

    if ("HTTP_4XX".equals(bucket)) {
      int nextState = Math.max(0, currentBackoffState - 1);
      return new FrontierBackoffDecision(
          bucket, nextState, now.plusMillis(properties.getPerHostDelayMs()));
    }

    int nextState = Math.min(MAX_TRANSIENT_BACKOFF_STATE, Math.max(0, currentBackoffState) + 1);
    return new FrontierBackoffDecision(bucket, nextState, now.plus(Duration.ofSeconds(20)));
  }

  public String classifyBucket(int httpStatus, String errorCode) {
    if (httpStatus >= 200 && httpStatus < 300) {
      return "HTTP_2XX";
    }
    if (httpStatus == 429) {
      return "HTTP_429";
    }
    if (httpStatus == 403) {
      return "HTTP_403";
    }
    if (httpStatus >= 500) {
      return "HTTP_5XX";
    }
    if (httpStatus >= 400) {
      return "HTTP_4XX";
    }
    if (errorCode != null && errorCode.toLowerCase(Locale.ROOT).contains("timeout")) {
      return "ERROR_TIMEOUT";
    }
    return "ERROR";
  }
}
