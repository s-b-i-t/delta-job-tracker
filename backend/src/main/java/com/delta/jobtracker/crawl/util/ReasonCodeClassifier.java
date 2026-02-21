package com.delta.jobtracker.crawl.util;

import java.util.Locale;

public final class ReasonCodeClassifier {
  public static final String NO_DOMAIN = "NO_DOMAIN";
  public static final String ROBOTS_BLOCKED = "ROBOTS_BLOCKED";
  public static final String SITEMAP_NOT_FOUND = "SITEMAP_NOT_FOUND";
  public static final String TIMEOUT = "TIMEOUT";
  public static final String DNS_FAILURE = "DNS_FAILURE";
  public static final String TLS_FAILURE = "TLS_FAILURE";
  public static final String HTTP_401_403 = "HTTP_401_403";
  public static final String HTTP_404 = "HTTP_404";
  public static final String HTTP_429_RATE_LIMIT = "HTTP_429_RATE_LIMIT";
  public static final String HTTP_5XX = "HTTP_5XX";
  public static final String PARSING_FAILED = "PARSING_FAILED";
  public static final String ATS_NOT_FOUND = "ATS_NOT_FOUND";
  public static final String HOST_COOLDOWN = "HOST_COOLDOWN";
  public static final String UNKNOWN = "UNKNOWN";

  private ReasonCodeClassifier() {}

  public static String fromHttpStatus(Integer status) {
    if (status == null || status <= 0) {
      return UNKNOWN;
    }
    if (status == 401 || status == 403) {
      return HTTP_401_403;
    }
    if (status == 404) {
      return HTTP_404;
    }
    if (status == 408) {
      return TIMEOUT;
    }
    if (status == 429) {
      return HTTP_429_RATE_LIMIT;
    }
    if (status >= 500 && status < 600) {
      return HTTP_5XX;
    }
    return UNKNOWN;
  }

  public static String fromErrorCode(String errorCode, String errorMessage) {
    if (errorCode == null || errorCode.isBlank()) {
      return UNKNOWN;
    }
    String code = errorCode.toLowerCase(Locale.ROOT);
    if (code.contains("timeout")) {
      return TIMEOUT;
    }
    if (code.contains("io_error")) {
      String lower = errorMessage == null ? "" : errorMessage.toLowerCase(Locale.ROOT);
      if (lower.contains("unknownhost")
          || lower.contains("name or service not known")
          || lower.contains("no such host")) {
        return DNS_FAILURE;
      }
      if (lower.contains("ssl") || lower.contains("handshake")) {
        return TLS_FAILURE;
      }
      return UNKNOWN;
    }
    return UNKNOWN;
  }

  public static String fromErrorKey(String key) {
    if (key == null || key.isBlank()) {
      return UNKNOWN;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    if (lower.contains("blocked_by_robots")) {
      return ROBOTS_BLOCKED;
    }
    if (lower.contains("homepage_blocked_by_robots")
        || lower.contains("sitemap_blocked_by_robots")) {
      return ROBOTS_BLOCKED;
    }
    if (lower.contains("host_cooldown")) {
      return HOST_COOLDOWN;
    }
    if (lower.contains("no_sitemaps")
        || lower.contains("sitemap_no_urls")
        || lower.contains("no_candidate_urls")
        || lower.contains("sitemap_fetch_failed")) {
      return SITEMAP_NOT_FOUND;
    }
    if (lower.contains("no_jobposting") || lower.contains("ats_detected_no_endpoint")) {
      return ATS_NOT_FOUND;
    }
    if (lower.contains("parse")
        || lower.contains("invalid_payload")
        || lower.contains("gzip_decode_error")
        || lower.contains("empty_sitemap_payload")) {
      return PARSING_FAILED;
    }
    if (lower.contains("time_budget") || lower.contains("budget_exceeded")) {
      return TIMEOUT;
    }
    if (lower.contains("timeout")) {
      return TIMEOUT;
    }
    Integer httpStatus = parseHttpStatus(lower);
    if (httpStatus != null) {
      return fromHttpStatus(httpStatus);
    }
    return UNKNOWN;
  }

  public static boolean isRetryable(String reasonCode) {
    if (reasonCode == null) {
      return false;
    }
    return switch (reasonCode) {
      case TIMEOUT, DNS_FAILURE, TLS_FAILURE, HTTP_429_RATE_LIMIT, HTTP_5XX -> true;
      default -> false;
    };
  }

  public static Integer parseHttpStatus(String key) {
    if (key == null || key.isBlank()) {
      return null;
    }
    String lower = key.toLowerCase(Locale.ROOT);
    int idx = lower.lastIndexOf("http_");
    if (idx < 0) {
      return null;
    }
    String tail = lower.substring(idx + 5);
    int end = 0;
    while (end < tail.length() && Character.isDigit(tail.charAt(end))) {
      end++;
    }
    if (end == 0) {
      return null;
    }
    try {
      return Integer.parseInt(tail.substring(0, end));
    } catch (NumberFormatException ignored) {
      return null;
    }
  }
}
