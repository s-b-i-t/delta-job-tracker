package com.delta.jobtracker.crawl.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class FrontierUrlCanonicalizer {
  private static final List<String> TRACKING_QUERY_PREFIXES = List.of("utm_");
  private static final List<String> TRACKING_QUERY_KEYS =
      List.of("gclid", "fbclid", "mc_cid", "mc_eid", "_hsenc", "_hsmi");

  public String canonicalize(String rawUrl) {
    if (rawUrl == null || rawUrl.isBlank()) {
      return null;
    }
    URI uri = parseUri(rawUrl.trim());
    if (uri == null || uri.getHost() == null || uri.getScheme() == null) {
      return null;
    }

    String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
    if (!"http".equals(scheme) && !"https".equals(scheme)) {
      return null;
    }

    String host = uri.getHost().toLowerCase(Locale.ROOT);
    int port = uri.getPort();
    String path = normalizePath(uri.getPath());
    String query = normalizeQuery(uri.getRawQuery());

    StringBuilder normalized = new StringBuilder();
    normalized.append(scheme).append("://").append(host);
    if (port > 0 && !isDefaultPort(scheme, port)) {
      normalized.append(':').append(port);
    }
    normalized.append(path);
    if (query != null && !query.isBlank()) {
      normalized.append('?').append(query);
    }
    return normalized.toString();
  }

  public String extractHost(String canonicalUrl) {
    if (canonicalUrl == null || canonicalUrl.isBlank()) {
      return null;
    }
    URI uri = parseUri(canonicalUrl);
    if (uri == null || uri.getHost() == null) {
      return null;
    }
    return uri.getHost().toLowerCase(Locale.ROOT);
  }

  private URI parseUri(String candidate) {
    if (candidate == null || candidate.isBlank()) {
      return null;
    }
    String value = candidate.trim();
    String lower = value.toLowerCase(Locale.ROOT);
    if (!lower.startsWith("http://") && !lower.startsWith("https://")) {
      value = "https://" + value;
    }
    try {
      return new URI(value);
    } catch (URISyntaxException ignored) {
      return null;
    }
  }

  private String normalizePath(String rawPath) {
    if (rawPath == null || rawPath.isBlank()) {
      return "/";
    }
    String path = rawPath.replaceAll("/{2,}", "/");
    if (path.length() > 1 && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  private String normalizeQuery(String rawQuery) {
    if (rawQuery == null || rawQuery.isBlank()) {
      return null;
    }
    Map<String, List<String>> valuesByKey = new LinkedHashMap<>();
    for (String pair : rawQuery.split("&")) {
      if (pair == null || pair.isBlank()) {
        continue;
      }
      int idx = pair.indexOf('=');
      String rawKey = idx < 0 ? pair : pair.substring(0, idx);
      String rawValue = idx < 0 ? "" : pair.substring(idx + 1);
      String key = urlDecode(rawKey).toLowerCase(Locale.ROOT);
      if (isTrackingQueryParam(key)) {
        continue;
      }
      String value = urlDecode(rawValue);
      valuesByKey.computeIfAbsent(key, ignored -> new ArrayList<>()).add(value);
    }

    if (valuesByKey.isEmpty()) {
      return null;
    }

    List<String> encoded = new ArrayList<>();
    valuesByKey.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              List<String> values = new ArrayList<>(entry.getValue());
              values.sort(Comparator.naturalOrder());
              String encodedKey = urlEncode(entry.getKey());
              for (String value : values) {
                encoded.add(encodedKey + "=" + urlEncode(value == null ? "" : value));
              }
            });
    return String.join("&", encoded);
  }

  private boolean isTrackingQueryParam(String key) {
    if (key == null || key.isBlank()) {
      return false;
    }
    for (String prefix : TRACKING_QUERY_PREFIXES) {
      if (key.startsWith(prefix)) {
        return true;
      }
    }
    return TRACKING_QUERY_KEYS.contains(key);
  }

  private boolean isDefaultPort(String scheme, int port) {
    return ("http".equals(scheme) && port == 80) || ("https".equals(scheme) && port == 443);
  }

  private String urlDecode(String value) {
    return URLDecoder.decode(value == null ? "" : value, StandardCharsets.UTF_8);
  }

  private String urlEncode(String value) {
    return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8)
        .replace("+", "%20");
  }
}
