package com.delta.jobtracker.crawl.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;

public final class JobUrlUtils {
    public static final String WORKDAY_INVALID_URL_PREFIX = "https://community.workday.com/invalid-url";

    private JobUrlUtils() {
    }

    public static String sanitizeCanonicalUrl(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return null;
        }
        String trimmed = candidate.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        if (trimmed.toLowerCase(Locale.ROOT).contains("invalid-url")) {
            return null;
        }
        URI uri = safeUri(trimmed);
        if (uri == null || uri.getHost() == null) {
            return null;
        }
        String scheme = uri.getScheme();
        if (scheme == null || (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme))) {
            return null;
        }
        if (isAtsApiUrl(uri)) {
            return null;
        }
        return trimmed;
    }

    public static boolean isAtsApiUrl(URI uri) {
        if (uri == null || uri.getHost() == null) {
            return false;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        String path = uri.getPath() == null ? "" : uri.getPath().toLowerCase(Locale.ROOT);
        if (host.contains("boards-api.greenhouse.io") || host.contains("api.greenhouse.io")) {
            return true;
        }
        if (host.contains("api.lever.co") && path.contains("/postings/")) {
            return true;
        }
        if (host.contains("myworkdayjobs.com") && path.contains("/wday/cxs/")) {
            return true;
        }
        if (host.contains("myworkdayjobs.com") && (path.endsWith("/jobs") || path.endsWith("/jobs/"))) {
            return true;
        }
        return false;
    }

    public static boolean isInvalidWorkdayUrl(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return false;
        }
        return candidate.trim().toLowerCase(Locale.ROOT).startsWith(WORKDAY_INVALID_URL_PREFIX);
    }

    public static boolean isInvalidWorkdayRedirectUrl(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return false;
        }
        return candidate.trim().toLowerCase(Locale.ROOT).contains("community.workday.com/invalid-url");
    }

    public static URI safeUri(String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException ignored) {
            return null;
        }
    }
}
