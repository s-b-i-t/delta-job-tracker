package com.delta.jobtracker.crawl.util;

import com.delta.jobtracker.crawl.model.DiscoveredUrlType;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Locale;

public final class UrlClassifier {
    private static final List<String> JOB_PATH_HINTS = List.of(
        "/careers",
        "/jobs",
        "/job",
        "/openings",
        "/positions",
        "/job-search",
        "/search-jobs"
    );

    private UrlClassifier() {
    }

    public static DiscoveredUrlType classify(String url) {
        URI uri = safeUri(url);
        if (uri == null || uri.getHost() == null) {
            return DiscoveredUrlType.OTHER;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (isAtsHost(host)) {
            return DiscoveredUrlType.ATS_LANDING;
        }
        String path = (uri.getPath() == null ? "" : uri.getPath()).toLowerCase(Locale.ROOT);
        for (String hint : JOB_PATH_HINTS) {
            if (path.contains(hint)) {
                return DiscoveredUrlType.CANDIDATE_JOB;
            }
        }
        return DiscoveredUrlType.OTHER;
    }

    public static boolean isAtsHost(String host) {
        String h = host.toLowerCase(Locale.ROOT);
        return h.endsWith("myworkdayjobs.com")
            || h.contains("workdayjobs")
            || h.contains("boards.greenhouse.io")
            || h.contains("boards-api.greenhouse.io")
            || h.contains("api.greenhouse.io")
            || h.contains("greenhouse.io")
            || h.contains("grnh.se")
            || h.contains("jobs.lever.co")
            || h.contains("api.lever.co");
    }

    public static URI safeUri(String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException ignored) {
            return null;
        }
    }
}
