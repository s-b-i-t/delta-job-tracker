package com.delta.jobtracker.crawl.sitemap;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.SitemapDiscoveryResult;
import com.delta.jobtracker.crawl.model.SitemapFetchRecord;
import com.delta.jobtracker.crawl.model.SitemapUrlEntry;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Service
public class SitemapService {
    private static final Logger log = LoggerFactory.getLogger(SitemapService.class);
    private static final int MAX_SITEMAP_BYTES = 2_000_000;
    private final PoliteHttpClient httpClient;
    private final RobotsTxtService robotsTxtService;

    public SitemapService(PoliteHttpClient httpClient, RobotsTxtService robotsTxtService) {
        this.httpClient = httpClient;
        this.robotsTxtService = robotsTxtService;
    }

    public SitemapDiscoveryResult discover(
        List<String> seedSitemaps,
        int maxDepth,
        int maxSitemaps,
        int maxUrls
    ) {
        ArrayDeque<SitemapTask> queue = new ArrayDeque<>();
        for (String seed : seedSitemaps) {
            String normalized = normalizeSitemapUrl(seed);
            if (normalized != null) {
                queue.addLast(new SitemapTask(normalized, 0));
            }
        }

        LinkedHashSet<String> visitedSitemaps = new LinkedHashSet<>();
        LinkedHashMap<String, String> discoveredUrls = new LinkedHashMap<>();
        List<SitemapFetchRecord> fetchedRecords = new ArrayList<>();
        Map<String, Integer> errors = new LinkedHashMap<>();

        while (!queue.isEmpty() && visitedSitemaps.size() < maxSitemaps) {
            SitemapTask current = queue.removeFirst();
            if (current.depth() > maxDepth || visitedSitemaps.contains(current.url())) {
                continue;
            }
            visitedSitemaps.add(current.url());

            if (!robotsTxtService.isAllowed(current.url())) {
                log.debug("Sitemap blocked by robots: {}", current.url());
                increment(errors, "blocked_by_robots");
                continue;
            }

            HttpFetchResult fetch = httpClient.get(
                current.url(),
                "application/xml,text/xml;q=0.9,*/*;q=0.1",
                MAX_SITEMAP_BYTES
            );
            if (!fetch.isSuccessful()) {
                increment(errors, errorKey(fetch));
                continue;
            }
            String xmlPayload;
            try {
                xmlPayload = extractXmlPayload(current.url(), fetch);
            } catch (IOException e) {
                increment(errors, "gzip_decode_error");
                continue;
            }
            if (xmlPayload == null || xmlPayload.isBlank()) {
                increment(errors, "empty_sitemap_payload");
                continue;
            }

            int urlCountFromCurrent = 0;
            Document xml = Jsoup.parse(xmlPayload, "", Parser.xmlParser());

            List<Element> childSitemaps = xml.select("sitemap > loc");
            if (!childSitemaps.isEmpty() && current.depth() < maxDepth) {
                for (Element loc : childSitemaps) {
                    String child = normalizeSitemapUrl(loc.text());
                    if (child != null && !visitedSitemaps.contains(child) && visitedSitemaps.size() + queue.size() < maxSitemaps) {
                        queue.addLast(new SitemapTask(child, current.depth() + 1));
                    }
                }
            }

            for (Element urlElement : xml.select("url")) {
                Element locElement = urlElement.selectFirst("loc");
                if (locElement == null) {
                    continue;
                }
                String loc = normalizeSitemapUrl(locElement.text());
                if (loc == null || discoveredUrls.containsKey(loc) || discoveredUrls.size() >= maxUrls) {
                    continue;
                }
                Element lastmodElement = urlElement.selectFirst("lastmod");
                String lastmod = lastmodElement == null ? null : lastmodElement.text().trim();
                discoveredUrls.put(loc, lastmod);
                urlCountFromCurrent++;
            }

            fetchedRecords.add(new SitemapFetchRecord(current.url(), Instant.now(), urlCountFromCurrent));
            if (discoveredUrls.size() >= maxUrls) {
                break;
            }
        }

        List<SitemapUrlEntry> entries = discoveredUrls.entrySet().stream()
            .map(entry -> new SitemapUrlEntry(entry.getKey(), entry.getValue()))
            .toList();
        return new SitemapDiscoveryResult(fetchedRecords, entries, errors);
    }

    private String errorKey(HttpFetchResult fetch) {
        if (fetch.errorCode() != null) {
            return fetch.errorCode();
        }
        if (fetch.statusCode() > 0) {
            return "http_" + fetch.statusCode();
        }
        return "unknown_error";
    }

    private void increment(Map<String, Integer> errors, String key) {
        errors.put(key, errors.getOrDefault(key, 0) + 1);
    }

    private String extractXmlPayload(String sitemapUrl, HttpFetchResult fetch) throws IOException {
        byte[] bodyBytes = fetch.bodyBytes();
        if (bodyBytes == null && fetch.body() != null) {
            bodyBytes = fetch.body().getBytes(StandardCharsets.UTF_8);
        }
        if (bodyBytes == null) {
            return fetch.body();
        }

        if (isGzipPayload(sitemapUrl, fetch, bodyBytes)) {
            try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bodyBytes))) {
                return new String(gzipInputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
        return new String(bodyBytes, StandardCharsets.UTF_8);
    }

    private boolean isGzipPayload(String sitemapUrl, HttpFetchResult fetch, byte[] bodyBytes) {
        String requestedUrl = sitemapUrl == null ? "" : sitemapUrl.toLowerCase(Locale.ROOT);
        String resolvedUrl = fetch.finalUrlOrRequested() == null
            ? ""
            : fetch.finalUrlOrRequested().toLowerCase(Locale.ROOT);
        if (requestedUrl.endsWith(".gz") || resolvedUrl.endsWith(".gz")) {
            return true;
        }
        if (containsIgnoreCase(fetch.contentEncoding(), "gzip")) {
            return true;
        }
        return bodyBytes.length >= 2
            && (bodyBytes[0] & 0xFF) == 0x1f
            && (bodyBytes[1] & 0xFF) == 0x8b;
    }

    private boolean containsIgnoreCase(String value, String token) {
        return value != null && value.toLowerCase(Locale.ROOT).contains(token);
    }

    private String normalizeSitemapUrl(String url) {
        if (url == null || url.isBlank()) {
            return null;
        }
        String normalized = url.trim();
        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            normalized = "https://" + normalized;
        }
        if (!normalized.contains("://")) {
            return null;
        }
        return normalized.toLowerCase(Locale.ROOT).startsWith("http") ? normalized : null;
    }

    private record SitemapTask(String url, int depth) {
    }
}
