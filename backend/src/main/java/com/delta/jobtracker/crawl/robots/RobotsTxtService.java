package com.delta.jobtracker.crawl.robots;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class RobotsTxtService {
    private static final Logger log = LoggerFactory.getLogger(RobotsTxtService.class);
    private static final Duration ROBOTS_UNAVAILABLE_TTL = Duration.ofHours(6);
    private static final int MAX_UNAVAILABLE_HOSTS = 2048;

    private final CrawlerProperties properties;
    private final PoliteHttpClient httpClient;
    private final Map<String, RobotsRules> cache = new ConcurrentHashMap<>();
    private final Map<String, Instant> robotsUnavailableByHost = new ConcurrentHashMap<>();

    public RobotsTxtService(CrawlerProperties properties, PoliteHttpClient httpClient) {
        this.properties = properties;
        this.httpClient = httpClient;
    }

    public RobotsRules getRulesForHost(String host) {
        if (host == null || host.isBlank()) {
            return RobotsRules.allowAll();
        }
        String key = host.toLowerCase(Locale.ROOT);
        return cache.computeIfAbsent(key, this::loadRulesForHost);
    }

    public boolean isAllowed(String url) {
        URI uri = toUri(url);
        if (uri == null || uri.getHost() == null) {
            return true;
        }
        RobotsRules rules = getRulesForHost(uri.getHost());
        String path = uri.getRawPath() == null || uri.getRawPath().isBlank() ? "/" : uri.getRawPath();
        if (uri.getRawQuery() != null && !uri.getRawQuery().isBlank()) {
            path = path + "?" + uri.getRawQuery();
        }
        return rules.isAllowed(path);
    }

    public boolean isAllowedForAtsAdapter(String url) {
        if (isAllowed(url)) {
            return true;
        }
        boolean robotsUnavailable = isRobotsUnavailableForUrl(url);
        boolean allowedByPolicy = robotsUnavailable && properties.getRobots().isAllowAtsAdapterWhenUnavailable();
        if (allowedByPolicy) {
            URI uri = toUri(url);
            String host = uri == null ? null : uri.getHost();
            log.info(
                "robots policy bypass for ats adapter host={} robots_unavailable=true allowAtsAdapterWhenUnavailable=true",
                host
            );
        }
        return allowedByPolicy;
    }

    public boolean isRobotsUnavailableForUrl(String url) {
        URI uri = toUri(url);
        if (uri == null || uri.getHost() == null) {
            return false;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        getRulesForHost(host);
        return isRobotsUnavailableForHost(host);
    }

    private RobotsRules loadRulesForHost(String host) {
        String robotsUrl = "https://" + host + "/robots.txt";
        HttpFetchResult fetch = httpClient.get(robotsUrl, "text/plain,text/*;q=0.9,*/*;q=0.1");
        if (!fetch.isSuccessful()) {
            markRobotsUnavailable(host);
            boolean failOpen = properties.getRobots().isFailOpen();
            boolean atsBypass = properties.getRobots().isAllowAtsAdapterWhenUnavailable();
            String decision = failOpen ? "allow_all" : "disallow_all";
            log.warn(
                "robots fetch failed host={} robots_unavailable=true status={} errorCode={} errorMessage={} decision={} atsAdapterBypass={}",
                host,
                fetch.statusCode(),
                fetch.errorCode(),
                fetch.errorMessage(),
                decision,
                atsBypass
            );
            return failOpen ? RobotsRules.allowAll() : RobotsRules.disallowAll();
        }
        robotsUnavailableByHost.remove(host);
        RobotsRules rules = RobotsRules.parse(fetch.body());
        log.debug("Loaded robots for host {} with {} sitemap hints", host, rules.getSitemapUrls().size());
        return rules;
    }

    private void markRobotsUnavailable(String host) {
        Instant now = Instant.now();
        robotsUnavailableByHost.put(host, now);
        evictUnavailableHosts(now);
    }

    private boolean isRobotsUnavailableForHost(String host) {
        Instant markedAt = robotsUnavailableByHost.get(host);
        if (markedAt == null) {
            return false;
        }
        if (markedAt.plus(ROBOTS_UNAVAILABLE_TTL).isBefore(Instant.now())) {
            robotsUnavailableByHost.remove(host, markedAt);
            cache.remove(host);
            return false;
        }
        return true;
    }

    private void evictUnavailableHosts(Instant now) {
        Instant cutoff = now.minus(ROBOTS_UNAVAILABLE_TTL);
        List<String> expiredHosts = robotsUnavailableByHost.entrySet().stream()
            .filter(entry -> entry.getValue().isBefore(cutoff))
            .map(Map.Entry::getKey)
            .toList();
        for (String host : expiredHosts) {
            robotsUnavailableByHost.remove(host);
            cache.remove(host);
        }

        int overflow = robotsUnavailableByHost.size() - MAX_UNAVAILABLE_HOSTS;
        if (overflow <= 0) {
            return;
        }
        List<String> oldestHosts = robotsUnavailableByHost.entrySet().stream()
            .sorted(Map.Entry.comparingByValue(Comparator.naturalOrder()))
            .limit(overflow)
            .map(Map.Entry::getKey)
            .toList();
        for (String host : oldestHosts) {
            robotsUnavailableByHost.remove(host);
            cache.remove(host);
        }
    }

    private URI toUri(String url) {
        try {
            return new URI(url);
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
