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
import java.util.Iterator;
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
    private final Map<String, Instant> robotsUnavailableAtByHost = new ConcurrentHashMap<>();

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
        clearRobotsUnavailable(host);
        RobotsRules rules = RobotsRules.parse(fetch.body());
        log.debug("Loaded robots for host {} with {} sitemap hints", host, rules.getSitemapUrls().size());
        return rules;
    }

    private void markRobotsUnavailable(String host) {
        Instant now = Instant.now();
        robotsUnavailableAtByHost.put(host, now);
        evictUnavailableHosts(now);
    }

    private void clearRobotsUnavailable(String host) {
        robotsUnavailableAtByHost.remove(host);
    }

    private boolean isRobotsUnavailableForHost(String host) {
        Instant markedAt = robotsUnavailableAtByHost.get(host);
        if (markedAt == null) {
            return false;
        }
        if (markedAt.plus(ROBOTS_UNAVAILABLE_TTL).isBefore(Instant.now())) {
            robotsUnavailableAtByHost.remove(host, markedAt);
            cache.remove(host);
            return false;
        }
        return true;
    }

    private void evictUnavailableHosts(Instant now) {
        Instant cutoff = now.minus(ROBOTS_UNAVAILABLE_TTL);
        Iterator<Map.Entry<String, Instant>> iterator = robotsUnavailableAtByHost.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Instant> entry = iterator.next();
            if (entry.getValue().isBefore(cutoff)) {
                iterator.remove();
                cache.remove(entry.getKey());
            }
        }

        while (robotsUnavailableAtByHost.size() > MAX_UNAVAILABLE_HOSTS) {
            String oldestHost = null;
            Instant oldestSeen = null;
            for (Map.Entry<String, Instant> entry : robotsUnavailableAtByHost.entrySet()) {
                if (oldestSeen == null || entry.getValue().isBefore(oldestSeen)) {
                    oldestSeen = entry.getValue();
                    oldestHost = entry.getKey();
                }
            }
            if (oldestHost == null) {
                break;
            }
            robotsUnavailableAtByHost.remove(oldestHost);
            cache.remove(oldestHost);
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
