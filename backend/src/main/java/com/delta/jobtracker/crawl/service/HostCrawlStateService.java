package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.model.HostCrawlState;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Service
public class HostCrawlStateService {
    private static final List<Duration> BACKOFF_STEPS = List.of(
        Duration.ofMinutes(5),
        Duration.ofMinutes(15),
        Duration.ofMinutes(60),
        Duration.ofMinutes(360),
        Duration.ofMinutes(1440)
    );

    private final CrawlJdbcRepository repository;

    public HostCrawlStateService(CrawlJdbcRepository repository) {
        this.repository = repository;
    }

    public boolean isInCooldown(String host) {
        return nextAllowedAt(host) != null;
    }

    public Instant nextAllowedAt(String host) {
        if (host == null || host.isBlank()) {
            return null;
        }
        HostCrawlState state = repository.findHostCrawlState(normalizeHost(host));
        if (state == null || state.nextAllowedAt() == null) {
            return null;
        }
        Instant now = Instant.now();
        if (state.nextAllowedAt().isAfter(now)) {
            return state.nextAllowedAt();
        }
        return null;
    }

    public void recordFailure(String host, String errorCategory) {
        if (host == null || host.isBlank()) {
            return;
        }
        String normalized = normalizeHost(host);
        HostCrawlState existing = repository.findHostCrawlState(normalized);
        int failures = existing == null ? 0 : existing.consecutiveFailures();
        int nextFailures = Math.max(1, failures + 1);
        Duration backoff = backoffFor(nextFailures);
        Instant now = Instant.now();
        Instant nextAllowedAt = backoff == null ? null : now.plus(backoff);
        repository.upsertHostCrawlState(
            normalized,
            nextFailures,
            errorCategory,
            now,
            nextAllowedAt
        );
    }

    public void recordSuccess(String host) {
        if (host == null || host.isBlank()) {
            return;
        }
        String normalized = normalizeHost(host);
        HostCrawlState existing = repository.findHostCrawlState(normalized);
        if (existing == null) {
            return;
        }
        repository.upsertHostCrawlState(
            normalized,
            0,
            existing.lastErrorCategory(),
            Instant.now(),
            null
        );
    }

    private Duration backoffFor(int failures) {
        if (BACKOFF_STEPS.isEmpty()) {
            return null;
        }
        int index = Math.max(0, Math.min(failures, BACKOFF_STEPS.size()) - 1);
        return BACKOFF_STEPS.get(index);
    }

    private String normalizeHost(String host) {
        return host.trim().toLowerCase(Locale.ROOT);
    }
}
