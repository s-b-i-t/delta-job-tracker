package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlDaemonBootstrapResponse;
import com.delta.jobtracker.crawl.model.CrawlDaemonStatusResponse;
import com.delta.jobtracker.crawl.model.CrawlQueueStats;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.persistence.CrawlQueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Service
public class CrawlDaemonService {
    private static final Logger log = LoggerFactory.getLogger(CrawlDaemonService.class);
    private static final int ERROR_SAMPLE_LIMIT = 5;
    private static final int MAX_ERROR_LENGTH = 500;

    private final CrawlQueueRepository queueRepository;
    private final CrawlJdbcRepository repository;
    private final CompanyCrawlerService companyCrawlerService;
    private final UniverseIngestionService ingestionService;
    private final CrawlerProperties properties;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    private final String instanceId;

    private ExecutorService executor;
    private int activeWorkerCount;

    public CrawlDaemonService(
        CrawlQueueRepository queueRepository,
        CrawlJdbcRepository repository,
        CompanyCrawlerService companyCrawlerService,
        UniverseIngestionService ingestionService,
        CrawlerProperties properties
    ) {
        this.queueRepository = queueRepository;
        this.repository = repository;
        this.companyCrawlerService = companyCrawlerService;
        this.ingestionService = ingestionService;
        this.properties = properties;
        this.instanceId = "daemon-" + ManagementFactory.getRuntimeMXBean().getName();
    }

    @PostConstruct
    public void startIfEnabled() {
        if (properties.getDaemon().isEnabled()) {
            start();
        }
    }

    @PreDestroy
    public void stopOnShutdown() {
        stop();
    }

    public CrawlDaemonStatusResponse getStatus() {
        CrawlQueueStats stats;
        try {
            stats = queueRepository.fetchQueueStats(ERROR_SAMPLE_LIMIT);
        } catch (Exception e) {
            log.warn("Failed to load crawl queue stats", e);
            stats = new CrawlQueueStats(0, 0, null, List.of());
        }
        return new CrawlDaemonStatusResponse(running.get(), activeWorkerCount, stats);
    }

    public CrawlDaemonBootstrapResponse bootstrap(String source) {
        IngestionSummary ingestion = ingestionService.ingest(source);
        int queued = queueRepository.bootstrapQueue();
        return new CrawlDaemonBootstrapResponse(source, ingestion, queued);
    }

    public void start() {
        synchronized (lifecycleLock) {
            if (running.get()) {
                return;
            }
            int workerCount = properties.getDaemon().getWorkerCount();
            int pollIntervalMs = properties.getDaemon().getPollIntervalMs();
            long lockTtlSeconds = properties.getDaemon().getLockTtlSeconds();
            activeWorkerCount = workerCount;
            executor = Executors.newFixedThreadPool(workerCount, runnable -> {
                Thread thread = new Thread(runnable);
                thread.setName("crawl-daemon-worker");
                thread.setDaemon(true);
                return thread;
            });
            running.set(true);
            for (int i = 0; i < workerCount; i++) {
                int workerIndex = i + 1;
                executor.submit(() -> workerLoop(workerIndex, pollIntervalMs, lockTtlSeconds));
            }
        }
    }

    public void stop() {
        synchronized (lifecycleLock) {
            if (!running.get()) {
                return;
            }
            running.set(false);
            if (executor != null) {
                executor.shutdownNow();
                try {
                    executor.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
                executor = null;
            }
            activeWorkerCount = 0;
        }
    }

    private void workerLoop(int workerIndex, int pollIntervalMs, long lockTtlSeconds) {
        Thread.currentThread().setName("crawl-daemon-worker-" + workerIndex);
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            Long companyId = null;
            try {
                companyId = queueRepository.claimNextCompany(instanceId, lockTtlSeconds);
            } catch (Exception e) {
                log.warn("Daemon worker {} failed to claim queue item", workerIndex, e);
                sleep(pollIntervalMs);
                continue;
            }

            if (companyId == null) {
                sleep(pollIntervalMs);
                continue;
            }

            try {
                crawlCompany(companyId);
            } catch (Exception e) {
                log.warn("Daemon worker {} failed while crawling company {}", workerIndex, companyId, e);
                recordUnexpectedFailure(companyId);
            }
        }
    }

    private void crawlCompany(long companyId) {
        int failuresSoFar = queueRepository.getConsecutiveFailures(companyId);
        Instant startedAt = Instant.now();
        long crawlRunId = repository.insertCrawlRun(startedAt, "RUNNING", "daemon");
        repository.updateCrawlRunProgress(crawlRunId, 1, 0, 0, 0, startedAt);

        CompanyTarget target = repository.findCompanyTargetById(companyId);
        if (target == null) {
            String error = "missing_domain";
            repository.completeCrawlRun(crawlRunId, Instant.now(), "FAILED", error);
            queueRepository.markFailure(companyId, nextFailureRunAt(failuresSoFar), error);
            return;
        }

        CrawlRunRequest request = new CrawlRunRequest(
            List.of(),
            1,
            null,
            null,
            null,
            null,
            false,
            false,
            null,
            null
        );

        try {
            CompanyCrawlSummary summary = companyCrawlerService.crawlCompany(crawlRunId, target, request);
            Instant finishedAt = Instant.now();
            boolean success = summary.closeoutSafe();
            repository.updateCrawlRunProgress(
                crawlRunId,
                1,
                success ? 1 : 0,
                success ? 0 : 1,
                summary.jobsExtractedCount(),
                finishedAt
            );
            if (success) {
                repository.markPostingsInactiveNotSeenInRun(target.companyId(), crawlRunId);
                queueRepository.markSuccess(companyId, nextSuccessRunAt());
                repository.completeCrawlRun(crawlRunId, finishedAt, "COMPLETED", "daemon");
            } else {
                String error = summarizeErrors(summary.topErrors());
                queueRepository.markFailure(companyId, nextFailureRunAt(failuresSoFar), error);
                repository.completeCrawlRun(crawlRunId, finishedAt, "FAILED", error);
            }
        } catch (Exception e) {
            String error = "exception=" + e.getClass().getSimpleName();
            queueRepository.markFailure(companyId, nextFailureRunAt(failuresSoFar), error);
            repository.completeCrawlRun(crawlRunId, Instant.now(), "FAILED", error);
            log.warn("Daemon crawl failed for company {}", companyId, e);
        }
    }

    private void recordUnexpectedFailure(long companyId) {
        try {
            int failuresSoFar = queueRepository.getConsecutiveFailures(companyId);
            queueRepository.markFailure(companyId, nextFailureRunAt(failuresSoFar), "daemon_exception");
        } catch (Exception ex) {
            log.warn("Failed to record daemon failure for company {}", companyId, ex);
        }
    }

    private Instant nextSuccessRunAt() {
        int minutes = properties.getDaemon().getSuccessIntervalMinutes();
        return Instant.now().plusSeconds(Math.max(1, minutes) * 60L);
    }

    private Instant nextFailureRunAt(int failuresSoFar) {
        List<Integer> backoff = properties.getDaemon().getFailureBackoffMinutes();
        int safeFailures = Math.max(0, failuresSoFar);
        int index = Math.min(safeFailures, backoff.size() - 1);
        int minutes = backoff.get(index);
        int jitterSeconds = ThreadLocalRandom.current().nextInt(5, 30);
        return Instant.now().plusSeconds(Math.max(1, minutes) * 60L + jitterSeconds);
    }

    private String summarizeErrors(Map<String, Integer> errors) {
        if (errors == null || errors.isEmpty()) {
            return "company_crawl_failed";
        }
        String summary = errors.keySet().stream()
            .limit(3)
            .collect(Collectors.joining(","));
        if (summary.length() > MAX_ERROR_LENGTH) {
            return summary.substring(0, MAX_ERROR_LENGTH);
        }
        return summary;
    }

    private void sleep(int pollIntervalMs) {
        try {
            TimeUnit.MILLISECONDS.sleep(Math.max(100, pollIntervalMs));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
