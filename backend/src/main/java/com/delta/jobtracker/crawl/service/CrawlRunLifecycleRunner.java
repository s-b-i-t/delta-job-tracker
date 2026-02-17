package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CrawlRunActivityCounts;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Component
public class CrawlRunLifecycleRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(CrawlRunLifecycleRunner.class);

    private final CrawlJdbcRepository repository;
    private final CrawlerProperties properties;

    public CrawlRunLifecycleRunner(CrawlJdbcRepository repository, CrawlerProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception e) {
            dbConnected = false;
        }
        if (!dbConnected) {
            log.warn("Skipping crawl run cleanup because database is unreachable");
            return;
        }

        int staleMinutes = properties.getStaleRunMinutes();
        Instant cutoff = Instant.now().minus(Duration.ofMinutes(staleMinutes));
        List<CrawlRunMeta> running = repository.findRunningCrawlRuns();
        for (CrawlRunMeta run : running) {
            if (run.startedAt().isAfter(cutoff)) {
                continue;
            }
            CrawlRunActivityCounts counts = repository.findRunActivityCounts(run.crawlRunId());
            if (counts.totalActivity() == 0) {
                repository.completeCrawlRun(
                    run.crawlRunId(),
                    Instant.now(),
                    "ABORTED",
                    "aborted_on_startup_no_activity"
                );
                log.info("Aborted stale crawl run {} startedAt={} with no activity", run.crawlRunId(), run.startedAt());
            }
        }
    }
}
