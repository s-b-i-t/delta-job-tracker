package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.RecentCrawlStatus;
import com.delta.jobtracker.crawl.model.StatusResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class CrawlStatusService {
    private static final Logger log = LoggerFactory.getLogger(CrawlStatusService.class);
    private final CrawlJdbcRepository repository;

    public CrawlStatusService(CrawlJdbcRepository repository) {
        this.repository = repository;
    }

    public StatusResponse getStatus() {
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception ignored) {
            dbConnected = false;
        }
        if (!dbConnected) {
            return new StatusResponse(false, new LinkedHashMap<>(), null);
        }

        Map<String, Long> counts = repository.tableCounts();
        CrawlRunMeta latestMeta = repository.findMostRecentCrawlRun();
        RecentCrawlStatus recent = null;
        if (latestMeta != null) {
            recent = new RecentCrawlStatus(
                latestMeta.crawlRunId(),
                latestMeta.startedAt(),
                latestMeta.finishedAt(),
                latestMeta.status(),
                repository.countJobsForRun(latestMeta),
                repository.findTopErrorsForRun(latestMeta.crawlRunId(), 5)
            );
        }
        return new StatusResponse(true, counts, recent);
    }

    public List<JobPostingView> getNewestJobs(Integer limit, Long companyId, AtsType atsType) {
        int safeLimit = limit == null ? 50 : Math.max(1, Math.min(limit, 500));
        try {
            return repository.findNewestJobs(safeLimit, companyId, atsType);
        } catch (Exception e) {
            log.warn("Failed to load newest jobs", e);
            return List.of();
        }
    }
}
