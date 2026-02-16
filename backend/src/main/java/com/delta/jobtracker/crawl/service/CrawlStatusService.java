package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanySearchResult;
import com.delta.jobtracker.crawl.model.CoverageDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.JobDeltaItem;
import com.delta.jobtracker.crawl.model.JobDeltaResponse;
import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.RecentCrawlStatus;
import com.delta.jobtracker.crawl.model.StatusResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

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

    public CoverageDiagnosticsResponse getCoverageDiagnostics() {
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception ignored) {
            dbConnected = false;
        }
        if (!dbConnected) {
            return new CoverageDiagnosticsResponse(new LinkedHashMap<>(), new LinkedHashMap<>());
        }
        Map<String, Long> counts = repository.coverageCounts();
        Map<String, Long> atsByType = repository.countAtsEndpointsByType();
        return new CoverageDiagnosticsResponse(counts, atsByType);
    }

    public List<JobPostingListView> getNewestJobs(Integer limit, Long companyId, AtsType atsType, Boolean active, String query) {
        int safeLimit = limit == null ? 50 : Math.max(1, Math.min(limit, 500));
        try {
            return repository.findNewestJobs(safeLimit, companyId, atsType, active, query);
        } catch (Exception e) {
            log.warn("Failed to load newest jobs", e);
            return List.of();
        }
    }

    public JobDeltaResponse getJobDelta(long companyId, long fromRunId, long toRunId, Integer limit) {
        int safeLimit = limit == null ? 50 : Math.max(1, Math.min(limit, 200));
        CrawlRunMeta fromRun = repository.findCrawlRunById(fromRunId);
        if (fromRun == null) {
            throw new ResponseStatusException(BAD_REQUEST, "fromRunId not found: " + fromRunId);
        }
        CrawlRunMeta toRun = repository.findCrawlRunById(toRunId);
        if (toRun == null) {
            throw new ResponseStatusException(BAD_REQUEST, "toRunId not found: " + toRunId);
        }
        Instant toRunFinished = toRun.finishedAt() == null ? Instant.now() : toRun.finishedAt();
        int newCount = repository.countNewJobsForRun(companyId, toRunId, toRun.startedAt(), toRunFinished);
        int removedCount = repository.countRemovedJobsForRun(companyId, fromRunId);
        List<JobDeltaItem> newJobs = repository.findNewJobsForRun(companyId, toRunId, toRun.startedAt(), toRunFinished, safeLimit);
        List<JobDeltaItem> removedJobs = repository.findRemovedJobsForRun(companyId, fromRunId, safeLimit);
        return new JobDeltaResponse(
            companyId,
            fromRunId,
            toRunId,
            newCount,
            removedCount,
            0,
            newJobs,
            removedJobs,
            List.of()
        );
    }

    public List<JobPostingListView> getNewJobs(String since, Long companyId, Integer limit, String query) {
        Instant sinceInstant = parseSince(since);
        int safeLimit = limit == null ? 50 : Math.max(1, Math.min(limit, 500));
        return repository.findNewJobsSince(sinceInstant, companyId, safeLimit, query);
    }

    public List<JobPostingListView> getClosedJobs(String since, Long companyId, Integer limit, String query) {
        Instant sinceInstant = parseSince(since);
        int safeLimit = limit == null ? 50 : Math.max(1, Math.min(limit, 500));
        return repository.findClosedJobsSince(sinceInstant, companyId, safeLimit, query);
    }

    public JobPostingView getJobDetail(long jobId) {
        JobPostingView view = repository.findJobPostingById(jobId);
        if (view == null) {
            throw new ResponseStatusException(NOT_FOUND, "Job not found: " + jobId);
        }
        return view;
    }

    public List<CompanySearchResult> searchCompanies(String search, Integer limit) {
        if (search == null || search.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "search is required");
        }
        int safeLimit = limit == null ? 20 : Math.max(1, Math.min(limit, 100));
        return repository.searchCompanies(search, safeLimit);
    }

    private Instant parseSince(String since) {
        if (since == null || since.isBlank()) {
            throw new ResponseStatusException(BAD_REQUEST, "since is required");
        }
        try {
            return Instant.parse(since);
        } catch (Exception e) {
            throw new ResponseStatusException(BAD_REQUEST, "Invalid since timestamp: " + since);
        }
    }
}
