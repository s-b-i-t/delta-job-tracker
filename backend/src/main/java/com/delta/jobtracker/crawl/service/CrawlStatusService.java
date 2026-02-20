package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.AtsAttemptsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.AtsAttemptSample;
import com.delta.jobtracker.crawl.model.CompanySearchResult;
import com.delta.jobtracker.crawl.model.CoverageDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.CrawlRunActivityCounts;
import com.delta.jobtracker.crawl.model.CrawlRunDiagnosticsEntry;
import com.delta.jobtracker.crawl.model.CrawlRunDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlRunCompanyFailureView;
import com.delta.jobtracker.crawl.model.CrawlRunCompanyResultView;
import com.delta.jobtracker.crawl.model.CrawlRunFailuresResponse;
import com.delta.jobtracker.crawl.model.DiscoveryFailuresDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.JobDeltaItem;
import com.delta.jobtracker.crawl.model.JobDeltaResponse;
import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.JobPostingPageResponse;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.RecentCrawlStatus;
import com.delta.jobtracker.crawl.model.StatusResponse;
import com.delta.jobtracker.crawl.model.CrawlRunStatus;
import com.delta.jobtracker.crawl.model.CrawlRunStatusResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
            Instant lastActivityAt = repository.findLastActivityAtForRun(latestMeta.crawlRunId());
            if (lastActivityAt == null) {
                lastActivityAt = latestMeta.startedAt();
            }
            Instant finishedOrNow = latestMeta.finishedAt() == null ? Instant.now() : latestMeta.finishedAt();
            long secondsRunning = Duration.between(latestMeta.startedAt(), finishedOrNow).getSeconds();
            recent = new RecentCrawlStatus(
                latestMeta.crawlRunId(),
                latestMeta.startedAt(),
                latestMeta.finishedAt(),
                latestMeta.status(),
                repository.countJobsForRun(latestMeta),
                repository.findTopErrorsForRun(latestMeta.crawlRunId(), 5),
                secondsRunning,
                lastActivityAt
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
            return new CoverageDiagnosticsResponse(new LinkedHashMap<>(), new LinkedHashMap<>(), new LinkedHashMap<>());
        }
        Map<String, Long> counts = repository.coverageCounts();
        Map<String, Long> atsByType = repository.countAtsEndpointsByType();
        Map<String, Long> atsByMethod = repository.countAtsEndpointsByDetectionMethod();
        return new CoverageDiagnosticsResponse(counts, atsByType, atsByMethod);
    }

    public DiscoveryFailuresDiagnosticsResponse getDiscoveryFailuresDiagnostics() {
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception ignored) {
            dbConnected = false;
        }
        if (!dbConnected) {
            return new DiscoveryFailuresDiagnosticsResponse(new LinkedHashMap<>(), List.of());
        }
        Map<String, Long> counts = repository.countDiscoveryFailuresByReason();
        return new DiscoveryFailuresDiagnosticsResponse(
            counts,
            repository.findRecentDiscoveryFailures(20)
        );
    }

    public AtsAttemptsDiagnosticsResponse getAtsAttemptsDiagnostics() {
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception ignored) {
            dbConnected = false;
        }
        if (!dbConnected) {
            return new AtsAttemptsDiagnosticsResponse(new LinkedHashMap<>(), List.of());
        }
        Map<String, Long> counts = repository.countAtsApiAttemptsByStatus();
        List<AtsAttemptSample> samples = repository.findRecentAtsApiFailures(20);
        return new AtsAttemptsDiagnosticsResponse(counts, samples);
    }

    public CrawlRunDiagnosticsResponse getCrawlRunDiagnostics(Integer limit) {
        int safeLimit = limit == null ? 10 : Math.max(1, Math.min(limit, 100));
        boolean dbConnected;
        try {
            dbConnected = repository.isDbReachable();
        } catch (Exception ignored) {
            dbConnected = false;
        }
        if (!dbConnected) {
            return new CrawlRunDiagnosticsResponse(safeLimit, 0, List.of());
        }
        List<CrawlRunMeta> runs = repository.findRecentCrawlRuns(safeLimit);
        List<CrawlRunDiagnosticsEntry> entries = new ArrayList<>();
        for (CrawlRunMeta run : runs) {
            CrawlRunActivityCounts counts = repository.findRunActivityCounts(run.crawlRunId());
            entries.add(new CrawlRunDiagnosticsEntry(
                run.crawlRunId(),
                run.startedAt(),
                run.finishedAt(),
                run.status(),
                counts.discoveredUrls(),
                counts.discoveredSitemaps(),
                counts.jobPostings()
            ));
        }
        return new CrawlRunDiagnosticsResponse(safeLimit, entries.size(), entries);
    }

    public CrawlRunStatusResponse getCrawlRunStatus(long crawlRunId) {
        CrawlRunStatus status = repository.findCrawlRunStatus(crawlRunId);
        if (status == null) {
            throw new ResponseStatusException(NOT_FOUND, "Crawl run not found: " + crawlRunId);
        }
        Instant lastActivityAt = repository.findLastActivityAtForRun(crawlRunId);
        if (lastActivityAt == null) {
            lastActivityAt = status.lastHeartbeatAt() == null ? status.startedAt() : status.lastHeartbeatAt();
        }
        return new CrawlRunStatusResponse(
            status.crawlRunId(),
            status.startedAt(),
            status.finishedAt(),
            status.status(),
            status.companiesAttempted(),
            status.companiesSucceeded(),
            status.companiesFailed(),
            status.jobsExtractedCount(),
            status.lastHeartbeatAt(),
            lastActivityAt
        );
    }

    public List<CrawlRunCompanyResultView> getCrawlRunCompanyResults(long crawlRunId, String status, Integer limit) {
        ensureRunExists(crawlRunId);
        int safeLimit = limit == null ? 200 : Math.max(1, Math.min(limit, 500));
        return repository.findCrawlRunCompanyOverallResults(crawlRunId, normalizeStatus(status), safeLimit);
    }

    public List<CrawlRunCompanyResultView> getCrawlRunCompanyStageResults(long crawlRunId, String status, Integer limit) {
        ensureRunExists(crawlRunId);
        int safeLimit = limit == null ? 200 : Math.max(1, Math.min(limit, 500));
        return repository.findCrawlRunCompanyResults(crawlRunId, normalizeStatus(status), safeLimit);
    }

    public CrawlRunFailuresResponse getCrawlRunFailures(long crawlRunId) {
        ensureRunExists(crawlRunId);
        Map<String, Long> counts = repository.countCrawlRunCompanyFailures(crawlRunId);
        List<CrawlRunCompanyFailureView> failures = repository.findRecentCrawlRunCompanyFailures(crawlRunId, 20);
        return new CrawlRunFailuresResponse(counts, failures);
    }

    public List<JobPostingListView> getNewestJobs(Integer limit, Long companyId, AtsType atsType, Boolean active, String query) {
        JobPostingPageResponse page = getJobPage(0, limit, companyId, atsType, active, query);
        return page.items();
    }

    public JobPostingPageResponse getJobPage(
        Integer page,
        Integer pageSize,
        Long companyId,
        AtsType atsType,
        Boolean active,
        String query
    ) {
        int safePage = page == null ? 0 : Math.max(0, page);
        int safePageSize = pageSize == null ? 50 : Math.max(1, Math.min(pageSize, 500));
        int offset = safePage * safePageSize;
        try {
            long total = repository.countJobPostingsFiltered(companyId, atsType, active, query);
            List<JobPostingListView> items = repository.findJobPostingsPage(
                safePageSize,
                offset,
                companyId,
                atsType,
                active,
                query
            );
            return new JobPostingPageResponse(total, safePage, safePageSize, items);
        } catch (Exception e) {
            log.warn("Failed to load paged jobs", e);
            return new JobPostingPageResponse(0, safePage, safePageSize, List.of());
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

    private void ensureRunExists(long crawlRunId) {
        CrawlRunMeta run = repository.findCrawlRunById(crawlRunId);
        if (run == null) {
            throw new ResponseStatusException(NOT_FOUND, "Crawl run not found: " + crawlRunId);
        }
    }

    private String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        return status.trim().toUpperCase();
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
