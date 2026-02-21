package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.CrawlRunAsyncResponse;
import com.delta.jobtracker.crawl.model.CrawlRunStatusResponse;
import com.delta.jobtracker.crawl.model.CrawlRunCompanyResultView;
import com.delta.jobtracker.crawl.model.CrawlRunFailuresResponse;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.AtsAttemptsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CareersDiscoveryFailuresResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyResultView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunStatus;
import com.delta.jobtracker.crawl.model.CanaryRunResponse;
import com.delta.jobtracker.crawl.model.CanaryRunStatusResponse;
import com.delta.jobtracker.crawl.model.CompanySearchResult;
import com.delta.jobtracker.crawl.model.CoverageDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlTargetsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlRunDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.DiscoveryFailuresDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.FullCycleSummary;
import com.delta.jobtracker.crawl.model.HostCrawlState;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.model.JobDeltaResponse;
import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.JobPostingPageResponse;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.MissingDomainsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.SecUniverseIngestionSummary;
import com.delta.jobtracker.crawl.model.WorkdayInvalidUrlCleanupResponse;
import com.delta.jobtracker.crawl.model.StatusResponse;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.CrawlStatusService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryRunService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.service.SecCanaryService;
import com.delta.jobtracker.crawl.service.UniverseIngestionService;
import com.delta.jobtracker.crawl.service.WorkdayInvalidUrlCleanupService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@RequestMapping("/api")
public class CrawlController {
    private final UniverseIngestionService ingestionService;
    private final CrawlOrchestratorService crawlOrchestratorService;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;
    private final CareersDiscoveryRunService careersDiscoveryRunService;
    private final CrawlStatusService crawlStatusService;
    private final WorkdayInvalidUrlCleanupService workdayInvalidUrlCleanupService;
    private final CrawlerProperties crawlerProperties;
    private final SecCanaryService secCanaryService;
    private final HostCrawlStateService hostCrawlStateService;

    public CrawlController(
        UniverseIngestionService ingestionService,
        CrawlOrchestratorService crawlOrchestratorService,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService,
        CareersDiscoveryRunService careersDiscoveryRunService,
        CrawlStatusService crawlStatusService,
        WorkdayInvalidUrlCleanupService workdayInvalidUrlCleanupService,
        CrawlerProperties crawlerProperties,
        SecCanaryService secCanaryService,
        HostCrawlStateService hostCrawlStateService
    ) {
        this.ingestionService = ingestionService;
        this.crawlOrchestratorService = crawlOrchestratorService;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
        this.careersDiscoveryRunService = careersDiscoveryRunService;
        this.crawlStatusService = crawlStatusService;
        this.workdayInvalidUrlCleanupService = workdayInvalidUrlCleanupService;
        this.crawlerProperties = crawlerProperties;
        this.secCanaryService = secCanaryService;
        this.hostCrawlStateService = hostCrawlStateService;
    }

    @PostMapping("/ingest")
    public IngestionSummary ingestUniverse(
        @RequestParam(name = "source", required = false, defaultValue = "wiki") String source
    ) {
        return ingestionService.ingest(source);
    }

    @PostMapping("/universe/ingest/sec")
    public SecUniverseIngestionSummary ingestSecUniverse(
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        int safeLimit = limit == null ? 20000 : Math.max(1, limit);
        return ingestionService.ingestSecUniverse(safeLimit);
    }

    @PostMapping("/canary/sec")
    public CanaryRunResponse runSecCanary(
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return secCanaryService.startSecCanary(limit);
    }

    @PostMapping("/canary/sec-full-cycle")
    public CanaryRunResponse runSecFullCycleCanary(
        @RequestParam(name = "companyLimit", required = false) Integer limit,
        @RequestParam(name = "discoverVendorProbeOnly", required = false) Boolean vendorProbeOnly,
        @RequestParam(name = "crawl", required = false) Boolean crawl
    ) {
        return secCanaryService.startSecFullCycleCanary(limit, vendorProbeOnly, crawl);
    }

    @GetMapping("/canary/{runId:\\d+}")
    public CanaryRunStatusResponse getCanaryRunStatus(@PathVariable("runId") long runId) {
        CanaryRunStatusResponse status = secCanaryService.getCanaryRunStatus(runId);
        if (status == null) {
            throw new ResponseStatusException(NOT_FOUND, "Canary run not found: " + runId);
        }
        return status;
    }

    @GetMapping("/canary/latest")
    public CanaryRunStatusResponse getLatestCanaryRun(
        @RequestParam(name = "type", required = false) String type
    ) {
        CanaryRunStatusResponse status = secCanaryService.getLatestCanaryRunStatus(type);
        if (status == null && type != null) {
            throw new ResponseStatusException(NOT_FOUND, "Canary run not found for type: " + type);
        }
        if (status == null) {
            throw new ResponseStatusException(NOT_FOUND, "Canary run not found");
        }
        return status;
    }

    @GetMapping("/hosts/cooldown")
    public List<HostCrawlState> getHostsCoolingDown(
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return hostCrawlStateService.listCooldownHosts(limit);
    }

    @PostMapping("/crawl/run")
    public CrawlRunSummary runCrawl(@RequestBody(required = false) CrawlApiRunRequest request) {
        boolean ingestBeforeCrawl = request == null || request.ingestBeforeCrawl() == null || request.ingestBeforeCrawl();
        if (ingestBeforeCrawl) {
            ingestionService.ingest("wiki");
        }
        int companyLimit = request == null || request.companyLimit() == null
            ? crawlerProperties.getApi().getDefaultCompanyLimit()
            : Math.max(1, request.companyLimit());

        CrawlRunRequest runRequest = new CrawlRunRequest(
            request == null ? null : request.tickers(),
            companyLimit,
            request == null ? null : request.resolveLimit(),
            request == null ? null : request.discoverLimit(),
            request == null ? null : request.maxJobPages(),
            request == null ? null : request.maxSitemapUrls(),
            request == null ? null : request.maxJobsPerCompanyWorkday(),
            request == null ? null : request.resolveDomains(),
            request == null ? null : request.discoverCareers(),
            request == null ? null : request.atsOnly(),
            null
        );
        return crawlOrchestratorService.run(runRequest);
    }

    @PostMapping("/crawl/run/async")
    public CrawlRunAsyncResponse runCrawlAsync(@RequestBody(required = false) CrawlApiRunRequest request) {
        boolean ingestBeforeCrawl = request == null || request.ingestBeforeCrawl() == null || request.ingestBeforeCrawl();
        int companyLimit = request == null || request.companyLimit() == null
            ? crawlerProperties.getApi().getDefaultCompanyLimit()
            : Math.max(1, request.companyLimit());
        CrawlRunRequest runRequest = new CrawlRunRequest(
            request == null ? null : request.tickers(),
            companyLimit,
            request == null ? null : request.resolveLimit(),
            request == null ? null : request.discoverLimit(),
            request == null ? null : request.maxJobPages(),
            request == null ? null : request.maxSitemapUrls(),
            request == null ? null : request.maxJobsPerCompanyWorkday(),
            request == null ? null : request.resolveDomains(),
            request == null ? null : request.discoverCareers(),
            request == null ? null : request.atsOnly(),
            null
        );
        long crawlRunId = ingestBeforeCrawl
            ? crawlOrchestratorService.startAsync(runRequest, () -> ingestionService.ingest("wiki"))
            : crawlOrchestratorService.startAsync(runRequest);
        return new CrawlRunAsyncResponse(crawlRunId, "RUNNING");
    }

    @GetMapping("/crawl/run/{id:\\d+}")
    public CrawlRunStatusResponse getCrawlRunStatus(@PathVariable("id") long crawlRunId) {
        return crawlStatusService.getCrawlRunStatus(crawlRunId);
    }

    @GetMapping("/crawl/run/{id:\\d+}/companies")
    public List<CrawlRunCompanyResultView> getCrawlRunCompanies(
        @PathVariable("id") long crawlRunId,
        @RequestParam(name = "status", required = false) String status,
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return crawlStatusService.getCrawlRunCompanyResults(crawlRunId, status, limit);
    }

    @GetMapping("/crawl/run/{id:\\d+}/companies/stages")
    public List<CrawlRunCompanyResultView> getCrawlRunCompanyStages(
        @PathVariable("id") long crawlRunId,
        @RequestParam(name = "status", required = false) String status,
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return crawlStatusService.getCrawlRunCompanyStageResults(crawlRunId, status, limit);
    }

    @GetMapping("/crawl/run/{id:\\d+}/failures")
    public CrawlRunFailuresResponse getCrawlRunFailures(@PathVariable("id") long crawlRunId) {
        return crawlStatusService.getCrawlRunFailures(crawlRunId);
    }

    @PostMapping("/domains/resolve")
    public DomainResolutionResult resolveDomains(@RequestParam(name = "limit", required = false) Integer limit) {
        return domainResolutionService.resolveMissingDomains(limit);
    }

    @PostMapping("/careers/discover")
    public CareersDiscoveryRunResponse discoverCareers(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "batchSize", required = false) Integer batchSize,
        @RequestParam(name = "vendorProbeOnly", required = false) Boolean vendorProbeOnly
    ) {
        return careersDiscoveryRunService.startAsync(limit, batchSize, vendorProbeOnly);
    }

    @PostMapping("/careers/discover/async")
    public CareersDiscoveryRunResponse discoverCareersAsync(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "batchSize", required = false) Integer batchSize,
        @RequestParam(name = "vendorProbeOnly", required = false) Boolean vendorProbeOnly
    ) {
        return careersDiscoveryRunService.startAsync(limit, batchSize, vendorProbeOnly);
    }

    @GetMapping("/careers/discover/run/{id:\\d+}")
    public CareersDiscoveryRunStatus getCareersDiscoveryRun(@PathVariable("id") long runId) {
        CareersDiscoveryRunStatus status = careersDiscoveryRunService.getRunStatus(runId);
        if (status == null) {
            throw new ResponseStatusException(NOT_FOUND, "Discovery run not found: " + runId);
        }
        return status;
    }

    @GetMapping("/careers/discover/runs/latest")
    public CareersDiscoveryRunStatus getLatestCareersDiscoveryRun() {
        CareersDiscoveryRunStatus status = careersDiscoveryRunService.getLatestRunStatus();
        if (status == null) {
            throw new ResponseStatusException(NOT_FOUND, "No discovery runs found");
        }
        return status;
    }

    @GetMapping("/careers/discover/run/{id:\\d+}/companies")
    public List<CareersDiscoveryCompanyResultView> getCareersDiscoveryRunCompanies(
        @PathVariable("id") long runId,
        @RequestParam(name = "status", required = false) String status,
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return careersDiscoveryRunService.getCompanyResults(runId, status, limit);
    }

    @GetMapping("/careers/discover/run/{id:\\d+}/failures")
    public CareersDiscoveryFailuresResponse getCareersDiscoveryRunFailures(@PathVariable("id") long runId) {
        return careersDiscoveryRunService.getFailures(runId);
    }

    @PostMapping("/automation/full-cycle")
    public FullCycleSummary runFullCycle(
        @RequestParam(name = "companies", required = false) Integer companies,
        @RequestParam(name = "resolveLimit", required = false) Integer resolveLimit,
        @RequestParam(name = "discoverLimit", required = false) Integer discoverLimit,
        @RequestParam(name = "crawlLimit", required = false) Integer crawlLimit,
        @RequestParam(name = "maxJobPages", required = false) Integer maxJobPages,
        @RequestParam(name = "maxSitemapUrls", required = false) Integer maxSitemapUrls,
        @RequestParam(name = "atsOnly", required = false) Boolean atsOnly
    ) {
        int base = companies == null
            ? crawlerProperties.getAutomation().getDiscoverLimit()
            : Math.max(1, companies);
        int safeResolve = resolveLimit == null ? base : Math.max(1, resolveLimit);
        int safeDiscover = discoverLimit == null ? base : Math.max(1, discoverLimit);
        int safeCrawl = crawlLimit == null ? base : Math.max(1, crawlLimit);
        boolean crawlAtsOnly = atsOnly == null ? true : atsOnly;

        DomainResolutionResult resolution = domainResolutionService.resolveMissingDomains(safeResolve);
        Instant discoveryStartedAt = Instant.now();
        CareersDiscoveryResult discovery = careersDiscoveryService.discover(safeDiscover);

        CrawlRunRequest runRequest = new CrawlRunRequest(
            List.of(),
            safeCrawl,
            null,
            null,
            maxJobPages,
            maxSitemapUrls,
            null,
            false,
            false,
            crawlAtsOnly,
            crawlAtsOnly ? discoveryStartedAt : null
        );
        CrawlRunSummary crawlSummary = crawlOrchestratorService.run(runRequest);

        int jobsExtracted = 0;
        for (CompanyCrawlSummary summary : crawlSummary.companies()) {
            jobsExtracted += summary.jobsExtractedCount();
        }

        return new FullCycleSummary(
            safeResolve,
            safeDiscover,
            safeCrawl,
            resolution,
            discovery,
            crawlSummary,
            jobsExtracted,
            aggregateErrors(crawlSummary.companies(), 5)
        );
    }

    @GetMapping("/status")
    public StatusResponse getStatus() {
        return crawlStatusService.getStatus();
    }

    @GetMapping("/diagnostics/coverage")
    public CoverageDiagnosticsResponse getCoverageDiagnostics() {
        return crawlStatusService.getCoverageDiagnostics();
    }

    @GetMapping("/diagnostics/missing-domains")
    public MissingDomainsDiagnosticsResponse getMissingDomainsDiagnostics(
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return crawlStatusService.getMissingDomainsDiagnostics(limit);
    }

    @GetMapping("/diagnostics/discovery-failures")
    public DiscoveryFailuresDiagnosticsResponse getDiscoveryFailuresDiagnostics() {
        return crawlStatusService.getDiscoveryFailuresDiagnostics();
    }

    @GetMapping("/diagnostics/ats-attempts")
    public AtsAttemptsDiagnosticsResponse getAtsAttemptsDiagnostics() {
        return crawlStatusService.getAtsAttemptsDiagnostics();
    }

    @GetMapping("/diagnostics/crawl-runs")
    public CrawlRunDiagnosticsResponse getCrawlRunsDiagnostics(
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return crawlStatusService.getCrawlRunDiagnostics(limit);
    }

    @GetMapping("/diagnostics/crawl-targets")
    public CrawlTargetsDiagnosticsResponse getCrawlTargetsDiagnostics(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "atsOnly", required = false) Boolean atsOnly
    ) {
        int safeLimit = limit == null
            ? crawlerProperties.getApi().getDefaultCompanyLimit()
            : Math.max(1, limit);
        boolean crawlAtsOnly = atsOnly == null ? true : atsOnly;
        CrawlRunRequest request = new CrawlRunRequest(
            List.of(),
            safeLimit,
            null,
            null,
            null,
            null,
            null,
            false,
            false,
            crawlAtsOnly,
            null
        );
        List<String> tickers = crawlOrchestratorService.previewTargets(request).stream()
            .map(target -> target.ticker())
            .toList();
        return new CrawlTargetsDiagnosticsResponse(crawlAtsOnly, safeLimit, tickers.size(), tickers);
    }

    @GetMapping("/jobs")
    public List<JobPostingListView> getJobs(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "companyId", required = false) Long companyId,
        @RequestParam(name = "ats", required = false) String ats,
        @RequestParam(name = "active", required = false) Boolean active,
        @RequestParam(name = "q", required = false) String query
    ) {
        AtsType atsType = parseAtsType(ats);
        return crawlStatusService.getNewestJobs(limit, companyId, atsType, active, query);
    }

    @PostMapping("/jobs/cleanup/workday-invalid")
    public WorkdayInvalidUrlCleanupResponse cleanupInvalidWorkdayUrls(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "batchSize", required = false) Integer batchSize,
        @RequestParam(name = "dryRun", required = false, defaultValue = "false") boolean dryRun
    ) {
        return workdayInvalidUrlCleanupService.cleanupInvalidWorkdayUrls(limit, batchSize, dryRun);
    }

    @GetMapping("/jobs/page")
    public JobPostingPageResponse getJobsPage(
        @RequestParam(name="page", defaultValue="0") int page,
        @RequestParam(name="pageSize", defaultValue="50") int pageSize,
        @RequestParam(name="companyId", required=false) Long companyId,
        @RequestParam(name="ats", required=false) String ats,
        @RequestParam(name="active", required=false) Boolean active,
        @RequestParam(name="q", required=false) String query
    ) {
        AtsType atsType = parseAtsType(ats);
        return crawlStatusService.getJobPage(page, pageSize, companyId, atsType, active, query);
    }


    @GetMapping("/jobs/new")
    public List<JobPostingListView> getNewJobs(
        @RequestParam(name = "since") String since,
        @RequestParam(name = "companyId", required = false) Long companyId,
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "q", required = false) String query
    ) {
        return crawlStatusService.getNewJobs(since, companyId, limit, query);
    }

    @GetMapping("/jobs/closed")
    public List<JobPostingListView> getClosedJobs(
        @RequestParam(name = "since") String since,
        @RequestParam(name = "companyId", required = false) Long companyId,
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "q", required = false) String query
    ) {
        return crawlStatusService.getClosedJobs(since, companyId, limit, query);
    }

    @GetMapping("/jobs/delta")
    public JobDeltaResponse getJobDelta(
        @RequestParam(name = "companyId") Long companyId,
        @RequestParam(name = "fromRunId") Long fromRunId,
        @RequestParam(name = "toRunId") Long toRunId,
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        if (companyId == null || fromRunId == null || toRunId == null) {
            throw new ResponseStatusException(BAD_REQUEST, "companyId, fromRunId, and toRunId are required");
        }
        return crawlStatusService.getJobDelta(companyId, fromRunId, toRunId, limit);
    }

    @GetMapping("/jobs/{id:\\d+}")
    public JobPostingView getJobDetail(@PathVariable("id") long jobId) {
        return crawlStatusService.getJobDetail(jobId);
    }

    @GetMapping("/companies")
    public List<CompanySearchResult> searchCompanies(
        @RequestParam(name = "search") String search,
        @RequestParam(name = "limit", required = false) Integer limit
    ) {
        return crawlStatusService.searchCompanies(search, limit);
    }

    private AtsType parseAtsType(String ats) {
        if (ats == null || ats.isBlank()) {
            return null;
        }
        try {
            return AtsType.valueOf(ats.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(BAD_REQUEST, "Unsupported ats value: " + ats);
        }
    }

    private java.util.Map<String, Integer> aggregateErrors(List<CompanyCrawlSummary> companies, int limit) {
        java.util.Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        if (companies == null) {
            return counts;
        }
        for (CompanyCrawlSummary summary : companies) {
            if (summary.topErrors() == null) {
                continue;
            }
            summary.topErrors().forEach((key, value) -> counts.put(key, counts.getOrDefault(key, 0) + value));
        }
        return counts.entrySet().stream()
            .sorted(java.util.Map.Entry.<String, Integer>comparingByValue().reversed())
            .limit(limit)
            .collect(
                java.util.LinkedHashMap::new,
                (map, entry) -> map.put(entry.getKey(), entry.getValue()),
                java.util.LinkedHashMap::putAll
            );
    }
}
