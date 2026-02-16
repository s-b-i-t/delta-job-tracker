package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.AtsAttemptsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanySearchResult;
import com.delta.jobtracker.crawl.model.CoverageDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.CrawlTargetsDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.DiscoveryFailuresDiagnosticsResponse;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.FullCycleSummary;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.model.JobDeltaResponse;
import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.StatusResponse;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.CrawlStatusService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.service.UniverseIngestionService;
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

@RestController
@RequestMapping("/api")
public class CrawlController {
    private final UniverseIngestionService ingestionService;
    private final CrawlOrchestratorService crawlOrchestratorService;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;
    private final CrawlStatusService crawlStatusService;
    private final CrawlerProperties crawlerProperties;

    public CrawlController(
        UniverseIngestionService ingestionService,
        CrawlOrchestratorService crawlOrchestratorService,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService,
        CrawlStatusService crawlStatusService,
        CrawlerProperties crawlerProperties
    ) {
        this.ingestionService = ingestionService;
        this.crawlOrchestratorService = crawlOrchestratorService;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
        this.crawlStatusService = crawlStatusService;
        this.crawlerProperties = crawlerProperties;
    }

    @PostMapping("/ingest")
    public IngestionSummary ingestUniverse(
        @RequestParam(name = "source", required = false, defaultValue = "wiki") String source
    ) {
        return ingestionService.ingest(source);
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
            request == null ? null : request.resolveDomains(),
            request == null ? null : request.discoverCareers(),
            request == null ? null : request.atsOnly(),
            null
        );
        return crawlOrchestratorService.run(runRequest);
    }

    @PostMapping("/domains/resolve")
    public DomainResolutionResult resolveDomains(@RequestParam(name = "limit", required = false) Integer limit) {
        return domainResolutionService.resolveMissingDomains(limit);
    }

    @PostMapping("/careers/discover")
    public CareersDiscoveryResult discoverCareers(@RequestParam(name = "limit", required = false) Integer limit) {
        return careersDiscoveryService.discover(limit);
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

    @GetMapping("/diagnostics/discovery-failures")
    public DiscoveryFailuresDiagnosticsResponse getDiscoveryFailuresDiagnostics() {
        return crawlStatusService.getDiscoveryFailuresDiagnostics();
    }

    @GetMapping("/diagnostics/ats-attempts")
    public AtsAttemptsDiagnosticsResponse getAtsAttemptsDiagnostics() {
        return crawlStatusService.getAtsAttemptsDiagnostics();
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
        AtsType atsType = null;
        if (ats != null && !ats.isBlank()) {
            try {
                atsType = AtsType.valueOf(ats.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw new ResponseStatusException(BAD_REQUEST, "Unsupported ats value: " + ats);
            }
        }
        return crawlStatusService.getNewestJobs(limit, companyId, atsType, active, query);
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

    @GetMapping("/jobs/{id}")
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
