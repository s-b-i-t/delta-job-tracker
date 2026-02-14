package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.StatusResponse;
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
import org.springframework.web.server.ResponseStatusException;

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
            request == null ? null : request.discoverCareers()
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

    @GetMapping("/status")
    public StatusResponse getStatus() {
        return crawlStatusService.getStatus();
    }

    @GetMapping("/jobs")
    public List<JobPostingView> getJobs(
        @RequestParam(name = "limit", required = false) Integer limit,
        @RequestParam(name = "companyId", required = false) Long companyId,
        @RequestParam(name = "ats", required = false) String ats
    ) {
        AtsType atsType = null;
        if (ats != null && !ats.isBlank()) {
            try {
                atsType = AtsType.valueOf(ats.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw new ResponseStatusException(BAD_REQUEST, "Unsupported ats value: " + ats);
            }
        }
        return crawlStatusService.getNewestJobs(limit, companyId, atsType);
    }
}
