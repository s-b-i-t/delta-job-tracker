package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

@Service
public class CrawlOrchestratorService {
    private static final Logger log = LoggerFactory.getLogger(CrawlOrchestratorService.class);

    private final CrawlJdbcRepository repository;
    private final CompanyCrawlerService companyCrawlerService;
    private final ExecutorService crawlExecutor;
    private final CrawlerProperties properties;
    private final DomainResolutionService domainResolutionService;
    private final CareersDiscoveryService careersDiscoveryService;

    public CrawlOrchestratorService(
        CrawlJdbcRepository repository,
        CompanyCrawlerService companyCrawlerService,
        ExecutorService crawlExecutor,
        CrawlerProperties properties,
        DomainResolutionService domainResolutionService,
        CareersDiscoveryService careersDiscoveryService
    ) {
        this.repository = repository;
        this.companyCrawlerService = companyCrawlerService;
        this.crawlExecutor = crawlExecutor;
        this.properties = properties;
        this.domainResolutionService = domainResolutionService;
        this.careersDiscoveryService = careersDiscoveryService;
    }

    public CrawlRunSummary run(CrawlRunRequest request) {
        Instant startedAt = Instant.now();
        long crawlRunId = repository.insertCrawlRun(startedAt, "RUNNING", "crawl started");

        List<String> tickers = request.normalizedTickers();
        int companyLimit = request.companyLimit() == null
            ? properties.getApi().getDefaultCompanyLimit()
            : Math.max(1, request.companyLimit());
        int resolveLimit = request.resolveLimit() == null
            ? properties.getAutomation().getResolveLimit()
            : Math.max(1, request.resolveLimit());
        int discoverLimit = request.discoverLimit() == null
            ? properties.getAutomation().getDiscoverLimit()
            : Math.max(1, request.discoverLimit());
        if (shouldResolveDomains(request)) {
            DomainResolutionResult resolution = tickers.isEmpty()
                ? domainResolutionService.resolveMissingDomains(resolveLimit)
                : domainResolutionService.resolveMissingDomainsForTickers(tickers, resolveLimit);
            log.info(
                "Domain resolver before crawl: resolved={} no_wikipedia_title={} no_item={} no_p856={} wdqs_error={}",
                resolution.resolvedCount(),
                resolution.noWikipediaTitleCount(),
                resolution.noItemCount(),
                resolution.noP856Count(),
                resolution.wdqsErrorCount()
            );
        }
        if (shouldDiscoverCareers(request)) {
            CareersDiscoveryResult discovery = tickers.isEmpty()
                ? careersDiscoveryService.discover(discoverLimit)
                : careersDiscoveryService.discoverForTickers(tickers, discoverLimit);
            log.info("Careers discovery before crawl: discovered={}, failed={}", discovery.discoveredCountByAtsType(), discovery.failedCount());
        }

        List<CompanyTarget> targets = repository.findCompanyTargets(tickers, companyLimit);

        if (targets.isEmpty()) {
            Instant finishedAt = Instant.now();
            repository.completeCrawlRun(crawlRunId, finishedAt, "NO_TARGETS", "No company domains matched");
            return new CrawlRunSummary(crawlRunId, startedAt, finishedAt, "NO_TARGETS", List.of());
        }

        List<CompletableFuture<CompanyCrawlSummary>> futures = new ArrayList<>();
        for (CompanyTarget target : targets) {
            futures.add(CompletableFuture.supplyAsync(
                () -> companyCrawlerService.crawlCompany(crawlRunId, target, request),
                crawlExecutor
            ));
        }

        List<CompanyCrawlSummary> summaries = new ArrayList<>();
        boolean hadErrors = false;
        for (int i = 0; i < futures.size(); i++) {
            CompanyTarget target = targets.get(i);
            try {
                summaries.add(futures.get(i).join());
            } catch (Exception e) {
                hadErrors = true;
                log.warn("Company crawl failed for {} ({})", target.ticker(), target.domain(), e);
                Map<String, Integer> errorMap = new LinkedHashMap<>();
                errorMap.put("company_crawl_exception", 1);
                summaries.add(new CompanyCrawlSummary(
                    target.companyId(),
                    target.ticker(),
                    target.domain(),
                    0,
                    0,
                    List.of(),
                    0,
                    0,
                    errorMap
                ));
            }
        }

        Instant finishedAt = Instant.now();
        String status = hadErrors ? "COMPLETED_WITH_ERRORS" : "COMPLETED";
        repository.completeCrawlRun(
            crawlRunId,
            finishedAt,
            status,
            "companies=" + summaries.size()
        );
        return new CrawlRunSummary(crawlRunId, startedAt, finishedAt, status, summaries);
    }

    private boolean shouldResolveDomains(CrawlRunRequest request) {
        if (request.resolveDomains() != null) {
            return request.resolveDomains();
        }
        return properties.getAutomation().isResolveMissingDomains();
    }

    private boolean shouldDiscoverCareers(CrawlRunRequest request) {
        if (request.discoverCareers() != null) {
            return request.discoverCareers();
        }
        return properties.getAutomation().isDiscoverCareersEndpoints();
    }
}
