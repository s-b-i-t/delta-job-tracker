package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class CrawlCliRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(CrawlCliRunner.class);

    private final CrawlerProperties properties;
    private final UniverseIngestionService ingestionService;
    private final CrawlOrchestratorService crawlOrchestratorService;
    private final ConfigurableApplicationContext applicationContext;

    public CrawlCliRunner(
        CrawlerProperties properties,
        UniverseIngestionService ingestionService,
        CrawlOrchestratorService crawlOrchestratorService,
        ConfigurableApplicationContext applicationContext
    ) {
        this.properties = properties;
        this.ingestionService = ingestionService;
        this.crawlOrchestratorService = crawlOrchestratorService;
        this.applicationContext = applicationContext;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.getCli().isRun()) {
            return;
        }

        if (properties.getCli().isIngestBeforeCrawl()) {
            ingestionService.ingest();
        }

        List<String> tickers = Arrays.stream(properties.getCli().getTickers().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .toList();

        CrawlRunRequest request = new CrawlRunRequest(
            tickers,
            properties.getCli().getLimit(),
            null,
            null,
            properties.getExtraction().getMaxJobPages(),
            properties.getSitemap().getMaxUrlsPerDomain(),
            null,
            null,
            null,
            null
        );

        CrawlRunSummary summary = crawlOrchestratorService.run(request);
        log.info("Crawl run {} completed with status {}", summary.crawlRunId(), summary.status());
        for (CompanyCrawlSummary company : summary.companies()) {
            log.info(
                "Summary {}: sitemaps={}, candidates={}, ats={}, jobPages={}, jobs={}, errors={}",
                company.ticker(),
                company.sitemapsFoundCount(),
                company.candidateUrlsCount(),
                company.atsDetected(),
                company.jobpostingPagesFoundCount(),
                company.jobsExtractedCount(),
                company.topErrors()
            );
        }

        if (properties.getCli().isExitAfterRun()) {
            int exitCode = SpringApplication.exit(applicationContext, () -> 0);
            System.exit(exitCode);
        }
    }
}
