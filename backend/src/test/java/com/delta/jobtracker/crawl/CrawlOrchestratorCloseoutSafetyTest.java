package com.delta.jobtracker.crawl;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CompanyCrawlerService;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CrawlOrchestratorCloseoutSafetyTest {

    @Mock
    private CrawlJdbcRepository repository;
    @Mock
    private CompanyCrawlerService companyCrawlerService;
    @Mock
    private DomainResolutionService domainResolutionService;
    @Mock
    private CareersDiscoveryService careersDiscoveryService;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final ExecutorService runExecutor = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
        runExecutor.shutdownNow();
    }

    @Test
    void doesNotCloseOutWhenCompanyCrawlUnsafe() {
        CrawlerProperties properties = new CrawlerProperties();
        CrawlerProperties.Automation automation = new CrawlerProperties.Automation();
        automation.setResolveMissingDomains(false);
        automation.setDiscoverCareersEndpoints(false);
        properties.setAutomation(automation);
        properties.getApi().setDefaultCompanyLimit(1);

        when(repository.insertCrawlRun(any(), any(), any())).thenReturn(1L);
        when(repository.findCompanyTargetsWithAts(any(), anyInt())).thenReturn(
            List.of(new CompanyTarget(1L, "ABC", "ABC Inc", null, "abc.com", null))
        );
        when(companyCrawlerService.crawlCompany(eq(1L), any(), any())).thenReturn(
            new CompanyCrawlSummary(
                1L,
                "ABC",
                "abc.com",
                0,
                0,
                List.of(),
                0,
                0,
                false,
                Map.of("crawl_failed", 1)
            )
        );

        CrawlOrchestratorService service = new CrawlOrchestratorService(
            repository,
            companyCrawlerService,
            executor,
            runExecutor,
            properties,
            domainResolutionService,
            careersDiscoveryService
        );

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

        service.run(request);

        verify(repository, never()).markPostingsInactiveNotSeenInRun(eq(1L), eq(1L));
    }
}
