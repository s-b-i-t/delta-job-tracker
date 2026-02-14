package com.delta.jobtracker.crawl;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CrawlOrchestratorServiceLimitTest {

    @Mock
    private CrawlJdbcRepository repository;
    @Mock
    private CompanyCrawlerService companyCrawlerService;
    @Mock
    private DomainResolutionService domainResolutionService;
    @Mock
    private CareersDiscoveryService careersDiscoveryService;

    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    void usesApiDefaultCompanyLimitAndIndependentAutomationLimits() {
        CrawlerProperties properties = baseProperties(37, 111, 222);
        CrawlOrchestratorService service = createService(properties);

        CrawlRunRequest request = new CrawlRunRequest(
            List.of(),
            null,
            null,
            null,
            null,
            null,
            true,
            true
        );
        service.run(request);

        verify(domainResolutionService).resolveMissingDomains(111);
        verify(careersDiscoveryService).discover(222);
        verify(repository).findCompanyTargets(eq(List.of()), eq(37));
    }

    @Test
    void requestLimitsOverrideDefaultsIndependently() {
        CrawlerProperties properties = baseProperties(50, 200, 200);
        CrawlOrchestratorService service = createService(properties);

        CrawlRunRequest request = new CrawlRunRequest(
            List.of(),
            7,
            91,
            92,
            null,
            null,
            true,
            true
        );
        service.run(request);

        verify(domainResolutionService).resolveMissingDomains(91);
        verify(careersDiscoveryService).discover(92);
        verify(repository).findCompanyTargets(eq(List.of()), eq(7));
    }

    private CrawlOrchestratorService createService(CrawlerProperties properties) {
        when(repository.insertCrawlRun(any(), any(), any())).thenReturn(1L);
        when(domainResolutionService.resolveMissingDomains(anyInt())).thenReturn(new DomainResolutionResult(0, 0, List.of()));
        when(careersDiscoveryService.discover(anyInt())).thenReturn(new CareersDiscoveryResult(new LinkedHashMap<>(), 0, new LinkedHashMap<>()));
        when(repository.findCompanyTargets(any(), anyInt())).thenReturn(List.of());
        return new CrawlOrchestratorService(
            repository,
            companyCrawlerService,
            executor,
            properties,
            domainResolutionService,
            careersDiscoveryService
        );
    }

    private CrawlerProperties baseProperties(int companyLimit, int resolveLimit, int discoverLimit) {
        CrawlerProperties properties = new CrawlerProperties();
        CrawlerProperties.Api api = new CrawlerProperties.Api();
        api.setDefaultCompanyLimit(companyLimit);
        properties.setApi(api);

        CrawlerProperties.Automation automation = new CrawlerProperties.Automation();
        automation.setResolveMissingDomains(true);
        automation.setDiscoverCareersEndpoints(true);
        automation.setResolveLimit(resolveLimit);
        automation.setDiscoverLimit(discoverLimit);
        properties.setAutomation(automation);
        return properties;
    }
}
