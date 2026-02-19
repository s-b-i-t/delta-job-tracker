package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.SecCanarySummary;
import com.delta.jobtracker.crawl.model.SecIngestionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SecCanaryServiceTest {

    @Mock
    private UniverseIngestionService ingestionService;

    @Mock
    private DomainResolutionService domainResolutionService;

    @Mock
    private CareersDiscoveryService careersDiscoveryService;

    @Mock
    private CompanyCrawlerService companyCrawlerService;

    @Mock
    private CrawlJdbcRepository repository;

    @Test
    void runSecCanaryExecutesAllSteps() {
        List<String> tickers = List.of("AAA", "BBB");
        when(ingestionService.ingestSecCompanies(2))
            .thenReturn(new SecIngestionResult(tickers, 2, 0, List.of()));

        DomainResolutionResult domainResult = new DomainResolutionResult(1, 0, 0, 0, 0, List.of());
        when(domainResolutionService.resolveMissingDomainsForTickers(eq(tickers), eq(2), any()))
            .thenReturn(domainResult);

        Map<String, Integer> discovered = new LinkedHashMap<>();
        discovered.put("WORKDAY", 2);
        CareersDiscoveryResult discoveryResult = new CareersDiscoveryResult(discovered, 0, Map.of());
        when(careersDiscoveryService.discoverForTickers(eq(tickers), eq(2), any()))
            .thenReturn(discoveryResult);

        CompanyTarget targetOne = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);
        CompanyTarget targetTwo = new CompanyTarget(2L, "BBB", "Beta", null, "beta.com", null);
        when(repository.insertCrawlRun(any(), any(), any())).thenReturn(123L);
        when(repository.findCompanyTargetsWithAts(eq(tickers), eq(2)))
            .thenReturn(List.of(targetOne, targetTwo));

        CompanyCrawlSummary summaryOne = new CompanyCrawlSummary(
            1L,
            "AAA",
            "alpha.com",
            0,
            0,
            List.of(),
            0,
            3,
            true,
            Map.of()
        );
        CompanyCrawlSummary summaryTwo = new CompanyCrawlSummary(
            2L,
            "BBB",
            "beta.com",
            0,
            0,
            List.of(),
            0,
            2,
            false,
            Map.of()
        );
        when(companyCrawlerService.crawlCompany(eq(123L), eq(targetOne), ArgumentMatchers.any()))
            .thenReturn(summaryOne);
        when(companyCrawlerService.crawlCompany(eq(123L), eq(targetTwo), ArgumentMatchers.any()))
            .thenReturn(summaryTwo);

        when(repository.countCrawlRunCompaniesByAtsType(123L))
            .thenReturn(Map.of("WORKDAY", 2));
        when(repository.countCrawlRunCompanyFailures(123L))
            .thenReturn(Map.of("HTTP_404", 1L));

        SecCanaryService service = new SecCanaryService(
            ingestionService,
            domainResolutionService,
            careersDiscoveryService,
            companyCrawlerService,
            repository,
            new CrawlerProperties()
        );

        SecCanarySummary summary = service.runSecCanary(2);
        assertNotNull(summary);
        assertEquals(2, summary.companiesIngested());
        assertEquals(5, summary.jobsExtracted());
        assertEquals("COMPLETED_WITH_ERRORS", summary.status());
        assertEquals(domainResult, summary.domainResolution());
        assertEquals(discoveryResult, summary.careersDiscovery());
        assertEquals(2, summary.companiesCrawledByAtsType().get("WORKDAY"));
        assertEquals(1, summary.topErrors().get("HTTP_404"));
    }

    @Test
    void runSecCanaryStopsWhenNoCompanies() {
        when(ingestionService.ingestSecCompanies(5))
            .thenReturn(new SecIngestionResult(List.of(), 0, 0, List.of()));

        SecCanaryService service = new SecCanaryService(
            ingestionService,
            domainResolutionService,
            careersDiscoveryService,
            companyCrawlerService,
            repository,
            new CrawlerProperties()
        );

        SecCanarySummary summary = service.runSecCanary(5);
        assertEquals("NO_COMPANIES", summary.status());
        verify(domainResolutionService, never()).resolveMissingDomainsForTickers(any(), any(), any());
        verify(careersDiscoveryService, never()).discoverForTickers(any(), any(), any());
        verify(repository, never()).insertCrawlRun(any(), any(), any());
    }
}
