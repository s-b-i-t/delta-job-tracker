package com.delta.jobtracker.crawl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.lenient;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.DomainResolutionMetrics;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CompanyCrawlerService;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CrawlOrchestratorServiceLimitTest {

  @Mock private CrawlJdbcRepository repository;
  @Mock private CompanyCrawlerService companyCrawlerService;
  @Mock private DomainResolutionService domainResolutionService;
  @Mock private CareersDiscoveryService careersDiscoveryService;

  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  private final ExecutorService runExecutor = Executors.newSingleThreadExecutor();

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
    runExecutor.shutdownNow();
  }

  @Test
  void usesApiDefaultCompanyLimitAndIndependentAutomationLimits() {
    CrawlerProperties properties = baseProperties(37, 111, 222);
    CrawlOrchestratorService service = createService(properties);

    CrawlRunRequest request =
        new CrawlRunRequest(List.of(), null, null, null, null, null, null, true, true, null, null);
    service.run(request);

    verify(domainResolutionService).resolveMissingDomains(eq(111), any());
    verify(careersDiscoveryService).discover(eq(222), any());
    verify(repository).findCompanyTargetsWithAts(eq(List.of()), eq(37));
  }

  @Test
  void requestLimitsOverrideDefaultsIndependently() {
    CrawlerProperties properties = baseProperties(50, 200, 200);
    CrawlOrchestratorService service = createService(properties);

    CrawlRunRequest request =
        new CrawlRunRequest(List.of(), 7, 91, 92, null, null, null, true, true, null, null);
    service.run(request);

    verify(domainResolutionService).resolveMissingDomains(eq(91), any());
    verify(careersDiscoveryService).discover(eq(92), any());
    verify(repository).findCompanyTargetsWithAts(eq(List.of()), eq(7));
  }

  @Test
  void targetedDomainOnlyRunReportsCompletedWhenDomainWorkExecutesButNoCrawlTargetsRemain() {
    CrawlerProperties properties = baseProperties(50, 200, 200);
    CrawlOrchestratorService service = createService(properties);
    List<String> tickers = List.of("AAA");
    DomainResolutionResult resolution =
        new DomainResolutionResult(
            0,
            1,
            0,
            0,
            0,
            0,
            List.of(),
            new DomainResolutionMetrics(
                1,
                1,
                1,
                0,
                List.of(),
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                0,
                0,
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of(),
                Map.of()));

    when(domainResolutionService.resolveMissingDomainsForTickers(eq(tickers), eq(91), any()))
        .thenReturn(resolution);
    when(repository.findCompanyTargets(eq(tickers), eq(7))).thenReturn(List.of());

    CrawlRunRequest request =
        new CrawlRunRequest(tickers, 7, 91, 92, null, null, null, true, false, false, null);

    CrawlRunSummary summary = service.run(request);

    verify(domainResolutionService).resolveMissingDomainsForTickers(eq(tickers), eq(91), any());
    verify(careersDiscoveryService, org.mockito.Mockito.never()).discoverForTickers(any(), anyInt(), any(), anyBoolean());
    verify(repository).findCompanyTargets(eq(tickers), eq(7));
    verify(repository).completeCrawlRun(eq(1L), any(), eq("COMPLETED"), eq("domain_resolution_only companies=0"));
    org.assertj.core.api.Assertions.assertThat(summary.status()).isEqualTo("COMPLETED");
    org.assertj.core.api.Assertions.assertThat(summary.companies()).isEmpty();
  }

  private CrawlOrchestratorService createService(CrawlerProperties properties) {
    when(repository.insertCrawlRun(any(), any(), any())).thenReturn(1L);
    lenient()
        .when(domainResolutionService.resolveMissingDomains(anyInt(), any()))
        .thenReturn(new DomainResolutionResult(0, 0, 0, 0, 0, 0, List.of()));
    lenient()
        .when(careersDiscoveryService.discover(anyInt(), any()))
        .thenReturn(new CareersDiscoveryResult(new LinkedHashMap<>(), 0, 0, new LinkedHashMap<>()));
    lenient().when(repository.findCompanyTargetsWithAts(any(), anyInt())).thenReturn(List.of());
    return new CrawlOrchestratorService(
        repository,
        companyCrawlerService,
        executor,
        runExecutor,
        properties,
        domainResolutionService,
        careersDiscoveryService);
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
