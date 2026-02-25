package com.delta.jobtracker.crawl.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CareersDiscoveryMethodMetrics;
import com.delta.jobtracker.crawl.model.CareersDiscoveryResult;
import com.delta.jobtracker.crawl.model.CareersDiscoveryWithMetrics;
import com.delta.jobtracker.crawl.model.CompanyCrawlSummary;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.SecCanarySummary;
import com.delta.jobtracker.crawl.model.SecIngestionResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SecCanaryServiceTest {

  @Mock private UniverseIngestionService ingestionService;

  @Mock private DomainResolutionService domainResolutionService;

  @Mock private CareersDiscoveryService careersDiscoveryService;

  @Mock private CompanyCrawlerService companyCrawlerService;

  @Mock private CrawlJdbcRepository repository;

  @Test
  void runSecCanaryExecutesAllSteps() {
    List<String> tickers = List.of("AAA", "BBB");
    when(ingestionService.ingestSecCompanies(2))
        .thenReturn(new SecIngestionResult(tickers, 2, 0, List.of()));

    DomainResolutionResult domainResult = new DomainResolutionResult(1, 0, 0, 0, 0, 0, List.of());
    when(domainResolutionService.resolveMissingDomainsForTickers(eq(tickers), eq(2), any()))
        .thenReturn(domainResult);

    Map<String, Integer> discovered = new LinkedHashMap<>();
    discovered.put("WORKDAY", 2);
    CareersDiscoveryResult discoveryResult = new CareersDiscoveryResult(discovered, 0, 0, Map.of());
    CareersDiscoveryMethodMetrics discoveryMetrics =
        new CareersDiscoveryMethodMetrics(
            1, 0, 0, 0, 0, 0, 0, Map.of("WORKDAY", 2), Map.of(), Map.of());
    when(careersDiscoveryService.discoverForTickersWithMetrics(
            eq(tickers), eq(2), any(), org.mockito.ArgumentMatchers.anyBoolean()))
        .thenReturn(new CareersDiscoveryWithMetrics(discoveryResult, discoveryMetrics));

    CompanyTarget targetOne = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);
    CompanyTarget targetTwo = new CompanyTarget(2L, "BBB", "Beta", null, "beta.com", null);
    when(repository.insertCrawlRun(any(), any(), any())).thenReturn(123L);
    when(repository.findCompanyTargetsWithAts(eq(tickers), eq(2)))
        .thenReturn(List.of(targetOne, targetTwo));

    CompanyCrawlSummary summaryOne =
        new CompanyCrawlSummary(1L, "AAA", "alpha.com", 0, 0, List.of(), 0, 3, true, Map.of());
    CompanyCrawlSummary summaryTwo =
        new CompanyCrawlSummary(2L, "BBB", "beta.com", 0, 0, List.of(), 0, 2, false, Map.of());
    when(companyCrawlerService.crawlCompany(eq(123L), eq(targetOne), ArgumentMatchers.any()))
        .thenReturn(summaryOne);
    when(companyCrawlerService.crawlCompany(eq(123L), eq(targetTwo), ArgumentMatchers.any()))
        .thenReturn(summaryTwo);

    when(repository.countCrawlRunCompaniesByAtsType(123L)).thenReturn(Map.of("WORKDAY", 2));
    when(repository.countCrawlRunJobsExtractedByAtsType(123L)).thenReturn(Map.of("WORKDAY", 5));
    when(repository.countCrawlRunCompanyFailures(123L)).thenReturn(Map.of("HTTP_404", 1L));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SecCanaryService service =
          new SecCanaryService(
              ingestionService,
              domainResolutionService,
              careersDiscoveryService,
              companyCrawlerService,
              repository,
              new CrawlerProperties(),
              executor,
              testObjectMapper());

      SecCanarySummary summary = service.runSecCanary(2);
      assertNotNull(summary);
      assertEquals(2, summary.companiesIngested());
      assertEquals(1, summary.domainsResolved());
      assertEquals(5, summary.jobsExtracted());
      assertEquals(1, summary.domainResolutionSucceeded());
      assertEquals(0, summary.domainResolutionFailed());
      assertEquals(1, summary.careersDiscoveryMetrics().homepageScanned());
      assertEquals(5, summary.jobsExtractedByAtsType().get("WORKDAY"));
      assertEquals("COMPLETED_WITH_ERRORS", summary.status());
      assertEquals(domainResult, summary.domainResolution());
      assertEquals(discoveryResult, summary.careersDiscovery());
      assertEquals(discovered, summary.endpointsDiscoveredByAtsType());
      assertEquals(2, summary.companiesCrawledByAtsType().get("WORKDAY"));
      assertEquals(1, summary.topErrors().get("HTTP_404"));
      assertNotNull(summary.stepDurationsMs());
      assertTrue(summary.stepDurationsMs().containsKey("ingestSec"));
      assertTrue(summary.stepDurationsMs().containsKey("resolveDomains"));
      assertTrue(summary.stepDurationsMs().containsKey("discoverCareers"));
      assertTrue(summary.stepDurationsMs().containsKey("crawlAts"));
      assertEquals(summary.durationIngestMs(), summary.stepDurationsMs().get("ingestSec"));
      assertEquals(
          summary.durationDomainResolutionMs(), summary.stepDurationsMs().get("resolveDomains"));
      assertEquals(summary.durationDiscoveryMs(), summary.stepDurationsMs().get("discoverCareers"));
      assertEquals(summary.durationCrawlMs(), summary.stepDurationsMs().get("crawlAts"));
      assertTrue(summary.durationTotalMs() >= summary.durationCrawlMs());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void runSecCanaryStopsWhenNoCompanies() {
    when(ingestionService.ingestSecCompanies(5))
        .thenReturn(new SecIngestionResult(List.of(), 0, 0, List.of()));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SecCanaryService service =
          new SecCanaryService(
              ingestionService,
              domainResolutionService,
              careersDiscoveryService,
              companyCrawlerService,
              repository,
              new CrawlerProperties(),
              executor,
              testObjectMapper());

      SecCanarySummary summary = service.runSecCanary(5);
      assertEquals("NO_COMPANIES", summary.status());
      verify(domainResolutionService, never()).resolveMissingDomainsForTickers(any(), any(), any());
      verify(careersDiscoveryService, never())
          .discoverForTickersWithMetrics(
              any(), any(), any(), org.mockito.ArgumentMatchers.anyBoolean());
      verify(repository, never()).insertCrawlRun(any(), any(), any());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void runSecCanaryCountsHostCooldownInTopErrors() {
    List<String> tickers = List.of("AAA");
    when(ingestionService.ingestSecCompanies(1))
        .thenReturn(new SecIngestionResult(tickers, 1, 0, List.of()));

    when(domainResolutionService.resolveMissingDomainsForTickers(eq(tickers), eq(1), any()))
        .thenReturn(new DomainResolutionResult(0, 0, 0, 0, 0, 0, List.of()));

    CareersDiscoveryResult discoveryResult =
        new CareersDiscoveryResult(Map.of(), 1, 1, Map.of("discovery_host_cooldown", 1));
    when(careersDiscoveryService.discoverForTickersWithMetrics(
            eq(tickers), eq(1), any(), org.mockito.ArgumentMatchers.anyBoolean()))
        .thenReturn(
            new CareersDiscoveryWithMetrics(
                discoveryResult,
                new CareersDiscoveryMethodMetrics(
                    0, 0, 0, 0, 0, 0, 0, Map.of(), Map.of(), Map.of())));

    when(repository.insertCrawlRun(any(), any(), any())).thenReturn(123L);
    when(repository.findCompanyTargetsWithAts(eq(tickers), eq(1))).thenReturn(List.of());
    when(repository.countCrawlRunCompaniesByAtsType(123L)).thenReturn(Map.of());
    when(repository.countCrawlRunJobsExtractedByAtsType(123L)).thenReturn(Map.of());
    when(repository.countCrawlRunCompanyFailures(123L)).thenReturn(Map.of());

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      SecCanaryService service =
          new SecCanaryService(
              ingestionService,
              domainResolutionService,
              careersDiscoveryService,
              companyCrawlerService,
              repository,
              new CrawlerProperties(),
              executor,
              testObjectMapper());

      SecCanarySummary summary = service.runSecCanary(1);
      assertEquals(1, summary.topErrors().get(ReasonCodeClassifier.HOST_COOLDOWN));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void startSecCanaryPersistsAbortStatus() throws Exception {
    when(repository.findRunningCanaryRun("SEC")).thenReturn(null);
    when(repository.insertCanaryRun(eq("SEC"), eq(1), any())).thenReturn(99L);
    when(ingestionService.ingestSecCompanies(1))
        .thenThrow(
            new com.delta.jobtracker.crawl.http.CanaryAbortException(
                "canary_time_budget_exceeded"));

    ExecutorService executor = new DirectExecutorService();
    SecCanaryService service =
        new SecCanaryService(
            ingestionService,
            domainResolutionService,
            careersDiscoveryService,
            companyCrawlerService,
            repository,
            new CrawlerProperties(),
            executor,
            testObjectMapper());

    service.startSecCanary(1);

    org.mockito.ArgumentCaptor<String> statusCaptor =
        org.mockito.ArgumentCaptor.forClass(String.class);
    org.mockito.ArgumentCaptor<String> summaryCaptor =
        org.mockito.ArgumentCaptor.forClass(String.class);
    verify(repository)
        .updateCanaryRun(eq(99L), any(), statusCaptor.capture(), summaryCaptor.capture(), any());

    assertEquals("ABORTED", statusCaptor.getValue());
    SecCanarySummary persisted =
        testObjectMapper().readValue(summaryCaptor.getValue(), SecCanarySummary.class);
    assertEquals("ABORTED", persisted.status());
    assertEquals("canary_time_budget_exceeded", persisted.abortReason());
  }

  private ObjectMapper testObjectMapper() {
    return new ObjectMapper().findAndRegisterModules();
  }

  private static final class DirectExecutorService extends AbstractExecutorService {
    private boolean shutdown;

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }
}
