package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.HostCrawlState;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CareersDiscoveryRunServiceTest {

  @Mock private CrawlJdbcRepository repository;
  @Mock private CareersDiscoveryService discoveryService;

  @Test
  void discoveryRunAbortsWhenTimeBudgetExceeded() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setMaxDurationSeconds(1);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible()).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(99L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);

    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure("discovery_time_budget_exceeded", null, null);
    CareersDiscoveryService.DiscoveryOutcome outcome =
        new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, true);

    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              CareersDiscoveryService.DiscoveryMetrics metrics = invocation.getArgument(2);
              metrics.incrementTimeBudgetExceeded();
              return outcome;
            });

    ExecutorService executor = new DirectExecutorService();
    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(repository, discoveryService, executor, properties);

    service.startAsync(1, 1, false);

    verify(repository)
        .completeCareersDiscoveryRun(
            eq(99L),
            any(Instant.class),
            eq("ABORTED"),
            eq("time_budget_exceeded"),
            eq("time_budget_exceeded"));

    ArgumentCaptor<Integer> budgetCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(repository, atLeastOnce())
        .updateCareersDiscoveryRunProgress(
            eq(99L),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyInt(),
            budgetCaptor.capture(),
            anyInt(),
            anyInt());
    assertThat(budgetCaptor.getValue()).isGreaterThanOrEqualTo(1);
  }

  @Test
  void discoveryRunAbortsWhenRequestBudgetExceeded() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(1);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget c1 = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);
    CompanyTarget c2 = new CompanyTarget(2L, "BBB", "Beta", null, "beta.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible()).thenReturn(2);
    when(repository.findCompaniesWithDomainWithoutAts(2)).thenReturn(List.of(c1, c2));
    when(repository.insertCareersDiscoveryRun(
            eq(2), eq(2), eq(2), eq(2), anyInt(), anyInt(), anyInt()))
        .thenReturn(100L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);

    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure("discovery_no_match", null, null);
    CareersDiscoveryService.DiscoveryOutcome outcome =
        new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, false);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean()))
        .thenAnswer(
            invocation -> {
              CareersDiscoveryService.DiscoveryMetrics metrics = invocation.getArgument(2);
              metrics.incrementRequestsIssued(2);
              return outcome;
            });

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(2, 2, false);

    verify(discoveryService, times(1)).discoverForCompany(any(), any(), any(), any(), anyBoolean());
    verify(repository)
        .completeCareersDiscoveryRun(
            eq(100L),
            any(Instant.class),
            eq("ABORTED"),
            eq("request_budget_exceeded"),
            eq("request_budget_exceeded"));
  }

  @Test
  void discoveryRunMarksStopReasonAsExceptionWhenUnexpectedErrorEscapesLoop() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(100);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible()).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(101L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean()))
        .thenReturn(new CareersDiscoveryService.DiscoveryOutcome(Map.of(), null, 0, false, false));
    doThrow(new IllegalStateException("progress write failed"))
        .when(repository)
        .updateCareersDiscoveryRunProgress(
            anyLong(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt());

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    verify(repository)
        .completeCareersDiscoveryRun(
            eq(101L),
            any(Instant.class),
            eq("FAILED"),
            eq("progress write failed"),
            eq("exception"));
  }

  @Test
  void discoveryRunTracksHostFailureCutoffSkipsInProgressMetrics() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(100);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    properties.getCareersDiscovery().setPerHostFailureCutoff(6);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible()).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(102L);
    when(repository.findHostCrawlState("alpha.com"))
        .thenReturn(new HostCrawlState("alpha.com", 6, "timeout", Instant.now(), Instant.now()));

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    ArgumentCaptor<Integer> hostSkipCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(repository, atLeastOnce())
        .updateCareersDiscoveryRunProgress(
            eq(102L),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyMap(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            hostSkipCaptor.capture());
    assertThat(hostSkipCaptor.getValue()).isEqualTo(1);
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
