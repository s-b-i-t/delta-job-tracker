package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.CanaryHttpBudget;
import com.delta.jobtracker.crawl.http.CanaryHttpBudgetContext;
import com.delta.jobtracker.crawl.model.HostCrawlState;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ArrayBlockingQueue;
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

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(99L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);

    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure("discovery_time_budget_exceeded", null, null);
    CareersDiscoveryService.DiscoveryOutcome outcome =
        new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, true);

    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
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

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(2);
    when(repository.findCompaniesWithDomainWithoutAts(2, false)).thenReturn(List.of(c1, c2));
    when(repository.insertCareersDiscoveryRun(
            eq(2), eq(2), eq(2), eq(2), anyInt(), anyInt(), anyInt()))
        .thenReturn(100L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);

    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure("discovery_no_match", null, null);
    CareersDiscoveryService.DiscoveryOutcome outcome =
        new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, false);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
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

    verify(discoveryService, times(1))
        .discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt());
    verify(repository)
        .completeCareersDiscoveryRun(
            eq(100L),
            any(Instant.class),
            eq("ABORTED"),
            eq("request_budget_exceeded"),
            eq("request_budget_exceeded"));
  }

  @Test
  void discoveryRunPersistsSelectionStateWhenRunBudgetAbortIsRaised() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(100);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(104L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);
    when(repository.findCareersDiscoveryState(1L)).thenReturn(null);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenThrow(new com.delta.jobtracker.crawl.http.CanaryAbortException("canary_time_budget_exceeded"));

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    verify(repository)
        .upsertCareersDiscoveryState(
            eq(1L),
            any(Instant.class),
            eq("discovery_time_budget_exceeded"),
            isNull(),
            eq(1),
            isNull());
    verify(repository)
        .completeCareersDiscoveryRun(
            eq(104L),
            any(Instant.class),
            eq("ABORTED"),
            eq("time_budget_exceeded"),
            eq("time_budget_exceeded"));
  }

  @Test
  void discoveryRunMarksStopReasonAsExceptionWhenUnexpectedErrorEscapesLoop() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(100);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(101L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
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

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
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

  @Test
  void discoveryRunTreatsHostFailureCutoffOutcomeAsSkippedAndCountsSkip() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(100);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    properties.getCareersDiscovery().setPerHostFailureCutoff(6);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(103L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);
    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure(
            "discovery_host_failure_cutoff",
            "https://boards.greenhouse.io/alpha",
            "HOST_COOLDOWN");
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenReturn(new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, false));

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    verify(repository)
        .upsertCareersDiscoveryCompanyResult(
            eq(103L),
            eq(1L),
            eq("SKIPPED"),
            eq("HOST_COOLDOWN"),
            eq("COOLDOWN"),
            anyInt(),
            any(),
            any(),
            eq("HOST_COOLDOWN"),
            anyBoolean(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean(),
            any(),
            anyBoolean(),
            any(),
            any(),
            any());
    ArgumentCaptor<Integer> hostSkipCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(repository, atLeastOnce())
        .updateCareersDiscoveryRunProgress(
            eq(103L),
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

  @Test
  void discoveryRunUsesFrozenGuardrailsFromRunStart() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(1);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    properties.getCareersDiscovery().setPerHostFailureCutoff(3);
    CompanyTarget c1 = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);
    CompanyTarget c2 = new CompanyTarget(2L, "BBB", "Beta", null, "beta.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(2);
    when(repository.findCompaniesWithDomainWithoutAts(2, false)).thenReturn(List.of(c1, c2));
    when(repository.insertCareersDiscoveryRun(eq(2), eq(2), eq(2), eq(2), eq(1), eq(120), eq(3)))
        .thenReturn(104L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);
    CareersDiscoveryService.DiscoveryFailure failure =
        new CareersDiscoveryService.DiscoveryFailure("discovery_no_match", null, null);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenAnswer(
            invocation -> {
              CareersDiscoveryService.DiscoveryMetrics metrics = invocation.getArgument(2);
              metrics.incrementRequestsIssued(2);
              return new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, false);
            });

    QueuedExecutorService executor = new QueuedExecutorService();
    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(repository, discoveryService, executor, properties);

    service.startAsync(2, 2, false);
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(999);
    properties.getCareersDiscovery().setHardTimeoutSeconds(999);
    properties.getCareersDiscovery().setPerHostFailureCutoff(99);
    executor.runNext();

    verify(discoveryService, times(1))
        .discoverForCompany(any(), any(), any(), any(), anyBoolean(), eq(3));
    verify(repository)
        .completeCareersDiscoveryRun(
            eq(104L),
            any(Instant.class),
            eq("ABORTED"),
            eq("request_budget_exceeded"),
            eq("request_budget_exceeded"));
  }

  @Test
  void discoveryRunEnforcesRequestBudgetMidCompany() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(1);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(105L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenAnswer(
            invocation -> {
              CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
              budget.beforeRequest("alpha.com");
              budget.beforeRequest("alpha.com");
              return new CareersDiscoveryService.DiscoveryOutcome(
                  Map.of(),
                  new CareersDiscoveryService.DiscoveryFailure("discovery_no_match", null, null),
                  0,
                  false,
                  false);
            });

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    verify(repository)
        .upsertCareersDiscoveryCompanyResult(
            eq(105L),
            eq(1L),
            eq("FAILED"),
            eq("TIMEOUT"),
            eq("BUDGET"),
            anyInt(),
            any(),
            any(),
            eq("request_budget_exceeded"),
            anyBoolean(),
            any(),
            any(),
            any(),
            any(),
            anyBoolean(),
            any(),
            anyBoolean(),
            any(),
            any(),
            any());
    verify(repository)
        .completeCareersDiscoveryRun(
            eq(105L),
            any(Instant.class),
            eq("ABORTED"),
            eq("request_budget_exceeded"),
            eq("request_budget_exceeded"));
    ArgumentCaptor<Integer> requestCountCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(repository, atLeastOnce())
        .updateCareersDiscoveryRunProgress(
            eq(105L),
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
            requestCountCaptor.capture(),
            anyInt());
    assertThat(requestCountCaptor.getValue()).isEqualTo(1);
  }

  @Test
  void discoveryRunCountsSitemapPathRequestsFromRunBudgetContext() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getCareersDiscovery().setGlobalRequestBudgetPerRun(10);
    properties.getCareersDiscovery().setHardTimeoutSeconds(120);
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(106L);
    when(repository.countAtsEndpointsForCompany(anyLong())).thenReturn(0);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenAnswer(
            invocation -> {
              CanaryHttpBudget budget = CanaryHttpBudgetContext.current();
              budget.beforeRequest("sitemap.alpha.com");
              return new CareersDiscoveryService.DiscoveryOutcome(
                  Map.of(),
                  new CareersDiscoveryService.DiscoveryFailure("discovery_no_match", null, null),
                  0,
                  false,
                  false);
            });

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    ArgumentCaptor<Integer> requestCountCaptor = ArgumentCaptor.forClass(Integer.class);
    verify(repository, atLeastOnce())
        .updateCareersDiscoveryRunProgress(
            eq(106L),
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
            requestCountCaptor.capture(),
            anyInt());
    assertThat(requestCountCaptor.getValue()).isEqualTo(1);
  }

  @Test
  void fullModeSelectionIncludesVendorProbeOnlyEndpointsWhileVendorProbeModeStaysStrict() {
    CrawlerProperties properties = new CrawlerProperties();
    CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

    when(repository.countCompaniesWithDomainWithoutAtsEligible(false)).thenReturn(1);
    when(repository.findCompaniesWithDomainWithoutAts(1, false)).thenReturn(List.of(company));
    when(repository.insertCareersDiscoveryRun(
            eq(1), eq(1), eq(1), eq(1), anyInt(), anyInt(), anyInt()))
        .thenReturn(107L);
    when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);
    when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean(), anyInt()))
        .thenReturn(new CareersDiscoveryService.DiscoveryOutcome(Map.of(), null, 0, false, false));

    CareersDiscoveryRunService service =
        new CareersDiscoveryRunService(
            repository, discoveryService, new DirectExecutorService(), properties);

    service.startAsync(1, 1, false);

    verify(repository).countCompaniesWithDomainWithoutAtsEligible(false);
    verify(repository).findCompaniesWithDomainWithoutAts(1, false);
    verify(repository, never()).countCompaniesWithDomainWithoutAtsEligible(true);
    verify(repository, never()).findCompaniesWithDomainWithoutAts(1, true);
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

  private static final class QueuedExecutorService extends AbstractExecutorService {
    private final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(4);
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
      queue.offer(command);
    }

    void runNext() {
      Runnable task = queue.poll();
      if (task != null) {
        task.run();
      }
    }
  }
}
