package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CareersDiscoveryRunServiceTest {

    @Mock
    private CrawlJdbcRepository repository;
    @Mock
    private CareersDiscoveryService discoveryService;

    @Test
    void discoveryRunAbortsWhenTimeBudgetExceeded() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.getCareersDiscovery().setMaxDurationSeconds(1);
        CompanyTarget company = new CompanyTarget(1L, "AAA", "Alpha", null, "alpha.com", null);

        when(repository.findCompaniesWithDomainWithoutAts(1)).thenReturn(List.of(company));
        when(repository.insertCareersDiscoveryRun(1)).thenReturn(99L);
        when(repository.countAtsEndpointsForCompany(1L)).thenReturn(0);

        CareersDiscoveryService.DiscoveryFailure failure =
            new CareersDiscoveryService.DiscoveryFailure("discovery_time_budget_exceeded", null, null);
        CareersDiscoveryService.DiscoveryOutcome outcome =
            new CareersDiscoveryService.DiscoveryOutcome(Map.of(), failure, 0, false, true);

        when(discoveryService.discoverForCompany(any(), any(), any(), any(), anyBoolean()))
            .thenAnswer(invocation -> {
                CareersDiscoveryService.DiscoveryMetrics metrics = invocation.getArgument(2);
                metrics.incrementTimeBudgetExceeded();
                return outcome;
            });

        ExecutorService executor = new DirectExecutorService();
        CareersDiscoveryRunService service = new CareersDiscoveryRunService(
            repository,
            discoveryService,
            executor,
            properties
        );

        service.startAsync(1, 1, false);

        verify(repository).completeCareersDiscoveryRun(eq(99L), any(Instant.class), eq("ABORTED"), eq("time_budget_exceeded"));

        ArgumentCaptor<Integer> budgetCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(repository, atLeastOnce()).updateCareersDiscoveryRunProgress(
            eq(99L),
            anyInt(),
            anyInt(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyInt(),
            any(),
            any(),
            anyInt(),
            anyInt(),
            anyInt(),
            budgetCaptor.capture()
        );
        assertThat(budgetCaptor.getValue()).isGreaterThanOrEqualTo(1);
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
