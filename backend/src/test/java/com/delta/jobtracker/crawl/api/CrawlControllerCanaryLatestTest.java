package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.CanaryRunStatusResponse;
import com.delta.jobtracker.crawl.service.CareersDiscoveryRunService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.CrawlStatusService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.service.SecCanaryService;
import com.delta.jobtracker.crawl.service.UniverseIngestionService;
import com.delta.jobtracker.crawl.service.WorkdayInvalidUrlCleanupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CrawlControllerCanaryLatestTest {

    @Mock
    private UniverseIngestionService ingestionService;
    @Mock
    private CrawlOrchestratorService crawlOrchestratorService;
    @Mock
    private DomainResolutionService domainResolutionService;
    @Mock
    private CareersDiscoveryService careersDiscoveryService;
    @Mock
    private CareersDiscoveryRunService careersDiscoveryRunService;
    @Mock
    private CrawlStatusService crawlStatusService;
    @Mock
    private WorkdayInvalidUrlCleanupService workdayInvalidUrlCleanupService;
    @Mock
    private SecCanaryService secCanaryService;
    @Mock
    private HostCrawlStateService hostCrawlStateService;

    @Test
    void latestCanaryEndpointAcceptsMissingType() {
        CrawlerProperties properties = new CrawlerProperties();
        CanaryRunStatusResponse response = new CanaryRunStatusResponse(
            10L,
            "SEC",
            5,
            Instant.parse("2026-02-18T12:00:00Z"),
            Instant.parse("2026-02-18T12:05:00Z"),
            "COMPLETED",
            null,
            Map.of()
        );
        when(secCanaryService.getLatestCanaryRunStatus(null)).thenReturn(response);

        CrawlController controller = new CrawlController(
            ingestionService,
            crawlOrchestratorService,
            domainResolutionService,
            careersDiscoveryService,
            careersDiscoveryRunService,
            crawlStatusService,
            workdayInvalidUrlCleanupService,
            properties,
            secCanaryService,
            hostCrawlStateService
        );

        CanaryRunStatusResponse result = controller.getLatestCanaryRun(null);
        assertEquals(10L, result.runId());
        verify(secCanaryService).getLatestCanaryRunStatus(null);
    }
}
