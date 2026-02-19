package com.delta.jobtracker.crawl;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.api.CrawlApiRunRequest;
import com.delta.jobtracker.crawl.api.CrawlController;
import com.delta.jobtracker.crawl.model.CrawlRunRequest;
import com.delta.jobtracker.crawl.model.CrawlRunSummary;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryRunService;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.CrawlStatusService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.service.SecCanaryService;
import com.delta.jobtracker.crawl.service.UniverseIngestionService;
import com.delta.jobtracker.crawl.service.WorkdayInvalidUrlCleanupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CrawlControllerRunRequestDefaultsTest {

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
    void appliesApiDefaultCompanyLimitWhenMissingFromRequest() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.getApi().setDefaultCompanyLimit(42);
        when(crawlOrchestratorService.run(any())).thenReturn(
            new CrawlRunSummary(1L, Instant.now(), Instant.now(), "COMPLETED", List.of())
        );

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
        controller.runCrawl(new CrawlApiRunRequest(
            List.of("AAPL"),
            null,
            200,
            150,
            null,
            null,
            null,
            false,
            true,
            true,
            null
        ));

        ArgumentCaptor<CrawlRunRequest> captor = ArgumentCaptor.forClass(CrawlRunRequest.class);
        verify(crawlOrchestratorService).run(captor.capture());
        CrawlRunRequest forwarded = captor.getValue();
        assertEquals(42, forwarded.companyLimit());
        assertEquals(200, forwarded.resolveLimit());
        assertEquals(150, forwarded.discoverLimit());
    }
}
