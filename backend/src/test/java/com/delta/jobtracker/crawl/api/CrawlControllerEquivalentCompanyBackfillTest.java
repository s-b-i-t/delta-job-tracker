package com.delta.jobtracker.crawl.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.EquivalentCompanyEvidenceBackfillResponse;
import com.delta.jobtracker.crawl.service.CareersDiscoveryRunService;
import com.delta.jobtracker.crawl.service.CareersDiscoveryService;
import com.delta.jobtracker.crawl.service.CrawlOrchestratorService;
import com.delta.jobtracker.crawl.service.CrawlStatusService;
import com.delta.jobtracker.crawl.service.DomainResolutionService;
import com.delta.jobtracker.crawl.service.EquivalentCompanyEvidenceBackfillService;
import com.delta.jobtracker.crawl.service.HostCrawlStateService;
import com.delta.jobtracker.crawl.service.SecCanaryService;
import com.delta.jobtracker.crawl.service.UniverseIngestionService;
import com.delta.jobtracker.crawl.service.WorkdayInvalidUrlCleanupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CrawlControllerEquivalentCompanyBackfillTest {

  @Mock private UniverseIngestionService ingestionService;
  @Mock private CrawlOrchestratorService crawlOrchestratorService;
  @Mock private DomainResolutionService domainResolutionService;
  @Mock private CareersDiscoveryService careersDiscoveryService;
  @Mock private CareersDiscoveryRunService careersDiscoveryRunService;
  @Mock private CrawlStatusService crawlStatusService;
  @Mock private WorkdayInvalidUrlCleanupService workdayInvalidUrlCleanupService;
  @Mock private EquivalentCompanyEvidenceBackfillService equivalentCompanyEvidenceBackfillService;
  @Mock private SecCanaryService secCanaryService;
  @Mock private HostCrawlStateService hostCrawlStateService;

  @Test
  void backfillEquivalentCompanyEvidenceDelegatesToService() {
    CrawlerProperties properties = new CrawlerProperties();
    EquivalentCompanyEvidenceBackfillResponse response =
        new EquivalentCompanyEvidenceBackfillResponse(2, 1, 3);
    when(equivalentCompanyEvidenceBackfillService.backfill()).thenReturn(response);

    CrawlController controller =
        new CrawlController(
            ingestionService,
            crawlOrchestratorService,
            domainResolutionService,
            careersDiscoveryService,
            careersDiscoveryRunService,
            crawlStatusService,
            workdayInvalidUrlCleanupService,
            equivalentCompanyEvidenceBackfillService,
            properties,
            secCanaryService,
            hostCrawlStateService);

    EquivalentCompanyEvidenceBackfillResponse result = controller.backfillEquivalentCompanyEvidence();
    assertEquals(3, result.totalRowsInserted());
    verify(equivalentCompanyEvidenceBackfillService).backfill();
  }
}
