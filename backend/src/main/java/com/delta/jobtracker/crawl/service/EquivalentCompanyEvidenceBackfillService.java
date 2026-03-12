package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.model.EquivalentCompanyEvidenceBackfillResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.springframework.stereotype.Service;

@Service
public class EquivalentCompanyEvidenceBackfillService {
  private final CrawlJdbcRepository repository;

  public EquivalentCompanyEvidenceBackfillService(CrawlJdbcRepository repository) {
    this.repository = repository;
  }

  public EquivalentCompanyEvidenceBackfillResponse backfill() {
    int domainRowsInserted = repository.backfillEquivalentCompanyDomains();
    int atsRowsInserted = repository.backfillEquivalentAtsEndpoints();
    return new EquivalentCompanyEvidenceBackfillResponse(
        domainRowsInserted, atsRowsInserted, domainRowsInserted + atsRowsInserted);
  }
}
