package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.EquivalentCompanyEvidenceBackfillResponse;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class EquivalentCompanyEvidenceBackfillServiceTest {

  @Autowired private CrawlJdbcRepository repository;
  @Autowired private EquivalentCompanyEvidenceBackfillService service;

  @Test
  void backfillsExistingDomainAndAtsEvidenceAcrossSecStyleSiblingRows() {
    long acrId =
        repository.upsertCompany("ACR", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");
    repository.upsertCompanyDomain(
        acrId, "acrescommercialrealty.com", null, "test", 0.95, Instant.now());
    long acrPcId =
        repository.upsertCompany(
            "ACR-PC", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");
    long acrPdId =
        repository.upsertCompany(
            "ACR-PD", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");

    long aconId =
        repository.upsertCompany("ACON", "Aclarion, Inc.", "Healthcare", null, "1635077");
    repository.upsertAtsEndpoint(
        aconId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/aclarion",
        "https://aclarion.com/careers/",
        0.9,
        Instant.now(),
        "test",
        true);
    long aconwId =
        repository.upsertCompany("ACONW", "Aclarion, Inc.", "Healthcare", null, "1635077");

    assertThat(repository.findCompanyDomains(acrPcId)).isEmpty();
    assertThat(repository.findCompanyDomains(acrPdId)).isEmpty();
    assertThat(repository.findAtsEndpoints(aconwId)).isEmpty();

    EquivalentCompanyEvidenceBackfillResponse response = service.backfill();

    assertThat(response.domainRowsInserted()).isEqualTo(2);
    assertThat(response.atsRowsInserted()).isEqualTo(1);
    assertThat(response.totalRowsInserted()).isEqualTo(3);
    assertThat(repository.findCompanyDomains(acrPcId)).containsExactly("acrescommercialrealty.com");
    assertThat(repository.findCompanyDomains(acrPdId)).containsExactly("acrescommercialrealty.com");
    assertThat(repository.findAtsEndpoints(aconwId))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://boards.greenhouse.io/aclarion");
  }
}
