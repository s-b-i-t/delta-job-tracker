package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class EquivalentCompanyFanoutTest {

  @Autowired private CrawlJdbcRepository repository;

  @Test
  void resolvedDomainFansOutAcrossEquivalentShareClassRows() {
    long commonId =
        repository.upsertCompany("ACR", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");
    long preferredCId =
        repository.upsertCompany(
            "ACR-PC",
            "ACRES Commercial Realty Corp.",
            "Real Estate",
            null,
            "1332551");
    long preferredDId =
        repository.upsertCompany(
            "ACR-PD",
            "ACRES Commercial Realty Corp.",
            "Real Estate",
            null,
            "1332551");

    repository.upsertCompanyDomain(
        commonId, "acrescommercialrealty.com", null, "test", 0.95, Instant.now());

    assertThat(repository.findCompanyDomains(commonId)).containsExactly("acrescommercialrealty.com");
    assertThat(repository.findCompanyDomains(preferredCId))
        .containsExactly("acrescommercialrealty.com");
    assertThat(repository.findCompanyDomains(preferredDId))
        .containsExactly("acrescommercialrealty.com");
  }

  @Test
  void resolvedDomainFanoutSkipsSiblingWithConflictingExistingDomain() {
    long commonId =
        repository.upsertCompany("ACR", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");
    long preferredCId =
        repository.upsertCompany(
            "ACR-PC",
            "ACRES Commercial Realty Corp.",
            "Real Estate",
            null,
            "1332551");

    repository.upsertCompanyDomain(
        preferredCId, "preferred-only.example.com", null, "MANUAL", 1.0, Instant.now());
    repository.upsertCompanyDomain(
        commonId, "acrescommercialrealty.com", null, "test", 0.95, Instant.now());

    assertThat(repository.findCompanyDomains(commonId))
        .containsExactly("acrescommercialrealty.com", "preferred-only.example.com");
    assertThat(repository.findCompanyDomains(preferredCId))
        .containsExactly("preferred-only.example.com");
  }

  @Test
  void atsEndpointFansOutAcrossEquivalentWarrantRows() {
    long commonId =
        repository.upsertCompany("ACON", "Aclarion, Inc.", "Healthcare", null, "1635077");
    long warrantId =
        repository.upsertCompany("ACONW", "Aclarion, Inc.", "Healthcare", null, "1635077");

    repository.upsertAtsEndpoint(
        commonId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/aclarion",
        "https://aclarion.com/careers/",
        0.9,
        Instant.now(),
        "test",
        true);

    assertThat(repository.findAtsEndpoints(commonId))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://boards.greenhouse.io/aclarion");
    assertThat(repository.findAtsEndpoints(warrantId))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://boards.greenhouse.io/aclarion");
  }

  @Test
  void atsEndpointFanoutSkipsSiblingWithConflictingExistingEndpoint() {
    long commonId =
        repository.upsertCompany("ACON", "Aclarion, Inc.", "Healthcare", null, "1635077");
    long warrantId =
        repository.upsertCompany("ACONW", "Aclarion, Inc.", "Healthcare", null, "1635077");

    repository.upsertAtsEndpoint(
        warrantId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/aclarion-warrants",
        "https://aclarion.com/careers/",
        0.9,
        Instant.now(),
        "test",
        true);
    repository.upsertAtsEndpoint(
        commonId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/aclarion",
        "https://aclarion.com/careers/",
        0.9,
        Instant.now(),
        "test",
        true);

    assertThat(repository.findAtsEndpoints(commonId))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactlyInAnyOrder(
            "https://boards.greenhouse.io/aclarion",
            "https://boards.greenhouse.io/aclarion-warrants");
    assertThat(repository.findAtsEndpoints(warrantId))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://boards.greenhouse.io/aclarion-warrants");
  }
}
