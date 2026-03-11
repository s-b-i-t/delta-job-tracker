package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.CompanyTarget;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class CareersDiscoverySelectionEligibilityTest {

  @Autowired private CrawlJdbcRepository repository;
  @Autowired private JdbcTemplate jdbcTemplate;

  @Test
  void repeatedSelectionSkipsRecentlyAttemptedSemiPermanentRows() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String tickerA = "DA" + suffix;
    String tickerB = "DB" + suffix;
    String tickerC = "DC" + suffix;

    long companyA = repository.upsertCompany(tickerA, "Delta ATS A " + suffix, "Tech");
    long companyB = repository.upsertCompany(tickerB, "Delta ATS B " + suffix, "Tech");
    long companyC = repository.upsertCompany(tickerC, "Delta ATS C " + suffix, "Tech");

    repository.upsertCompanyDomain(companyA, "a-" + suffix.toLowerCase() + ".example.com", null);
    repository.upsertCompanyDomain(companyB, "b-" + suffix.toLowerCase() + ".example.com", null);
    repository.upsertCompanyDomain(companyC, "c-" + suffix.toLowerCase() + ".example.com", null);

    List<String> tickers = List.of(tickerA, tickerB, tickerC);
    List<CompanyTarget> first = repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, 2);
    assertThat(first).hasSize(2);

    Instant now = Instant.now();
    for (CompanyTarget selected : first) {
      jdbcTemplate.update(
          """
          MERGE INTO careers_discovery_state (
            company_id,
            last_attempt_at,
            last_reason_code,
            last_candidate_url,
            consecutive_failures,
            next_attempt_at
          ) KEY(company_id)
          VALUES (?, ?, ?, ?, ?, ?)
          """,
          selected.companyId(),
          java.sql.Timestamp.from(now),
          "discovery_no_match",
          "https://" + selected.domain() + "/careers",
          1,
          null);
    }

    List<CompanyTarget> second = repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, 2);
    Set<Long> firstIds =
        first.stream()
            .map(CompanyTarget::companyId)
            .collect(Collectors.toCollection(java.util.LinkedHashSet::new));
    Set<Long> secondIds =
        second.stream()
            .map(CompanyTarget::companyId)
            .collect(Collectors.toCollection(java.util.LinkedHashSet::new));

    assertThat(secondIds).doesNotContainAnyElementsOf(firstIds);
    assertThat(second).extracting(CompanyTarget::ticker).contains(tickerC);
  }

  @Test
  void immediateFullModeHandoffKeepsVendorProbeFailuresSelectable() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String tickerA = "HA" + suffix;
    String tickerB = "HB" + suffix;

    long companyA = repository.upsertCompany(tickerA, "Handoff A " + suffix, "Tech");
    long companyB = repository.upsertCompany(tickerB, "Handoff B " + suffix, "Tech");

    repository.upsertCompanyDomain(
        companyA, "handoff-a-" + suffix.toLowerCase() + ".example.com", null);
    repository.upsertCompanyDomain(
        companyB, "handoff-b-" + suffix.toLowerCase() + ".example.com", null);

    Instant now = Instant.now();
    for (long companyId : List.of(companyA, companyB)) {
      String domainPrefix = companyId == companyA ? "handoff-a-" : "handoff-b-";
      jdbcTemplate.update(
          """
          MERGE INTO careers_discovery_state (
            company_id,
            last_attempt_at,
            last_reason_code,
            last_candidate_url,
            consecutive_failures,
            next_attempt_at
          ) KEY(company_id)
          VALUES (?, ?, ?, ?, ?, ?)
          """,
          companyId,
          java.sql.Timestamp.from(now),
          "discovery_no_match",
          "https://" + domainPrefix + suffix.toLowerCase() + ".example.com/careers",
          1,
          null);
    }

    List<CompanyTarget> fullModeSelection =
        repository.findCompaniesWithDomainWithoutAtsByTickers(List.of(tickerA, tickerB), 10);

    // Organic full mode should be able to pick up the same vendor-probed cohort immediately.
    assertThat(fullModeSelection)
        .extracting(CompanyTarget::ticker)
        .containsExactlyInAnyOrder(tickerA, tickerB);
  }
}
