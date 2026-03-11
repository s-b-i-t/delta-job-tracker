package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.util.ReasonCodeClassifier;
import java.time.Duration;
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
  void organicFullModeCanReuseRecentVendorProbeFailuresWithoutReusingDeferredFailures() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String retryTicker = "RA" + suffix;
    String deferredTicker = "RB" + suffix;

    long retryCompany = repository.upsertCompany(retryTicker, "Retry ATS " + suffix, "Tech");
    long deferredCompany =
        repository.upsertCompany(deferredTicker, "Deferred ATS " + suffix, "Tech");

    repository.upsertCompanyDomain(
        retryCompany, "retry-" + suffix.toLowerCase() + ".example.com", null);
    repository.upsertCompanyDomain(
        deferredCompany, "deferred-" + suffix.toLowerCase() + ".example.com", null);

    Instant now = Instant.now();
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
        retryCompany,
        java.sql.Timestamp.from(now),
        ReasonCodeClassifier.vendorProbeReason("discovery_no_match"),
        "https://boards.greenhouse.io/retry-" + suffix.toLowerCase(),
        1,
        null);
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
        deferredCompany,
        java.sql.Timestamp.from(now),
        ReasonCodeClassifier.vendorProbeReason("discovery_fetch_failed"),
        "https://boards.greenhouse.io/deferred-" + suffix.toLowerCase(),
        1,
        java.sql.Timestamp.from(now.plus(Duration.ofMinutes(30))));

    List<String> tickers = List.of(retryTicker, deferredTicker);

    assertThat(repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, 10, true)).isEmpty();

    List<CompanyTarget> organic =
        repository.findCompaniesWithDomainWithoutAtsByTickers(tickers, 10, false);
    assertThat(organic).extracting(CompanyTarget::ticker).containsExactly(retryTicker);
  }
}
