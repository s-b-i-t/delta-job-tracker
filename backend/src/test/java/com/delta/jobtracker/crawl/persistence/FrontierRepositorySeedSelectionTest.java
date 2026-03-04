package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class FrontierRepositorySeedSelectionTest {

  @Autowired private FrontierRepository frontierRepository;
  @Autowired private JdbcTemplate jdbcTemplate;

  @Test
  void findSeedDomainsPicksBestDomainPerCompanyByConfidenceThenFreshness() {
    long alphaId = insertCompany("AALPHA", "Alpha Inc");
    insertCompanyDomain(alphaId, "alpha-low.example.com", 0.60, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(alphaId, "alpha-high-old.example.com", 0.95, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(alphaId, "https://WINNER-ALPHA.example.com/jobs", 0.95, Instant.parse("2025-01-03T00:00:00Z"));
    insertCompanyDomain(alphaId, "alpha-high-null.example.com", 0.95, null);

    long betaId = insertCompany("BBETA", "Beta Inc");
    insertCompanyDomain(betaId, "beta-old.example.com", 0.90, Instant.parse("2025-02-01T00:00:00Z"));
    insertCompanyDomain(betaId, "winner-beta.example.com/path", 0.90, Instant.parse("2025-02-05T00:00:00Z"));

    List<String> domains = frontierRepository.findSeedDomains(2);

    assertThat(domains)
        .containsExactly(
            "winner-alpha.example.com",
            "winner-beta.example.com");
  }

  @Test
  void findSeedDomainsOverSamplesBeforeHostDedupSoRequestedLimitIsMet() {
    long c1 = insertCompany("A001", "A001");
    long c2 = insertCompany("A002", "A002");
    long c3 = insertCompany("A003", "A003");
    long c4 = insertCompany("A004", "A004");
    long c5 = insertCompany("A005", "A005");
    long c6 = insertCompany("A006", "A006");

    insertCompanyDomain(c1, "https://shared.example.com/jobs", 1.0, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(c2, "shared.example.com", 1.0, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(c3, "http://shared.example.com/careers", 1.0, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(c4, "u1.example.com", 1.0, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(c5, "u2.example.com", 1.0, Instant.parse("2025-01-01T00:00:00Z"));
    insertCompanyDomain(c6, "u3.example.com", 1.0, Instant.parse("2025-01-01T00:00:00Z"));

    List<String> domains = frontierRepository.findSeedDomains(3);

    assertThat(domains)
        .hasSize(3)
        .containsExactly(
            "shared.example.com",
            "u1.example.com",
            "u2.example.com");
  }

  private long insertCompany(String ticker, String name) {
    jdbcTemplate.update(
        """
            INSERT INTO companies (ticker, name, sector)
            VALUES (?, ?, ?)
            """,
        ticker,
        name,
        "Test");
    Long id =
        jdbcTemplate.queryForObject(
            "SELECT id FROM companies WHERE ticker = ?",
            Long.class,
            ticker);
    assertThat(id).isNotNull();
    return id;
  }

  private void insertCompanyDomain(long companyId, String domain, double confidence, Instant resolvedAt) {
    jdbcTemplate.update(
        """
            INSERT INTO company_domains (company_id, domain, careers_hint_url, source, confidence, resolved_at)
            VALUES (?, ?, NULL, 'TEST', ?, ?)
            """,
        companyId,
        domain,
        confidence,
        resolvedAt == null ? null : Timestamp.from(resolvedAt));
  }
}
