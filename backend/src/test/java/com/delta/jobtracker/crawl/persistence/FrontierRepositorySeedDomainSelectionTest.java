package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class FrontierRepositorySeedDomainSelectionTest {

  @Autowired private CrawlJdbcRepository crawlJdbcRepository;
  @Autowired private FrontierRepository frontierRepository;

  @Test
  void findSeedDomainsUsesHighestConfidenceAndNewestResolvedAtPerCompany() {
    String suffix = UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);

    long companyA = crawlJdbcRepository.upsertCompany("FSD" + suffix + "A", "Frontier Seed A", "Tech");
    long companyB = crawlJdbcRepository.upsertCompany("FSD" + suffix + "B", "Frontier Seed B", "Tech");

    Instant now = Instant.now();

    String aLow = "a-low-" + suffix.toLowerCase(Locale.ROOT) + ".example.com";
    String aHighOlder = "a-high-older-" + suffix.toLowerCase(Locale.ROOT) + ".example.com";
    String aHighNewest = "a-high-newest-" + suffix.toLowerCase(Locale.ROOT) + ".example.com";

    crawlJdbcRepository.upsertCompanyDomain(companyA, aLow, null, "TEST", 0.70, now.minus(2, ChronoUnit.HOURS));
    crawlJdbcRepository.upsertCompanyDomain(
        companyA, aHighOlder, null, "TEST", 0.95, now.minus(5, ChronoUnit.HOURS));
    crawlJdbcRepository.upsertCompanyDomain(
        companyA, aHighNewest, null, "TEST", 0.95, now.minus(1, ChronoUnit.HOURS));

    String bLow = "b-low-" + suffix.toLowerCase(Locale.ROOT) + ".example.com";
    String bHigh = "b-high-" + suffix.toLowerCase(Locale.ROOT) + ".example.com";

    crawlJdbcRepository.upsertCompanyDomain(companyB, bLow, null, "TEST", 0.40, now.minus(2, ChronoUnit.HOURS));
    crawlJdbcRepository.upsertCompanyDomain(companyB, bHigh, null, "TEST", 0.99, now.minus(3, ChronoUnit.HOURS));

    List<String> seeds = frontierRepository.findSeedDomains(1000);

    assertThat(seeds).contains(aHighNewest, bHigh);
    assertThat(seeds).doesNotContain(aLow, aHighOlder, bLow);
  }
}
