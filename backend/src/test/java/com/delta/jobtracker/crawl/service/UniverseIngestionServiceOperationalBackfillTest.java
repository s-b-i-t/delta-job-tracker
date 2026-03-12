package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.SecIngestionResult;
import com.delta.jobtracker.crawl.model.SecUniverseIngestionSummary;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class UniverseIngestionServiceOperationalBackfillTest {

  @Autowired private UniverseIngestionService service;
  @Autowired private CrawlJdbcRepository repository;
  @Autowired private CrawlerProperties properties;

  @Test
  void ingestSecUniverseAutomaticallyBackfillsEquivalentEvidence(@TempDir Path tempDir)
      throws IOException {
    seedSourceEvidence();

    SecUniverseIngestionSummary summary =
        withSecCachePayload(
            tempDir,
            """
            {
              "0":{"cik_str":1332551,"ticker":"ACR","title":"ACRES Commercial Realty Corp."},
              "1":{"cik_str":1332551,"ticker":"ACR-PC","title":"ACRES Commercial Realty Corp."},
              "2":{"cik_str":1332551,"ticker":"ACR-PD","title":"ACRES Commercial Realty Corp."},
              "3":{"cik_str":70858,"ticker":"BAC","title":"BANK OF AMERICA CORP /DE/"},
              "4":{"cik_str":70858,"ticker":"BAC-PB","title":"BANK OF AMERICA CORP /DE/"}
            }
            """,
            () -> service.ingestSecUniverse(5));

    assertThat(summary.total()).isEqualTo(5);
    assertThat(repository.findCompanyDomains(findCompanyId("ACR-PC")))
        .containsExactly("acrescommercialrealty.com");
    assertThat(repository.findCompanyDomains(findCompanyId("ACR-PD")))
        .containsExactly("acrescommercialrealty.com");
    assertThat(repository.findAtsEndpoints(findCompanyId("BAC-PB")))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://bankofamerica.wd1.myworkdayjobs.com/BankofAmericaCareers");
  }

  @Test
  void ingestSecCompaniesAutomaticallyBackfillsEquivalentEvidence(@TempDir Path tempDir)
      throws IOException {
    seedSourceEvidence();

    SecIngestionResult result =
        withSecCachePayload(
            tempDir,
            """
            {
              "0":{"cik_str":1332551,"ticker":"ACR","title":"ACRES Commercial Realty Corp."},
              "1":{"cik_str":1332551,"ticker":"ACR-PC","title":"ACRES Commercial Realty Corp."},
              "2":{"cik_str":70858,"ticker":"BAC","title":"BANK OF AMERICA CORP /DE/"},
              "3":{"cik_str":70858,"ticker":"BAC-PB","title":"BANK OF AMERICA CORP /DE/"}
            }
            """,
            () -> service.ingestSecCompanies(4));

    assertThat(result.tickers()).containsExactly("ACR", "ACR-PC", "BAC", "BAC-PB");
    assertThat(repository.findCompanyDomains(findCompanyId("ACR-PC")))
        .containsExactly("acrescommercialrealty.com");
    assertThat(repository.findAtsEndpoints(findCompanyId("BAC-PB")))
        .extracting(AtsEndpointRecord::endpointUrl)
        .containsExactly("https://bankofamerica.wd1.myworkdayjobs.com/BankofAmericaCareers");
  }

  private void seedSourceEvidence() {
    long acrId =
        repository.upsertCompany("ACR", "ACRES Commercial Realty Corp.", "Real Estate", null, "1332551");
    repository.upsertCompanyDomain(
        acrId, "acrescommercialrealty.com", null, "MANUAL", 1.0, Instant.now());

    long bacId =
        repository.upsertCompany("BAC", "BANK OF AMERICA CORP /DE/", "Financials", null, "0000070858");
    repository.upsertAtsEndpoint(
        bacId,
        AtsType.WORKDAY,
        "https://bankofamerica.wd1.myworkdayjobs.com/BankofAmericaCareers",
        "https://careers.bankofamerica.com/",
        0.95,
        Instant.now(),
        "manual_demo",
        true);

    assertThat(findCompanyIdOrNull("ACR-PC")).isNull();
    assertThat(findCompanyIdOrNull("BAC-PB")).isNull();
  }

  private long findCompanyId(String ticker) {
    Long companyId = findCompanyIdOrNull(ticker);
    assertThat(companyId).isNotNull();
    return companyId;
  }

  private Long findCompanyIdOrNull(String ticker) {
    return repository.searchCompanies(ticker, 10).stream()
        .filter(result -> ticker.equals(result.ticker()))
        .findFirst()
        .map(result -> result.id())
        .orElse(null);
  }

  private <T> T withSecCachePayload(Path tempDir, String payload, ThrowingSupplier<T> supplier)
      throws IOException {
    Path cachePath = tempDir.resolve("sec_company_tickers_cache.json");
    Files.writeString(cachePath, payload, StandardCharsets.UTF_8);

    String originalUrl = properties.getData().getSecCompanyTickersUrl();
    String originalCachePath = properties.getData().getSecCompanyTickersCachePath();
    int originalTtl = properties.getData().getSecCompanyTickersCacheTtlHours();
    try {
      properties.getData().setSecCompanyTickersUrl("https://example.test/sec.json");
      properties.getData().setSecCompanyTickersCachePath(cachePath.toString());
      properties.getData().setSecCompanyTickersCacheTtlHours(24);
      return supplier.get();
    } finally {
      properties.getData().setSecCompanyTickersUrl(originalUrl);
      properties.getData().setSecCompanyTickersCachePath(originalCachePath);
      properties.getData().setSecCompanyTickersCacheTtlHours(originalTtl);
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get();
  }
}
