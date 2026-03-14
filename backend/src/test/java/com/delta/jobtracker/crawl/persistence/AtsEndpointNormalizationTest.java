package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.delta.jobtracker.crawl.model.AtsType;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class AtsEndpointNormalizationTest {

  @Autowired private CrawlJdbcRepository repository;

  @Autowired private NamedParameterJdbcTemplate jdbc;

  @Test
  void greenhouseApiHostIsCanonicalizedAndDeduped() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("GN" + suffix, "Greenhouse Norm " + suffix, "Technology");

    String apiUrl = "https://api.greenhouse.io/v1/boards/airbnb/jobs?content=true";
    String canonicalUrl = "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs?content=true";

    repository.upsertAtsEndpoint(
        companyId, AtsType.GREENHOUSE, apiUrl, null, 0.9, Instant.now(), "test", true);
    repository.upsertAtsEndpoint(
        companyId, AtsType.GREENHOUSE, canonicalUrl, null, 0.9, Instant.now(), "test", true);

    String stored =
        jdbc.queryForObject(
            """
                SELECT ats_url
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                LIMIT 1
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            String.class);

    Integer count =
        jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            Integer.class);

    assertEquals(canonicalUrl, stored);
    assertEquals(1, count);
  }

  @Test
  void icimsSearchEndpointIsCanonicalizedAndDeduped() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId = repository.upsertCompany("IC" + suffix, "iCIMS Norm " + suffix, "Technology");

    String urlA = "https://jobs.icims.com/jobs/search?ss=1&mobile=false";
    String urlB = "https://jobs.icims.com/jobs/search?mode=job&iis=Careers+Site";
    String canonicalUrl = "https://jobs.icims.com/jobs/search";

    repository.upsertAtsEndpoint(
        companyId, AtsType.ICIMS, urlA, null, 0.9, Instant.now(), "test", false);
    repository.upsertAtsEndpoint(
        companyId, AtsType.ICIMS, urlB, null, 0.9, Instant.now(), "test", true);

    String stored =
        jdbc.queryForObject(
            """
                SELECT ats_url
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'ICIMS'
                LIMIT 1
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            String.class);

    Integer count =
        jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'ICIMS'
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            Integer.class);

    assertEquals(canonicalUrl, stored);
    assertEquals(1, count);
  }

  @Test
  void sameCanonicalEndpointPromotionFromVendorProbeIsPersistedAndMeasured() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("PR" + suffix, "Promotion Norm " + suffix, "Technology");

    Instant detectedAt = Instant.now();
    repository.upsertAtsEndpoint(
        companyId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/promoco",
        "https://promoco.com/jobs",
        0.9,
        detectedAt.minusSeconds(60),
        "vendor_probe",
        true);

    CrawlJdbcRepository.AtsEndpointUpsertOutcome outcome =
        repository.upsertAtsEndpoint(
            companyId,
            AtsType.GREENHOUSE,
            "https://boards.greenhouse.io/promoco",
            "https://promoco.com/careers",
            0.95,
            detectedAt,
            "homepage_link",
            true);

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("companyId", companyId);
    Map<String, Object> row =
        jdbc.queryForMap(
            """
                SELECT ats_url,
                       detection_method,
                       discovered_from_url,
                       promoted_from_vendor_probe_at,
                       last_revalidated_at
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                """,
            params);
    Integer count =
        jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                """,
            params,
            Integer.class);

    assertThat(outcome.inserted()).isFalse();
    assertThat(outcome.promotedFromVendorProbe()).isTrue();
    assertThat(outcome.confirmedExisting()).isFalse();
    assertThat(outcome.previousDetectionMethod()).isEqualTo("vendor_probe");
    assertThat(outcome.currentDetectionMethod()).isEqualTo("homepage_link");
    assertEquals(1, count);
    assertThat(row.get("ats_url")).isEqualTo("https://boards.greenhouse.io/promoco");
    assertThat(row.get("detection_method")).isEqualTo("homepage_link");
    assertThat(row.get("discovered_from_url")).isEqualTo("https://promoco.com/careers");
    assertThat(row.get("promoted_from_vendor_probe_at")).isNotNull();
    assertThat(row.get("last_revalidated_at")).isNotNull();
  }

  @Test
  void sameCanonicalEndpointConfirmationWithoutPromotionKeepsPromotionTimestampNull() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("CF" + suffix, "Confirmation Norm " + suffix, "Technology");

    Instant detectedAt = Instant.now();
    repository.upsertAtsEndpoint(
        companyId,
        AtsType.GREENHOUSE,
        "https://boards.greenhouse.io/confirmco",
        "https://confirmco.com/jobs",
        0.9,
        detectedAt.minusSeconds(60),
        "homepage_link",
        true);

    CrawlJdbcRepository.AtsEndpointUpsertOutcome outcome =
        repository.upsertAtsEndpoint(
            companyId,
            AtsType.GREENHOUSE,
            "https://boards.greenhouse.io/confirmco",
            "https://confirmco.com/careers",
            0.95,
            detectedAt,
            "homepage_link",
            true);

    MapSqlParameterSource params = new MapSqlParameterSource().addValue("companyId", companyId);
    Map<String, Object> row =
        jdbc.queryForMap(
            """
                SELECT detection_method,
                       discovered_from_url,
                       promoted_from_vendor_probe_at,
                       last_revalidated_at
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                """,
            params);

    assertThat(outcome.inserted()).isFalse();
    assertThat(outcome.promotedFromVendorProbe()).isFalse();
    assertThat(outcome.confirmedExisting()).isTrue();
    assertThat(outcome.previousDetectionMethod()).isEqualTo("homepage_link");
    assertThat(outcome.currentDetectionMethod()).isEqualTo("homepage_link");
    assertThat(row.get("detection_method")).isEqualTo("homepage_link");
    assertThat(row.get("discovered_from_url")).isEqualTo("https://confirmco.com/careers");
    assertThat(row.get("promoted_from_vendor_probe_at")).isNull();
    assertThat(row.get("last_revalidated_at")).isNotNull();
  }
}
