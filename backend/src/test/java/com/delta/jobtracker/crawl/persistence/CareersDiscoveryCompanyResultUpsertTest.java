package com.delta.jobtracker.crawl.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyResultView;
import java.util.List;
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
class CareersDiscoveryCompanyResultUpsertTest {

  @Autowired private CrawlJdbcRepository repository;

  @Autowired private NamedParameterJdbcTemplate jdbc;

  @Test
  void upsertUpdatesDiscoveryCompanyResult() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    long companyId =
        repository.upsertCompany("CD" + suffix, "Careers Discovery " + suffix, "Technology");
    long runId = repository.insertCareersDiscoveryRun(10);

    repository.upsertCareersDiscoveryCompanyResult(
        runId, companyId, "FAILED", "TIMEOUT", "PAGE_SCAN", 0, 120L, 504, "timeout");
    repository.upsertCareersDiscoveryCompanyResult(
        runId, companyId, "SUCCEEDED", null, "ATS_DETECTED", 2, 200L, null, null);

    Integer rowCount =
        jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM careers_discovery_company_results
                WHERE discovery_run_id = :runId
                  AND company_id = :companyId
                """,
            new MapSqlParameterSource().addValue("runId", runId).addValue("companyId", companyId),
            Integer.class);

    List<CareersDiscoveryCompanyResultView> results =
        repository.findCareersDiscoveryCompanyResults(runId, null, 10);
    assertEquals(1, rowCount);
    assertEquals(1, results.size());
    CareersDiscoveryCompanyResultView view = results.getFirst();
    assertEquals("SUCCEEDED", view.status());
    assertEquals(2, view.foundEndpointsCount());
    assertEquals("ATS_DETECTED", view.stage());
  }
}
