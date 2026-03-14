package com.delta.jobtracker.crawl.persistence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.delta.jobtracker.crawl.model.CareersDiscoveryRunStatus;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class CareersDiscoveryRunGuardrailPersistenceTest {

  @Autowired private CrawlJdbcRepository repository;

  @Test
  void guardrailMetricsPersistAcrossRunLifecycle() {
    long runId = repository.insertCareersDiscoveryRun(25, 40, 20, 20, 5000, 900, 6);

    repository.updateCareersDiscoveryRunProgress(
        runId,
        5,
        3,
        2,
        4,
        2,
        3,
        "partial",
        5,
        4,
        1,
        7,
        Map.of("WORKDAY", 2),
        Map.of("GREENHOUSE", 1),
        2,
        8,
        Map.of("LEVER", 1),
        9,
        1,
        2,
        3,
        77,
        5);

    repository.completeCareersDiscoveryRun(
        runId,
        Instant.now(),
        "ABORTED",
        "request_budget_exceeded",
        "request_budget_exceeded");

    CareersDiscoveryRunStatus status = repository.findCareersDiscoveryRun(runId);
    assertNotNull(status);
    assertEquals("ABORTED", status.status());
    assertEquals("request_budget_exceeded", status.stopReason());
    assertEquals(5000, status.requestBudget());
    assertEquals(77, status.requestCountTotal());
    assertEquals(900, status.hardTimeoutSeconds());
    assertEquals(6, status.hostFailureCutoffCount());
    assertEquals(5, status.hostFailureCutoffSkips());
    assertEquals(5, status.processedCount());
    assertEquals(3, status.succeededCount());
    assertEquals(2, status.failedCount());
    assertEquals(4, status.endpointsAdded());
    assertEquals(2, status.endpointsPromoted());
    assertEquals(3, status.endpointsConfirmed());
  }
}
