package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.CrawlRunCompanyResultView;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class CrawlRunCompanyResultUpsertTest {

    @Autowired
    private CrawlJdbcRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbc;

    @Test
    void upsertUpdatesCrawlRunCompanyResult() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("CR" + suffix, "Crawl Result " + suffix, "Technology");
        long runId = repository.insertCrawlRun(Instant.now(), "RUNNING", "test");

        String endpointUrl = "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/jobs";
        Instant startedAt = Instant.now();
        repository.upsertCrawlRunCompanyResultStart(
            runId,
            companyId,
            "RUNNING",
            "ATS_ADAPTER",
            "WORKDAY",
            endpointUrl,
            startedAt,
            false
        );

        repository.upsertCrawlRunCompanyResultFinish(
            runId,
            companyId,
            "SUCCEEDED",
            "ATS_ADAPTER",
            "WORKDAY",
            endpointUrl,
            startedAt,
            startedAt.plusSeconds(2),
            2000L,
            5,
            true,
            12,
            "HIT_MAX_JOBS",
            null,
            null,
            null,
            false
        );

        Integer rowCount = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM crawl_run_company_results
                WHERE crawl_run_id = :runId
                  AND company_id = :companyId
                  AND stage = 'ATS_ADAPTER'
                """,
            new MapSqlParameterSource()
                .addValue("runId", runId)
                .addValue("companyId", companyId),
            Integer.class
        );

        List<CrawlRunCompanyResultView> results = repository.findCrawlRunCompanyResults(runId, null, 10);
        assertEquals(1, rowCount);
        assertEquals(1, results.size());
        CrawlRunCompanyResultView view = results.getFirst();
        assertEquals("SUCCEEDED", view.status());
        assertEquals(5, view.jobsExtracted());
        assertTrue(view.truncated());
        assertEquals(12, view.totalJobsAvailable());
        assertEquals("HIT_MAX_JOBS", view.stopReason());
        assertEquals("ATS_ADAPTER", view.stage());
    }
}
