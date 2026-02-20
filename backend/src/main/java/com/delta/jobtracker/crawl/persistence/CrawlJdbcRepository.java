package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsAttemptSample;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CanaryRunStatus;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyFailureView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryCompanyResultView;
import com.delta.jobtracker.crawl.model.CareersDiscoveryRunStatus;
import com.delta.jobtracker.crawl.model.CareersDiscoveryState;
import com.delta.jobtracker.crawl.model.CompanySearchResult;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunActivityCounts;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.CrawlRunStatus;
import com.delta.jobtracker.crawl.model.CrawlRunCompanyFailureView;
import com.delta.jobtracker.crawl.model.CrawlRunCompanyResultView;
import com.delta.jobtracker.crawl.model.DiscoveredUrlType;
import com.delta.jobtracker.crawl.model.DiscoveryFailureEntry;
import com.delta.jobtracker.crawl.model.HostCrawlState;
import com.delta.jobtracker.crawl.model.JobDeltaItem;
import com.delta.jobtracker.crawl.model.JobPostingListView;
import com.delta.jobtracker.crawl.model.JobPostingUrlRef;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.util.JobUrlUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;
import org.springframework.dao.DataIntegrityViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Repository
public class CrawlJdbcRepository {
    private static final Logger log = LoggerFactory.getLogger(CrawlJdbcRepository.class);
    private static final TypeReference<Map<String, Integer>> MAP_INT = new TypeReference<>() {};
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final NamedParameterJdbcTemplate jdbc;
    private final boolean postgres;
    private final CrawlerProperties properties;

    public CrawlJdbcRepository(NamedParameterJdbcTemplate jdbc, CrawlerProperties properties) {
        this.jdbc = jdbc;
        this.postgres = detectPostgres(jdbc);
        this.properties = properties;
    }

    public boolean isDbReachable() {
        Integer value = jdbc.getJdbcTemplate().queryForObject("SELECT 1", Integer.class);
        return value != null && value == 1;
    }

    public Map<String, Long> tableCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("companies", countTable("companies"));
        counts.put("company_domains", countTable("company_domains"));
        counts.put("ats_endpoints", countTable("ats_endpoints"));
        counts.put("crawl_runs", countTable("crawl_runs"));
        counts.put("discovered_urls", countTable("discovered_urls"));
        counts.put("job_postings", countTable("job_postings"));
        return counts;
    }

    public Map<String, Long> coverageCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("company_domains", countTable("company_domains"));
        counts.put("discovered_urls", countTable("discovered_urls"));
        counts.put("ats_endpoints", countTable("ats_endpoints"));
        counts.put("job_postings", countTable("job_postings"));
        return counts;
    }

    public Map<String, Long> countAtsEndpointsByType() {
        Map<String, Long> counts = new LinkedHashMap<>();
        jdbc.query(
            """
                SELECT ats_type, COUNT(*) AS total
                FROM ats_endpoints
                GROUP BY ats_type
                ORDER BY total DESC, ats_type
                """,
            new MapSqlParameterSource(),
            rs -> {
                String type = rs.getString("ats_type");
                long total = rs.getLong("total");
                if (type != null) {
                    counts.put(type, total);
                }
            }
        );
        return counts;
    }

    public Map<String, Long> countAtsEndpointsByDetectionMethod() {
        Map<String, Long> counts = new LinkedHashMap<>();
        jdbc.query(
            """
                SELECT detection_method, COUNT(*) AS total
                FROM ats_endpoints
                GROUP BY detection_method
                ORDER BY total DESC, detection_method
                """,
            new MapSqlParameterSource(),
            rs -> {
                String method = rs.getString("detection_method");
                long total = rs.getLong("total");
                if (method != null) {
                    counts.put(method, total);
                }
            }
        );
        return counts;
    }

    public int countAtsEndpointsForCompany(long companyId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId);
        Integer count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM ats_endpoints
                WHERE company_id = :companyId
                """,
            params,
            Integer.class
        );
        return count == null ? 0 : count;
    }

    public Map<String, Long> countDiscoveryFailuresByReason() {
        Map<String, Long> counts = new LinkedHashMap<>();
        jdbc.query(
            """
                SELECT reason_code, COUNT(DISTINCT company_id) AS total
                FROM careers_discovery_failures
                GROUP BY reason_code
                ORDER BY total DESC, reason_code
                """,
            new MapSqlParameterSource(),
            rs -> {
                String reason = rs.getString("reason_code");
                long total = rs.getLong("total");
                if (reason != null) {
                    counts.put(reason, total);
                }
            }
        );
        return counts;
    }

    public List<DiscoveryFailureEntry> findRecentDiscoveryFailures(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit);
        return jdbc.query(
            """
                SELECT c.ticker,
                       c.name AS company_name,
                       f.candidate_url,
                       f.detail,
                       f.observed_at,
                       f.reason_code
                FROM careers_discovery_failures f
                JOIN companies c ON c.id = f.company_id
                ORDER BY f.observed_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new DiscoveryFailureEntry(
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("candidate_url"),
                rs.getString("detail"),
                toInstant(rs.getTimestamp("observed_at")),
                rs.getString("reason_code")
            )
        );
    }

    public long insertCareersDiscoveryRun(int companyLimit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyLimit", companyLimit);
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(
            """
                INSERT INTO careers_discovery_runs (
                    status, company_limit
                )
                VALUES (
                    'RUNNING', :companyLimit
                )
                """,
            params,
            keyHolder,
            new String[] {"id"}
        );
        Number key = keyHolder.getKey();
        return key == null ? 0L : key.longValue();
    }

    public void updateCareersDiscoveryRunProgress(
        long runId,
        int processedCount,
        int succeededCount,
        int failedCount,
        int endpointsAdded,
        String lastError,
        int companiesConsidered,
        int homepageScanned,
        Map<String, Integer> endpointsFoundHomepageByAtsType,
        Map<String, Integer> endpointsFoundVendorProbeByAtsType,
        int sitemapsScanned,
        int sitemapUrlsChecked,
        Map<String, Integer> endpointsFoundSitemapByAtsType,
        int careersPathsChecked,
        int robotsBlockedCount,
        int fetchFailedCount,
        int timeBudgetExceededCount
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("processedCount", processedCount)
            .addValue("succeededCount", succeededCount)
            .addValue("failedCount", failedCount)
            .addValue("endpointsAdded", endpointsAdded)
            .addValue("lastError", truncateErrorDetail(lastError))
            .addValue("companiesConsidered", Math.max(0, companiesConsidered))
            .addValue("homepageScanned", Math.max(0, homepageScanned))
            .addValue("endpointsFoundHomepageJson", writeJson(endpointsFoundHomepageByAtsType))
            .addValue("endpointsFoundVendorJson", writeJson(endpointsFoundVendorProbeByAtsType))
            .addValue("sitemapsScanned", Math.max(0, sitemapsScanned))
            .addValue("sitemapUrlsChecked", Math.max(0, sitemapUrlsChecked))
            .addValue("endpointsFoundSitemapJson", writeJson(endpointsFoundSitemapByAtsType))
            .addValue("careersPathsChecked", Math.max(0, careersPathsChecked))
            .addValue("robotsBlockedCount", Math.max(0, robotsBlockedCount))
            .addValue("fetchFailedCount", Math.max(0, fetchFailedCount))
            .addValue("timeBudgetExceededCount", Math.max(0, timeBudgetExceededCount));
        jdbc.update(
            """
                UPDATE careers_discovery_runs
                SET processed_count = :processedCount,
                    succeeded_count = :succeededCount,
                    failed_count = :failedCount,
                    endpoints_added = :endpointsAdded,
                    last_error = COALESCE(:lastError, last_error),
                    companies_considered = :companiesConsidered,
                    homepage_scanned = :homepageScanned,
                    endpoints_found_homepage_json = :endpointsFoundHomepageJson,
                    endpoints_found_vendor_json = :endpointsFoundVendorJson,
                    sitemaps_scanned = :sitemapsScanned,
                    sitemap_urls_checked = :sitemapUrlsChecked,
                    endpoints_found_sitemap_json = :endpointsFoundSitemapJson,
                    careers_paths_checked = :careersPathsChecked,
                    robots_blocked_count = :robotsBlockedCount,
                    fetch_failed_count = :fetchFailedCount,
                    time_budget_exceeded_count = :timeBudgetExceededCount
                WHERE id = :runId
                """,
            params
        );
    }

    public void completeCareersDiscoveryRun(long runId, Instant finishedAt, String status, String lastError) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("finishedAt", toTimestamp(finishedAt))
            .addValue("status", status)
            .addValue("lastError", truncateErrorDetail(lastError));
        jdbc.update(
            """
                UPDATE careers_discovery_runs
                SET finished_at = :finishedAt,
                    status = :status,
                    last_error = COALESCE(:lastError, last_error)
                WHERE id = :runId
                """,
            params
        );
    }

    public long insertCanaryRun(String type, Integer requestedLimit, Instant startedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("type", type)
            .addValue("requestedLimit", requestedLimit)
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("status", "RUNNING");
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(
            """
                INSERT INTO canary_runs (
                    type,
                    requested_limit,
                    started_at,
                    status
                )
                VALUES (
                    :type,
                    :requestedLimit,
                    :startedAt,
                    :status
                )
                """,
            params,
            keyHolder,
            new String[] {"id"}
        );
        Number key = keyHolder.getKey();
        return key == null ? 0L : key.longValue();
    }

    public CanaryRunStatus findCanaryRun(long runId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId);
        List<CanaryRunStatus> rows = jdbc.query(
            """
                SELECT id,
                       type,
                       requested_limit,
                       started_at,
                       finished_at,
                       status,
                       summary_json,
                       error_summary_json
                FROM canary_runs
                WHERE id = :runId
                """,
            params,
            (rs, rowNum) -> new CanaryRunStatus(
                rs.getLong("id"),
                rs.getString("type"),
                (Integer) rs.getObject("requested_limit"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getString("status"),
                rs.getString("summary_json"),
                rs.getString("error_summary_json")
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public CanaryRunStatus findRunningCanaryRun(String type) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("type", type);
        List<CanaryRunStatus> rows = jdbc.query(
            """
                SELECT id,
                       type,
                       requested_limit,
                       started_at,
                       finished_at,
                       status,
                       summary_json,
                       error_summary_json
                FROM canary_runs
                WHERE type = :type
                  AND status = 'RUNNING'
                ORDER BY started_at DESC
                LIMIT 1
                """,
            params,
            (rs, rowNum) -> new CanaryRunStatus(
                rs.getLong("id"),
                rs.getString("type"),
                (Integer) rs.getObject("requested_limit"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getString("status"),
                rs.getString("summary_json"),
                rs.getString("error_summary_json")
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public CanaryRunStatus findLatestCanaryRun(String type) {
        if (type == null || type.isBlank()) {
            return null;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("type", type);
        List<CanaryRunStatus> rows = jdbc.query(
            """
                SELECT id,
                       type,
                       requested_limit,
                       started_at,
                       finished_at,
                       status,
                       summary_json,
                       error_summary_json
                FROM canary_runs
                WHERE type = :type
                ORDER BY started_at DESC
                LIMIT 1
                """,
            params,
            (rs, rowNum) -> new CanaryRunStatus(
                rs.getLong("id"),
                rs.getString("type"),
                (Integer) rs.getObject("requested_limit"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getString("status"),
                rs.getString("summary_json"),
                rs.getString("error_summary_json")
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public void updateCanaryRun(
        long runId,
        Instant finishedAt,
        String status,
        String summaryJson,
        String errorSummaryJson
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("finishedAt", toTimestamp(finishedAt))
            .addValue("status", status)
            .addValue("summaryJson", summaryJson)
            .addValue("errorSummaryJson", errorSummaryJson);
        jdbc.update(
            """
                UPDATE canary_runs
                SET finished_at = :finishedAt,
                    status = :status,
                    summary_json = :summaryJson,
                    error_summary_json = :errorSummaryJson
                WHERE id = :runId
                """,
            params
        );
    }

    public CareersDiscoveryRunStatus findCareersDiscoveryRun(long runId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId);
        List<CareersDiscoveryRunStatus> rows = jdbc.query(
            """
                SELECT id,
                       started_at,
                       finished_at,
                       status,
                       company_limit,
                       processed_count,
                       succeeded_count,
                       failed_count,
                       endpoints_added,
                       last_error,
                       companies_considered,
                       homepage_scanned,
                       endpoints_found_homepage_json,
                       endpoints_found_vendor_json,
                       sitemaps_scanned,
                       sitemap_urls_checked,
                       endpoints_found_sitemap_json,
                       careers_paths_checked,
                       robots_blocked_count,
                       fetch_failed_count,
                       time_budget_exceeded_count
                FROM careers_discovery_runs
                WHERE id = :runId
                """,
            params,
            (rs, rowNum) -> new CareersDiscoveryRunStatus(
                rs.getLong("id"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getString("status"),
                rs.getInt("company_limit"),
                rs.getInt("processed_count"),
                rs.getInt("succeeded_count"),
                rs.getInt("failed_count"),
                rs.getInt("endpoints_added"),
                rs.getString("last_error"),
                rs.getInt("companies_considered"),
                rs.getInt("homepage_scanned"),
                readJsonMap(rs.getString("endpoints_found_homepage_json")),
                readJsonMap(rs.getString("endpoints_found_vendor_json")),
                rs.getInt("sitemaps_scanned"),
                rs.getInt("sitemap_urls_checked"),
                readJsonMap(rs.getString("endpoints_found_sitemap_json")),
                rs.getInt("careers_paths_checked"),
                rs.getInt("robots_blocked_count"),
                rs.getInt("fetch_failed_count"),
                rs.getInt("time_budget_exceeded_count"),
                Map.of()
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public CareersDiscoveryRunStatus findLatestCareersDiscoveryRun() {
        List<CareersDiscoveryRunStatus> rows = jdbc.query(
            """
                SELECT id,
                       started_at,
                       finished_at,
                       status,
                       company_limit,
                       processed_count,
                       succeeded_count,
                       failed_count,
                       endpoints_added,
                       last_error,
                       companies_considered,
                       homepage_scanned,
                       endpoints_found_homepage_json,
                       endpoints_found_vendor_json,
                       sitemaps_scanned,
                       sitemap_urls_checked,
                       endpoints_found_sitemap_json,
                       careers_paths_checked,
                       robots_blocked_count,
                       fetch_failed_count,
                       time_budget_exceeded_count
                FROM careers_discovery_runs
                ORDER BY started_at DESC
                LIMIT 1
                """,
            new MapSqlParameterSource(),
            (rs, rowNum) -> new CareersDiscoveryRunStatus(
                rs.getLong("id"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getString("status"),
                rs.getInt("company_limit"),
                rs.getInt("processed_count"),
                rs.getInt("succeeded_count"),
                rs.getInt("failed_count"),
                rs.getInt("endpoints_added"),
                rs.getString("last_error"),
                rs.getInt("companies_considered"),
                rs.getInt("homepage_scanned"),
                readJsonMap(rs.getString("endpoints_found_homepage_json")),
                readJsonMap(rs.getString("endpoints_found_vendor_json")),
                rs.getInt("sitemaps_scanned"),
                rs.getInt("sitemap_urls_checked"),
                readJsonMap(rs.getString("endpoints_found_sitemap_json")),
                rs.getInt("careers_paths_checked"),
                rs.getInt("robots_blocked_count"),
                rs.getInt("fetch_failed_count"),
                rs.getInt("time_budget_exceeded_count"),
                Map.of()
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public void upsertCareersDiscoveryCompanyResult(
        long runId,
        long companyId,
        String status,
        String reasonCode,
        String stage,
        int foundEndpointsCount,
        Long durationMs,
        Integer httpStatus,
        String errorDetail
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("companyId", companyId)
            .addValue("status", status)
            .addValue("reasonCode", reasonCode)
            .addValue("stage", stage)
            .addValue("foundEndpointsCount", foundEndpointsCount)
            .addValue("durationMs", durationMs)
            .addValue("httpStatus", httpStatus)
            .addValue("errorDetail", truncateErrorDetail(errorDetail));
        if (postgres) {
            jdbc.update(
                """
                    INSERT INTO careers_discovery_company_results (
                        discovery_run_id,
                        company_id,
                        status,
                        reason_code,
                        stage,
                        found_endpoints_count,
                        duration_ms,
                        http_status,
                        error_detail,
                        created_at
                    )
                    VALUES (
                        :runId,
                        :companyId,
                        :status,
                        :reasonCode,
                        :stage,
                        :foundEndpointsCount,
                        :durationMs,
                        :httpStatus,
                        :errorDetail,
                        NOW()
                    )
                    ON CONFLICT (discovery_run_id, company_id)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        reason_code = EXCLUDED.reason_code,
                        stage = EXCLUDED.stage,
                        found_endpoints_count = EXCLUDED.found_endpoints_count,
                        duration_ms = EXCLUDED.duration_ms,
                        http_status = EXCLUDED.http_status,
                        error_detail = EXCLUDED.error_detail,
                        created_at = NOW()
                    """,
                params
            );
            return;
        }

        jdbc.update(
            """
                MERGE INTO careers_discovery_company_results (
                    discovery_run_id,
                    company_id,
                    status,
                    reason_code,
                    stage,
                    found_endpoints_count,
                    duration_ms,
                    http_status,
                    error_detail,
                    created_at
                )
                KEY(discovery_run_id, company_id)
                VALUES (
                    :runId,
                    :companyId,
                    :status,
                    :reasonCode,
                    :stage,
                    :foundEndpointsCount,
                    :durationMs,
                    :httpStatus,
                    :errorDetail,
                    CURRENT_TIMESTAMP()
                )
                """,
            params
        );
    }

    public List<CareersDiscoveryCompanyResultView> findCareersDiscoveryCompanyResults(
        long runId,
        String status,
        int limit
    ) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("status", status)
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT r.company_id,
                       c.ticker,
                       c.name AS company_name,
                       r.status,
                       r.reason_code,
                       r.stage,
                       r.found_endpoints_count,
                       r.duration_ms,
                       r.http_status,
                       r.error_detail,
                       r.created_at
                FROM careers_discovery_company_results r
                JOIN companies c ON c.id = r.company_id
                WHERE r.discovery_run_id = :runId
                  AND (:status IS NULL OR r.status = :status)
                ORDER BY r.created_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CareersDiscoveryCompanyResultView(
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("status"),
                rs.getString("reason_code"),
                rs.getString("stage"),
                rs.getInt("found_endpoints_count"),
                rs.getObject("duration_ms", Long.class),
                rs.getObject("http_status", Integer.class),
                rs.getString("error_detail"),
                toInstant(rs.getTimestamp("created_at"))
            )
        );
    }

    public Map<String, Long> countCareersDiscoveryCompanyFailures(long runId) {
        Map<String, Long> counts = new LinkedHashMap<>();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId);
        jdbc.query(
            """
                SELECT reason_code, COUNT(*) AS total
                FROM careers_discovery_company_results
                WHERE discovery_run_id = :runId
                  AND status = 'FAILED'
                GROUP BY reason_code
                ORDER BY total DESC, reason_code
                """,
            params,
            rs -> {
                String reason = rs.getString("reason_code");
                long total = rs.getLong("total");
                if (reason != null) {
                    counts.put(reason, total);
                }
            }
        );
        return counts;
    }

    public List<CareersDiscoveryCompanyFailureView> findRecentCareersDiscoveryCompanyFailures(long runId, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 200));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("runId", runId)
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT c.ticker,
                       c.name AS company_name,
                       r.stage,
                       r.reason_code,
                       r.http_status,
                       r.error_detail,
                       r.created_at
                FROM careers_discovery_company_results r
                JOIN companies c ON c.id = r.company_id
                WHERE r.discovery_run_id = :runId
                  AND r.status = 'FAILED'
                ORDER BY r.created_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CareersDiscoveryCompanyFailureView(
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("stage"),
                rs.getString("reason_code"),
                rs.getObject("http_status", Integer.class),
                rs.getString("error_detail"),
                toInstant(rs.getTimestamp("created_at"))
            )
        );
    }

    public Map<String, Long> countAtsApiAttemptsByStatus() {
        Map<String, Long> counts = new LinkedHashMap<>();
        jdbc.query(
            """
                SELECT fetch_status, COUNT(*) AS total
                FROM discovered_urls
                WHERE url_type = :urlType
                GROUP BY fetch_status
                ORDER BY total DESC, fetch_status
                """,
            new MapSqlParameterSource()
                .addValue("urlType", DiscoveredUrlType.ATS_API.name()),
            rs -> {
                String status = rs.getString("fetch_status");
                long total = rs.getLong("total");
                if (status != null) {
                    counts.put(status, total);
                }
            }
        );
        return counts;
    }

    public List<AtsAttemptSample> findRecentAtsApiFailures(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("urlType", DiscoveredUrlType.ATS_API.name())
            .addValue("success", "ats_fetch_success");
        return jdbc.query(
            """
                SELECT c.ticker,
                       du.adapter AS ats_type,
                       du.url,
                       du.fetch_status,
                       du.last_fetched_at
                FROM discovered_urls du
                JOIN companies c ON c.id = du.company_id
                WHERE du.url_type = :urlType
                  AND du.fetch_status IS NOT NULL
                  AND du.fetch_status <> :success
                ORDER BY du.last_fetched_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new AtsAttemptSample(
                rs.getString("ticker"),
                rs.getString("ats_type"),
                rs.getString("url"),
                rs.getString("fetch_status"),
                toInstant(rs.getTimestamp("last_fetched_at"))
            )
        );
    }

    public long countTable(String tableName) {
        Long count = jdbc.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class);
        return count == null ? 0L : count;
    }

    public HostCrawlState findHostCrawlState(String host) {
        if (host == null || host.isBlank()) {
            return null;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("host", host.toLowerCase(Locale.ROOT));
        List<HostCrawlState> rows = jdbc.query(
            """
                SELECT host,
                       consecutive_failures,
                       last_error_category,
                       last_attempt_at,
                       next_allowed_at
                FROM host_crawl_state
                WHERE host = :host
                """,
            params,
            (rs, rowNum) -> new HostCrawlState(
                rs.getString("host"),
                rs.getInt("consecutive_failures"),
                rs.getString("last_error_category"),
                toInstant(rs.getTimestamp("last_attempt_at")),
                toInstant(rs.getTimestamp("next_allowed_at"))
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public List<HostCrawlState> findHostsInCooldown(Instant now, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("now", toTimestamp(now == null ? Instant.now() : now))
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT host,
                       consecutive_failures,
                       last_error_category,
                       last_attempt_at,
                       next_allowed_at
                FROM host_crawl_state
                WHERE next_allowed_at IS NOT NULL
                  AND next_allowed_at > :now
                ORDER BY next_allowed_at ASC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new HostCrawlState(
                rs.getString("host"),
                rs.getInt("consecutive_failures"),
                rs.getString("last_error_category"),
                toInstant(rs.getTimestamp("last_attempt_at")),
                toInstant(rs.getTimestamp("next_allowed_at"))
            )
        );
    }

    public CareersDiscoveryState findCareersDiscoveryState(long companyId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId);
        List<CareersDiscoveryState> rows = jdbc.query(
            """
                SELECT company_id,
                       last_attempt_at,
                       last_reason_code,
                       last_candidate_url,
                       consecutive_failures,
                       next_attempt_at
                FROM careers_discovery_state
                WHERE company_id = :companyId
                """,
            params,
            (rs, rowNum) -> new CareersDiscoveryState(
                rs.getLong("company_id"),
                toInstant(rs.getTimestamp("last_attempt_at")),
                rs.getString("last_reason_code"),
                rs.getString("last_candidate_url"),
                rs.getInt("consecutive_failures"),
                toInstant(rs.getTimestamp("next_attempt_at"))
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public void upsertCareersDiscoveryState(
        long companyId,
        Instant lastAttemptAt,
        String lastReasonCode,
        String lastCandidateUrl,
        int consecutiveFailures,
        Instant nextAttemptAt
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("lastAttemptAt", toTimestamp(lastAttemptAt))
            .addValue("lastReasonCode", lastReasonCode)
            .addValue("lastCandidateUrl", lastCandidateUrl)
            .addValue("consecutiveFailures", Math.max(0, consecutiveFailures))
            .addValue("nextAttemptAt", toTimestamp(nextAttemptAt));
        jdbc.update(
            """
                INSERT INTO careers_discovery_state (
                    company_id,
                    last_attempt_at,
                    last_reason_code,
                    last_candidate_url,
                    consecutive_failures,
                    next_attempt_at
                )
                VALUES (
                    :companyId,
                    :lastAttemptAt,
                    :lastReasonCode,
                    :lastCandidateUrl,
                    :consecutiveFailures,
                    :nextAttemptAt
                )
                ON CONFLICT (company_id)
                DO UPDATE SET
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    last_reason_code = EXCLUDED.last_reason_code,
                    last_candidate_url = EXCLUDED.last_candidate_url,
                    consecutive_failures = EXCLUDED.consecutive_failures,
                    next_attempt_at = EXCLUDED.next_attempt_at
                """,
            params
        );
    }

    public void upsertHostCrawlState(
        String host,
        int consecutiveFailures,
        String lastErrorCategory,
        Instant lastAttemptAt,
        Instant nextAllowedAt
    ) {
        if (host == null || host.isBlank()) {
            return;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("host", host.toLowerCase(Locale.ROOT))
            .addValue("consecutiveFailures", Math.max(0, consecutiveFailures))
            .addValue("lastErrorCategory", lastErrorCategory)
            .addValue("lastAttemptAt", toTimestamp(lastAttemptAt))
            .addValue("nextAllowedAt", toTimestamp(nextAllowedAt));
        jdbc.update(
            """
                INSERT INTO host_crawl_state (
                    host,
                    consecutive_failures,
                    last_error_category,
                    last_attempt_at,
                    next_allowed_at
                )
                VALUES (
                    :host,
                    :consecutiveFailures,
                    :lastErrorCategory,
                    :lastAttemptAt,
                    :nextAllowedAt
                )
                ON CONFLICT (host)
                DO UPDATE SET
                    consecutive_failures = EXCLUDED.consecutive_failures,
                    last_error_category = EXCLUDED.last_error_category,
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    next_allowed_at = EXCLUDED.next_allowed_at
                """,
            params
        );
    }

    public long upsertCompany(String ticker, String name, String sector) {
        return upsertCompany(ticker, name, sector, null, null);
    }

    public Set<String> findExistingCompanyTickers(List<String> tickers) {
        if (tickers == null || tickers.isEmpty()) {
            return Set.of();
        }
        Set<String> existing = new LinkedHashSet<>();
        int batchSize = 1000;
        for (int i = 0; i < tickers.size(); i += batchSize) {
            int end = Math.min(tickers.size(), i + batchSize);
            List<String> slice = tickers.subList(i, end);
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("tickers", slice);
            jdbc.query(
                """
                    SELECT ticker
                    FROM companies
                    WHERE ticker IN (:tickers)
                    """,
                params,
                rs -> {
                    String ticker = rs.getString("ticker");
                    if (ticker != null) {
                        existing.add(ticker);
                    }
                }
            );
        }
        return existing;
    }

    public long upsertCompany(String ticker, String name, String sector, String wikipediaTitle) {
        return upsertCompany(ticker, name, sector, wikipediaTitle, null);
    }

    public long upsertCompany(String ticker, String name, String sector, String wikipediaTitle, String cik) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("ticker", ticker)
            .addValue("name", name)
            .addValue("sector", sector)
            .addValue("wikipediaTitle", wikipediaTitle)
            .addValue("cik", cik);

        int updated = jdbc.update(
            """
                UPDATE companies
                SET name = :name,
                    sector = COALESCE(:sector, sector),
                    wikipedia_title = COALESCE(:wikipediaTitle, wikipedia_title),
                    cik = COALESCE(:cik, cik)
                WHERE ticker = :ticker
                """,
            params
        );
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO companies (ticker, name, sector, wikipedia_title, cik)
                        VALUES (:ticker, :name, :sector, :wikipediaTitle, :cik)
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE companies
                        SET name = :name,
                            sector = COALESCE(:sector, sector),
                            wikipedia_title = COALESCE(:wikipediaTitle, wikipedia_title),
                            cik = COALESCE(:cik, cik)
                        WHERE ticker = :ticker
                        """,
                    params
                );
            }
        }

        Long id = jdbc.queryForObject(
            """
                SELECT id
                FROM companies
                WHERE ticker = :ticker
                """,
            params,
            Long.class
        );
        if (id == null) {
            throw new IllegalStateException("Failed to upsert company for ticker " + ticker);
        }
        return id;
    }

    public void updateCompanyDomainResolutionCache(
        long companyId,
        String method,
        String status,
        String errorCategory,
        Instant attemptedAt
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("method", method)
            .addValue("status", status)
            .addValue("errorCategory", errorCategory)
            .addValue("attemptedAt", toTimestamp(attemptedAt));
        jdbc.update(
            """
                UPDATE companies
                SET domain_resolution_method = :method,
                    domain_resolution_status = :status,
                    domain_resolution_error = :errorCategory,
                    domain_resolution_attempted_at = :attemptedAt
                WHERE id = :companyId
                """,
            params
        );
    }

    public void upsertCompanyDomain(long companyId, String domain, String careersHintUrl) {
        upsertCompanyDomain(companyId, domain, careersHintUrl, "MANUAL", 1.0, Instant.now());
    }

    public void upsertCompanyDomain(
        long companyId,
        String domain,
        String careersHintUrl,
        String source,
        double confidence,
        Instant resolvedAt
    ) {
        upsertCompanyDomain(companyId, domain, careersHintUrl, source, confidence, resolvedAt, null, null);
    }

    public void upsertCompanyDomain(
        long companyId,
        String domain,
        String careersHintUrl,
        String source,
        double confidence,
        Instant resolvedAt,
        String resolutionMethod,
        String wikidataQid
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("domain", domain)
            .addValue("careersHintUrl", careersHintUrl)
            .addValue("source", source)
            .addValue("confidence", confidence)
            .addValue("resolvedAt", toTimestamp(resolvedAt))
            .addValue("resolutionMethod", resolutionMethod)
            .addValue("wikidataQid", wikidataQid);

        int updated = jdbc.update(
            """
                UPDATE company_domains
                SET careers_hint_url = COALESCE(:careersHintUrl, careers_hint_url),
                    source = :source,
                    confidence = :confidence,
                    resolved_at = :resolvedAt,
                    resolution_method = COALESCE(:resolutionMethod, resolution_method),
                    wikidata_qid = COALESCE(:wikidataQid, wikidata_qid)
                WHERE company_id = :companyId
                  AND domain = :domain
                """,
            params
        );
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO company_domains (
                            company_id, domain, careers_hint_url, source, confidence, resolved_at,
                            resolution_method, wikidata_qid
                        )
                        VALUES (
                            :companyId, :domain, :careersHintUrl, :source, :confidence, :resolvedAt,
                            :resolutionMethod, :wikidataQid
                        )
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE company_domains
                        SET careers_hint_url = COALESCE(:careersHintUrl, careers_hint_url),
                            source = :source,
                            confidence = :confidence,
                            resolved_at = :resolvedAt,
                            resolution_method = COALESCE(:resolutionMethod, resolution_method),
                            wikidata_qid = COALESCE(:wikidataQid, wikidata_qid)
                        WHERE company_id = :companyId
                          AND domain = :domain
                        """,
                    params
                );
            }
        }
    }

    public long insertCrawlRun(Instant startedAt, String status, String notes) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("status", status)
            .addValue("notes", notes)
            .addValue("companiesAttempted", 0)
            .addValue("companiesSucceeded", 0)
            .addValue("companiesFailed", 0)
            .addValue("jobsExtractedCount", 0)
            .addValue("lastHeartbeatAt", toTimestamp(startedAt));

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(
            """
                INSERT INTO crawl_runs (
                    started_at,
                    status,
                    notes,
                    companies_attempted,
                    companies_succeeded,
                    companies_failed,
                    jobs_extracted_count,
                    last_heartbeat_at
                )
                VALUES (
                    :startedAt,
                    :status,
                    :notes,
                    :companiesAttempted,
                    :companiesSucceeded,
                    :companiesFailed,
                    :jobsExtractedCount,
                    :lastHeartbeatAt
                )
                """,
            params,
            keyHolder,
            new String[]{"id"}
        );
        Number key = keyHolder.getKey();
        Long id = key == null ? null : key.longValue();
        if (id == null) {
            id = jdbc.queryForObject(
                """
                    SELECT id
                    FROM crawl_runs
                    WHERE started_at = :startedAt
                      AND status = :status
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                params,
                Long.class
            );
            if (id == null) {
                throw new IllegalStateException("Failed to insert crawl run");
            }
        }
        return id;
    }

    public void completeCrawlRun(long crawlRunId, Instant finishedAt, String status, String notes) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("finishedAt", toTimestamp(finishedAt))
            .addValue("status", status)
            .addValue("notes", notes)
            .addValue("lastHeartbeatAt", toTimestamp(finishedAt));
        jdbc.update(
            """
                UPDATE crawl_runs
                SET finished_at = :finishedAt,
                    status = :status,
                    notes = :notes,
                    last_heartbeat_at = COALESCE(:lastHeartbeatAt, last_heartbeat_at)
                WHERE id = :crawlRunId
                """,
            params
        );
    }

    public void updateCrawlRunProgress(
        long crawlRunId,
        int companiesAttempted,
        int companiesSucceeded,
        int companiesFailed,
        int jobsExtractedCount,
        Instant heartbeatAt
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companiesAttempted", companiesAttempted)
            .addValue("companiesSucceeded", companiesSucceeded)
            .addValue("companiesFailed", companiesFailed)
            .addValue("jobsExtractedCount", jobsExtractedCount)
            .addValue("lastHeartbeatAt", toTimestamp(heartbeatAt));
        jdbc.update(
            """
                UPDATE crawl_runs
                SET companies_attempted = :companiesAttempted,
                    companies_succeeded = :companiesSucceeded,
                    companies_failed = :companiesFailed,
                    jobs_extracted_count = :jobsExtractedCount,
                    last_heartbeat_at = COALESCE(:lastHeartbeatAt, last_heartbeat_at)
                WHERE id = :crawlRunId
                """,
            params
        );
    }

    public void updateCrawlRunHeartbeat(long crawlRunId, Instant heartbeatAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("lastHeartbeatAt", toTimestamp(heartbeatAt));
        jdbc.update(
            """
                UPDATE crawl_runs
                SET last_heartbeat_at = COALESCE(:lastHeartbeatAt, last_heartbeat_at)
                WHERE id = :crawlRunId
                """,
            params
        );
    }

    public List<CompanyIdentity> findCompaniesMissingDomain(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit);
        return jdbc.query(
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       c.wikipedia_title,
                       c.cik,
                       c.domain_resolution_method,
                       c.domain_resolution_status,
                       c.domain_resolution_error,
                       c.domain_resolution_attempted_at
                FROM companies c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM company_domains cd
                    WHERE cd.company_id = c.id
                )
                ORDER BY c.ticker
                LIMIT :limit
                """,
            params,
            companyIdentityRowMapper()
        );
    }

    public List<CompanyIdentity> findCompaniesMissingDomainByTickers(List<String> tickers, int limit) {
        if (tickers == null || tickers.isEmpty()) {
            return List.of();
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("tickers", tickers)
            .addValue("limit", limit);
        return jdbc.query(
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       c.wikipedia_title,
                       c.cik,
                       c.domain_resolution_method,
                       c.domain_resolution_status,
                       c.domain_resolution_error,
                       c.domain_resolution_attempted_at
                FROM companies c
                WHERE c.ticker IN (:tickers)
                  AND NOT EXISTS (
                    SELECT 1
                    FROM company_domains cd
                    WHERE cd.company_id = c.id
                )
                ORDER BY c.ticker
                LIMIT :limit
                """,
            params,
            companyIdentityRowMapper()
        );
    }

    public List<CompanyTarget> findCompaniesWithDomainWithoutAts(int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit);

        return jdbc.query(
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM ats_endpoints ae
                    WHERE ae.company_id = c.id
                )
                ORDER BY c.ticker
                LIMIT :limit
                """,
            params,
            companyTargetRowMapper()
        );
    }

    public List<CompanyTarget> findCompaniesWithDomainWithoutAtsByTickers(List<String> tickers, int limit) {
        if (tickers == null || tickers.isEmpty()) {
            return List.of();
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("tickers", tickers)
            .addValue("limit", limit);
        return jdbc.query(
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                WHERE c.ticker IN (:tickers)
                  AND NOT EXISTS (
                    SELECT 1
                    FROM ats_endpoints ae
                    WHERE ae.company_id = c.id
                )
                ORDER BY c.ticker
                LIMIT :limit
                """,
            params,
            companyTargetRowMapper()
        );
    }

    public List<CompanyTarget> findCompanyTargets(List<String> tickers, int limit) {
        boolean tickerFilter = tickers != null && !tickers.isEmpty();
        String sql =
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                """ +
                (tickerFilter ? " WHERE c.ticker IN (:tickers) " : " ") +
                " ORDER BY c.ticker " +
                (limit > 0 ? " LIMIT :limit" : "");

        MapSqlParameterSource params = new MapSqlParameterSource();
        if (tickerFilter) {
            params.addValue("tickers", tickers);
        }
        if (limit > 0) {
            params.addValue("limit", limit);
        }

        return jdbc.query(sql, params, companyTargetRowMapper());
    }

    public List<CompanyTarget> findCompanyTargetsWithAts(List<String> tickers, int limit) {
        boolean tickerFilter = tickers != null && !tickers.isEmpty();
        String sql =
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                JOIN (
                    SELECT company_id, MAX(detected_at) AS max_detected_at
                    FROM ats_endpoints
                    GROUP BY company_id
                ) latest_ats ON latest_ats.company_id = c.id
                """ +
                (tickerFilter ? " WHERE c.ticker IN (:tickers) " : " ") +
                " ORDER BY latest_ats.max_detected_at DESC, c.ticker " +
                (limit > 0 ? " LIMIT :limit" : "");

        MapSqlParameterSource params = new MapSqlParameterSource();
        if (tickerFilter) {
            params.addValue("tickers", tickers);
        }
        if (limit > 0) {
            params.addValue("limit", limit);
        }

        return jdbc.query(sql, params, companyTargetRowMapper());
    }

    public List<CompanyTarget> findCompanyTargetsWithAtsDetectedSince(List<String> tickers, int limit, Instant detectedSince) {
        if (detectedSince == null) {
            return List.of();
        }
        boolean tickerFilter = tickers != null && !tickers.isEmpty();
        String sql =
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                JOIN (
                    SELECT company_id, MAX(detected_at) AS max_detected_at
                    FROM ats_endpoints
                    WHERE detected_at >= :detectedSince
                    GROUP BY company_id
                ) latest_ats ON latest_ats.company_id = c.id
                """ +
                (tickerFilter ? " WHERE c.ticker IN (:tickers) " : " ") +
                " ORDER BY latest_ats.max_detected_at DESC, c.ticker " +
                (limit > 0 ? " LIMIT :limit" : "");

        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("detectedSince", toTimestamp(detectedSince));
        if (tickerFilter) {
            params.addValue("tickers", tickers);
        }
        if (limit > 0) {
            params.addValue("limit", limit);
        }

        return jdbc.query(sql, params, companyTargetRowMapper());
    }

    public CompanyTarget findCompanyTargetById(long companyId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId);
        List<CompanyTarget> targets = jdbc.query(
            """
                SELECT c.id AS company_id,
                       c.ticker,
                       c.name,
                       c.sector,
                       cd.domain,
                       cd.careers_hint_url
                FROM companies c
                JOIN company_domains cd ON cd.id = (
                    SELECT cd2.id
                    FROM company_domains cd2
                    WHERE cd2.company_id = c.id
                    ORDER BY cd2.confidence DESC,
                             CASE WHEN cd2.resolved_at IS NULL THEN 1 ELSE 0 END,
                             cd2.resolved_at DESC,
                             cd2.id DESC
                    LIMIT 1
                )
                WHERE c.id = :companyId
                """,
            params,
            companyTargetRowMapper()
        );
        return targets.isEmpty() ? null : targets.getFirst();
    }

    public List<AtsEndpointRecord> findAtsEndpoints(long companyId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId);
        return jdbc.query(
            """
                SELECT company_id,
                       ats_type,
                       ats_url,
                       discovered_from_url,
                       confidence,
                       detected_at
                FROM ats_endpoints
                WHERE company_id = :companyId
                ORDER BY detected_at DESC
                """,
            params,
            (rs, rowNum) -> new AtsEndpointRecord(
                rs.getLong("company_id"),
                AtsType.valueOf(rs.getString("ats_type")),
                rs.getString("ats_url"),
                rs.getString("discovered_from_url"),
                rs.getDouble("confidence"),
                rs.getTimestamp("detected_at").toInstant()
            )
        );
    }

    public void insertDiscoveredSitemap(long crawlRunId, long companyId, String sitemapUrl, Instant fetchedAt, int urlCount) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companyId", companyId)
            .addValue("sitemapUrl", sitemapUrl)
            .addValue("fetchedAt", toTimestamp(fetchedAt))
            .addValue("urlCount", urlCount);
        jdbc.update(
            """
                INSERT INTO discovered_sitemaps (crawl_run_id, company_id, sitemap_url, fetched_at, url_count)
                VALUES (:crawlRunId, :companyId, :sitemapUrl, :fetchedAt, :urlCount)
                """,
            params
        );
    }

    public void upsertDiscoveredUrl(
        long crawlRunId,
        long companyId,
        String url,
        DiscoveredUrlType urlType,
        String fetchStatus,
        Instant lastFetchedAt
    ) {
        upsertDiscoveredUrl(
            crawlRunId,
            companyId,
            url,
            urlType,
            fetchStatus,
            lastFetchedAt,
            null,
            null,
            null
        );
    }

    public void upsertDiscoveredUrl(
        long crawlRunId,
        long companyId,
        String url,
        DiscoveredUrlType urlType,
        String fetchStatus,
        Instant lastFetchedAt,
        Integer httpStatus,
        String errorCode,
        String adapter
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companyId", companyId)
            .addValue("url", url)
            .addValue("urlType", urlType.name())
            .addValue("fetchStatus", fetchStatus)
            .addValue("lastFetchedAt", toTimestamp(lastFetchedAt))
            .addValue("httpStatus", httpStatus)
            .addValue("errorCode", errorCode)
            .addValue("adapter", adapter);

        int updated = jdbc.update(
            """
                UPDATE discovered_urls
                SET url_type = :urlType,
                    fetch_status = COALESCE(:fetchStatus, fetch_status),
                    last_fetched_at = COALESCE(:lastFetchedAt, last_fetched_at),
                    http_status = COALESCE(:httpStatus, http_status),
                    error_code = COALESCE(:errorCode, error_code),
                    adapter = COALESCE(:adapter, adapter)
                WHERE crawl_run_id = :crawlRunId
                  AND company_id = :companyId
                  AND url = :url
                """,
            params
        );
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO discovered_urls (
                            crawl_run_id, company_id, url, url_type, fetch_status, last_fetched_at,
                            http_status, error_code, adapter
                        )
                        VALUES (
                            :crawlRunId, :companyId, :url, :urlType, :fetchStatus, :lastFetchedAt,
                            :httpStatus, :errorCode, :adapter
                        )
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE discovered_urls
                        SET url_type = :urlType,
                            fetch_status = COALESCE(:fetchStatus, fetch_status),
                            last_fetched_at = COALESCE(:lastFetchedAt, last_fetched_at),
                            http_status = COALESCE(:httpStatus, http_status),
                            error_code = COALESCE(:errorCode, error_code),
                            adapter = COALESCE(:adapter, adapter)
                        WHERE crawl_run_id = :crawlRunId
                          AND company_id = :companyId
                          AND url = :url
                        """,
                    params
                );
            }
        }
    }

    public void insertCareersDiscoveryFailure(
        long companyId,
        String reasonCode,
        String candidateUrl,
        String detail,
        Instant observedAt
    ) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("reasonCode", reasonCode)
            .addValue("candidateUrl", candidateUrl)
            .addValue("detail", detail)
            .addValue("observedAt", toTimestamp(observedAt));

        jdbc.update(
            """
                INSERT INTO careers_discovery_failures (
                    company_id, reason_code, candidate_url, detail, observed_at
                )
                VALUES (:companyId, :reasonCode, :candidateUrl, :detail, :observedAt)
                """,
            params
        );
    }

    public void updateDiscoveredUrlStatus(long crawlRunId, long companyId, String url, String fetchStatus, Instant lastFetchedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companyId", companyId)
            .addValue("url", url)
            .addValue("fetchStatus", fetchStatus)
            .addValue("lastFetchedAt", toTimestamp(lastFetchedAt));
        jdbc.update(
            """
                UPDATE discovered_urls
                SET fetch_status = :fetchStatus,
                    last_fetched_at = :lastFetchedAt
                WHERE crawl_run_id = :crawlRunId
                  AND company_id = :companyId
                  AND url = :url
                """,
            params
        );
    }

    public boolean seenNoStructuredData(long companyId, String url) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("url", url);
        Boolean found = jdbc.queryForObject(
            """
                SELECT EXISTS(
                    SELECT 1
                    FROM discovered_urls
                    WHERE company_id = :companyId
                      AND url = :url
                      AND fetch_status = 'no_jobposting_structured_data'
                )
                """,
            params,
            Boolean.class
        );
        return Boolean.TRUE.equals(found);
    }

    public void upsertAtsEndpoint(
        long companyId,
        AtsType atsType,
        String atsUrl,
        String discoveredFromUrl,
        double confidence,
        Instant detectedAt
    ) {
        upsertAtsEndpoint(companyId, atsType, atsUrl, discoveredFromUrl, confidence, detectedAt, "legacy", true);
    }

    public void upsertAtsEndpoint(
        long companyId,
        AtsType atsType,
        String atsUrl,
        String discoveredFromUrl,
        double confidence,
        Instant detectedAt,
        String detectionMethod,
        boolean verified
    ) {
        String normalizedUrl = normalizeAtsEndpointUrl(atsType, atsUrl);
        if (normalizedUrl == null) {
            return;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("atsType", atsType.name())
            .addValue("atsUrl", normalizedUrl)
            .addValue("discoveredFromUrl", discoveredFromUrl)
            .addValue("confidence", confidence)
            .addValue("detectedAt", toTimestamp(detectedAt))
            .addValue("detectionMethod", detectionMethod)
            .addValue("verified", verified);
        int updated = jdbc.update(
            """
                UPDATE ats_endpoints
                SET detected_at = :detectedAt,
                    discovered_from_url = COALESCE(:discoveredFromUrl, discovered_from_url),
                    confidence = :confidence,
                    detection_method = COALESCE(:detectionMethod, detection_method),
                    verified = COALESCE(:verified, verified)
                WHERE company_id = :companyId
                  AND ats_type = :atsType
                  AND ats_url = :atsUrl
                """,
            params
        );
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO ats_endpoints (
                            company_id, ats_type, ats_url, detected_at, discovered_from_url, confidence,
                            detection_method, verified
                        )
                        VALUES (
                            :companyId, :atsType, :atsUrl, :detectedAt, :discoveredFromUrl, :confidence,
                            :detectionMethod, :verified
                        )
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE ats_endpoints
                        SET detected_at = :detectedAt,
                            discovered_from_url = COALESCE(:discoveredFromUrl, discovered_from_url),
                            confidence = :confidence,
                            detection_method = COALESCE(:detectionMethod, detection_method),
                            verified = COALESCE(:verified, verified)
                        WHERE company_id = :companyId
                          AND ats_type = :atsType
                          AND ats_url = :atsUrl
                        """,
                    params
                );
            }
        }
    }

    public void upsertJobPosting(long companyId, Long crawlRunId, NormalizedJobPosting posting, Instant fetchedAt) {
        upsertJobPostingsBatch(companyId, crawlRunId, List.of(posting), fetchedAt);
    }

    public void upsertJobPostingsBatch(long companyId, Long crawlRunId, List<NormalizedJobPosting> postings, Instant fetchedAt) {
        if (postings == null || postings.isEmpty()) {
            return;
        }

        int batchSize = properties.getJobPostingBatchSize();
        List<MapSqlParameterSource> paramsList = buildJobPostingParams(companyId, crawlRunId, postings, fetchedAt);
        if (!postgres) {
            for (MapSqlParameterSource params : paramsList) {
                upsertJobPostingLegacy(params);
            }
            return;
        }
        for (int i = 0; i < paramsList.size(); i += batchSize) {
            int end = Math.min(paramsList.size(), i + batchSize);
            List<MapSqlParameterSource> chunk = paramsList.subList(i, end);
            batchUpdateJobPostings(chunk);
        }
    }

    private void batchUpdateJobPostings(List<MapSqlParameterSource> paramsList) {
        if (paramsList == null || paramsList.isEmpty()) {
            return;
        }
        List<MapSqlParameterSource> byIdentifier = paramsList.stream()
            .filter(params -> {
                Object value = params.getValue("externalIdentifier");
                return value != null && !value.toString().isBlank();
            })
            .toList();
        if (!byIdentifier.isEmpty()) {
            jdbc.batchUpdate(
                """
                    UPDATE job_postings
                    SET source_url = :sourceUrl,
                        canonical_url = COALESCE(:canonicalUrl, canonical_url),
                        crawl_run_id = COALESCE(:crawlRunId, crawl_run_id),
                        title = :title,
                        org_name = :orgName,
                        location_text = :locationText,
                        employment_type = :employmentType,
                        date_posted = :datePosted,
                        description_text = :descriptionText,
                        description_plain = :descriptionPlain,
                        external_identifier = :externalIdentifier,
                        content_hash = :contentHash,
                        last_seen_at = :fetchedAt,
                        is_active = :isActive
                    WHERE company_id = :companyId
                      AND external_identifier = :externalIdentifier
                    """,
                byIdentifier.toArray(new MapSqlParameterSource[0])
            );
        }

        jdbc.batchUpdate(
            """
                INSERT INTO job_postings (
                    company_id, crawl_run_id, source_url, canonical_url, title, org_name, location_text,
                    employment_type, date_posted, description_text, description_plain, external_identifier,
                    content_hash, first_seen_at, last_seen_at, is_active
                )
                VALUES (
                    :companyId, :crawlRunId, :sourceUrl, :canonicalUrl, :title, :orgName, :locationText,
                    :employmentType, :datePosted, :descriptionText, :descriptionPlain, :externalIdentifier,
                    :contentHash, :fetchedAt, :fetchedAt, :isActive
                )
                ON CONFLICT (company_id, content_hash)
                DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    canonical_url = COALESCE(EXCLUDED.canonical_url, job_postings.canonical_url),
                    crawl_run_id = COALESCE(EXCLUDED.crawl_run_id, job_postings.crawl_run_id),
                    title = EXCLUDED.title,
                    org_name = EXCLUDED.org_name,
                    location_text = EXCLUDED.location_text,
                    employment_type = EXCLUDED.employment_type,
                    date_posted = EXCLUDED.date_posted,
                    description_text = EXCLUDED.description_text,
                    description_plain = EXCLUDED.description_plain,
                    external_identifier = EXCLUDED.external_identifier,
                    last_seen_at = EXCLUDED.last_seen_at,
                    is_active = EXCLUDED.is_active
                """,
            paramsList.toArray(new MapSqlParameterSource[0])
        );
    }

    private void upsertJobPostingLegacy(MapSqlParameterSource params) {
        int updated = jdbc.update(
            """
                UPDATE job_postings
                SET source_url = :sourceUrl,
                    canonical_url = COALESCE(:canonicalUrl, canonical_url),
                    crawl_run_id = COALESCE(:crawlRunId, crawl_run_id),
                    title = :title,
                    org_name = :orgName,
                    location_text = :locationText,
                    employment_type = :employmentType,
                    date_posted = :datePosted,
                    description_text = :descriptionText,
                    description_plain = :descriptionPlain,
                    external_identifier = :externalIdentifier,
                    last_seen_at = :fetchedAt,
                    is_active = :isActive
                WHERE company_id = :companyId
                  AND content_hash = :contentHash
                """,
            params
        );
        Object identifier = params.getValue("externalIdentifier");
        if (updated == 0 && identifier != null && !identifier.toString().isBlank()) {
            updated = jdbc.update(
                """
                    UPDATE job_postings
                    SET source_url = :sourceUrl,
                        canonical_url = COALESCE(:canonicalUrl, canonical_url),
                        crawl_run_id = COALESCE(:crawlRunId, crawl_run_id),
                        title = :title,
                        org_name = :orgName,
                        location_text = :locationText,
                        employment_type = :employmentType,
                        date_posted = :datePosted,
                        description_text = :descriptionText,
                        description_plain = :descriptionPlain,
                        external_identifier = :externalIdentifier,
                        content_hash = :contentHash,
                        last_seen_at = :fetchedAt,
                        is_active = :isActive
                    WHERE company_id = :companyId
                      AND external_identifier = :externalIdentifier
                    """,
                params
            );
        }

        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO job_postings (
                            company_id, crawl_run_id, source_url, canonical_url, title, org_name, location_text,
                            employment_type, date_posted, description_text, description_plain, external_identifier,
                            content_hash, first_seen_at, last_seen_at, is_active
                        )
                        VALUES (
                            :companyId, :crawlRunId, :sourceUrl, :canonicalUrl, :title, :orgName, :locationText,
                            :employmentType, :datePosted, :descriptionText, :descriptionPlain, :externalIdentifier,
                            :contentHash, :fetchedAt, :fetchedAt, :isActive
                        )
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE job_postings
                        SET source_url = :sourceUrl,
                            canonical_url = COALESCE(:canonicalUrl, canonical_url),
                            crawl_run_id = COALESCE(:crawlRunId, crawl_run_id),
                            title = :title,
                            org_name = :orgName,
                            location_text = :locationText,
                            employment_type = :employmentType,
                            date_posted = :datePosted,
                            description_text = :descriptionText,
                            description_plain = :descriptionPlain,
                            external_identifier = :externalIdentifier,
                            last_seen_at = :fetchedAt,
                            is_active = :isActive
                        WHERE company_id = :companyId
                          AND content_hash = :contentHash
                        """,
                    params
                );
            }
        }
    }

    private List<MapSqlParameterSource> buildJobPostingParams(
        long companyId,
        Long crawlRunId,
        List<NormalizedJobPosting> postings,
        Instant fetchedAt
    ) {
        List<MapSqlParameterSource> paramsList = new java.util.ArrayList<>(postings.size());
        for (NormalizedJobPosting posting : postings) {
            if (posting == null) {
                continue;
            }
            String descriptionPlain = null;
            if (posting.descriptionText() != null && !posting.descriptionText().isBlank()) {
                descriptionPlain = Jsoup.parse(posting.descriptionText()).text();
            }
            String canonicalUrl = JobUrlUtils.sanitizeCanonicalUrl(posting.canonicalUrl());
            if (canonicalUrl == null || canonicalUrl.isBlank()) {
                continue;
            }
            if (JobUrlUtils.isInvalidWorkdayUrl(posting.sourceUrl())) {
                continue;
            }
            String externalIdentifier = posting.externalIdentifier();
            MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("companyId", companyId)
                .addValue("crawlRunId", crawlRunId)
                .addValue("sourceUrl", posting.sourceUrl())
                .addValue("canonicalUrl", canonicalUrl)
                .addValue("title", posting.title())
                .addValue("orgName", posting.orgName())
                .addValue("locationText", posting.locationText())
                .addValue("employmentType", posting.employmentType())
                .addValue("datePosted", posting.datePosted())
                .addValue("descriptionText", posting.descriptionText())
                .addValue("descriptionPlain", descriptionPlain)
                .addValue("externalIdentifier", externalIdentifier)
                .addValue("contentHash", posting.contentHash())
                .addValue("fetchedAt", toTimestamp(fetchedAt))
                .addValue("isActive", true);
            paramsList.add(params);
        }
        return paramsList;
    }

    public void upsertJobPosting(long companyId, NormalizedJobPosting posting, Instant fetchedAt) {
        upsertJobPosting(companyId, null, posting, fetchedAt);
    }

    public CrawlRunMeta findMostRecentCrawlRun() {
        List<CrawlRunMeta> runs = jdbc.query(
            """
                SELECT id, started_at, finished_at, status
                FROM crawl_runs
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
            new MapSqlParameterSource(),
            (rs, rowNum) -> new CrawlRunMeta(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
                rs.getString("status")
            )
        );
        return runs.isEmpty() ? null : runs.getFirst();
    }

    public List<CrawlRunMeta> findRecentCrawlRuns(int limit) {
        int safeLimit = limit <= 0 ? 10 : limit;
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT id, started_at, finished_at, status
                FROM crawl_runs
                ORDER BY started_at DESC, id DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CrawlRunMeta(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
                rs.getString("status")
            )
        );
    }

    public List<CrawlRunMeta> findRunningCrawlRuns() {
        return jdbc.query(
            """
                SELECT id, started_at, finished_at, status
                FROM crawl_runs
                WHERE status = 'RUNNING'
                ORDER BY started_at ASC, id ASC
                """,
            new MapSqlParameterSource(),
            (rs, rowNum) -> new CrawlRunMeta(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
                rs.getString("status")
            )
        );
    }

    public CrawlRunActivityCounts findRunActivityCounts(long crawlRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        return jdbc.queryForObject(
            """
                SELECT
                    (SELECT COUNT(*) FROM discovered_urls WHERE crawl_run_id = :crawlRunId) AS discovered_urls,
                    (SELECT COUNT(*) FROM discovered_sitemaps WHERE crawl_run_id = :crawlRunId) AS discovered_sitemaps,
                    (SELECT COUNT(*) FROM job_postings WHERE crawl_run_id = :crawlRunId) AS job_postings
                """,
            params,
            (rs, rowNum) -> new CrawlRunActivityCounts(
                rs.getLong("discovered_urls"),
                rs.getLong("discovered_sitemaps"),
                rs.getLong("job_postings")
            )
        );
    }

    public Instant findLastActivityAtForRun(long crawlRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        Timestamp lastActivity = jdbc.queryForObject(
            """
                SELECT MAX(activity_at) AS last_activity
                FROM (
                    SELECT MAX(last_fetched_at) AS activity_at
                    FROM discovered_urls
                    WHERE crawl_run_id = :crawlRunId
                    UNION ALL
                    SELECT MAX(fetched_at) AS activity_at
                    FROM discovered_sitemaps
                    WHERE crawl_run_id = :crawlRunId
                    UNION ALL
                    SELECT MAX(last_seen_at) AS activity_at
                    FROM job_postings
                    WHERE crawl_run_id = :crawlRunId
                ) activity
                """,
            params,
            Timestamp.class
        );
        return toInstant(lastActivity);
    }

    public CrawlRunMeta findCrawlRunById(long crawlRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        List<CrawlRunMeta> runs = jdbc.query(
            """
                SELECT id, started_at, finished_at, status
                FROM crawl_runs
                WHERE id = :crawlRunId
                """,
            params,
            (rs, rowNum) -> new CrawlRunMeta(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
                rs.getString("status")
            )
        );
        return runs.isEmpty() ? null : runs.getFirst();
    }

    public CrawlRunStatus findCrawlRunStatus(long crawlRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        List<CrawlRunStatus> runs = jdbc.query(
            """
                SELECT id,
                       started_at,
                       finished_at,
                       status,
                       companies_attempted,
                       companies_succeeded,
                       companies_failed,
                       jobs_extracted_count,
                       last_heartbeat_at
                FROM crawl_runs
                WHERE id = :crawlRunId
                """,
            params,
            (rs, rowNum) -> new CrawlRunStatus(
                rs.getLong("id"),
                rs.getTimestamp("started_at").toInstant(),
                rs.getTimestamp("finished_at") == null ? null : rs.getTimestamp("finished_at").toInstant(),
                rs.getString("status"),
                rs.getInt("companies_attempted"),
                rs.getInt("companies_succeeded"),
                rs.getInt("companies_failed"),
                rs.getInt("jobs_extracted_count"),
                rs.getTimestamp("last_heartbeat_at") == null ? null : rs.getTimestamp("last_heartbeat_at").toInstant()
            )
        );
        return runs.isEmpty() ? null : runs.getFirst();
    }

    public void upsertCrawlRunCompanyResultStart(
        long crawlRunId,
        long companyId,
        String status,
        String stage,
        String atsType,
        String endpointUrl,
        Instant startedAt,
        boolean retryable
    ) {
        String atsTypeKey = normalizeAtsTypeKey(atsType);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companyId", companyId)
            .addValue("status", status)
            .addValue("stage", stage)
            .addValue("atsType", atsType)
            .addValue("atsTypeKey", atsTypeKey)
            .addValue("endpointUrl", endpointUrl)
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("retryable", retryable);
        if (postgres) {
            jdbc.update(
                """
                    INSERT INTO crawl_run_company_results (
                        crawl_run_id,
                        company_id,
                        status,
                        stage,
                        ats_type,
                        ats_type_key,
                        endpoint_url,
                        started_at,
                        retryable
                    )
                    VALUES (
                        :crawlRunId,
                        :companyId,
                        :status,
                        :stage,
                        :atsType,
                        :atsTypeKey,
                        :endpointUrl,
                        :startedAt,
                        :retryable
                    )
                    ON CONFLICT (crawl_run_id, company_id, stage, ats_type_key)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        ats_type = COALESCE(EXCLUDED.ats_type, crawl_run_company_results.ats_type),
                        ats_type_key = EXCLUDED.ats_type_key,
                        endpoint_url = COALESCE(EXCLUDED.endpoint_url, crawl_run_company_results.endpoint_url),
                        started_at = COALESCE(crawl_run_company_results.started_at, EXCLUDED.started_at),
                        retryable = EXCLUDED.retryable
                    """,
                params
            );
            return;
        }

        int updated = jdbc.update(
            """
                UPDATE crawl_run_company_results
                SET status = :status,
                    ats_type = COALESCE(:atsType, ats_type),
                    ats_type_key = :atsTypeKey,
                    endpoint_url = COALESCE(:endpointUrl, endpoint_url),
                    started_at = COALESCE(started_at, :startedAt),
                    retryable = :retryable
                WHERE crawl_run_id = :crawlRunId
                  AND company_id = :companyId
                  AND stage = :stage
                  AND ats_type_key = :atsTypeKey
                """,
            params
        );
        if (updated == 0) {
            jdbc.update(
                """
                    INSERT INTO crawl_run_company_results (
                        crawl_run_id,
                        company_id,
                        status,
                        stage,
                        ats_type,
                        ats_type_key,
                        endpoint_url,
                        started_at,
                        retryable
                    )
                    VALUES (
                        :crawlRunId,
                        :companyId,
                        :status,
                        :stage,
                        :atsType,
                        :atsTypeKey,
                        :endpointUrl,
                        :startedAt,
                        :retryable
                    )
                    """,
                params
            );
        }
    }

    public void upsertCrawlRunCompanyResultFinish(
        long crawlRunId,
        long companyId,
        String status,
        String stage,
        String atsType,
        String endpointUrl,
        Instant startedAt,
        Instant finishedAt,
        Long durationMs,
        int jobsExtracted,
        boolean truncated,
        Integer totalJobsAvailable,
        String stopReason,
        String reasonCode,
        Integer httpStatus,
        String errorDetail,
        boolean retryable
    ) {
        String atsTypeKey = normalizeAtsTypeKey(atsType);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("companyId", companyId)
            .addValue("status", status)
            .addValue("stage", stage)
            .addValue("atsType", atsType)
            .addValue("atsTypeKey", atsTypeKey)
            .addValue("endpointUrl", endpointUrl)
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("finishedAt", toTimestamp(finishedAt))
            .addValue("durationMs", durationMs)
            .addValue("jobsExtracted", jobsExtracted)
            .addValue("truncated", truncated)
            .addValue("totalJobsAvailable", totalJobsAvailable)
            .addValue("stopReason", stopReason)
            .addValue("reasonCode", reasonCode)
            .addValue("httpStatus", httpStatus)
            .addValue("errorDetail", truncateErrorDetail(errorDetail))
            .addValue("retryable", retryable);
        if (postgres) {
            jdbc.update(
                """
                    INSERT INTO crawl_run_company_results (
                        crawl_run_id,
                        company_id,
                        status,
                        stage,
                        ats_type,
                        ats_type_key,
                        endpoint_url,
                        started_at,
                        finished_at,
                        duration_ms,
                        jobs_extracted,
                        truncated,
                        total_jobs_available,
                        stop_reason,
                        reason_code,
                        http_status,
                        error_detail,
                        retryable
                    )
                    VALUES (
                        :crawlRunId,
                        :companyId,
                        :status,
                        :stage,
                        :atsType,
                        :atsTypeKey,
                        :endpointUrl,
                        :startedAt,
                        :finishedAt,
                        :durationMs,
                        :jobsExtracted,
                        :truncated,
                        :totalJobsAvailable,
                        :stopReason,
                        :reasonCode,
                        :httpStatus,
                        :errorDetail,
                        :retryable
                    )
                    ON CONFLICT (crawl_run_id, company_id, stage, ats_type_key)
                    DO UPDATE SET
                        status = EXCLUDED.status,
                        ats_type = COALESCE(EXCLUDED.ats_type, crawl_run_company_results.ats_type),
                        ats_type_key = EXCLUDED.ats_type_key,
                        endpoint_url = COALESCE(EXCLUDED.endpoint_url, crawl_run_company_results.endpoint_url),
                        finished_at = EXCLUDED.finished_at,
                        duration_ms = EXCLUDED.duration_ms,
                        jobs_extracted = EXCLUDED.jobs_extracted,
                        truncated = EXCLUDED.truncated,
                        total_jobs_available = EXCLUDED.total_jobs_available,
                        stop_reason = EXCLUDED.stop_reason,
                        reason_code = EXCLUDED.reason_code,
                        http_status = EXCLUDED.http_status,
                        error_detail = EXCLUDED.error_detail,
                        retryable = EXCLUDED.retryable
                    """,
                params
            );
            return;
        }

        int updated = jdbc.update(
            """
                UPDATE crawl_run_company_results
                SET status = :status,
                    ats_type = COALESCE(:atsType, ats_type),
                    ats_type_key = :atsTypeKey,
                    endpoint_url = COALESCE(:endpointUrl, endpoint_url),
                    finished_at = :finishedAt,
                    duration_ms = :durationMs,
                    jobs_extracted = :jobsExtracted,
                    truncated = :truncated,
                    total_jobs_available = :totalJobsAvailable,
                    stop_reason = :stopReason,
                    reason_code = :reasonCode,
                    http_status = :httpStatus,
                    error_detail = :errorDetail,
                    retryable = :retryable
                WHERE crawl_run_id = :crawlRunId
                  AND company_id = :companyId
                  AND stage = :stage
                  AND ats_type_key = :atsTypeKey
                """,
            params
        );
        if (updated == 0) {
            jdbc.update(
                """
                    INSERT INTO crawl_run_company_results (
                        crawl_run_id,
                        company_id,
                        status,
                        stage,
                        ats_type,
                        ats_type_key,
                        endpoint_url,
                        started_at,
                        finished_at,
                        duration_ms,
                        jobs_extracted,
                        truncated,
                        total_jobs_available,
                        stop_reason,
                        reason_code,
                        http_status,
                        error_detail,
                        retryable
                    )
                    VALUES (
                        :crawlRunId,
                        :companyId,
                        :status,
                        :stage,
                        :atsType,
                        :atsTypeKey,
                        :endpointUrl,
                        :startedAt,
                        :finishedAt,
                        :durationMs,
                        :jobsExtracted,
                        :truncated,
                        :totalJobsAvailable,
                        :stopReason,
                        :reasonCode,
                        :httpStatus,
                        :errorDetail,
                        :retryable
                    )
                    """,
                params
            );
        }
    }

    public List<CrawlRunCompanyResultView> findCrawlRunCompanyResults(long crawlRunId, String status, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("status", status)
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT r.company_id,
                       c.ticker,
                       c.name AS company_name,
                       r.status,
                       r.stage,
                       r.ats_type,
                       r.ats_type_key,
                       r.endpoint_url,
                       r.started_at,
                       r.finished_at,
                       r.duration_ms,
                       r.jobs_extracted,
                       r.truncated,
                       r.total_jobs_available,
                       r.stop_reason,
                       r.reason_code,
                       r.http_status,
                       r.error_detail,
                       r.retryable
                FROM crawl_run_company_results r
                JOIN companies c ON c.id = r.company_id
                WHERE r.crawl_run_id = :crawlRunId
                  AND (:status IS NULL OR r.status = :status)
                ORDER BY r.started_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CrawlRunCompanyResultView(
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("status"),
                rs.getString("stage"),
                rs.getString("ats_type"),
                rs.getString("ats_type_key"),
                rs.getString("endpoint_url"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getObject("duration_ms", Long.class),
                rs.getInt("jobs_extracted"),
                rs.getBoolean("truncated"),
                rs.getObject("total_jobs_available", Integer.class),
                rs.getString("stop_reason"),
                rs.getString("reason_code"),
                rs.getObject("http_status", Integer.class),
                rs.getString("error_detail"),
                rs.getBoolean("retryable")
            )
        );
    }

    public Map<String, Integer> countCrawlRunCompaniesByAtsType(long crawlRunId) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        jdbc.query(
            """
                SELECT COALESCE(r.ats_type, 'UNKNOWN') AS ats_type,
                       COUNT(DISTINCT r.company_id) AS total
                FROM crawl_run_company_results r
                WHERE r.crawl_run_id = :crawlRunId
                  AND r.stage = 'ATS_ADAPTER'
                GROUP BY COALESCE(r.ats_type, 'UNKNOWN')
                ORDER BY total DESC, ats_type
                """,
            params,
            rs -> {
                String atsType = rs.getString("ats_type");
                int total = rs.getInt("total");
                if (atsType != null) {
                    counts.put(atsType, total);
                }
            }
        );
        return counts;
    }

    public List<CrawlRunCompanyResultView> findCrawlRunCompanyOverallResults(long crawlRunId, String status, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 500));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("status", status)
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                WITH ranked AS (
                    SELECT r.*,
                           ROW_NUMBER() OVER (
                               PARTITION BY r.company_id
                               ORDER BY
                                 CASE r.status
                                   WHEN 'RUNNING' THEN 4
                                   WHEN 'SUCCEEDED' THEN 3
                                   WHEN 'FAILED' THEN 2
                                   WHEN 'SKIPPED' THEN 1
                                   ELSE 0
                                 END DESC,
                                 CASE r.stage
                                   WHEN 'ATS_ADAPTER' THEN 4
                                   WHEN 'JSONLD' THEN 3
                                   WHEN 'ROBOTS_SITEMAP' THEN 2
                                   WHEN 'DOMAIN' THEN 1
                                   ELSE 0
                                 END DESC,
                                 CASE WHEN r.started_at IS NULL THEN 1 ELSE 0 END,
                                 r.started_at DESC
                           ) AS rn
                    FROM crawl_run_company_results r
                    WHERE r.crawl_run_id = :crawlRunId
                )
                SELECT r.company_id,
                       c.ticker,
                       c.name AS company_name,
                       r.status,
                       r.stage,
                       r.ats_type,
                       r.ats_type_key,
                       r.endpoint_url,
                       r.started_at,
                       r.finished_at,
                       r.duration_ms,
                       r.jobs_extracted,
                       r.truncated,
                       r.total_jobs_available,
                       r.stop_reason,
                       r.reason_code,
                       r.http_status,
                       r.error_detail,
                       r.retryable
                FROM ranked r
                JOIN companies c ON c.id = r.company_id
                WHERE r.rn = 1
                  AND (:status IS NULL OR r.status = :status)
                ORDER BY r.started_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CrawlRunCompanyResultView(
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("status"),
                rs.getString("stage"),
                rs.getString("ats_type"),
                rs.getString("ats_type_key"),
                rs.getString("endpoint_url"),
                toInstant(rs.getTimestamp("started_at")),
                toInstant(rs.getTimestamp("finished_at")),
                rs.getObject("duration_ms", Long.class),
                rs.getInt("jobs_extracted"),
                rs.getBoolean("truncated"),
                rs.getObject("total_jobs_available", Integer.class),
                rs.getString("stop_reason"),
                rs.getString("reason_code"),
                rs.getObject("http_status", Integer.class),
                rs.getString("error_detail"),
                rs.getBoolean("retryable")
            )
        );
    }

    public Map<String, Long> countCrawlRunCompanyFailures(long crawlRunId) {
        Map<String, Long> counts = new LinkedHashMap<>();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId);
        jdbc.query(
            """
                SELECT reason_code, COUNT(*) AS total
                FROM crawl_run_company_results
                WHERE crawl_run_id = :crawlRunId
                  AND status = 'FAILED'
                GROUP BY reason_code
                ORDER BY total DESC, reason_code
                """,
            params,
            rs -> {
                String reason = rs.getString("reason_code");
                long total = rs.getLong("total");
                if (reason != null) {
                    counts.put(reason, total);
                }
            }
        );
        return counts;
    }

    public List<CrawlRunCompanyFailureView> findRecentCrawlRunCompanyFailures(long crawlRunId, int limit) {
        int safeLimit = Math.max(1, Math.min(limit, 200));
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("limit", safeLimit);
        return jdbc.query(
            """
                SELECT c.ticker,
                       c.name AS company_name,
                       r.stage,
                       r.reason_code,
                       r.http_status,
                       r.error_detail,
                       r.finished_at
                FROM crawl_run_company_results r
                JOIN companies c ON c.id = r.company_id
                WHERE r.crawl_run_id = :crawlRunId
                  AND r.status = 'FAILED'
                ORDER BY r.finished_at DESC NULLS LAST
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CrawlRunCompanyFailureView(
                rs.getString("ticker"),
                rs.getString("company_name"),
                rs.getString("stage"),
                rs.getString("reason_code"),
                rs.getObject("http_status", Integer.class),
                rs.getString("error_detail"),
                toInstant(rs.getTimestamp("finished_at"))
            )
        );
    }

    public void markPostingsInactiveNotSeenInRun(long companyId, long crawlRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("crawlRunId", crawlRunId);
        jdbc.update(
            """
                UPDATE job_postings
                SET is_active = FALSE
                WHERE company_id = :companyId
                  AND is_active = TRUE
                  AND (crawl_run_id IS NULL OR crawl_run_id <> :crawlRunId)
                """,
            params
        );
    }

    public long countJobsForRun(CrawlRunMeta runMeta) {
        if (runMeta == null) {
            return 0L;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", runMeta.crawlRunId())
            .addValue("startedAt", Timestamp.from(runMeta.startedAt()))
            .addValue("finishedAt", runMeta.finishedAt() == null ? Timestamp.from(Instant.now()) : Timestamp.from(runMeta.finishedAt()));
        Long count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM job_postings
                WHERE crawl_run_id = :crawlRunId
                   OR (
                       crawl_run_id IS NULL
                       AND last_seen_at >= :startedAt
                       AND last_seen_at <= :finishedAt
                   )
                """,
            params,
            Long.class
        );
        return count == null ? 0L : count;
    }

    public Map<String, Integer> findTopErrorsForRun(long crawlRunId, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("crawlRunId", crawlRunId)
            .addValue("limit", limit);
        List<Map.Entry<String, Integer>> rows = jdbc.query(
            """
                SELECT fetch_status, COUNT(*) AS cnt
                FROM discovered_urls
                WHERE crawl_run_id = :crawlRunId
                  AND fetch_status IS NOT NULL
                  AND fetch_status NOT IN (
                    'discovered',
                    'jobposting_found',
                    'ats_detected',
                    'ats_detected_probe_failed',
                    'ats_detected_from_hint',
                    'ats_fetch_success',
                    'no_jobposting_structured_data',
                    'skipped_known_no_structured_data'
                  )
                GROUP BY fetch_status
                ORDER BY cnt DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> Map.entry(rs.getString("fetch_status"), rs.getInt("cnt"))
        );
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> row : rows) {
            out.put(row.getKey(), row.getValue());
        }
        return out;
    }

    public int countNewJobsForRun(long companyId, long toRunId, Instant startedAt, Instant finishedAt) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("toRunId", toRunId)
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("finishedAt", toTimestamp(finishedAt));
        Integer count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM job_postings
                WHERE company_id = :companyId
                  AND crawl_run_id = :toRunId
                  AND first_seen_at >= :startedAt
                  AND first_seen_at <= :finishedAt
                """,
            params,
            Integer.class
        );
        return count == null ? 0 : count;
    }

    public int countRemovedJobsForRun(long companyId, long fromRunId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("fromRunId", fromRunId);
        Integer count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM job_postings
                WHERE company_id = :companyId
                  AND is_active = FALSE
                  AND crawl_run_id = :fromRunId
                """,
            params,
            Integer.class
        );
        return count == null ? 0 : count;
    }

    public List<JobDeltaItem> findNewJobsForRun(long companyId, long toRunId, Instant startedAt, Instant finishedAt, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("toRunId", toRunId)
            .addValue("startedAt", toTimestamp(startedAt))
            .addValue("finishedAt", toTimestamp(finishedAt))
            .addValue("limit", limit);

        return jdbc.query(
            """
                SELECT jp.id,
                       jp.title,
                       jp.location_text,
                       jp.source_url,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.first_seen_at,
                       jp.last_seen_at
                FROM job_postings jp
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE jp.company_id = :companyId
                  AND jp.crawl_run_id = :toRunId
                  AND jp.first_seen_at >= :startedAt
                  AND jp.first_seen_at <= :finishedAt
                ORDER BY jp.first_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobDeltaItem(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("location_text"),
                rs.getString("source_url"),
                parseAtsType(rs.getString("latest_ats_type")),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at"))
            )
        );
    }

    public List<JobDeltaItem> findRemovedJobsForRun(long companyId, long fromRunId, int limit) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("fromRunId", fromRunId)
            .addValue("limit", limit);

        return jdbc.query(
            """
                SELECT jp.id,
                       jp.title,
                       jp.location_text,
                       jp.source_url,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.first_seen_at,
                       jp.last_seen_at
                FROM job_postings jp
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE jp.company_id = :companyId
                  AND jp.is_active = FALSE
                  AND jp.crawl_run_id = :fromRunId
                ORDER BY jp.last_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobDeltaItem(
                rs.getLong("id"),
                rs.getString("title"),
                rs.getString("location_text"),
                rs.getString("source_url"),
                parseAtsType(rs.getString("latest_ats_type")),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at"))
            )
        );
    }

    public List<JobPostingListView> findNewestJobs(int limit, Long companyId, AtsType atsType, Boolean active, String query) {
        String normalizedQuery = normalizeQuery(query);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", limit)
            .addValue("companyId", companyId, Types.BIGINT)
            .addValue("atsType", atsType == null ? null : atsType.name(), Types.VARCHAR)
            .addValue("active", active, Types.BOOLEAN);
        addSearchParams(params, normalizedQuery);

        return jdbc.query(
            """
                SELECT jp.id,
                       jp.company_id,
                       c.ticker,
                       c.name AS company_name,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.source_url,
                       jp.canonical_url,
                       jp.title,
                       jp.org_name,
                       jp.location_text,
                       jp.employment_type,
                       jp.date_posted,
                       jp.first_seen_at,
                       jp.last_seen_at,
                       jp.is_active
                FROM job_postings jp
                JOIN companies c ON c.id = jp.company_id
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE (:companyId IS NULL OR jp.company_id = :companyId)
                  AND (:active IS NULL OR jp.is_active = :active)
                  AND (
                    :atsType IS NULL
                    OR EXISTS (
                        SELECT 1
                        FROM ats_endpoints ae2
                        WHERE ae2.company_id = jp.company_id
                          AND ae2.ats_type = :atsType
                    )
                  )
                """ + searchClause("jp", normalizedQuery) + """
                ORDER BY jp.last_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingListView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
                rs.getString("canonical_url"),
                rs.getString("title"),
                rs.getString("org_name"),
                rs.getString("location_text"),
                rs.getString("employment_type"),
                rs.getDate("date_posted") == null ? null : rs.getDate("date_posted").toLocalDate(),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at")),
                rs.getBoolean("is_active")
            )
        );
    }

    public long countJobPostingsFiltered(Long companyId, AtsType atsType, Boolean active, String query) {
        String normalizedQuery = normalizeQuery(query);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId, Types.BIGINT)
            .addValue("atsType", atsType == null ? null : atsType.name(), Types.VARCHAR)
            .addValue("active", active, Types.BOOLEAN);
        addSearchParams(params, normalizedQuery);

        String filterClause = jobPostingsFilterClause("jp", normalizedQuery);
        Long count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM job_postings jp
                """ + filterClause + """
                """,
            params,
            Long.class
        );
        return count == null ? 0L : count;
    }

    public List<JobPostingListView> findJobPostingsPage(
        int pageSize,
        int offset,
        Long companyId,
        AtsType atsType,
        Boolean active,
        String query
    ) {
        String normalizedQuery = normalizeQuery(query);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("limit", pageSize)
            .addValue("offset", offset)
            .addValue("companyId", companyId, Types.BIGINT)
            .addValue("atsType", atsType == null ? null : atsType.name(), Types.VARCHAR)
            .addValue("active", active, Types.BOOLEAN);
        addSearchParams(params, normalizedQuery);

        String filterClause = jobPostingsFilterClause("jp", normalizedQuery);
        return jdbc.query(
            """
                SELECT jp.id,
                       jp.company_id,
                       c.ticker,
                       c.name AS company_name,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.source_url,
                       jp.canonical_url,
                       jp.title,
                       jp.org_name,
                       jp.location_text,
                       jp.employment_type,
                       jp.date_posted,
                       jp.first_seen_at,
                       jp.last_seen_at,
                       jp.is_active
                FROM job_postings jp
                JOIN companies c ON c.id = jp.company_id
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                """ + filterClause + """
                ORDER BY jp.first_seen_at DESC, jp.id DESC
                LIMIT :limit
                OFFSET :offset
                """,
            params,
            (rs, rowNum) -> new JobPostingListView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
                rs.getString("canonical_url"),
                rs.getString("title"),
                rs.getString("org_name"),
                rs.getString("location_text"),
                rs.getString("employment_type"),
                rs.getDate("date_posted") == null ? null : rs.getDate("date_posted").toLocalDate(),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at")),
                rs.getBoolean("is_active")
            )
        );
    }

    public List<JobPostingListView> findNewJobsSince(Instant since, Long companyId, int limit, String query) {
        String normalizedQuery = normalizeQuery(query);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("since", toTimestamp(since))
            .addValue("companyId", companyId, Types.BIGINT)
            .addValue("limit", limit);
        addSearchParams(params, normalizedQuery);

        return jdbc.query(
            """
                SELECT jp.id,
                       jp.company_id,
                       c.ticker,
                       c.name AS company_name,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.source_url,
                       jp.canonical_url,
                       jp.title,
                       jp.org_name,
                       jp.location_text,
                       jp.employment_type,
                       jp.date_posted,
                       jp.first_seen_at,
                       jp.last_seen_at,
                       jp.is_active
                FROM job_postings jp
                JOIN companies c ON c.id = jp.company_id
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE jp.first_seen_at > :since
                  AND (:companyId IS NULL OR jp.company_id = :companyId)
                """ + searchClause("jp", normalizedQuery) + """
                ORDER BY jp.first_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingListView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
                rs.getString("canonical_url"),
                rs.getString("title"),
                rs.getString("org_name"),
                rs.getString("location_text"),
                rs.getString("employment_type"),
                rs.getDate("date_posted") == null ? null : rs.getDate("date_posted").toLocalDate(),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at")),
                rs.getBoolean("is_active")
            )
        );
    }

    public List<JobPostingListView> findClosedJobsSince(Instant since, Long companyId, int limit, String query) {
        String normalizedQuery = normalizeQuery(query);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("since", toTimestamp(since))
            .addValue("companyId", companyId, Types.BIGINT)
            .addValue("limit", limit);
        addSearchParams(params, normalizedQuery);

        return jdbc.query(
            """
                SELECT jp.id,
                       jp.company_id,
                       c.ticker,
                       c.name AS company_name,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.source_url,
                       jp.canonical_url,
                       jp.title,
                       jp.org_name,
                       jp.location_text,
                       jp.employment_type,
                       jp.date_posted,
                       jp.first_seen_at,
                       jp.last_seen_at,
                       jp.is_active
                FROM job_postings jp
                JOIN companies c ON c.id = jp.company_id
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE jp.is_active = FALSE
                  AND jp.last_seen_at > :since
                  AND (:companyId IS NULL OR jp.company_id = :companyId)
                """ + searchClause("jp", normalizedQuery) + """
                ORDER BY jp.last_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingListView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
                rs.getString("canonical_url"),
                rs.getString("title"),
                rs.getString("org_name"),
                rs.getString("location_text"),
                rs.getString("employment_type"),
                rs.getDate("date_posted") == null ? null : rs.getDate("date_posted").toLocalDate(),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at")),
                rs.getBoolean("is_active")
            )
        );
    }

    public JobPostingView findJobPostingById(long jobId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("jobId", jobId);

        List<JobPostingView> rows = jdbc.query(
            """
                SELECT jp.id,
                       jp.company_id,
                       c.ticker,
                       c.name AS company_name,
                       latest_ats.ats_type AS latest_ats_type,
                       jp.source_url,
                       jp.canonical_url,
                       jp.title,
                       jp.org_name,
                       jp.location_text,
                       jp.employment_type,
                       jp.date_posted,
                       jp.description_text,
                       jp.content_hash,
                       jp.first_seen_at,
                       jp.last_seen_at,
                       jp.is_active
                FROM job_postings jp
                JOIN companies c ON c.id = jp.company_id
                LEFT JOIN (
                    SELECT ae.company_id,
                           ae.ats_type
                    FROM ats_endpoints ae
                    JOIN (
                        SELECT company_id, MAX(detected_at) AS max_detected_at
                        FROM ats_endpoints
                        GROUP BY company_id
                    ) ranked
                      ON ranked.company_id = ae.company_id
                     AND ranked.max_detected_at = ae.detected_at
                ) latest_ats ON latest_ats.company_id = jp.company_id
                WHERE jp.id = :jobId
                  AND jp.canonical_url IS NOT NULL
                  AND jp.canonical_url NOT LIKE 'https://community.workday.com/invalid-url%'
                """,
            params,
            (rs, rowNum) -> new JobPostingView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
                rs.getString("canonical_url"),
                rs.getString("title"),
                rs.getString("org_name"),
                rs.getString("location_text"),
                rs.getString("employment_type"),
                rs.getDate("date_posted") == null ? null : rs.getDate("date_posted").toLocalDate(),
                rs.getString("description_text"),
                rs.getString("content_hash"),
                toInstant(rs.getTimestamp("first_seen_at")),
                toInstant(rs.getTimestamp("last_seen_at")),
                rs.getBoolean("is_active")
            )
        );
        return rows.isEmpty() ? null : rows.getFirst();
    }

    public List<JobPostingUrlRef> findWorkdayJobPostingUrls(long afterId, int limit) {
        long safeAfterId = Math.max(0L, afterId);
        int safeLimit = Math.max(1, limit);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("afterId", safeAfterId)
            .addValue("limit", safeLimit);

        return jdbc.query(
            """
                SELECT id,
                       canonical_url
                FROM job_postings
                WHERE id > :afterId
                  AND canonical_url IS NOT NULL
                  AND LOWER(canonical_url) LIKE '%workdayjobs%'
                ORDER BY id ASC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingUrlRef(
                rs.getLong("id"),
                rs.getString("canonical_url")
            )
        );
    }

    public int deleteJobPostingsByIds(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("ids", ids);
        return jdbc.update(
            """
                DELETE FROM job_postings
                WHERE id IN (:ids)
                """,
            params
        );
    }

    public List<CompanySearchResult> searchCompanies(String search, int limit) {
        String normalized = search == null ? null : search.trim();
        String lowered = normalized == null ? null : normalized.toLowerCase(Locale.ROOT);
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("search", lowered, Types.VARCHAR)
            .addValue("searchLike", lowered == null ? null : "%" + lowered + "%", Types.VARCHAR)
            .addValue("limit", limit);

        return jdbc.query(
            """
                SELECT c.id,
                       c.ticker,
                       c.name,
                       cd.domain
                FROM companies c
                LEFT JOIN (
                    SELECT company_id, MIN(domain) AS domain
                    FROM company_domains
                    GROUP BY company_id
                ) cd ON cd.company_id = c.id
                WHERE :search IS NOT NULL
                  AND (
                    LOWER(c.ticker) LIKE :searchLike
                    OR LOWER(c.name) LIKE :searchLike
                    OR LOWER(COALESCE(cd.domain, '')) LIKE :searchLike
                  )
                ORDER BY CASE
                    WHEN LOWER(c.ticker) = :search THEN 0
                    WHEN LOWER(c.name) = :search THEN 1
                    ELSE 2
                  END,
                  c.ticker
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CompanySearchResult(
                rs.getLong("id"),
                rs.getString("ticker"),
                rs.getString("name"),
                rs.getString("domain")
            )
        );
    }

    private RowMapper<CompanyIdentity> companyIdentityRowMapper() {
        return (rs, rowNum) -> new CompanyIdentity(
            rs.getLong("company_id"),
            rs.getString("ticker"),
            rs.getString("name"),
            rs.getString("sector"),
            rs.getString("wikipedia_title"),
            rs.getString("cik"),
            rs.getString("domain_resolution_method"),
            rs.getString("domain_resolution_status"),
            rs.getString("domain_resolution_error"),
            toInstant(rs.getTimestamp("domain_resolution_attempted_at"))
        );
    }

    private RowMapper<CompanyTarget> companyTargetRowMapper() {
        return (rs, rowNum) -> new CompanyTarget(
            rs.getLong("company_id"),
            rs.getString("ticker"),
            rs.getString("name"),
            rs.getString("sector"),
            rs.getString("domain"),
            rs.getString("careers_hint_url")
        );
    }

    private Timestamp toTimestamp(Instant value) {
        return value == null ? null : Timestamp.from(value);
    }

    private Instant toInstant(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant();
    }

    private AtsType parseAtsType(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return AtsType.valueOf(raw);
        } catch (IllegalArgumentException e) {
            log.warn("Unknown ats_type value in job view: {}", raw);
            return null;
        }
    }

    private String normalizeAtsTypeKey(String atsType) {
        if (atsType == null || atsType.isBlank()) {
            return "NONE";
        }
        return atsType.trim().toUpperCase(Locale.ROOT);
    }

    private boolean detectPostgres(NamedParameterJdbcTemplate jdbcTemplate) {
        if (jdbcTemplate.getJdbcTemplate().getDataSource() == null) {
            return false;
        }
        try (Connection connection = jdbcTemplate.getJdbcTemplate().getDataSource().getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            String productName = metaData == null ? null : metaData.getDatabaseProductName();
            String url = metaData == null ? null : metaData.getURL();
            if (url != null && url.toLowerCase(Locale.ROOT).startsWith("jdbc:h2:")) {
                return false;
            }
            return productName != null && productName.toLowerCase(Locale.ROOT).contains("postgres");
        } catch (Exception e) {
            log.warn("Unable to detect database product; defaulting to non-Postgres search", e);
            return false;
        }
    }

    private String normalizeQuery(String query) {
        if (query == null || query.isBlank()) {
            return null;
        }
        return query.trim();
    }

    private String truncateErrorDetail(String detail) {
        if (detail == null) {
            return null;
        }
        String trimmed = detail.trim();
        if (trimmed.length() <= 1000) {
            return trimmed;
        }
        return trimmed.substring(0, 1000);
    }

    private void addSearchParams(MapSqlParameterSource params, String query) {
        params.addValue("q", query, Types.VARCHAR);
        params.addValue(
            "qLike",
            query == null ? null : "%" + query.toLowerCase(Locale.ROOT) + "%",
            Types.VARCHAR
        );
    }

    private String searchClause(String alias, String query) {
        if (query == null) {
            return "";
        }
        if (postgres) {
            return " AND " + alias + ".search_tsv @@ websearch_to_tsquery('english', :q)";
        }
        return " AND (" +
            "LOWER(CAST(" + alias + ".title AS VARCHAR)) LIKE :qLike OR " +
            "LOWER(CAST(" + alias + ".org_name AS VARCHAR)) LIKE :qLike OR " +
            "LOWER(CAST(" + alias + ".location_text AS VARCHAR)) LIKE :qLike OR " +
            "LOWER(CAST(" + alias + ".employment_type AS VARCHAR)) LIKE :qLike OR " +
            "LOWER(CAST(" + alias + ".description_plain AS VARCHAR)) LIKE :qLike" +
            ")";
    }

    private String jobPostingsFilterClause(String alias, String normalizedQuery) {
        String safeAlias = alias == null || alias.isBlank() ? "jp" : alias;
        String clause = """
            WHERE (:companyId IS NULL OR %s.company_id = :companyId)
              AND (:active IS NULL OR %s.is_active = :active)
              AND (
                :atsType IS NULL
                OR EXISTS (
                    SELECT 1
                    FROM ats_endpoints ae2
                    WHERE ae2.company_id = %s.company_id
                      AND ae2.ats_type = :atsType
                )
              )
            """.formatted(safeAlias, safeAlias, safeAlias);
        clause = clause
            + "  AND " + safeAlias + ".canonical_url IS NOT NULL\n"
            + "  AND " + safeAlias + ".canonical_url NOT LIKE '" + JobUrlUtils.WORKDAY_INVALID_URL_PREFIX + "%'\n";
        return clause + searchClause(safeAlias, normalizedQuery);
    }

    private String normalizeAtsEndpointUrl(AtsType atsType, String raw) {
        if (raw == null || raw.isBlank() || atsType == null) {
            return null;
        }
        String value = raw.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            return raw.trim();
        }
        if (uri.getHost() == null) {
            return raw.trim();
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        boolean greenhouseApi = false;
        if (atsType == AtsType.GREENHOUSE) {
            if (host.equals("api.greenhouse.io") || host.equals("boards-api.greenhouse.io")) {
                host = "boards-api.greenhouse.io";
                greenhouseApi = true;
            } else if (host.equals("job-boards.greenhouse.io")) {
                host = "boards.greenhouse.io";
            }
        }
        String path = uri.getPath() == null ? "" : uri.getPath();
        if (atsType == AtsType.WORKDAY) {
            path = stripTrailingPunctuation(path);
        }
        String normalized = "https://" + host + path;
        if (greenhouseApi) {
            String query = uri.getRawQuery();
            if (query != null && !query.isBlank()) {
                normalized = normalized + "?" + query;
            }
        }
        if (normalized.endsWith("/") && normalized.length() > "https://x/".length()) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private Map<String, Integer> readJsonMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            Map<String, Integer> parsed = objectMapper.readValue(json, MAP_INT);
            return parsed == null ? Map.of() : parsed;
        } catch (Exception e) {
            return Map.of();
        }
    }

    private String writeJson(Map<String, Integer> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(map);
        } catch (Exception e) {
            return null;
        }
    }

    private String stripTrailingPunctuation(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        while (!trimmed.isEmpty()) {
            char last = trimmed.charAt(trimmed.length() - 1);
            if (last == '.' || last == ',' || last == ';' || last == ')' || last == ']' || last == '}' || last == '"' || last == '&' || last == '?') {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
                continue;
            }
            break;
        }
        return trimmed;
    }
}
