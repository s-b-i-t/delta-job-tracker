package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.CrawlRunMeta;
import com.delta.jobtracker.crawl.model.DiscoveredUrlType;
import com.delta.jobtracker.crawl.model.JobDeltaItem;
import com.delta.jobtracker.crawl.model.JobPostingView;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Repository
public class CrawlJdbcRepository {
    private static final Logger log = LoggerFactory.getLogger(CrawlJdbcRepository.class);
    private final NamedParameterJdbcTemplate jdbc;
    private final boolean postgres;

    public CrawlJdbcRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
        this.postgres = detectPostgres(jdbc);
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

    public long countTable(String tableName) {
        Long count = jdbc.getJdbcTemplate().queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class);
        return count == null ? 0L : count;
    }

    public long upsertCompany(String ticker, String name, String sector) {
        return upsertCompany(ticker, name, sector, null);
    }

    public long upsertCompany(String ticker, String name, String sector, String wikipediaTitle) {
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("ticker", ticker)
            .addValue("name", name)
            .addValue("sector", sector)
            .addValue("wikipediaTitle", wikipediaTitle);

        int updated = jdbc.update(
            """
                UPDATE companies
                SET name = :name,
                    sector = COALESCE(:sector, sector),
                    wikipedia_title = COALESCE(:wikipediaTitle, wikipedia_title)
                WHERE ticker = :ticker
                """,
            params
        );
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO companies (ticker, name, sector, wikipedia_title)
                        VALUES (:ticker, :name, :sector, :wikipediaTitle)
                        """,
                    params
                );
            } catch (DataIntegrityViolationException ignored) {
                jdbc.update(
                    """
                        UPDATE companies
                        SET name = :name,
                            sector = COALESCE(:sector, sector),
                            wikipedia_title = COALESCE(:wikipediaTitle, wikipedia_title)
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
            .addValue("notes", notes);

        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbc.update(
            """
                INSERT INTO crawl_runs (started_at, status, notes)
                VALUES (:startedAt, :status, :notes)
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
            .addValue("notes", notes);
        jdbc.update(
            """
                UPDATE crawl_runs
                SET finished_at = :finishedAt,
                    status = :status,
                    notes = :notes
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
                SELECT c.id AS company_id, c.ticker, c.name, c.sector, c.wikipedia_title
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
                SELECT c.id AS company_id, c.ticker, c.name, c.sector, c.wikipedia_title
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
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("atsType", atsType.name())
            .addValue("atsUrl", atsUrl)
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
        String descriptionPlain = null;
        if (posting.descriptionText() != null && !posting.descriptionText().isBlank()) {
            descriptionPlain = Jsoup.parse(posting.descriptionText()).text();
        }
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("crawlRunId", crawlRunId)
            .addValue("sourceUrl", posting.sourceUrl())
            .addValue("title", posting.title())
            .addValue("orgName", posting.orgName())
            .addValue("locationText", posting.locationText())
            .addValue("employmentType", posting.employmentType())
            .addValue("datePosted", posting.datePosted())
            .addValue("descriptionText", posting.descriptionText())
            .addValue("descriptionPlain", descriptionPlain)
            .addValue("externalIdentifier", posting.externalIdentifier())
            .addValue("contentHash", posting.contentHash())
            .addValue("fetchedAt", toTimestamp(fetchedAt))
            .addValue("isActive", true);

        int updated = jdbc.update(
            """
                UPDATE job_postings
                SET source_url = :sourceUrl,
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
        if (updated == 0) {
            try {
                jdbc.update(
                    """
                        INSERT INTO job_postings (
                            company_id, crawl_run_id, source_url, title, org_name, location_text,
                            employment_type, date_posted, description_text, description_plain, external_identifier,
                            content_hash, first_seen_at, last_seen_at, is_active
                        )
                        VALUES (
                            :companyId, :crawlRunId, :sourceUrl, :title, :orgName, :locationText,
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

    public void upsertJobPosting(long companyId, NormalizedJobPosting posting, Instant fetchedAt) {
        upsertJobPosting(companyId, null, posting, fetchedAt);
    }

    public CrawlRunMeta findMostRecentCrawlRun() {
        List<CrawlRunMeta> runs = jdbc.query(
            """
                SELECT id, started_at, finished_at, status
                FROM crawl_runs
                ORDER BY started_at DESC
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

    public List<JobPostingView> findNewestJobs(int limit, Long companyId, AtsType atsType, Boolean active, String query) {
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
            (rs, rowNum) -> new JobPostingView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
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
    }

    public List<JobPostingView> findNewJobsSince(Instant since, Long companyId, int limit, String query) {
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
                WHERE jp.first_seen_at > :since
                  AND (:companyId IS NULL OR jp.company_id = :companyId)
                """ + searchClause("jp", normalizedQuery) + """
                ORDER BY jp.first_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
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
    }

    public List<JobPostingView> findClosedJobsSince(Instant since, Long companyId, int limit, String query) {
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
                WHERE jp.is_active = FALSE
                  AND jp.last_seen_at > :since
                  AND (:companyId IS NULL OR jp.company_id = :companyId)
                """ + searchClause("jp", normalizedQuery) + """
                ORDER BY jp.last_seen_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new JobPostingView(
                rs.getLong("id"),
                rs.getLong("company_id"),
                rs.getString("ticker"),
                rs.getString("company_name"),
                parseAtsType(rs.getString("latest_ats_type")),
                rs.getString("source_url"),
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
    }

    private RowMapper<CompanyIdentity> companyIdentityRowMapper() {
        return (rs, rowNum) -> new CompanyIdentity(
            rs.getLong("company_id"),
            rs.getString("ticker"),
            rs.getString("name"),
            rs.getString("sector"),
            rs.getString("wikipedia_title")
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
}
