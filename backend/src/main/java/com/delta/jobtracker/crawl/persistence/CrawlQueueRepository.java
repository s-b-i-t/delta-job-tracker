package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.CrawlQueueErrorSample;
import com.delta.jobtracker.crawl.model.CrawlQueueStats;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class CrawlQueueRepository {
    private final NamedParameterJdbcTemplate jdbc;

    public CrawlQueueRepository(NamedParameterJdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public Long claimNextCompany(String lockOwner, long lockTtlSeconds) {
        Instant now = Instant.now();
        Instant lockedUntil = now.plusSeconds(Math.max(1, lockTtlSeconds));
        String safeOwner = (lockOwner == null || lockOwner.isBlank()) ? "unknown" : lockOwner.trim();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("now", Timestamp.from(now))
            .addValue("lockedUntil", Timestamp.from(lockedUntil))
            .addValue("lockOwner", safeOwner);

        List<Long> results = jdbc.query(
            """
                WITH candidate AS (
                    SELECT company_id
                    FROM crawl_queue
                    WHERE next_run_at <= :now
                      AND (locked_until IS NULL OR locked_until < :now)
                    ORDER BY next_run_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE crawl_queue cq
                SET locked_until = :lockedUntil,
                    lock_owner = :lockOwner,
                    lock_count = cq.lock_count + 1,
                    last_started_at = :now,
                    updated_at = :now
                FROM candidate
                WHERE cq.company_id = candidate.company_id
                RETURNING cq.company_id
                """,
            params,
            (rs, rowNum) -> rs.getLong("company_id")
        );
        return results.isEmpty() ? null : results.getFirst();
    }

    public void markSuccess(long companyId, Instant nextRunAt) {
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("nextRunAt", Timestamp.from(nextRunAt))
            .addValue("now", Timestamp.from(now));
        jdbc.update(
            """
                UPDATE crawl_queue
                SET next_run_at = :nextRunAt,
                    locked_until = NULL,
                    lock_owner = NULL,
                    last_finished_at = :now,
                    last_success_at = :now,
                    last_error = NULL,
                    consecutive_failures = 0,
                    total_runs = total_runs + 1,
                    total_successes = total_successes + 1,
                    updated_at = :now
                WHERE company_id = :companyId
                """,
            params
        );
    }

    public void markFailure(long companyId, Instant nextRunAt, String error) {
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("nextRunAt", Timestamp.from(nextRunAt))
            .addValue("lastError", error)
            .addValue("now", Timestamp.from(now));
        jdbc.update(
            """
                UPDATE crawl_queue
                SET next_run_at = :nextRunAt,
                    locked_until = NULL,
                    lock_owner = NULL,
                    last_finished_at = :now,
                    last_error = :lastError,
                    consecutive_failures = consecutive_failures + 1,
                    total_runs = total_runs + 1,
                    total_failures = total_failures + 1,
                    updated_at = :now
                WHERE company_id = :companyId
                """,
            params
        );
    }

    public void releaseLock(long companyId) {
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("companyId", companyId)
            .addValue("now", Timestamp.from(now));
        jdbc.update(
            """
                UPDATE crawl_queue
                SET locked_until = NULL,
                    lock_owner = NULL,
                    updated_at = :now
                WHERE company_id = :companyId
                """,
            params
        );
    }

    public int getConsecutiveFailures(long companyId) {
        Integer value = jdbc.queryForObject(
            """
                SELECT consecutive_failures
                FROM crawl_queue
                WHERE company_id = :companyId
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            Integer.class
        );
        return value == null ? 0 : value;
    }

    public int bootstrapQueue() {
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("now", Timestamp.from(now));
        return jdbc.update(
            """
                INSERT INTO crawl_queue (company_id, next_run_at, updated_at)
                SELECT id, :now, :now
                FROM companies
                ON CONFLICT (company_id)
                DO UPDATE SET next_run_at = EXCLUDED.next_run_at,
                              updated_at = EXCLUDED.updated_at
                """,
            params
        );
    }

    public CrawlQueueStats fetchQueueStats(int errorSampleLimit) {
        Instant now = Instant.now();
        MapSqlParameterSource params = new MapSqlParameterSource()
            .addValue("now", Timestamp.from(now))
            .addValue("limit", errorSampleLimit);

        Long dueCount = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM crawl_queue
                WHERE next_run_at <= :now
                  AND (locked_until IS NULL OR locked_until < :now)
                """,
            params,
            Long.class
        );
        Long lockedCount = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM crawl_queue
                WHERE locked_until IS NOT NULL
                  AND locked_until > :now
                """,
            params,
            Long.class
        );
        Timestamp nextDue = jdbc.queryForObject(
            """
                SELECT MIN(next_run_at)
                FROM crawl_queue
                """,
            params,
            Timestamp.class
        );

        List<CrawlQueueErrorSample> errors = jdbc.query(
            """
                SELECT company_id,
                       last_error,
                       last_finished_at,
                       consecutive_failures
                FROM crawl_queue
                WHERE last_error IS NOT NULL
                ORDER BY last_finished_at DESC
                LIMIT :limit
                """,
            params,
            (rs, rowNum) -> new CrawlQueueErrorSample(
                rs.getLong("company_id"),
                rs.getString("last_error"),
                rs.getTimestamp("last_finished_at") == null
                    ? null
                    : rs.getTimestamp("last_finished_at").toInstant(),
                rs.getInt("consecutive_failures")
            )
        );

        return new CrawlQueueStats(
            dueCount == null ? 0L : dueCount,
            lockedCount == null ? 0L : lockedCount,
            nextDue == null ? null : nextDue.toInstant(),
            errors
        );
    }
}
