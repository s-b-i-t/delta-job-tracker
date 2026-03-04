package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierFetchOutcome;
import com.delta.jobtracker.crawl.model.FrontierHostState;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import com.delta.jobtracker.crawl.util.FrontierUrlCanonicalizer;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class FrontierRepository {
  private final NamedParameterJdbcTemplate jdbc;
  private final FrontierUrlCanonicalizer canonicalizer;

  public FrontierRepository(
      NamedParameterJdbcTemplate jdbc, FrontierUrlCanonicalizer canonicalizer) {
    this.jdbc = jdbc;
    this.canonicalizer = canonicalizer;
  }

  public List<String> findSeedDomains(int limit) {
    int safeLimit = Math.max(1, limit);
    int oversampledLimit = (int) Math.min(10000L, Math.max((long) safeLimit * 5L, safeLimit));
    List<String> rows =
        jdbc.query(
            """
                SELECT cd.domain
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
                ORDER BY c.ticker
                LIMIT :limit
                """,
            new MapSqlParameterSource().addValue("limit", oversampledLimit),
            (rs, rowNum) -> rs.getString("domain"));
    Set<String> deduped = new LinkedHashSet<>();
    for (String row : rows) {
      String normalized = normalizeDomainToHost(row);
      if (normalized != null) {
        deduped.add(normalized);
      }
    }
    return deduped.stream().limit(safeLimit).toList();
  }

  public FrontierEnqueueResult enqueueUrl(
      String rawUrl, FrontierUrlKind urlKind, int priority, Instant nextFetchAt) {
    String canonical = canonicalizer.canonicalize(rawUrl);
    String host = canonicalizer.extractHost(canonical);
    if (canonical == null || host == null) {
      return new FrontierEnqueueResult(false, 0L, null, null);
    }

    Instant now = Instant.now();
    ensureHost(host);

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("url", rawUrl)
            .addValue("host", host)
            .addValue("canonicalUrl", canonical)
            .addValue("urlKind", urlKind.name())
            .addValue("priority", Math.max(0, priority))
            .addValue("nextFetchAt", toTimestamp(nextFetchAt == null ? now : nextFetchAt))
            .addValue("now", toTimestamp(now));

    int updated =
        jdbc.update(
            """
                UPDATE crawl_urls
                SET priority = CASE WHEN priority < :priority THEN :priority ELSE priority END,
                    next_fetch_at = CASE WHEN next_fetch_at > :nextFetchAt THEN :nextFetchAt ELSE next_fetch_at END,
                    url_kind = CASE WHEN :urlKind = 'SITEMAP' THEN :urlKind ELSE url_kind END,
                    status = CASE WHEN status IN ('FAILED', 'BLOCKED') THEN 'QUEUED' ELSE status END,
                    updated_at = :now
                WHERE canonical_url = :canonicalUrl
                """,
            params);
    if (updated > 0) {
      Long existingId =
          jdbc.queryForObject(
              """
                  SELECT id
                  FROM crawl_urls
                  WHERE canonical_url = :canonicalUrl
                  """,
              params,
              Long.class);
      return new FrontierEnqueueResult(
          false, existingId == null ? 0L : existingId, canonical, host);
    }

    try {
      jdbc.update(
          """
              INSERT INTO crawl_urls (
                  url,
                  host,
                  canonical_url,
                  url_kind,
                  priority,
                  next_fetch_at,
                  status,
                  updated_at
              )
              VALUES (
                  :url,
                  :host,
                  :canonicalUrl,
                  :urlKind,
                  :priority,
                  :nextFetchAt,
                  'QUEUED',
                  :now
              )
              """,
          params);
      Long insertedId =
          jdbc.queryForObject(
              """
                  SELECT id
                  FROM crawl_urls
                  WHERE canonical_url = :canonicalUrl
                  """,
              params,
              Long.class);
      return new FrontierEnqueueResult(true, insertedId == null ? 0L : insertedId, canonical, host);
    } catch (DataIntegrityViolationException ignored) {
      Long existingId =
          jdbc.queryForObject(
              """
                  SELECT id
                  FROM crawl_urls
                  WHERE canonical_url = :canonicalUrl
                  """,
              params,
              Long.class);
      return new FrontierEnqueueResult(
          false, existingId == null ? 0L : existingId, canonical, host);
    }
  }

  public void ensureHost(String host) {
    String normalized = normalizeDomainToHost(host);
    if (normalized == null) {
      return;
    }
    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("host", normalized)
            .addValue("now", toTimestamp(Instant.now()));
    int updated =
        jdbc.update(
            """
                UPDATE crawl_hosts
                SET updated_at = :now
                WHERE host = :host
                """,
            params);
    if (updated > 0) {
      return;
    }
    try {
      jdbc.update(
          """
              INSERT INTO crawl_hosts (host, next_allowed_at, backoff_state, updated_at)
              VALUES (:host, :now, 0, :now)
              """,
          params);
    } catch (DataIntegrityViolationException ignored) {
      // Concurrent insert; ignore.
    }
  }

  @Transactional
  public FrontierQueueUrl claimNextDueUrl(
      String lockOwner, long lockTtlSeconds, FrontierUrlKind kind) {
    String safeOwner =
        (lockOwner == null || lockOwner.isBlank()) ? "frontier-worker" : lockOwner.trim();
    Instant now = Instant.now();
    Timestamp nowTs = toTimestamp(now);
    Timestamp lockedUntil = toTimestamp(now.plusSeconds(Math.max(5L, lockTtlSeconds)));

    for (int attempts = 0; attempts < 5; attempts++) {
      List<FrontierQueueUrl> candidates =
          jdbc.query(
              """
                  SELECT u.id,
                         u.url,
                         u.host,
                         u.canonical_url,
                         u.url_kind,
                         u.priority,
                         u.next_fetch_at,
                         u.status
                  FROM crawl_urls u
                  JOIN crawl_hosts h ON h.host = u.host
                  WHERE u.url_kind = :urlKind
                    AND u.status = 'QUEUED'
                    AND u.next_fetch_at <= :now
                    AND (u.locked_until IS NULL OR u.locked_until < :now)
                    AND (h.next_allowed_at IS NULL OR h.next_allowed_at <= :now)
                    AND h.inflight_count < 1
                  ORDER BY u.priority DESC, u.next_fetch_at ASC, u.id ASC
                  LIMIT 1
                  """,
              new MapSqlParameterSource().addValue("urlKind", kind.name()).addValue("now", nowTs),
              (rs, rowNum) ->
                  new FrontierQueueUrl(
                      rs.getLong("id"),
                      rs.getString("url"),
                      rs.getString("host"),
                      rs.getString("canonical_url"),
                      FrontierUrlKind.valueOf(rs.getString("url_kind")),
                      rs.getInt("priority"),
                      rs.getTimestamp("next_fetch_at").toInstant(),
                      rs.getString("status")));

      if (candidates.isEmpty()) {
        return null;
      }
      FrontierQueueUrl candidate = candidates.getFirst();

      int hostLocked =
          jdbc.update(
              """
                  UPDATE crawl_hosts
                  SET inflight_count = inflight_count + 1,
                      updated_at = :now
                  WHERE host = :host
                    AND inflight_count < 1
                    AND (next_allowed_at IS NULL OR next_allowed_at <= :now)
                  """,
              new MapSqlParameterSource()
                  .addValue("host", candidate.host())
                  .addValue("now", nowTs));
      if (hostLocked == 0) {
        continue;
      }

      int urlLocked =
          jdbc.update(
              """
                  UPDATE crawl_urls
                  SET status = 'FETCHING',
                      locked_until = :lockedUntil,
                      lock_owner = :lockOwner,
                      updated_at = :now
                  WHERE id = :id
                    AND status = 'QUEUED'
                    AND (locked_until IS NULL OR locked_until < :now)
                  """,
              new MapSqlParameterSource()
                  .addValue("id", candidate.id())
                  .addValue("lockOwner", safeOwner)
                  .addValue("lockedUntil", lockedUntil)
                  .addValue("now", nowTs));

      if (urlLocked > 0) {
        return candidate;
      }
      releaseHostInflight(candidate.host());
    }
    return null;
  }

  @Transactional
  public void completeFetch(FrontierQueueUrl claimedUrl, FrontierFetchOutcome outcome) {
    if (claimedUrl == null) {
      return;
    }
    Instant now = Instant.now();
    Instant fetchedAt = outcome == null || outcome.fetchedAt() == null ? now : outcome.fetchedAt();
    String status = outcome == null || outcome.urlStatus() == null ? "FAILED" : outcome.urlStatus();

    MapSqlParameterSource params =
        new MapSqlParameterSource()
            .addValue("id", claimedUrl.id())
            .addValue("status", status)
            .addValue("lastFetchAt", toTimestamp(fetchedAt))
            .addValue("lastError", outcome == null ? "missing_outcome" : outcome.lastError())
            .addValue("now", toTimestamp(now));

    jdbc.update(
        """
            UPDATE crawl_urls
            SET status = :status,
                last_fetch_at = :lastFetchAt,
                last_error = :lastError,
                locked_until = NULL,
                lock_owner = NULL,
                updated_at = :now
            WHERE id = :id
            """,
        params);

    MapSqlParameterSource attemptParams =
        new MapSqlParameterSource()
            .addValue("urlId", claimedUrl.id())
            .addValue("fetchedAt", toTimestamp(fetchedAt))
            .addValue("httpStatus", outcome == null ? null : outcome.httpStatus())
            .addValue("elapsedMs", outcome == null ? null : outcome.elapsedMs())
            .addValue("errorBucket", outcome == null ? "missing_outcome" : outcome.errorBucket());
    jdbc.update(
        """
            INSERT INTO crawl_url_attempts (url_id, fetched_at, http_status, elapsed_ms, error_bucket)
            VALUES (:urlId, :fetchedAt, :httpStatus, :elapsedMs, :errorBucket)
            """,
        attemptParams);

    MapSqlParameterSource hostParams =
        new MapSqlParameterSource()
            .addValue("host", claimedUrl.host())
            .addValue(
                "nextAllowedAt",
                toTimestamp(
                    outcome == null || outcome.hostNextAllowedAt() == null
                        ? now
                        : outcome.hostNextAllowedAt()))
            .addValue("backoffState", outcome == null ? 0 : Math.max(0, outcome.hostBackoffState()))
            .addValue("statusBucket", outcome == null ? "ERROR" : outcome.hostStatusBucket())
            .addValue("now", toTimestamp(now));
    jdbc.update(
        """
            UPDATE crawl_hosts
            SET next_allowed_at = :nextAllowedAt,
                backoff_state = :backoffState,
                last_status_bucket = :statusBucket,
                inflight_count = CASE WHEN inflight_count > 0 THEN inflight_count - 1 ELSE 0 END,
                updated_at = :now
            WHERE host = :host
            """,
        hostParams);
  }

  public void releaseHostInflight(String host) {
    if (host == null || host.isBlank()) {
      return;
    }
    jdbc.update(
        """
            UPDATE crawl_hosts
            SET inflight_count = CASE WHEN inflight_count > 0 THEN inflight_count - 1 ELSE 0 END,
                updated_at = :now
            WHERE host = :host
            """,
        new MapSqlParameterSource()
            .addValue("host", host.toLowerCase(Locale.ROOT))
            .addValue("now", toTimestamp(Instant.now())));
  }

  public int countDueUrlsBlockedByBackoff(FrontierUrlKind kind) {
    Integer value =
        jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM crawl_urls u
                JOIN crawl_hosts h ON h.host = u.host
                WHERE u.url_kind = :urlKind
                  AND u.status = 'QUEUED'
                  AND u.next_fetch_at <= :now
                  AND (u.locked_until IS NULL OR u.locked_until < :now)
                  AND h.next_allowed_at > :now
                """,
            new MapSqlParameterSource()
                .addValue("urlKind", kind.name())
                .addValue("now", toTimestamp(Instant.now())),
            Integer.class);
    return value == null ? 0 : value;
  }

  public FrontierHostState findHostState(String host) {
    if (host == null || host.isBlank()) {
      return null;
    }
    List<FrontierHostState> rows =
        jdbc.query(
            """
                SELECT host, backoff_state, last_status_bucket
                FROM crawl_hosts
                WHERE host = :host
                """,
            new MapSqlParameterSource().addValue("host", host.toLowerCase(Locale.ROOT)),
            (rs, rowNum) ->
                new FrontierHostState(
                    rs.getString("host"),
                    rs.getInt("backoff_state"),
                    rs.getString("last_status_bucket")));
    return rows.isEmpty() ? null : rows.getFirst();
  }

  public Map<String, Integer> countQueueStatuses() {
    Map<String, Integer> counts = new LinkedHashMap<>();
    List<Map.Entry<String, Integer>> rows =
        jdbc.query(
            """
                SELECT status, COUNT(*) AS total
                FROM crawl_urls
                GROUP BY status
                ORDER BY status
                """,
            new MapSqlParameterSource(),
            (rs, rowNum) -> Map.entry(rs.getString("status"), rs.getInt("total")));
    for (Map.Entry<String, Integer> row : rows) {
      counts.put(row.getKey(), row.getValue());
    }
    return counts;
  }

  private Timestamp toTimestamp(Instant instant) {
    return instant == null ? null : Timestamp.from(instant);
  }

  private String normalizeDomainToHost(String domainOrHost) {
    if (domainOrHost == null || domainOrHost.isBlank()) {
      return null;
    }
    String trimmed = domainOrHost.trim();
    String canonical = canonicalizer.canonicalize(trimmed);
    if (canonical != null) {
      String host = canonicalizer.extractHost(canonical);
      if (host != null) {
        return host;
      }
    }

    String host = trimmed.toLowerCase(Locale.ROOT);
    if (host.startsWith("http://")) {
      host = host.substring("http://".length());
    } else if (host.startsWith("https://")) {
      host = host.substring("https://".length());
    }
    int slashIdx = host.indexOf('/');
    if (slashIdx >= 0) {
      host = host.substring(0, slashIdx);
    }
    if (host.isBlank()) {
      return null;
    }
    return host;
  }
}
