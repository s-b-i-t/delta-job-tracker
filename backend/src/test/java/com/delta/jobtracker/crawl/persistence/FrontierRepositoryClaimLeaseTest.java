package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
import com.delta.jobtracker.crawl.model.FrontierFetchOutcome;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class FrontierRepositoryClaimLeaseTest {

  @Autowired private FrontierRepository frontierRepository;
  @Autowired private JdbcTemplate jdbcTemplate;

  @Test
  void claimIsHostLimitedUntilCompleteFetchReleasesInflight() {
    FrontierQueueUrl firstUrl =
        enqueueDueCandidate("https://lease.example.com/jobs/one", 100, Instant.now().minusSeconds(120));
    FrontierQueueUrl secondUrl =
        enqueueDueCandidate("https://lease.example.com/jobs/two", 90, Instant.now().minusSeconds(120));

    FrontierQueueUrl claimedFirst =
        frontierRepository.claimNextDueUrl("worker-one", 60, FrontierUrlKind.CANDIDATE);
    assertThat(claimedFirst).isNotNull();

    FrontierQueueUrl blockedSecond =
        frontierRepository.claimNextDueUrl("worker-two", 60, FrontierUrlKind.CANDIDATE);
    assertThat(blockedSecond).isNull();

    frontierRepository.completeFetch(
        claimedFirst,
        new FrontierFetchOutcome(
            "FETCHED",
            Instant.now(),
            200,
            15L,
            null,
            null,
            "SUCCESS",
            Instant.now().minusSeconds(1),
            0));

    Integer inflightAfterRelease =
        jdbcTemplate.queryForObject(
            "SELECT inflight_count FROM crawl_hosts WHERE host = ?",
            Integer.class,
            "lease.example.com");
    assertThat(inflightAfterRelease).isZero();

    FrontierQueueUrl claimedSecond =
        frontierRepository.claimNextDueUrl("worker-three", 60, FrontierUrlKind.CANDIDATE);
    assertThat(claimedSecond).isNotNull();
    assertThat(claimedSecond.id()).isNotEqualTo(claimedFirst.id());
    assertThat(claimedSecond.id()).isEqualTo(secondUrl.id());

    assertThat(firstUrl.id()).isNotEqualTo(secondUrl.id());
  }

  private FrontierQueueUrl enqueueDueCandidate(String url, int priority, Instant dueAt) {
    FrontierEnqueueResult result =
        frontierRepository.enqueueUrl(url, FrontierUrlKind.CANDIDATE, priority, dueAt);
    assertThat(result.inserted()).isTrue();
    FrontierQueueUrl row =
        jdbcTemplate.queryForObject(
            """
                SELECT id, url, host, canonical_url, url_kind, priority, next_fetch_at, status
                FROM crawl_urls
                WHERE id = ?
                """,
            (rs, rowNum) ->
                new FrontierQueueUrl(
                    rs.getLong("id"),
                    rs.getString("url"),
                    rs.getString("host"),
                    rs.getString("canonical_url"),
                    FrontierUrlKind.valueOf(rs.getString("url_kind")),
                    rs.getInt("priority"),
                    rs.getTimestamp("next_fetch_at").toInstant(),
                    rs.getString("status")),
            result.urlId());
    assertThat(row).isNotNull();
    return row;
  }
}
