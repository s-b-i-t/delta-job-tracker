package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.FrontierEnqueueResult;
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
class FrontierRepositoryCanonicalDedupeTest {

  @Autowired private FrontierRepository frontierRepository;
  @Autowired private JdbcTemplate jdbcTemplate;

  @Test
  void enqueueCanonicalizesAndDedupesEquivalentUrls() {
    FrontierEnqueueResult first =
        frontierRepository.enqueueUrl(
            "HTTPS://Example.com/careers/?utm_source=newsletter#top",
            FrontierUrlKind.CANDIDATE,
            10,
            Instant.now());
    FrontierEnqueueResult second =
        frontierRepository.enqueueUrl(
            "https://example.com/careers", FrontierUrlKind.CANDIDATE, 20, Instant.now());

    assertThat(first.inserted()).isTrue();
    assertThat(second.inserted()).isFalse();
    assertThat(first.canonicalUrl()).isEqualTo("https://example.com/careers");
    assertThat(second.canonicalUrl()).isEqualTo(first.canonicalUrl());

    Integer total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM crawl_urls", Integer.class);
    Integer priority =
        jdbcTemplate.queryForObject(
            "SELECT priority FROM crawl_urls WHERE canonical_url = ?",
            Integer.class,
            "https://example.com/careers");

    assertThat(total).isEqualTo(1);
    assertThat(priority).isEqualTo(20);
  }
}
