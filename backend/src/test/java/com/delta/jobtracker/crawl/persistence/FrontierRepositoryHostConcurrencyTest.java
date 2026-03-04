package com.delta.jobtracker.crawl.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.FrontierFetchOutcome;
import com.delta.jobtracker.crawl.model.FrontierQueueUrl;
import com.delta.jobtracker.crawl.model.FrontierUrlKind;
import java.time.Instant;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class FrontierRepositoryHostConcurrencyTest {

  @Autowired private FrontierRepository frontierRepository;

  @Test
  void secondClaimForSameHostBlocksUntilFirstCompletes() {
    String suffix = UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
    String host = "frontier-lock-" + suffix + ".example.com";

    Instant due = Instant.now().minusSeconds(60);
    frontierRepository.enqueueUrl(
        "https://" + host + "/jobs/one", FrontierUrlKind.CANDIDATE, 20, due);
    frontierRepository.enqueueUrl(
        "https://" + host + "/jobs/two", FrontierUrlKind.CANDIDATE, 10, due);

    FrontierQueueUrl first = frontierRepository.claimNextDueUrl("test-worker", 60, FrontierUrlKind.CANDIDATE);
    assertThat(first).isNotNull();

    FrontierQueueUrl blocked = frontierRepository.claimNextDueUrl("test-worker", 60, FrontierUrlKind.CANDIDATE);
    assertThat(blocked).isNull();

    FrontierFetchOutcome success =
        new FrontierFetchOutcome(
            "FETCHED",
            Instant.now(),
            200,
            15L,
            null,
            null,
            "HTTP_2XX",
            Instant.now(),
            0);
    frontierRepository.completeFetch(first, success);

    FrontierQueueUrl second = frontierRepository.claimNextDueUrl("test-worker", 60, FrontierUrlKind.CANDIDATE);
    assertThat(second).isNotNull();
    assertThat(second.id()).isNotEqualTo(first.id());
  }
}
