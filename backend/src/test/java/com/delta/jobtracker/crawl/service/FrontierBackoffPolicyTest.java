package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.FrontierBackoffDecision;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class FrontierBackoffPolicyTest {

  @Test
  void synthetic429ResponsesEscalateBackoffStateAndDelay() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.setPerHostDelayMs(1000);
    FrontierBackoffPolicy policy = new FrontierBackoffPolicy(properties);

    FrontierBackoffDecision first = policy.onResult(0, 429, null);
    FrontierBackoffDecision second = policy.onResult(first.nextBackoffState(), 429, null);

    assertThat(first.statusBucket()).isEqualTo("HTTP_429");
    assertThat(second.statusBucket()).isEqualTo("HTTP_429");
    assertThat(first.nextBackoffState()).isEqualTo(1);
    assertThat(second.nextBackoffState()).isEqualTo(2);

    Duration firstDelay = Duration.between(Instant.now(), first.nextAllowedAt());
    Duration secondDelay = Duration.between(Instant.now(), second.nextAllowedAt());

    assertThat(firstDelay).isGreaterThanOrEqualTo(Duration.ofSeconds(20));
    assertThat(secondDelay).isGreaterThan(firstDelay);

    FrontierBackoffDecision reset = policy.onResult(second.nextBackoffState(), 200, null);
    assertThat(reset.statusBucket()).isEqualTo("HTTP_2XX");
    assertThat(reset.nextBackoffState()).isEqualTo(0);
  }
}
