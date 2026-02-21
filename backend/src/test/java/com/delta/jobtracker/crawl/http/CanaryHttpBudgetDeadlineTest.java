package com.delta.jobtracker.crawl.http;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class CanaryHttpBudgetDeadlineTest {

  @Test
  void beforeRequestAbortsWhenDeadlineExpired() {
    CanaryHttpBudget budget =
        new CanaryHttpBudget(10, 100, 0.5, 1, 5, 1, 5, Instant.now().minusSeconds(1));

    assertThatThrownBy(() -> budget.beforeRequest("example.com"))
        .isInstanceOf(CanaryAbortException.class)
        .hasMessageContaining("canary_time_budget_exceeded");
  }
}
