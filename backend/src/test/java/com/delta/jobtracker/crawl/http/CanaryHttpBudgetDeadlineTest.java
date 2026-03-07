package com.delta.jobtracker.crawl.http;

import static org.assertj.core.api.Assertions.assertThat;
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

  @Test
  void effectiveRequestTimeoutIsClampedToRemainingDeadline() {
    Instant now = Instant.parse("2026-03-06T00:00:00Z");
    Instant deadline = now.plusSeconds(5);
    CanaryHttpBudget budget = new CanaryHttpBudget(10, 100, 0.5, 1, 5, 1, 60, deadline);

    assertThat(budget.effectiveRequestTimeoutSeconds(now)).isEqualTo(5);
    assertThat(budget.effectiveRequestTimeoutSeconds(now.plusMillis(4500))).isEqualTo(1);
    assertThatThrownBy(() -> budget.effectiveRequestTimeoutSeconds(deadline))
        .isInstanceOf(CanaryAbortException.class)
        .hasMessageContaining("canary_time_budget_exceeded");
  }
}
