package com.delta.jobtracker.crawl.http;

public final class CanaryHttpBudgetContext {
  private static final ThreadLocal<CanaryHttpBudget> CURRENT = new ThreadLocal<>();

  private CanaryHttpBudgetContext() {}

  public static CanaryHttpBudget current() {
    return CURRENT.get();
  }

  public static Scope activate(CanaryHttpBudget budget) {
    CURRENT.set(budget);
    return () -> CURRENT.remove();
  }

  public interface Scope extends AutoCloseable {
    @Override
    void close();
  }
}
