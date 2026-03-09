package com.delta.jobtracker.crawl.model;

public record FrontierSchedulerDiagnostics(
    long claimNextDueUrlDbMillis,
    long findHostStateDbMillis,
    long countDueBlockedDbMillis,
    long enqueueDbMillis,
    long completeFetchDbMillis,
    long robotsCheckMillis,
    long httpFetchMillis,
    long parseMillis,
    long totalMillis) {

  public long dbMillis() {
    return claimNextDueUrlDbMillis
        + findHostStateDbMillis
        + countDueBlockedDbMillis
        + enqueueDbMillis
        + completeFetchDbMillis;
  }

  public long downstreamMillis() {
    return robotsCheckMillis + httpFetchMillis;
  }

  public long otherMillis() {
    long classifiedMillis = dbMillis() + downstreamMillis() + parseMillis;
    return Math.max(0L, totalMillis - classifiedMillis);
  }
}
