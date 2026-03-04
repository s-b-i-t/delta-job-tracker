package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record FrontierBackoffDecision(
    String statusBucket, int nextBackoffState, Instant nextAllowedAt) {}
