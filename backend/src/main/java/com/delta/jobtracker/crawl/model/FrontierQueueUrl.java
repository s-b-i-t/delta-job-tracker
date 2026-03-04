package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record FrontierQueueUrl(
    long id,
    String url,
    String host,
    String canonicalUrl,
    FrontierUrlKind urlKind,
    int priority,
    Instant nextFetchAt,
    String status) {}
