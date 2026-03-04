package com.delta.jobtracker.crawl.model;

public record FrontierEnqueueResult(
    boolean inserted, long urlId, String canonicalUrl, String host) {}
