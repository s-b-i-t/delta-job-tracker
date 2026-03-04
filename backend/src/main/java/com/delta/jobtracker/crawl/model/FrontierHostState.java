package com.delta.jobtracker.crawl.model;

public record FrontierHostState(String host, int backoffState, String lastStatusBucket) {}
