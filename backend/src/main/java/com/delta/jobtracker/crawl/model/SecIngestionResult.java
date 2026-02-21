package com.delta.jobtracker.crawl.model;

import java.util.List;

public record SecIngestionResult(
    List<String> tickers, int companiesUpserted, int errorCount, List<String> sampleErrors) {}
