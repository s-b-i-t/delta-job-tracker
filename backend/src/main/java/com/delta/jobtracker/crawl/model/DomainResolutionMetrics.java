package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record DomainResolutionMetrics(
    int companiesInputCount,
    int companiesAttemptedCount,
    int cachedSkipCount,
    int wdqsTitleBatchCount,
    int wdqsCikBatchCount,
    int heuristicCompaniesTriedCount,
    int heuristicCandidatesTriedCount,
    int heuristicFetchSuccessCount,
    int heuristicResolvedCount,
    int heuristicRejectedCount,
    long totalDurationMs,
    long wdqsDurationMs,
    long heuristicDurationMs,
    Map<String, Integer> resolvedByMethod,
    Map<String, Integer> heuristicSuccessByReason) {}
