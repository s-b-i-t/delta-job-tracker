package com.delta.jobtracker.crawl.model;

import java.util.Map;

public record DomainResolutionMetrics(
    int companiesInputCount,
    int companiesAttemptedCount,
    int cachedSkipCount,
    int wdqsTitleBatchCount,
    int wdqsCikBatchCount,
    int wikipediaInfoboxTriedCount,
    int wikipediaInfoboxResolvedCount,
    int wikipediaInfoboxRejectedCount,
    int heuristicCompaniesTriedCount,
    int heuristicCandidatesTriedCount,
    int heuristicFetchSuccessCount,
    int heuristicResolvedCount,
    int heuristicRejectedCount,
    long totalDurationMs,
    long wdqsDurationMs,
    long wikipediaInfoboxDurationMs,
    long heuristicDurationMs,
    Map<String, Integer> heuristicCandidatesTriedByTld,
    Map<String, Integer> heuristicResolvedByTld,
    Map<String, Integer> resolvedByMethod,
    Map<String, Integer> heuristicSuccessByReason) {}
