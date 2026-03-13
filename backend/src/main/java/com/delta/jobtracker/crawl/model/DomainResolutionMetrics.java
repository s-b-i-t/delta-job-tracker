package com.delta.jobtracker.crawl.model;

import java.util.List;
import java.util.Map;

public record DomainResolutionMetrics(
    int companiesInputCount,
    int selectionReturnedCount,
    int selectionEligibleCount,
    int skippedNotEmployerCount,
    List<String> skippedNotEmployerSample,
    int companiesAttemptedCount,
    int cachedSkipCount,
    int wdqsTitleBatchCount,
    int wdqsCikBatchCount,
    int wdqsTitleRequestCount,
    int wdqsCikRequestCount,
    int wdqsRetryCount,
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
    Map<String, Integer> wdqsFailureByCategory,
    Map<String, Integer> wdqsRetryByCategory,
    Map<String, Integer> heuristicCandidatesTriedByTld,
    Map<String, Integer> heuristicResolvedByTld,
    Map<String, Integer> resolvedByMethod,
    Map<String, Integer> heuristicSuccessByReason) {}
