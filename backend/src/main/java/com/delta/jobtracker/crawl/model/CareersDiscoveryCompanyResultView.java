package com.delta.jobtracker.crawl.model;

import java.time.Instant;

public record CareersDiscoveryCompanyResultView(
    long companyId,
    String ticker,
    String companyName,
    String status,
    String reasonCode,
    String stage,
    int foundEndpointsCount,
    Long durationMs,
    Integer httpStatus,
    String errorDetail,
    Instant createdAt,
    boolean careersUrlFound,
    String careersUrlInitial,
    String careersUrlFinal,
    String careersDiscoveryMethod,
    String careersDiscoveryStageFailure,
    boolean vendorDetected,
    String vendorName,
    boolean endpointExtracted,
    String endpointUrl,
    Integer httpStatusFirstFailure,
    Integer requestCount,
    AtsDiscoveryResult atsDiscoveryResult) {}
