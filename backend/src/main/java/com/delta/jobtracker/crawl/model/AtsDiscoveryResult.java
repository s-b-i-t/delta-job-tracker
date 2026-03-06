package com.delta.jobtracker.crawl.model;

import java.util.Locale;

public record AtsDiscoveryResult(
    String vendor,
    String endpointUrl,
    String validationStatus,
    String evidence,
    String errorBucket) {
  public static final String VALIDATED = "VALIDATED";
  public static final String UNVALIDATED = "UNVALIDATED";
  public static final String NOT_FOUND = "NOT_FOUND";
  public static final String FAILED = "FAILED";

  public static AtsDiscoveryResult validated(AtsType type, String endpointUrl, String evidence) {
    return new AtsDiscoveryResult(
        vendor(type), endpointUrl, VALIDATED, normalizeEvidence(evidence), null);
  }

  public static AtsDiscoveryResult unvalidated(AtsType type, String evidence, String errorBucket) {
    return new AtsDiscoveryResult(
        vendor(type), null, UNVALIDATED, normalizeEvidence(evidence), normalizeError(errorBucket));
  }

  public static AtsDiscoveryResult notFound(String evidence, String errorBucket) {
    return new AtsDiscoveryResult(
        null, null, NOT_FOUND, normalizeEvidence(evidence), normalizeError(errorBucket));
  }

  public static AtsDiscoveryResult failed(String evidence, String errorBucket) {
    return new AtsDiscoveryResult(
        null, null, FAILED, normalizeEvidence(evidence), normalizeError(errorBucket));
  }

  public static AtsDiscoveryResult fromPersistence(
      String vendorName,
      String endpointUrl,
      boolean endpointExtracted,
      String evidence,
      String errorBucket) {
    String vendor = normalizeVendor(vendorName);
    if (endpointExtracted && endpointUrl != null && !endpointUrl.isBlank()) {
      return new AtsDiscoveryResult(
          vendor, endpointUrl, VALIDATED, normalizeEvidence(evidence), null);
    }
    if (vendor != null) {
      return new AtsDiscoveryResult(
          vendor, null, UNVALIDATED, normalizeEvidence(evidence), normalizeError(errorBucket));
    }
    if (errorBucket != null && !errorBucket.isBlank()) {
      return failed(evidence, errorBucket);
    }
    return notFound(evidence, null);
  }

  private static String vendor(AtsType type) {
    if (type == null || type == AtsType.UNKNOWN) {
      return null;
    }
    return type.name();
  }

  private static String normalizeVendor(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim().toUpperCase(Locale.ROOT);
  }

  private static String normalizeEvidence(String value) {
    if (value == null || value.isBlank()) {
      return "none";
    }
    return value.trim().toLowerCase(Locale.ROOT);
  }

  private static String normalizeError(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim().toUpperCase(Locale.ROOT);
  }
}
