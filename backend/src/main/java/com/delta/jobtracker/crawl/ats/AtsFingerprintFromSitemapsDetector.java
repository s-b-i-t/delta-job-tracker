package com.delta.jobtracker.crawl.ats;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Extract ATS endpoints from sitemap URL entries or any list of URLs. Returned map preserves the
 * first source URL that produced each endpoint.
 */
@Component
public class AtsFingerprintFromSitemapsDetector {
  private final AtsEndpointExtractor atsEndpointExtractor;

  public AtsFingerprintFromSitemapsDetector(AtsEndpointExtractor atsEndpointExtractor) {
    this.atsEndpointExtractor = atsEndpointExtractor;
  }

  /**
   * @param urls list of URLs from a sitemap (may be null/empty)
   * @return map of detected endpoints keyed by record with originating URL as value
   */
  public Map<AtsDetectionRecord, String> detect(List<String> urls) {
    if (urls == null || urls.isEmpty()) {
      return Map.of();
    }
    Map<String, AtsDetectionRecord> unique = new LinkedHashMap<>();
    Map<AtsDetectionRecord, String> discoveredFrom = new LinkedHashMap<>();

    for (String url : urls) {
      if (url == null || url.isBlank()) {
        continue;
      }
      List<AtsDetectionRecord> records = atsEndpointExtractor.extract(url, null);
      for (AtsDetectionRecord record : records) {
        String key = record.atsType().name() + "|" + record.atsUrl().toLowerCase(Locale.ROOT);
        if (unique.containsKey(key)) {
          continue;
        }
        unique.put(key, record);
        discoveredFrom.put(record, url);
      }
    }
    return discoveredFrom;
  }
}
