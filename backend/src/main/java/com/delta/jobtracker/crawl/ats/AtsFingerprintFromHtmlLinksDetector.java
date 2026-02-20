package com.delta.jobtracker.crawl.ats;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Extract ATS endpoints by scanning anchor hrefs within HTML documents.
 * The detector keeps results de-duplicated and preserves the source href for each endpoint.
 */
@Component
public class AtsFingerprintFromHtmlLinksDetector {
    private final AtsEndpointExtractor atsEndpointExtractor;

    public AtsFingerprintFromHtmlLinksDetector(AtsEndpointExtractor atsEndpointExtractor) {
        this.atsEndpointExtractor = atsEndpointExtractor;
    }

    /**
     * @param html    page HTML (nullable)
     * @param baseUrl base URL used to resolve relative hrefs
     * @return map of detected endpoints keyed by record with the originating href as value
     */
    public Map<AtsDetectionRecord, String> detect(String html, String baseUrl) {
        if (html == null || html.isBlank()) {
            return Map.of();
        }
        Document doc = Jsoup.parse(html, baseUrl);
        Map<String, AtsDetectionRecord> unique = new LinkedHashMap<>();
        Map<AtsDetectionRecord, String> discoveredFrom = new LinkedHashMap<>();

        for (Element anchor : doc.select("a[href]")) {
            String href = anchor.attr("abs:href");
            if (href == null || href.isBlank()) {
                continue;
            }
            String normalizedHref = href.trim();
            List<AtsDetectionRecord> records = atsEndpointExtractor.extract(normalizedHref, null);
            for (AtsDetectionRecord record : records) {
                String key = record.atsType().name() + "|" + record.atsUrl().toLowerCase(Locale.ROOT);
                if (unique.containsKey(key)) {
                    continue;
                }
                unique.put(key, record);
                discoveredFrom.put(record, normalizedHref);
            }
        }
        return discoveredFrom;
    }
}
