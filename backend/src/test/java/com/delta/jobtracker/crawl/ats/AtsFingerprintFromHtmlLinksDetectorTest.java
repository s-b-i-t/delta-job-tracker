package com.delta.jobtracker.crawl.ats;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AtsFingerprintFromHtmlLinksDetectorTest {
  private final AtsFingerprintFromHtmlLinksDetector detector =
      new AtsFingerprintFromHtmlLinksDetector(new AtsEndpointExtractor());

  @Test
  void detectsAtsLinksAndPreservesSourceHref() {
    String html =
        """
            <a href="https://boards.greenhouse.io/acme">Greenhouse</a>
            <a href="//jobs.lever.co/rocket">Lever</a>
            """;

    Map<AtsDetectionRecord, String> endpoints = detector.detect(html, "https://acme.com/");

    assertThat(endpoints)
        .containsEntry(
            new AtsDetectionRecord(AtsType.GREENHOUSE, "https://boards.greenhouse.io/acme"),
            "https://boards.greenhouse.io/acme")
        .containsEntry(
            new AtsDetectionRecord(AtsType.LEVER, "https://jobs.lever.co/rocket"),
            "https://jobs.lever.co/rocket");
  }
}
