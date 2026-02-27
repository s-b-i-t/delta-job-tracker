package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class CareersLandingLinkExtractorTest {
  private final CareersLandingLinkExtractor extractor = new CareersLandingLinkExtractor();

  @Test
  void ranksAndNormalizesHomepageAnchors() {
    String html =
        """
        <html><body>
          <a href=\"/about\">About</a>
          <a href=\"/careers\">Join our team</a>
          <a href=\"https://jobs.example.com/open-positions\">Open positions</a>
          <a href=\"mailto:jobs@example.com\">Email</a>
        </body></html>
        """;

    List<String> ranked = extractor.extractRanked(html, "https://example.com/", 5);

    assertThat(ranked).contains("https://example.com/careers", "https://jobs.example.com/open-positions");
    assertThat(ranked.getFirst()).isEqualTo("https://jobs.example.com/open-positions");
    assertThat(ranked).allMatch(url -> url.startsWith("http"));
  }

  @Test
  void defaultFallbackCandidatesAreBoundedAndOrdered() {
    List<String> candidates = extractor.defaultFallbackCandidates("example.com").stream().toList();

    assertThat(candidates).startsWith(
        "https://example.com/careers",
        "https://example.com/careers/",
        "https://example.com/jobs");
    assertThat(candidates).contains("https://careers.example.com/", "https://jobs.example.com/");
  }
}
