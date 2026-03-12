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

    assertThat(ranked)
        .contains("https://example.com/careers", "https://jobs.example.com/open-positions");
    assertThat(ranked.getFirst()).isEqualTo("https://jobs.example.com/open-positions");
    assertThat(ranked).allMatch(url -> url.startsWith("http"));
  }

  @Test
  void followsOnlySameSiteObviousTargets() {
    String html =
        """
        <html><body>
          <a href=\"/current-openings\">Current Openings</a>
          <a href=\"https://external.example.net/jobs\">External Jobs</a>
          <a href=\"/search-jobs\">Search Jobs</a>
        </body></html>
        """;

    List<String> ranked = extractor.extractRanked(html, "https://careers.example.com/", 5);

    assertThat(ranked)
        .containsExactlyInAnyOrder(
            "https://careers.example.com/current-openings",
            "https://careers.example.com/search-jobs");
  }

  @Test
  void followsSiblingCareersSubdomainTargets() {
    String html =
        """
        <html><body>
          <a href=\"https://careers.example.com/search/?q=&locationsearch=\">Search Jobs</a>
          <a href=\"https://examplecareers.net/jobs\">External</a>
        </body></html>
        """;

    List<String> ranked = extractor.extractRanked(html, "https://www.example.com/careers", 5);

    assertThat(ranked)
        .containsExactly("https://careers.example.com/search/?q=&locationsearch=");
  }

  @Test
  void defaultFallbackCandidatesAreBoundedAndOrdered() {
    List<String> candidates = extractor.defaultFallbackCandidates("example.com").stream().toList();

    assertThat(candidates)
        .startsWith(
            "https://example.com/careers",
            "https://example.com/careers/",
            "https://example.com/jobs");
    assertThat(candidates).contains("https://careers.example.com/", "https://jobs.example.com/");
  }
}
