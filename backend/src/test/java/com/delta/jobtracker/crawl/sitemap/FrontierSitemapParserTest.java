package com.delta.jobtracker.crawl.sitemap;

import static org.assertj.core.api.Assertions.assertThat;

import com.delta.jobtracker.crawl.model.FrontierSitemapParseResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class FrontierSitemapParserTest {

  private final FrontierSitemapParser parser = new FrontierSitemapParser();

  @Test
  void parseExtractsChildSitemapsAndUniqueUrls() {
    String xml =
        """
        <sitemapindex>
          <sitemap><loc>https://example.com/sitemap-jobs.xml</loc></sitemap>
          <sitemap><loc>https://example.com/sitemap-company.xml</loc></sitemap>
        </sitemapindex>
        <urlset>
          <url><loc>https://example.com/careers</loc></url>
          <url><loc>https://example.com/careers</loc></url>
          <url><loc>https://example.com/jobs/engineering</loc></url>
        </urlset>
        """;

    FrontierSitemapParseResult result = parser.parse(xml, 100);

    assertThat(result.childSitemaps())
        .containsExactlyInAnyOrder(
            "https://example.com/sitemap-jobs.xml", "https://example.com/sitemap-company.xml");
    assertThat(result.urls())
        .containsExactly("https://example.com/careers", "https://example.com/jobs/engineering");
  }

  @Test
  void extractXmlPayloadDecodesGzipSitemapFixture() throws Exception {
    byte[] gzipped =
        Files.readAllBytes(Path.of("src/test/resources/fixtures/sitemap-urlset.xml.gz"));
    HttpFetchResult fetchResult =
        new HttpFetchResult(
            "https://example.com/sitemap.xml.gz",
            new URI("https://example.com/sitemap.xml.gz"),
            200,
            null,
            gzipped,
            "application/xml",
            null,
            Instant.now(),
            Duration.ofMillis(12),
            null,
            null);

    String xml = parser.extractXmlPayload("https://example.com/sitemap.xml.gz", fetchResult);

    assertThat(xml).contains("<urlset");
    assertThat(xml).contains("<loc>");
  }
}
