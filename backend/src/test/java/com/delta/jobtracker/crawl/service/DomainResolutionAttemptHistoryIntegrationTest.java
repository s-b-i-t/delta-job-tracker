package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class DomainResolutionAttemptHistoryIntegrationTest {

  @Autowired private CrawlJdbcRepository repository;
  @Autowired private JdbcTemplate jdbcTemplate;
  @Autowired private CrawlerProperties properties;
  @Autowired private ObjectMapper objectMapper;

  private DomainResolutionService service;
  private WdqsHttpClient wdqsHttpClient;
  private PoliteHttpClient politeHttpClient;

  @BeforeEach
  void setUp() {
    wdqsHttpClient = mock(WdqsHttpClient.class);
    politeHttpClient = mock(PoliteHttpClient.class);
    properties.getData().setDomainsCsv("");
    properties.getDomainResolution().setWdqsMinDelayMs(0);
    properties.getDomainResolution().setBatchSize(10);
    service =
        new DomainResolutionService(
            properties, repository, wdqsHttpClient, politeHttpClient, objectMapper);
  }

  @Test
  void resolvedCompanyCreatesExactlyOneHistoryRow() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String ticker = "DR" + suffix;
    long companyId = repository.upsertCompany(ticker, "Domain Resolved " + suffix, "Tech");
    jdbcTemplate.update(
        "UPDATE companies SET wikipedia_title = ? WHERE id = ?",
        "Domain_Resolved_" + suffix,
        companyId);

    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(
            successFetch(
                wdqsResponseWithWebsite(
                    "Domain Resolved " + suffix,
                    "Domain Resolved " + suffix,
                    "Q" + suffix,
                    "https://www.resolved-" + suffix.toLowerCase() + ".com")));

    DomainResolutionResult result = service.resolveMissingDomainsForTickers(List.of(ticker), 1);

    assertThat(result.resolvedCount()).isEqualTo(1);
    assertThat(historyCount(companyId)).isEqualTo(1);
    assertThat(domainCount(companyId)).isEqualTo(1);

    Map<String, Object> row = latestAttempt(companyId);
    assertThat(row.get("selection_mode")).isEqualTo("ticker_targeted");
    assertThat(row.get("final_method")).isEqualTo("WIKIPEDIA_TITLE");
    assertThat(row.get("final_status")).isEqualTo("RESOLVED");
    assertThat(row.get("error_category")).isNull();
    assertThat(row.get("resolved_domain")).isEqualTo("resolved-" + suffix.toLowerCase() + ".com");
    assertThat(row.get("resolved_source")).isEqualTo("WIKIDATA");
    assertThat(row.get("resolved_method")).isEqualTo("enwiki_sitelink");
  }

  @Test
  void unresolvedNoItemCreatesExactlyOneHistoryRowAndPreservesFallbackTrace() throws Exception {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String ticker = "NI" + suffix;
    long companyId = repository.upsertCompany(ticker, "No Item Example " + suffix, "Tech");
    jdbcTemplate.update(
        "UPDATE companies SET wikipedia_title = ?, cik = ? WHERE id = ?",
        "No_Item_Example_" + suffix,
        "0000" + suffix.replaceAll("[^0-9]", "7"),
        companyId);

    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsEmptyResponse()));
    when(politeHttpClient.get(anyString(), anyString(), anyInt()))
        .thenAnswer(
            invocation -> failedHtml(invocation.getArgument(0, String.class), 404, "http_404"));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.noItemCount()).isEqualTo(1);
    assertThat(historyCount(companyId)).isEqualTo(1);
    assertThat(domainCount(companyId)).isZero();

    Map<String, Object> row = latestAttempt(companyId);
    assertThat(row.get("selection_mode")).isEqualTo("eligible_batch");
    assertThat(row.get("final_method")).isEqualTo("WIKIPEDIA_TITLE");
    assertThat(row.get("final_status")).isEqualTo("NO_ITEM");
    assertThat(row.get("error_category")).isEqualTo("no_item");

    JsonNode trace = objectMapper.readTree((String) row.get("step_trace_json"));
    List<String> stages = new java.util.ArrayList<>();
    trace.forEach(node -> stages.add(node.path("stage").asText()));
    assertThat(stages)
        .containsSubsequence(
            "selection", "wdqs_lookup", "cik_lookup", "wikipedia_infobox", "heuristic");
    assertThat(trace.get(1).path("outcome").asText()).isEqualTo("NO_ITEM");
    assertThat(trace.get(2).path("outcome").asText()).isEqualTo("NO_ITEM");
  }

  @Test
  void
      retrySuppressedRerunCreatesNoNewRowUntilEligibilityReturnsAndTargetedRerunGetsSeparateMode() {
    String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    String ticker = "RH" + suffix;
    long companyId = repository.upsertCompany(ticker, "Retry History " + suffix, "Tech");
    jdbcTemplate.update(
        "UPDATE companies SET wikipedia_title = ? WHERE id = ?",
        "Retry_History_" + suffix,
        companyId);

    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsEmptyResponse()));
    when(politeHttpClient.get(anyString(), anyString(), anyInt()))
        .thenAnswer(
            invocation -> failedHtml(invocation.getArgument(0, String.class), 404, "http_404"));

    DomainResolutionResult first = service.resolveMissingDomains(1);
    assertThat(first.noItemCount()).isEqualTo(1);
    assertThat(historyCount(companyId)).isEqualTo(1);

    DomainResolutionResult second = service.resolveMissingDomains(1);
    assertThat(second.metrics().companiesInputCount()).isZero();
    assertThat(historyCount(companyId)).isEqualTo(1);

    ageAttemptedAt(companyId);
    DomainResolutionResult third = service.resolveMissingDomains(1);
    assertThat(third.noItemCount()).isEqualTo(1);
    assertThat(historyCount(companyId)).isEqualTo(2);
    assertThat(selectionModes(companyId)).containsExactly("eligible_batch", "eligible_batch");

    ageAttemptedAt(companyId);
    DomainResolutionResult fourth = service.resolveMissingDomainsForTickers(List.of(ticker), 1);
    assertThat(fourth.noItemCount()).isEqualTo(1);
    assertThat(historyCount(companyId)).isEqualTo(3);
    assertThat(selectionModes(companyId))
        .containsExactly("eligible_batch", "eligible_batch", "ticker_targeted");
  }

  private void ageAttemptedAt(long companyId) {
    int ttlHours = Math.max(1, properties.getDomainResolution().getCacheTtlMinutes() / 60 + 2);
    int noItemHours = Math.max(1, properties.getDomainResolution().getNoItemRetryHours() + 2);
    Instant agedAt = Instant.now().minus(Duration.ofHours(Math.max(ttlHours, noItemHours)));
    jdbcTemplate.update(
        "UPDATE companies SET domain_resolution_attempted_at = ? WHERE id = ?",
        java.sql.Timestamp.from(agedAt),
        companyId);
  }

  private int historyCount(long companyId) {
    Integer count =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM domain_resolution_attempts WHERE company_id = ?",
            Integer.class,
            companyId);
    return count == null ? 0 : count;
  }

  private int domainCount(long companyId) {
    Integer count =
        jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM company_domains WHERE company_id = ?", Integer.class, companyId);
    return count == null ? 0 : count;
  }

  private Map<String, Object> latestAttempt(long companyId) {
    return jdbcTemplate.queryForMap(
        """
        SELECT selection_mode,
               final_method,
               final_status,
               error_category,
               resolved_domain,
               resolved_source,
               resolved_method,
               step_trace_json
        FROM domain_resolution_attempts
        WHERE company_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        companyId);
  }

  private List<String> selectionModes(long companyId) {
    return jdbcTemplate.queryForList(
        """
        SELECT selection_mode
        FROM domain_resolution_attempts
        WHERE company_id = ?
        ORDER BY id
        """,
        String.class,
        companyId);
  }

  private HttpFetchResult successFetch(String body) {
    return new HttpFetchResult(
        "https://query.wikidata.org",
        null,
        200,
        body,
        body == null ? null : body.getBytes(),
        "application/sparql-results+json",
        null,
        Instant.now(),
        Duration.ZERO,
        null,
        null);
  }

  private HttpFetchResult failedHtml(String url, int statusCode, String errorCode) {
    return new HttpFetchResult(
        url,
        URI.create(url),
        statusCode,
        null,
        null,
        "text/html",
        null,
        Instant.now(),
        Duration.ofMillis(5),
        errorCode,
        errorCode);
  }

  private String wdqsEmptyResponse() {
    return "{\"results\":{\"bindings\":[]}}";
  }

  private String wdqsResponseWithWebsite(
      String candidateTitle, String articleTitle, String qid, String website) {
    return """
        {"results":{"bindings":[
          {"candidateTitle":{"value":"%s"},"articleTitle":{"value":"%s"},"item":{"value":"http://www.wikidata.org/entity/%s"},"officialWebsite":{"value":"%s"}}
        ]}}
        """
        .formatted(candidateTitle, articleTitle, qid, website);
  }
}
