package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DomainResolutionServiceTest {

  @Mock private CrawlJdbcRepository repository;
  @Mock private WdqsHttpClient wdqsHttpClient;
  @Mock private PoliteHttpClient politeHttpClient;

  private DomainResolutionService service;

  @BeforeEach
  void setUp() {
    CrawlerProperties properties = new CrawlerProperties();
    properties.getData().setDomainsCsv("");
    properties.getDomainResolution().setWdqsMinDelayMs(0);
    properties.getDomainResolution().setBatchSize(10);
    service =
        new DomainResolutionService(
            properties, repository, wdqsHttpClient, politeHttpClient, new ObjectMapper());
  }

  @Test
  void resolvesWikipediaTitleWithP856() {
    CompanyIdentity company =
        new CompanyIdentity(
            1L, "AAPL", "Apple Inc.", "Tech", "Apple_Inc.", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsResponseWithWebsite()));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isEqualTo(1);
    assertThat(result.noP856Count()).isEqualTo(0);
    verify(repository)
        .upsertCompanyDomain(
            eq(1L),
            eq("apple.com"),
            isNull(),
            eq("WIKIDATA"),
            eq(0.95),
            any(Instant.class),
            eq("enwiki_sitelink"),
            eq("Q312"));
  }

  @Test
  void reportsNoP856WhenItemHasNoWebsite() {
    CompanyIdentity company =
        new CompanyIdentity(
            2L, "TEST", "Test Co", null, "Test_Company", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsResponseWithoutWebsite()));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isZero();
    assertThat(result.noP856Count()).isEqualTo(1);
    verify(repository, never())
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            anyString(),
            anyString());
  }

  @Test
  void reportsNoItemWhenSitelinkMissing() {
    CompanyIdentity company =
        new CompanyIdentity(
            3L, "NONE", "Missing Co", null, "Missing_Co", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsEmptyResponse()));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isZero();
    assertThat(result.noItemCount()).isEqualTo(1);
    verify(repository, never())
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            anyString(),
            anyString());
  }

  @Test
  void reportsWdqsErrorWhenQueryFails() {
    CompanyIdentity company =
        new CompanyIdentity(
            4L, "FAIL", "Failure Inc", null, "Failure_Inc", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(failureFetch());

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isZero();
    assertThat(result.wdqsErrorCount()).isEqualTo(1);
    assertThat(result.wdqsTimeoutCount()).isEqualTo(0);
    verify(repository, never())
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            anyString(),
            anyString());
  }

  @Test
  void resolvesCikWithP856() {
    CompanyIdentity company =
        new CompanyIdentity(
            5L, "CIK", "Cik Corp", null, null, "0000320193", null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsResponseWithCikWebsite()));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isEqualTo(1);
    verify(repository)
        .upsertCompanyDomain(
            eq(5L),
            eq("apple.com"),
            isNull(),
            eq("WIKIDATA"),
            eq(0.95),
            any(Instant.class),
            eq("cik"),
            eq("Q312"));
  }

  @Test
  void reportsWdqsTimeoutWhenQueryTimesOut() {
    CompanyIdentity company =
        new CompanyIdentity(
            6L, "TIME", "Timeout Co", null, "Timeout_Co", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(timeoutFetch());

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.wdqsTimeoutCount()).isEqualTo(1);
    assertThat(result.wdqsErrorCount()).isZero();
  }

  @Test
  void fallsBackToVerifiedHeuristicDomainWhenWdqsHasNoItem() {
    CompanyIdentity company =
        new CompanyIdentity(
            7L, "NET", "Netlify, Inc.", null, "Netlify_Inc.", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsEmptyResponse()));
    when(politeHttpClient.get(anyString(), anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              String url = invocation.getArgument(0, String.class);
              if ("https://netlify.com/".equals(url)) {
                return successHtml(
                    url,
                    "<html><head><title>Netlify: Build modern web apps</title></head>"
                        + "<body>Welcome to Netlify platform</body></html>");
              }
              return failedHtml(url, 404, "http_404");
            });

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isEqualTo(1);
    assertThat(result.noItemCount()).isZero();
    assertThat(result.metrics()).isNotNull();
    assertThat(result.metrics().heuristicResolvedCount()).isEqualTo(1);
    assertThat(result.metrics().resolvedByMethod()).containsEntry("HEURISTIC", 1);
    assertThat(result.metrics().heuristicSuccessByReason()).containsEntry("no_item", 1);
    verify(repository)
        .upsertCompanyDomain(
            eq(7L),
            eq("netlify.com"),
            isNull(),
            eq("HEURISTIC"),
            eq(0.7),
            any(Instant.class),
            eq("HEURISTIC_NAME_COM"),
            isNull());
  }

  @Test
  void heuristicFallbackRejectsParkedPageAndKeepsNoItemFailure() {
    CompanyIdentity company =
        new CompanyIdentity(
            8L, "RKT", "Rocket Lab USA, Inc.", null, "No_Match", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successFetch(wdqsEmptyResponse()));
    when(politeHttpClient.get(anyString(), anyString(), anyInt()))
        .thenReturn(
            successHtml(
                "https://rocketlabusa.com/",
                "<html><head><title>Buy this domain</title></head>"
                    + "<body>This domain may be for sale on Dan.com</body></html>"));

    DomainResolutionResult result = service.resolveMissingDomains(1);

    assertThat(result.resolvedCount()).isZero();
    assertThat(result.noItemCount()).isEqualTo(1);
    assertThat(result.metrics()).isNotNull();
    assertThat(result.metrics().heuristicCompaniesTriedCount()).isEqualTo(1);
    assertThat(result.metrics().heuristicRejectedCount()).isGreaterThanOrEqualTo(1);
    verify(repository, never())
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            eq("HEURISTIC"),
            anyDouble(),
            any(Instant.class),
            any(),
            any());
  }

  @Test
  void metricsIncludeTimingAndWdqsBatchCounts() {
    CompanyIdentity wdqsResolved =
        new CompanyIdentity(
            9L, "CRM", "Salesforce, Inc.", null, "Salesforce", null, null, null, null, null);
    CompanyIdentity heuristicResolved =
        new CompanyIdentity(
            10L, "DDOG", "Datadog, Inc.", null, "NoMatch", null, null, null, null, null);
    when(repository.findCompaniesMissingDomain(2)).thenReturn(List.of(wdqsResolved, heuristicResolved));
    when(wdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(
            successFetch(
                """
                {"results":{"bindings":[
                  {"candidateTitle":{"value":"Salesforce"},"articleTitle":{"value":"Salesforce"},"item":{"value":"http://www.wikidata.org/entity/Q123"},"officialWebsite":{"value":"https://www.salesforce.com"}}
                ]}}
                """));
    when(politeHttpClient.get(anyString(), anyString(), anyInt()))
        .thenAnswer(
            invocation -> {
              String url = invocation.getArgument(0, String.class);
              if ("https://datadog.com/".equals(url)) {
                return successHtml(
                    url,
                    "<html><head><title>Datadog Monitoring and Security Platform</title></head>"
                        + "<body>Datadog observability platform</body></html>");
              }
              return failedHtml(url, 404, "http_404");
            });

    DomainResolutionResult result = service.resolveMissingDomains(2);

    assertThat(result.resolvedCount()).isEqualTo(2);
    assertThat(result.metrics()).isNotNull();
    assertThat(result.metrics().wdqsTitleBatchCount()).isEqualTo(1);
    assertThat(result.metrics().totalDurationMs()).isGreaterThanOrEqualTo(0);
    assertThat(result.metrics().wdqsDurationMs()).isGreaterThanOrEqualTo(0);
    assertThat(result.metrics().heuristicDurationMs()).isGreaterThanOrEqualTo(0);
    assertThat(result.metrics().resolvedByMethod()).containsEntry("WIKIDATA", 1);
    assertThat(result.metrics().resolvedByMethod()).containsEntry("HEURISTIC", 1);
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

  private HttpFetchResult failureFetch() {
    return new HttpFetchResult(
        "https://query.wikidata.org",
        null,
        500,
        null,
        null,
        null,
        null,
        Instant.now(),
        Duration.ZERO,
        "http_error",
        "server error");
  }

  private HttpFetchResult timeoutFetch() {
    return new HttpFetchResult(
        "https://query.wikidata.org",
        null,
        0,
        null,
        null,
        null,
        null,
        Instant.now(),
        Duration.ZERO,
        "timeout",
        "timeout");
  }

  private HttpFetchResult successHtml(String url, String body) {
    return new HttpFetchResult(
        url,
        URI.create(url),
        200,
        body,
        body == null ? null : body.getBytes(),
        "text/html",
        null,
        Instant.now(),
        Duration.ofMillis(5),
        null,
        null);
  }

  private HttpFetchResult failedHtml(String url, int status, String errorCode) {
    return new HttpFetchResult(
        url,
        URI.create(url),
        status,
        null,
        null,
        "text/html",
        null,
        Instant.now(),
        Duration.ofMillis(5),
        errorCode,
        errorCode);
  }

  private String wdqsResponseWithWebsite() {
    return """
            {"results":{"bindings":[
              {"candidateTitle":{"value":"Apple Inc."},"articleTitle":{"value":"Apple Inc."},"item":{"value":"http://www.wikidata.org/entity/Q312"},"officialWebsite":{"value":"https://www.apple.com"}}
            ]}}
            """;
  }

  private String wdqsResponseWithoutWebsite() {
    return """
            {"results":{"bindings":[
              {"candidateTitle":{"value":"Test Company"},"articleTitle":{"value":"Test Company"},"item":{"value":"http://www.wikidata.org/entity/Q999"}}
            ]}}
            """;
  }

  private String wdqsEmptyResponse() {
    return """
            {"results":{"bindings":[]}}
            """;
  }

  private String wdqsResponseWithCikWebsite() {
    return """
            {"results":{"bindings":[
              {"candidateCik":{"value":"0000320193"},"item":{"value":"http://www.wikidata.org/entity/Q312"},"officialWebsite":{"value":"https://www.apple.com"}}
            ]}}
            """;
  }
}
