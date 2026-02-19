package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DomainResolutionServiceTest {

    @Mock
    private CrawlJdbcRepository repository;
    @Mock
    private WdqsHttpClient wdqsHttpClient;

    private DomainResolutionService service;

    @BeforeEach
    void setUp() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.getDomainResolution().setWdqsMinDelayMs(0);
        properties.getDomainResolution().setBatchSize(10);
        service = new DomainResolutionService(properties, repository, wdqsHttpClient, new ObjectMapper());
    }

    @Test
    void resolvesWikipediaTitleWithP856() {
        CompanyIdentity company = new CompanyIdentity(1L, "AAPL", "Apple Inc.", "Tech", "Apple_Inc.", null, null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(successFetch(wdqsResponseWithWebsite()));

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.resolvedCount()).isEqualTo(1);
        assertThat(result.noP856Count()).isEqualTo(0);
        verify(repository).upsertCompanyDomain(
            eq(1L),
            eq("apple.com"),
            isNull(),
            eq("WIKIDATA"),
            eq(0.95),
            any(Instant.class),
            eq("enwiki_sitelink"),
            eq("Q312")
        );
    }

    @Test
    void reportsNoP856WhenItemHasNoWebsite() {
        CompanyIdentity company = new CompanyIdentity(2L, "TEST", "Test Co", null, "Test_Company", null, null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(successFetch(wdqsResponseWithoutWebsite()));

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.resolvedCount()).isZero();
        assertThat(result.noP856Count()).isEqualTo(1);
        verify(repository, never()).upsertCompanyDomain(
            anyLong(), anyString(), any(), anyString(), anyDouble(), any(Instant.class), anyString(), anyString()
        );
    }

    @Test
    void reportsNoItemWhenSitelinkMissing() {
        CompanyIdentity company = new CompanyIdentity(3L, "NONE", "Missing Co", null, "Missing_Co", null, null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(successFetch(wdqsEmptyResponse()));

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.resolvedCount()).isZero();
        assertThat(result.noItemCount()).isEqualTo(1);
        verify(repository, never()).upsertCompanyDomain(
            anyLong(), anyString(), any(), anyString(), anyDouble(), any(Instant.class), anyString(), anyString()
        );
    }

    @Test
    void reportsWdqsErrorWhenQueryFails() {
        CompanyIdentity company = new CompanyIdentity(4L, "FAIL", "Failure Inc", null, "Failure_Inc", null, null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(failureFetch());

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.resolvedCount()).isZero();
        assertThat(result.wdqsErrorCount()).isEqualTo(1);
        assertThat(result.wdqsTimeoutCount()).isEqualTo(0);
        verify(repository, never()).upsertCompanyDomain(
            anyLong(), anyString(), any(), anyString(), anyDouble(), any(Instant.class), anyString(), anyString()
        );
    }

    @Test
    void resolvesCikWithP856() {
        CompanyIdentity company = new CompanyIdentity(5L, "CIK", "Cik Corp", null, null, "0000320193", null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(successFetch(wdqsResponseWithCikWebsite()));

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.resolvedCount()).isEqualTo(1);
        verify(repository).upsertCompanyDomain(
            eq(5L),
            eq("apple.com"),
            isNull(),
            eq("WIKIDATA"),
            eq(0.95),
            any(Instant.class),
            eq("cik"),
            eq("Q312")
        );
    }

    @Test
    void reportsWdqsTimeoutWhenQueryTimesOut() {
        CompanyIdentity company = new CompanyIdentity(6L, "TIME", "Timeout Co", null, "Timeout_Co", null, null, null, null, null);
        when(repository.findCompaniesMissingDomain(1)).thenReturn(List.of(company));
        when(wdqsHttpClient.postForm(anyString(), anyString(), anyString())).thenReturn(timeoutFetch());

        DomainResolutionResult result = service.resolveMissingDomains(1);

        assertThat(result.wdqsTimeoutCount()).isEqualTo(1);
        assertThat(result.wdqsErrorCount()).isZero();
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
            null
        );
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
            "server error"
        );
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
            "timeout"
        );
    }

    private String wdqsResponseWithWebsite() {
        return """
            {"results":{"bindings":[
              {"articleTitle":{"value":"Apple Inc."},"item":{"value":"http://www.wikidata.org/entity/Q312"},"officialWebsite":{"value":"https://www.apple.com"}}
            ]}}
            """;
    }

    private String wdqsResponseWithoutWebsite() {
        return """
            {"results":{"bindings":[
              {"articleTitle":{"value":"Test Company"},"item":{"value":"http://www.wikidata.org/entity/Q999"}}
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
