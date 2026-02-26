package com.delta.jobtracker.crawl.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.http.WdqsHttpClient;
import com.delta.jobtracker.crawl.model.CompanyIdentity;
import com.delta.jobtracker.crawl.model.DomainResolutionMetrics;
import com.delta.jobtracker.crawl.model.DomainResolutionResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DomainResolutionLiveBenchmarkTest {

  @Mock private CrawlJdbcRepository repository;
  @Mock private HostCrawlStateService hostCrawlStateService;

  private ExecutorService httpExecutor;

  @AfterEach
  void tearDown() {
    if (httpExecutor != null) {
      httpExecutor.shutdownNow();
    }
  }

  @Test
  @EnabledIfEnvironmentVariable(
      named = "RUN_LIVE_DOMAIN_RESOLUTION_BENCHMARK",
      matches = "(?i)true|1|yes")
  void benchmarksLiveDomainResolutionOnUntestedCompanies() {
    List<CompanyIdentity> sample =
        List.of(
            company(101L, "MSFT", "Microsoft Corporation", "Microsoft", "0000789019"),
            company(102L, "NVDA", "NVIDIA Corporation", "Nvidia", "0001045810"),
            company(103L, "NFLX", "Netflix, Inc.", "Netflix", "0001065280"),
            company(104L, "ADBE", "Adobe Inc.", "Adobe Inc.", "0000796343"),
            company(105L, "INTU", "Intuit Inc.", "Intuit", "0000896878"),
            company(106L, "NOW", "ServiceNow, Inc.", "ServiceNow", "0001373715"),
            company(107L, "PANW", "Palo Alto Networks, Inc.", "Palo Alto Networks", "0001327567"),
            company(108L, "ORCL", "Oracle Corporation", "Oracle Corporation", "0001341439"),
            company(109L, "COST", "Costco Wholesale Corporation", "Costco", "0000909832"),
            company(110L, "UBER", "Uber Technologies, Inc.", "Uber", "0001543151"));

    when(repository.findCompaniesMissingDomain(sample.size())).thenReturn(sample);
    lenient().doNothing().when(repository).updateCompanyDomainResolutionCache(anyLong(), any(), any(), any(), any());

    Map<Long, String> resolvedDomainsByCompanyId = new LinkedHashMap<>();
    doAnswer(
            invocation -> {
              long companyId = invocation.getArgument(0, Long.class);
              String domain = invocation.getArgument(1, String.class);
              resolvedDomainsByCompanyId.put(companyId, domain);
              return null;
            })
        .when(repository)
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            any(),
            any());

    CrawlerProperties properties = new CrawlerProperties();
    properties.setUserAgent("delta-job-tracker-benchmark/0.1");
    properties.setRequestTimeoutSeconds(6);
    properties.setRequestMaxRetries(0);
    properties.setPerHostDelayMs(200);
    properties.setGlobalConcurrency(4);
    properties.getDomainResolution().setBatchSize(sample.size());
    properties.getDomainResolution().setWdqsMinDelayMs(0);
    properties.getDomainResolution().setWdqsTimeoutSeconds(6);
    properties.getDomainResolution().setCacheTtlMinutes(0);

    httpExecutor = Executors.newFixedThreadPool(8);
    PoliteHttpClient politeHttpClient =
        new PoliteHttpClient(properties, httpExecutor, hostCrawlStateService);
    WdqsHttpClient wdqsHttpClient = new WdqsHttpClient(properties, httpExecutor);
    DomainResolutionService service =
        new DomainResolutionService(
            properties, repository, wdqsHttpClient, politeHttpClient, new ObjectMapper());

    Instant startedAt = Instant.now();
    DomainResolutionResult result = service.resolveMissingDomains(sample.size());
    long wallClockMs = java.time.Duration.between(startedAt, Instant.now()).toMillis();

    DomainResolutionMetrics metrics = result.metrics();
    assertThat(metrics).isNotNull();
    assertThat(metrics.companiesInputCount()).isEqualTo(sample.size());
    verify(repository).findCompaniesMissingDomain(eq(sample.size()));

    System.out.println("=== Domain Resolution Live Benchmark ===");
    System.out.println("sample_size=" + sample.size());
    System.out.println("resolved_count=" + result.resolvedCount());
    System.out.println("resolution_rate=" + percent(result.resolvedCount(), sample.size()));
    System.out.println("no_identifier_count=" + result.noWikipediaTitleCount());
    System.out.println("no_item_count=" + result.noItemCount());
    System.out.println("no_p856_count=" + result.noP856Count());
    System.out.println("wdqs_error_count=" + result.wdqsErrorCount());
    System.out.println("wdqs_timeout_count=" + result.wdqsTimeoutCount());
    System.out.println("wall_clock_ms=" + wallClockMs);
    System.out.println("metrics_total_duration_ms=" + metrics.totalDurationMs());
    System.out.println("metrics_wdqs_duration_ms=" + metrics.wdqsDurationMs());
    System.out.println("metrics_heuristic_duration_ms=" + metrics.heuristicDurationMs());
    System.out.println("heuristic_companies_tried=" + metrics.heuristicCompaniesTriedCount());
    System.out.println("heuristic_candidates_tried=" + metrics.heuristicCandidatesTriedCount());
    System.out.println("heuristic_fetch_success=" + metrics.heuristicFetchSuccessCount());
    System.out.println("heuristic_resolved=" + metrics.heuristicResolvedCount());
    System.out.println("heuristic_rejected=" + metrics.heuristicRejectedCount());
    System.out.println("resolved_by_method=" + metrics.resolvedByMethod());
    System.out.println("heuristic_success_by_reason=" + metrics.heuristicSuccessByReason());
    System.out.println("resolved_domains_by_company_id=" + resolvedDomainsByCompanyId);
    System.out.println("sample_errors=" + result.sampleErrors());
  }

  @Test
  @EnabledIfEnvironmentVariable(
      named = "RUN_LIVE_DOMAIN_RESOLUTION_CSV_BENCHMARK",
      matches = "(?i)true|1|yes")
  void benchmarksLiveDomainResolutionOnSp500UndiscoveredCsvSample() throws Exception {
    int sampleSize = readEnvInt("LIVE_DOMAIN_RESOLUTION_CSV_SAMPLE_SIZE", 150);
    List<CompanyIdentity> sample = loadSp500UndiscoveredCsvSample(sampleSize);
    assertThat(sample.size()).isGreaterThanOrEqualTo(100);

    when(repository.findCompaniesMissingDomain(sample.size())).thenReturn(sample);
    lenient()
        .doNothing()
        .when(repository)
        .updateCompanyDomainResolutionCache(anyLong(), any(), any(), any(), any());

    Map<Long, String> resolvedDomainsByCompanyId = new LinkedHashMap<>();
    doAnswer(
            invocation -> {
              long companyId = invocation.getArgument(0, Long.class);
              String domain = invocation.getArgument(1, String.class);
              resolvedDomainsByCompanyId.put(companyId, domain);
              return null;
            })
        .when(repository)
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            any(),
            any());

    CrawlerProperties properties = new CrawlerProperties();
    properties.setUserAgent("delta-job-tracker-benchmark/0.1");
    properties.setRequestTimeoutSeconds(6);
    properties.setRequestMaxRetries(0);
    properties.setPerHostDelayMs(200);
    properties.setGlobalConcurrency(4);
    properties.getDomainResolution().setBatchSize(Math.min(sample.size(), 50));
    properties.getDomainResolution().setWdqsMinDelayMs(200);
    properties.getDomainResolution().setWdqsTimeoutSeconds(6);
    properties.getDomainResolution().setCacheTtlMinutes(0);

    httpExecutor = Executors.newFixedThreadPool(8);
    PoliteHttpClient politeHttpClient =
        new PoliteHttpClient(properties, httpExecutor, hostCrawlStateService);
    WdqsHttpClient wdqsHttpClient = new WdqsHttpClient(properties, httpExecutor);
    DomainResolutionService service =
        new DomainResolutionService(
            properties, repository, wdqsHttpClient, politeHttpClient, new ObjectMapper());

    Instant startedAt = Instant.now();
    DomainResolutionResult result = service.resolveMissingDomains(sample.size());
    long wallClockMs = java.time.Duration.between(startedAt, Instant.now()).toMillis();

    DomainResolutionMetrics metrics = result.metrics();
    assertThat(metrics).isNotNull();
    assertThat(metrics.companiesInputCount()).isEqualTo(sample.size());
    verify(repository).findCompaniesMissingDomain(eq(sample.size()));

    Map<String, String> resolvedByTicker = new LinkedHashMap<>();
    for (CompanyIdentity company : sample) {
      String domain = resolvedDomainsByCompanyId.get(company.companyId());
      if (domain != null) {
        resolvedByTicker.put(company.ticker(), domain);
      }
    }

    System.out.println("=== Domain Resolution CSV Live Benchmark ===");
    System.out.println("dataset=sp500_minus_domains_csv");
    System.out.println("sample_size=" + sample.size());
    System.out.println("resolved_count=" + result.resolvedCount());
    System.out.println("resolution_rate=" + percent(result.resolvedCount(), sample.size()));
    System.out.println("no_identifier_count=" + result.noWikipediaTitleCount());
    System.out.println("no_item_count=" + result.noItemCount());
    System.out.println("no_p856_count=" + result.noP856Count());
    System.out.println("wdqs_error_count=" + result.wdqsErrorCount());
    System.out.println("wdqs_timeout_count=" + result.wdqsTimeoutCount());
    System.out.println("wall_clock_ms=" + wallClockMs);
    System.out.println("metrics_total_duration_ms=" + metrics.totalDurationMs());
    System.out.println("metrics_wdqs_duration_ms=" + metrics.wdqsDurationMs());
    System.out.println("metrics_heuristic_duration_ms=" + metrics.heuristicDurationMs());
    System.out.println("heuristic_companies_tried=" + metrics.heuristicCompaniesTriedCount());
    System.out.println("heuristic_candidates_tried=" + metrics.heuristicCandidatesTriedCount());
    System.out.println("heuristic_fetch_success=" + metrics.heuristicFetchSuccessCount());
    System.out.println("heuristic_resolved=" + metrics.heuristicResolvedCount());
    System.out.println("heuristic_rejected=" + metrics.heuristicRejectedCount());
    System.out.println("resolved_by_method=" + metrics.resolvedByMethod());
    System.out.println("heuristic_success_by_reason=" + metrics.heuristicSuccessByReason());
    System.out.println(
        "resolved_ticker_domain_sample="
            + resolvedByTicker.entrySet().stream().limit(25).toList());
    System.out.println("sample_errors=" + result.sampleErrors());
  }

  @Test
  @EnabledIfEnvironmentVariable(
      named = "RUN_LIVE_WIKIPEDIA_INFOBOX_DOMAIN_BENCHMARK",
      matches = "(?i)true|1|yes")
  void benchmarksLiveWikipediaInfoboxFallback() {
    List<CompanyIdentity> sample =
        List.of(
            company(20_001L, "MSFTX", "Microsoft Corporation", "Microsoft", null),
            company(20_002L, "NVDAx", "NVIDIA Corporation", "Nvidia", null),
            company(20_003L, "NFLXx", "Netflix, Inc.", "Netflix", null),
            company(20_004L, "ADBEx", "Adobe Inc.", "Adobe_Inc.", null),
            company(20_005L, "INTUx", "Intuit Inc.", "Intuit", null),
            company(20_006L, "NOWx", "ServiceNow, Inc.", "ServiceNow", null),
            company(20_007L, "ORCLx", "Oracle Corporation", "Oracle_Corporation", null),
            company(20_008L, "COSTx", "Costco Wholesale Corporation", "Costco", null),
            company(20_009L, "UBERx", "Uber Technologies, Inc.", "Uber", null),
            company(20_010L, "CRMx", "Salesforce, Inc.", "Salesforce", null),
            company(20_011L, "SHOPx", "Shopify Inc.", "Shopify", null),
            company(20_012L, "SQx", "Block, Inc.", "Block,_Inc.", null),
            company(20_013L, "SPOTx", "Spotify Technology S.A.", "Spotify", null),
            company(20_014L, "AMDx", "Advanced Micro Devices, Inc.", "Advanced_Micro_Devices", null),
            company(20_015L, "AAPLx", "Apple Inc.", "Apple_Inc.", null),
            company(20_016L, "GOOGx", "Alphabet Inc.", "Alphabet_Inc.", null),
            company(20_017L, "PANWx", "Palo Alto Networks, Inc.", "Palo_Alto_Networks", null),
            company(20_018L, "SNOWx", "Snowflake Inc.", "Snowflake_Inc.", null),
            company(20_019L, "DDOGx", "Datadog, Inc.", "Datadog", null),
            company(20_020L, "NETx", "Cloudflare, Inc.", "Cloudflare", null));

    when(repository.findCompaniesMissingDomain(sample.size())).thenReturn(sample);
    lenient()
        .doNothing()
        .when(repository)
        .updateCompanyDomainResolutionCache(anyLong(), any(), any(), any(), any());
    WdqsHttpClient mockedWdqsHttpClient = org.mockito.Mockito.mock(WdqsHttpClient.class);
    when(mockedWdqsHttpClient.postForm(anyString(), anyString(), anyString()))
        .thenReturn(successWdqsFetch("{\"results\":{\"bindings\":[]}}"));

    Map<Long, String> resolvedDomainsByCompanyId = new LinkedHashMap<>();
    doAnswer(
            invocation -> {
              long companyId = invocation.getArgument(0, Long.class);
              String domain = invocation.getArgument(1, String.class);
              resolvedDomainsByCompanyId.put(companyId, domain);
              return null;
            })
        .when(repository)
        .upsertCompanyDomain(
            anyLong(),
            anyString(),
            any(),
            anyString(),
            anyDouble(),
            any(Instant.class),
            any(),
            any());

    CrawlerProperties properties = new CrawlerProperties();
    properties.setUserAgent("delta-job-tracker-benchmark/0.1");
    properties.setRequestTimeoutSeconds(6);
    properties.setRequestMaxRetries(0);
    properties.setPerHostDelayMs(200);
    properties.setGlobalConcurrency(4);
    properties.getDomainResolution().setBatchSize(sample.size());
    properties.getDomainResolution().setWdqsMinDelayMs(0);
    properties.getDomainResolution().setWdqsTimeoutSeconds(6);
    properties.getDomainResolution().setCacheTtlMinutes(0);

    httpExecutor = Executors.newFixedThreadPool(8);
    PoliteHttpClient politeHttpClient =
        new PoliteHttpClient(properties, httpExecutor, hostCrawlStateService);
    DomainResolutionService service =
        new DomainResolutionService(
            properties, repository, mockedWdqsHttpClient, politeHttpClient, new ObjectMapper());

    Instant startedAt = Instant.now();
    DomainResolutionResult result = service.resolveMissingDomains(sample.size());
    long wallClockMs = java.time.Duration.between(startedAt, Instant.now()).toMillis();

    DomainResolutionMetrics metrics = result.metrics();
    assertThat(metrics).isNotNull();
    verify(repository).findCompaniesMissingDomain(eq(sample.size()));

    System.out.println("=== Wikipedia Infobox Domain Benchmark ===");
    System.out.println("sample_size=" + sample.size());
    System.out.println("resolved_count=" + result.resolvedCount());
    System.out.println("resolution_rate=" + percent(result.resolvedCount(), sample.size()));
    System.out.println("wall_clock_ms=" + wallClockMs);
    System.out.println("wdqs_batches_title=" + metrics.wdqsTitleBatchCount());
    System.out.println("infobox_tried=" + metrics.wikipediaInfoboxTriedCount());
    System.out.println("infobox_resolved=" + metrics.wikipediaInfoboxResolvedCount());
    System.out.println("infobox_rejected=" + metrics.wikipediaInfoboxRejectedCount());
    System.out.println("infobox_duration_ms=" + metrics.wikipediaInfoboxDurationMs());
    System.out.println("heuristic_resolved=" + metrics.heuristicResolvedCount());
    System.out.println("resolved_by_method=" + metrics.resolvedByMethod());
    System.out.println("sample_errors=" + result.sampleErrors());
    System.out.println("resolved_domains_by_company_id=" + resolvedDomainsByCompanyId);
  }

  private CompanyIdentity company(long id, String ticker, String name, String wikiTitle, String cik) {
    return new CompanyIdentity(id, ticker, name, null, wikiTitle, cik, null, null, null, null);
  }

  private List<CompanyIdentity> loadSp500UndiscoveredCsvSample(int sampleSize) throws Exception {
    Path sp500Path = Path.of("..", "data", "sp500_constituents.csv");
    Path domainsPath = Path.of("..", "data", "domains.csv");
    LinkedHashSet<String> knownTickers = new LinkedHashSet<>();
    try (Reader reader = Files.newBufferedReader(domainsPath);
        CSVParser parser =
            CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
      for (CSVRecord record : parser) {
        String ticker = record.get("ticker");
        if (ticker != null && !ticker.isBlank()) {
          knownTickers.add(ticker.trim().toUpperCase());
        }
      }
    }

    Map<String, CompanyIdentity> missingByTicker = new LinkedHashMap<>();
    long syntheticId = 10_000L;
    try (Reader reader = Files.newBufferedReader(sp500Path);
        CSVParser parser =
            CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader)) {
      for (CSVRecord record : parser) {
        String ticker = record.get("Symbol");
        String name = record.get("Security");
        String cik = record.get("CIK");
        if (ticker == null || ticker.isBlank() || knownTickers.contains(ticker.trim().toUpperCase())) {
          continue;
        }
        if (cik == null || cik.isBlank()) {
          continue;
        }
        missingByTicker.put(
            ticker.trim().toUpperCase(),
            new CompanyIdentity(
                syntheticId++,
                ticker.trim().toUpperCase(),
                name == null ? ticker.trim() : name.trim(),
                null,
                null,
                cik.trim(),
                null,
                null,
                null,
                null));
      }
    }
    return missingByTicker.values().stream().limit(Math.max(100, sampleSize)).toList();
  }

  private int readEnvInt(String envName, int defaultValue) {
    String raw = System.getenv(envName);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Math.max(1, Integer.parseInt(raw.trim()));
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private String percent(int numerator, int denominator) {
    if (denominator <= 0) {
      return "0.0%";
    }
    double value = (100.0 * numerator) / denominator;
    return String.format("%.1f%%", value);
  }

  private HttpFetchResult successWdqsFetch(String body) {
    return new HttpFetchResult(
        "https://query.wikidata.org",
        null,
        200,
        body,
        body == null ? null : body.getBytes(),
        "application/sparql-results+json",
        null,
        Instant.now(),
        java.time.Duration.ZERO,
        null,
        null);
  }
}
