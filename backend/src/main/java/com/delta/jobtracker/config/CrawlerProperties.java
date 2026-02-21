package com.delta.jobtracker.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "crawler")
public class CrawlerProperties {
  private static final String DEFAULT_USER_AGENT = "delta-job-tracker/0.1 (+contact)";

  private String userAgent;
  private int perHostDelayMs = 1000;
  private int perHostConcurrency = 2;
  private int globalConcurrency = 5;
  private int requestTimeoutSeconds = 60;
  private int requestMaxRetries = 2;
  private int requestRetryBaseDelayMs = 250;
  private int requestRetryMaxDelayMs = 2000;
  private int maxCompanySeconds = 300;
  private int crawlHeartbeatSeconds = 30;
  private int jobPostingBatchSize = 500;
  private int activeRunMinutes = 10;
  private int staleRunMinutes = 10;
  private Api api = new Api();
  private Automation automation = new Automation();
  private Daemon daemon = new Daemon();
  private DomainResolution domainResolution = new DomainResolution();
  private CareersDiscovery careersDiscovery = new CareersDiscovery();
  private Robots robots = new Robots();
  private Sitemap sitemap = new Sitemap();
  private Extraction extraction = new Extraction();
  private Data data = new Data();
  private Cli cli = new Cli();
  private Ats ats = new Ats();
  private Run run = new Run();
  private Canary canary = new Canary();

  public String getUserAgent() {
    return normalizeUserAgent(userAgent);
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = normalizeUserAgent(userAgent);
  }

  public int getPerHostDelayMs() {
    return Math.max(1, perHostDelayMs);
  }

  public void setPerHostDelayMs(int perHostDelayMs) {
    this.perHostDelayMs = Math.max(1, perHostDelayMs);
  }

  public int getPerHostConcurrency() {
    return Math.max(1, perHostConcurrency);
  }

  public void setPerHostConcurrency(int perHostConcurrency) {
    this.perHostConcurrency = Math.max(1, perHostConcurrency);
  }

  public int getGlobalConcurrency() {
    return Math.max(1, globalConcurrency);
  }

  public void setGlobalConcurrency(int globalConcurrency) {
    this.globalConcurrency = Math.max(1, globalConcurrency);
  }

  public int getRequestTimeoutSeconds() {
    return Math.max(1, requestTimeoutSeconds);
  }

  public void setRequestTimeoutSeconds(int requestTimeoutSeconds) {
    this.requestTimeoutSeconds = requestTimeoutSeconds;
  }

  public int getRequestMaxRetries() {
    return Math.max(0, requestMaxRetries);
  }

  public void setRequestMaxRetries(int requestMaxRetries) {
    this.requestMaxRetries = requestMaxRetries;
  }

  public int getRequestRetryBaseDelayMs() {
    return Math.max(0, requestRetryBaseDelayMs);
  }

  public void setRequestRetryBaseDelayMs(int requestRetryBaseDelayMs) {
    this.requestRetryBaseDelayMs = requestRetryBaseDelayMs;
  }

  public int getRequestRetryMaxDelayMs() {
    return Math.max(0, requestRetryMaxDelayMs);
  }

  public void setRequestRetryMaxDelayMs(int requestRetryMaxDelayMs) {
    this.requestRetryMaxDelayMs = requestRetryMaxDelayMs;
  }

  public int getMaxCompanySeconds() {
    return Math.max(1, maxCompanySeconds);
  }

  public void setMaxCompanySeconds(int maxCompanySeconds) {
    this.maxCompanySeconds = maxCompanySeconds;
  }

  public int getCrawlHeartbeatSeconds() {
    return Math.max(5, crawlHeartbeatSeconds);
  }

  public void setCrawlHeartbeatSeconds(int crawlHeartbeatSeconds) {
    this.crawlHeartbeatSeconds = Math.max(5, crawlHeartbeatSeconds);
  }

  public int getJobPostingBatchSize() {
    return Math.max(50, jobPostingBatchSize);
  }

  public void setJobPostingBatchSize(int jobPostingBatchSize) {
    this.jobPostingBatchSize = Math.max(50, jobPostingBatchSize);
  }

  public int getActiveRunMinutes() {
    return Math.max(1, activeRunMinutes);
  }

  public void setActiveRunMinutes(int activeRunMinutes) {
    this.activeRunMinutes = Math.max(1, activeRunMinutes);
  }

  public int getStaleRunMinutes() {
    return Math.max(1, staleRunMinutes);
  }

  public void setStaleRunMinutes(int staleRunMinutes) {
    this.staleRunMinutes = Math.max(1, staleRunMinutes);
  }

  public Automation getAutomation() {
    return automation;
  }

  public void setAutomation(Automation automation) {
    this.automation = automation;
  }

  public Daemon getDaemon() {
    return daemon;
  }

  public void setDaemon(Daemon daemon) {
    this.daemon = daemon;
  }

  public Api getApi() {
    return api;
  }

  public void setApi(Api api) {
    this.api = api;
  }

  public DomainResolution getDomainResolution() {
    return domainResolution;
  }

  public void setDomainResolution(DomainResolution domainResolution) {
    this.domainResolution = domainResolution;
  }

  public CareersDiscovery getCareersDiscovery() {
    return careersDiscovery;
  }

  public void setCareersDiscovery(CareersDiscovery careersDiscovery) {
    this.careersDiscovery = careersDiscovery;
  }

  public Sitemap getSitemap() {
    return sitemap;
  }

  public void setSitemap(Sitemap sitemap) {
    this.sitemap = sitemap;
  }

  public Robots getRobots() {
    return robots;
  }

  public void setRobots(Robots robots) {
    this.robots = robots;
  }

  public Extraction getExtraction() {
    return extraction;
  }

  public void setExtraction(Extraction extraction) {
    this.extraction = extraction;
  }

  public Data getData() {
    return data;
  }

  public void setData(Data data) {
    this.data = data;
  }

  public Cli getCli() {
    return cli;
  }

  public void setCli(Cli cli) {
    this.cli = cli;
  }

  public Ats getAts() {
    return ats;
  }

  public void setAts(Ats ats) {
    this.ats = ats;
  }

  public Run getRun() {
    return run;
  }

  public void setRun(Run run) {
    this.run = run;
  }

  public Canary getCanary() {
    return canary;
  }

  public void setCanary(Canary canary) {
    this.canary = canary;
  }

  public static String normalizeUserAgent(String candidate) {
    if (candidate == null || candidate.isBlank()) {
      return DEFAULT_USER_AGENT;
    }
    return candidate.trim();
  }

  public static class Automation {
    private boolean resolveMissingDomains = true;
    private boolean discoverCareersEndpoints = true;
    private int resolveLimit = 200;
    private int discoverLimit = 200;

    public boolean isResolveMissingDomains() {
      return resolveMissingDomains;
    }

    public void setResolveMissingDomains(boolean resolveMissingDomains) {
      this.resolveMissingDomains = resolveMissingDomains;
    }

    public boolean isDiscoverCareersEndpoints() {
      return discoverCareersEndpoints;
    }

    public void setDiscoverCareersEndpoints(boolean discoverCareersEndpoints) {
      this.discoverCareersEndpoints = discoverCareersEndpoints;
    }

    public int getResolveLimit() {
      return Math.max(1, resolveLimit);
    }

    public void setResolveLimit(int resolveLimit) {
      this.resolveLimit = Math.max(1, resolveLimit);
    }

    public int getDiscoverLimit() {
      return Math.max(1, discoverLimit);
    }

    public void setDiscoverLimit(int discoverLimit) {
      this.discoverLimit = Math.max(1, discoverLimit);
    }
  }

  public static class Api {
    private int defaultCompanyLimit = 50;

    public int getDefaultCompanyLimit() {
      return Math.max(1, defaultCompanyLimit);
    }

    public void setDefaultCompanyLimit(int defaultCompanyLimit) {
      this.defaultCompanyLimit = Math.max(1, defaultCompanyLimit);
    }
  }

  public static class Daemon {
    private static final List<Integer> DEFAULT_FAILURE_BACKOFF = List.of(5, 15, 60, 360, 1440);

    private boolean enabled;
    private int workerCount = 8;
    private int pollIntervalMs = 1000;
    private int lockTtlSeconds = 600;
    private int successIntervalMinutes = 60;
    private List<Integer> failureBackoffMinutes = new ArrayList<>(DEFAULT_FAILURE_BACKOFF);

    public boolean isEnabled() {
      return enabled;
    }

    public void setEnabled(boolean enabled) {
      this.enabled = enabled;
    }

    public int getWorkerCount() {
      return Math.max(1, workerCount);
    }

    public void setWorkerCount(int workerCount) {
      this.workerCount = Math.max(1, workerCount);
    }

    public int getPollIntervalMs() {
      return Math.max(100, pollIntervalMs);
    }

    public void setPollIntervalMs(int pollIntervalMs) {
      this.pollIntervalMs = Math.max(100, pollIntervalMs);
    }

    public int getLockTtlSeconds() {
      return Math.max(30, lockTtlSeconds);
    }

    public void setLockTtlSeconds(int lockTtlSeconds) {
      this.lockTtlSeconds = Math.max(30, lockTtlSeconds);
    }

    public int getSuccessIntervalMinutes() {
      return Math.max(1, successIntervalMinutes);
    }

    public void setSuccessIntervalMinutes(int successIntervalMinutes) {
      this.successIntervalMinutes = Math.max(1, successIntervalMinutes);
    }

    public List<Integer> getFailureBackoffMinutes() {
      if (failureBackoffMinutes == null || failureBackoffMinutes.isEmpty()) {
        return DEFAULT_FAILURE_BACKOFF;
      }
      List<Integer> sanitized = new ArrayList<>();
      for (Integer value : failureBackoffMinutes) {
        if (value != null && value > 0) {
          sanitized.add(value);
        }
      }
      return sanitized.isEmpty() ? DEFAULT_FAILURE_BACKOFF : sanitized;
    }

    public void setFailureBackoffMinutes(List<Integer> failureBackoffMinutes) {
      this.failureBackoffMinutes =
          failureBackoffMinutes == null
              ? new ArrayList<>(DEFAULT_FAILURE_BACKOFF)
              : new ArrayList<>(failureBackoffMinutes);
    }
  }

  public static class DomainResolution {
    private int defaultLimit = 50;
    private int batchSize = 50;
    private int wdqsMinDelayMs = 2000;
    private int wdqsTimeoutSeconds = 8;
    private int cacheTtlMinutes = 720;

    public int getDefaultLimit() {
      return Math.max(1, defaultLimit);
    }

    public void setDefaultLimit(int defaultLimit) {
      this.defaultLimit = Math.max(1, defaultLimit);
    }

    public int getBatchSize() {
      return Math.max(1, batchSize);
    }

    public void setBatchSize(int batchSize) {
      this.batchSize = Math.max(1, batchSize);
    }

    public int getWdqsMinDelayMs() {
      return Math.max(1, wdqsMinDelayMs);
    }

    public void setWdqsMinDelayMs(int wdqsMinDelayMs) {
      this.wdqsMinDelayMs = Math.max(1, wdqsMinDelayMs);
    }

    public int getWdqsTimeoutSeconds() {
      return Math.max(1, wdqsTimeoutSeconds);
    }

    public void setWdqsTimeoutSeconds(int wdqsTimeoutSeconds) {
      this.wdqsTimeoutSeconds = Math.max(1, wdqsTimeoutSeconds);
    }

    public int getCacheTtlMinutes() {
      return Math.max(0, cacheTtlMinutes);
    }

    public void setCacheTtlMinutes(int cacheTtlMinutes) {
      this.cacheTtlMinutes = Math.max(0, cacheTtlMinutes);
    }
  }

  public static class CareersDiscovery {
    private static final List<Integer> DEFAULT_FAILURE_BACKOFF = List.of(5, 15, 60, 360, 1440);

    private int defaultLimit = 50;
    private int maxCandidatesPerCompany = 60;
    private int maxSlugCandidates = 8;
    private int maxCareersPaths = 4;
    private int robotsCooldownDays = 14;
    private int maxDurationSeconds = 900;
    private int maxVendorProbeRequestsPerHost = 200;
    private List<Integer> failureBackoffMinutes = new ArrayList<>(DEFAULT_FAILURE_BACKOFF);

    public int getDefaultLimit() {
      return Math.max(1, defaultLimit);
    }

    public void setDefaultLimit(int defaultLimit) {
      this.defaultLimit = Math.max(1, defaultLimit);
    }

    public int getMaxCandidatesPerCompany() {
      return Math.max(1, maxCandidatesPerCompany);
    }

    public void setMaxCandidatesPerCompany(int maxCandidatesPerCompany) {
      this.maxCandidatesPerCompany = Math.max(1, maxCandidatesPerCompany);
    }

    public int getMaxSlugCandidates() {
      return Math.max(1, maxSlugCandidates);
    }

    public void setMaxSlugCandidates(int maxSlugCandidates) {
      this.maxSlugCandidates = Math.max(1, maxSlugCandidates);
    }

    public int getMaxCareersPaths() {
      return Math.max(0, maxCareersPaths);
    }

    public void setMaxCareersPaths(int maxCareersPaths) {
      this.maxCareersPaths = Math.max(0, maxCareersPaths);
    }

    public int getRobotsCooldownDays() {
      return Math.max(1, robotsCooldownDays);
    }

    public void setRobotsCooldownDays(int robotsCooldownDays) {
      this.robotsCooldownDays = Math.max(1, robotsCooldownDays);
    }

    public int getMaxDurationSeconds() {
      return Math.max(0, maxDurationSeconds);
    }

    public void setMaxDurationSeconds(int maxDurationSeconds) {
      this.maxDurationSeconds = Math.max(0, maxDurationSeconds);
    }

    public int getMaxVendorProbeRequestsPerHost() {
      return Math.max(1, maxVendorProbeRequestsPerHost);
    }

    public void setMaxVendorProbeRequestsPerHost(int maxVendorProbeRequestsPerHost) {
      this.maxVendorProbeRequestsPerHost = Math.max(1, maxVendorProbeRequestsPerHost);
    }

    public List<Integer> getFailureBackoffMinutes() {
      if (failureBackoffMinutes == null || failureBackoffMinutes.isEmpty()) {
        return DEFAULT_FAILURE_BACKOFF;
      }
      List<Integer> sanitized = new ArrayList<>();
      for (Integer value : failureBackoffMinutes) {
        if (value != null && value > 0) {
          sanitized.add(value);
        }
      }
      return sanitized.isEmpty() ? DEFAULT_FAILURE_BACKOFF : sanitized;
    }

    public void setFailureBackoffMinutes(List<Integer> failureBackoffMinutes) {
      this.failureBackoffMinutes =
          failureBackoffMinutes == null
              ? new ArrayList<>(DEFAULT_FAILURE_BACKOFF)
              : new ArrayList<>(failureBackoffMinutes);
    }
  }

  public static class Robots {
    private boolean failOpen = false;
    private boolean allowAtsAdapterWhenUnavailable = true;

    public boolean isFailOpen() {
      return failOpen;
    }

    public void setFailOpen(boolean failOpen) {
      this.failOpen = failOpen;
    }

    public boolean isAllowAtsAdapterWhenUnavailable() {
      return allowAtsAdapterWhenUnavailable;
    }

    public void setAllowAtsAdapterWhenUnavailable(boolean allowAtsAdapterWhenUnavailable) {
      this.allowAtsAdapterWhenUnavailable = allowAtsAdapterWhenUnavailable;
    }
  }

  public static class Sitemap {
    private int maxDepth = 3;
    private int maxSitemaps = 50;
    private int maxUrlsPerDomain = 200;

    public int getMaxDepth() {
      return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
      this.maxDepth = maxDepth;
    }

    public int getMaxSitemaps() {
      return maxSitemaps;
    }

    public void setMaxSitemaps(int maxSitemaps) {
      this.maxSitemaps = maxSitemaps;
    }

    public int getMaxUrlsPerDomain() {
      return maxUrlsPerDomain;
    }

    public void setMaxUrlsPerDomain(int maxUrlsPerDomain) {
      this.maxUrlsPerDomain = maxUrlsPerDomain;
    }
  }

  public static class Extraction {
    private int maxJobPages = 50;

    public int getMaxJobPages() {
      return maxJobPages;
    }

    public void setMaxJobPages(int maxJobPages) {
      this.maxJobPages = maxJobPages;
    }
  }

  public static class Data {
    private String sp500WikipediaUrl = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies";
    private String sp500Csv = "../data/sp500_constituents.csv";
    private String domainsCsv = "../data/domains.csv";
    private String secCompanyTickersUrl = "https://www.sec.gov/files/company_tickers.json";
    private String secCompanyTickersCachePath = "../data/sec_company_tickers_cache.json";
    private int secCompanyTickersCacheTtlHours = 24;
    private String secUserAgent;

    public String getSp500WikipediaUrl() {
      return sp500WikipediaUrl;
    }

    public void setSp500WikipediaUrl(String sp500WikipediaUrl) {
      this.sp500WikipediaUrl = sp500WikipediaUrl;
    }

    public String getSp500Csv() {
      return sp500Csv;
    }

    public void setSp500Csv(String sp500Csv) {
      this.sp500Csv = sp500Csv;
    }

    public String getDomainsCsv() {
      return domainsCsv;
    }

    public void setDomainsCsv(String domainsCsv) {
      this.domainsCsv = domainsCsv;
    }

    public String getSecCompanyTickersUrl() {
      return secCompanyTickersUrl;
    }

    public void setSecCompanyTickersUrl(String secCompanyTickersUrl) {
      this.secCompanyTickersUrl = secCompanyTickersUrl;
    }

    public String getSecCompanyTickersCachePath() {
      return secCompanyTickersCachePath;
    }

    public void setSecCompanyTickersCachePath(String secCompanyTickersCachePath) {
      this.secCompanyTickersCachePath = secCompanyTickersCachePath;
    }

    public int getSecCompanyTickersCacheTtlHours() {
      return Math.max(0, secCompanyTickersCacheTtlHours);
    }

    public void setSecCompanyTickersCacheTtlHours(int secCompanyTickersCacheTtlHours) {
      this.secCompanyTickersCacheTtlHours = Math.max(0, secCompanyTickersCacheTtlHours);
    }

    public String getSecUserAgent() {
      return secUserAgent;
    }

    public void setSecUserAgent(String secUserAgent) {
      this.secUserAgent = secUserAgent;
    }
  }

  public static class Cli {
    private boolean run;
    private boolean ingestBeforeCrawl = true;
    private String tickers = "";
    private int limit = 5;
    private boolean exitAfterRun = true;

    public boolean isRun() {
      return run;
    }

    public void setRun(boolean run) {
      this.run = run;
    }

    public boolean isIngestBeforeCrawl() {
      return ingestBeforeCrawl;
    }

    public void setIngestBeforeCrawl(boolean ingestBeforeCrawl) {
      this.ingestBeforeCrawl = ingestBeforeCrawl;
    }

    public String getTickers() {
      return tickers;
    }

    public void setTickers(String tickers) {
      this.tickers = tickers;
    }

    public int getLimit() {
      return limit;
    }

    public void setLimit(int limit) {
      this.limit = limit;
    }

    public boolean isExitAfterRun() {
      return exitAfterRun;
    }

    public void setExitAfterRun(boolean exitAfterRun) {
      this.exitAfterRun = exitAfterRun;
    }
  }

  public static class Ats {
    private Workday workday = new Workday();
    private Greenhouse greenhouse = new Greenhouse();
    private Lever lever = new Lever();

    public Workday getWorkday() {
      return workday;
    }

    public void setWorkday(Workday workday) {
      this.workday = workday;
    }

    public Greenhouse getGreenhouse() {
      return greenhouse;
    }

    public void setGreenhouse(Greenhouse greenhouse) {
      this.greenhouse = greenhouse;
    }

    public Lever getLever() {
      return lever;
    }

    public void setLever(Lever lever) {
      this.lever = lever;
    }
  }

  public static class Workday {
    private int maxJobsPerCompany = 400;

    public int getMaxJobsPerCompany() {
      return Math.max(1, maxJobsPerCompany);
    }

    public void setMaxJobsPerCompany(int maxJobsPerCompany) {
      this.maxJobsPerCompany = maxJobsPerCompany;
    }
  }

  public static class Greenhouse {
    private int maxJobsPerCompany = Integer.MAX_VALUE;

    public int getMaxJobsPerCompany() {
      return Math.max(1, maxJobsPerCompany);
    }

    public void setMaxJobsPerCompany(int maxJobsPerCompany) {
      this.maxJobsPerCompany = maxJobsPerCompany;
    }
  }

  public static class Lever {
    private int maxJobsPerCompany = Integer.MAX_VALUE;

    public int getMaxJobsPerCompany() {
      return Math.max(1, maxJobsPerCompany);
    }

    public void setMaxJobsPerCompany(int maxJobsPerCompany) {
      this.maxJobsPerCompany = maxJobsPerCompany;
    }
  }

  public static class Run {
    private int maxDurationSeconds = 0;

    public int getMaxDurationSeconds() {
      return Math.max(0, maxDurationSeconds);
    }

    public void setMaxDurationSeconds(int maxDurationSeconds) {
      this.maxDurationSeconds = Math.max(0, maxDurationSeconds);
    }
  }

  public static class Canary {
    private int defaultLimit = 50;
    private int maxDurationSeconds = 600;
    private int requestTimeoutSeconds = 20;
    private int maxRequestsPerHost = 75;
    private int maxTotalRequests = 5000;
    private double max429Rate = 0.08;
    private int minRequestsFor429Rate = 25;
    private int maxConsecutiveErrors = 25;
    private int maxAttemptsPerRequest = 1;

    public int getDefaultLimit() {
      return Math.max(1, defaultLimit);
    }

    public void setDefaultLimit(int defaultLimit) {
      this.defaultLimit = Math.max(1, defaultLimit);
    }

    public int getMaxDurationSeconds() {
      return Math.max(1, maxDurationSeconds);
    }

    public void setMaxDurationSeconds(int maxDurationSeconds) {
      this.maxDurationSeconds = Math.max(1, maxDurationSeconds);
    }

    public int getRequestTimeoutSeconds() {
      return Math.max(1, requestTimeoutSeconds);
    }

    public void setRequestTimeoutSeconds(int requestTimeoutSeconds) {
      this.requestTimeoutSeconds = Math.max(1, requestTimeoutSeconds);
    }

    public int getMaxRequestsPerHost() {
      return Math.max(1, maxRequestsPerHost);
    }

    public void setMaxRequestsPerHost(int maxRequestsPerHost) {
      this.maxRequestsPerHost = Math.max(1, maxRequestsPerHost);
    }

    public int getMaxTotalRequests() {
      return Math.max(1, maxTotalRequests);
    }

    public void setMaxTotalRequests(int maxTotalRequests) {
      this.maxTotalRequests = Math.max(1, maxTotalRequests);
    }

    public double getMax429Rate() {
      return Math.max(0.0, max429Rate);
    }

    public void setMax429Rate(double max429Rate) {
      this.max429Rate = Math.max(0.0, max429Rate);
    }

    public int getMinRequestsFor429Rate() {
      return Math.max(1, minRequestsFor429Rate);
    }

    public void setMinRequestsFor429Rate(int minRequestsFor429Rate) {
      this.minRequestsFor429Rate = Math.max(1, minRequestsFor429Rate);
    }

    public int getMaxConsecutiveErrors() {
      return Math.max(1, maxConsecutiveErrors);
    }

    public void setMaxConsecutiveErrors(int maxConsecutiveErrors) {
      this.maxConsecutiveErrors = Math.max(1, maxConsecutiveErrors);
    }

    public int getMaxAttemptsPerRequest() {
      return Math.max(1, maxAttemptsPerRequest);
    }

    public void setMaxAttemptsPerRequest(int maxAttemptsPerRequest) {
      this.maxAttemptsPerRequest = Math.max(1, maxAttemptsPerRequest);
    }
  }
}
