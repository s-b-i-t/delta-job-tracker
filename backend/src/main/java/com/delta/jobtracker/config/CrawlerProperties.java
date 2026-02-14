package com.delta.jobtracker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "crawler")
public class CrawlerProperties {
    private static final String DEFAULT_USER_AGENT = "delta-job-tracker/0.1 (+contact)";

    private String userAgent;
    private int perHostDelayMs = 1000;
    private int globalConcurrency = 5;
    private int requestTimeoutSeconds = 20;
    private Api api = new Api();
    private Automation automation = new Automation();
    private DomainResolution domainResolution = new DomainResolution();
    private CareersDiscovery careersDiscovery = new CareersDiscovery();
    private Robots robots = new Robots();
    private Sitemap sitemap = new Sitemap();
    private Extraction extraction = new Extraction();
    private Data data = new Data();
    private Cli cli = new Cli();

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

    public int getGlobalConcurrency() {
        return Math.max(1, globalConcurrency);
    }

    public void setGlobalConcurrency(int globalConcurrency) {
        this.globalConcurrency = Math.max(1, globalConcurrency);
    }

    public int getRequestTimeoutSeconds() {
        return requestTimeoutSeconds;
    }

    public void setRequestTimeoutSeconds(int requestTimeoutSeconds) {
        this.requestTimeoutSeconds = requestTimeoutSeconds;
    }

    public Automation getAutomation() {
        return automation;
    }

    public void setAutomation(Automation automation) {
        this.automation = automation;
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

    public static class DomainResolution {
        private int defaultLimit = 50;
        private int batchSize = 50;
        private int wdqsMinDelayMs = 2000;

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
    }

    public static class CareersDiscovery {
        private int defaultLimit = 50;
        private int maxCandidatesPerCompany = 60;

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
        private String sp500Csv = "../data/sp500_constituents.csv";
        private String domainsCsv = "../data/domains.csv";

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
}
