package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.AtsAdapterResult;
import com.delta.jobtracker.crawl.model.AtsEndpointRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import com.delta.jobtracker.crawl.model.DiscoveredUrlType;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.delta.jobtracker.crawl.util.HashUtils;
import com.delta.jobtracker.crawl.util.JobUrlUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class AtsAdapterIngestionService {
    private static final Logger log = LoggerFactory.getLogger(AtsAdapterIngestionService.class);
    private static final int WORKDAY_PAGE_SIZE = 20;
    private static final int WORKDAY_MAX_PAGES = 20;
    private static final int WORKDAY_MAX_ATTEMPTS = 3;
    private static final int WORKDAY_URL_VALIDATION_LIMIT = 5;
    private static final String HTML_ACCEPT = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";

    private final PoliteHttpClient httpClient;
    private final RobotsTxtService robotsTxtService;
    private final CrawlJdbcRepository repository;
    private final ObjectMapper objectMapper;

    public AtsAdapterIngestionService(
        PoliteHttpClient httpClient,
        RobotsTxtService robotsTxtService,
        CrawlJdbcRepository repository,
        ObjectMapper objectMapper
    ) {
        this.httpClient = httpClient;
        this.robotsTxtService = robotsTxtService;
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    public AtsAdapterResult ingestIfSupported(long crawlRunId, CompanyTarget company, List<AtsEndpointRecord> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }

        java.util.Set<String> greenhouseTokens = new java.util.LinkedHashSet<>();
        int jobsExtractedCount = 0;
        int jobpostingPagesFoundCount = 0;
        boolean successfulFetch = false;
        Map<String, Integer> errors = new LinkedHashMap<>();
        boolean attemptedSupportedAdapter = false;

        for (AtsEndpointRecord endpoint : endpoints) {
            if (endpoint.atsType() == AtsType.GREENHOUSE) {
                String token = extractGreenhouseToken(endpoint.endpointUrl());
                if (token != null) {
                    String key = token.toLowerCase(Locale.ROOT);
                    if (!greenhouseTokens.add(key)) {
                        continue;
                    }
                }
                attemptedSupportedAdapter = true;
                AdapterFetchResult result = ingestFromGreenhouse(crawlRunId, company, endpoint.endpointUrl());
                jobsExtractedCount += result.jobsExtractedCount();
                jobpostingPagesFoundCount += result.jobpostingPagesFoundCount();
                merge(errors, result.errors());
                successfulFetch = successfulFetch || result.successfulFetch();
                if (result.jobsExtractedCount() > 0) {
                    break;
                }
            } else if (endpoint.atsType() == AtsType.LEVER) {
                attemptedSupportedAdapter = true;
                AdapterFetchResult result = ingestFromLever(crawlRunId, company, endpoint.endpointUrl());
                jobsExtractedCount += result.jobsExtractedCount();
                jobpostingPagesFoundCount += result.jobpostingPagesFoundCount();
                merge(errors, result.errors());
                successfulFetch = successfulFetch || result.successfulFetch();
                if (result.jobsExtractedCount() > 0) {
                    break;
                }
            } else if (endpoint.atsType() == AtsType.WORKDAY) {
                attemptedSupportedAdapter = true;
                AdapterFetchResult result = ingestFromWorkday(crawlRunId, company, endpoint.endpointUrl());
                jobsExtractedCount += result.jobsExtractedCount();
                jobpostingPagesFoundCount += result.jobpostingPagesFoundCount();
                merge(errors, result.errors());
                successfulFetch = successfulFetch || result.successfulFetch();
                if (result.jobsExtractedCount() > 0) {
                    break;
                }
            }
        }

        if (!attemptedSupportedAdapter) {
            return null;
        }
        return new AtsAdapterResult(jobsExtractedCount, jobpostingPagesFoundCount, errors, successfulFetch);
    }

    private AdapterFetchResult ingestFromGreenhouse(long crawlRunId, CompanyTarget company, String endpointUrl) {
        String token = extractGreenhouseToken(endpointUrl);
        Map<String, Integer> errors = new LinkedHashMap<>();
        if (token == null) {
            increment(errors, "greenhouse_token_parse_failed");
            return new AdapterFetchResult(0, 0, errors, false);
        }

        String feedUrl = "https://boards-api.greenhouse.io/v1/boards/" + token + "/jobs?content=true";
        GreenhouseAttempt attempt = fetchGreenhouseFeed(crawlRunId, company, feedUrl, errors);
        return new AdapterFetchResult(attempt.jobsExtractedCount(), attempt.jobpostingPagesFoundCount(), errors, attempt.success());
    }

    private GreenhouseAttempt fetchGreenhouseFeed(
        long crawlRunId,
        CompanyTarget company,
        String feedUrl,
        Map<String, Integer> errors
    ) {
        String normalizedFeedUrl = normalizeGreenhouseApiUrl(feedUrl);
        if (!robotsTxtService.isAllowedForAtsAdapter(normalizedFeedUrl)) {
            String status = "greenhouse_blocked_by_robots";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, normalizedFeedUrl, status, null, "blocked_by_robots");
            increment(errors, status);
            return new GreenhouseAttempt(0, 0, false, false, false);
        }

        HttpFetchResult fetch = httpClient.get(normalizedFeedUrl, "application/json,*/*;q=0.8");
        if (!fetch.isSuccessful() || fetch.body() == null || fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
            String status = adapterFetchStatus("greenhouse", fetch);
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, normalizedFeedUrl, status, fetch, null);
            increment(errors, status);
            return new GreenhouseAttempt(0, 0, false, true, false);
        }

        try {
            JsonNode root = objectMapper.readTree(fetch.body());
            JsonNode jobs = root.path("jobs");
            if (!jobs.isArray()) {
                String status = "greenhouse_invalid_payload";
                recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, feedUrl, status, fetch, "invalid_payload");
                increment(errors, status);
                return new GreenhouseAttempt(0, 0, false, false, true);
            }
            int extracted = 0;
            List<NormalizedJobPosting> postings = new ArrayList<>();
            for (JsonNode job : jobs) {
                NormalizedJobPosting posting = normalizeGreenhousePosting(company, job, normalizedFeedUrl);
                if (posting == null) {
                    continue;
                }
                postings.add(posting);
            }
            if (!postings.isEmpty()) {
                repository.upsertJobPostingsBatch(company.companyId(), crawlRunId, postings, Instant.now());
                extracted = postings.size();
            }
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, normalizedFeedUrl, "ats_fetch_success", fetch, null);
            return new GreenhouseAttempt(extracted, extracted > 0 ? 1 : 0, true, false, false);
        } catch (Exception e) {
            log.warn("Failed to parse Greenhouse payload for {}", company.ticker(), e);
            String status = "greenhouse_parse_error";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, normalizedFeedUrl, status, fetch, "parse_error");
            increment(errors, status);
            return new GreenhouseAttempt(0, 0, false, false, false);
        }
    }

    private AdapterFetchResult ingestFromLever(long crawlRunId, CompanyTarget company, String endpointUrl) {
        String account = extractLeverAccount(endpointUrl);
        Map<String, Integer> errors = new LinkedHashMap<>();
        if (account == null) {
            increment(errors, "lever_account_parse_failed");
            return new AdapterFetchResult(0, 0, errors, false);
        }

        String feedUrl = "https://api.lever.co/v0/postings/" + account + "?mode=json";
        if (!robotsTxtService.isAllowedForAtsAdapter(feedUrl)) {
            String status = "lever_blocked_by_robots";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.LEVER, feedUrl, status, null, "blocked_by_robots");
            increment(errors, status);
            return new AdapterFetchResult(0, 0, errors, false);
        }

        HttpFetchResult fetch = httpClient.get(feedUrl, "application/json,*/*;q=0.8");
        if (!fetch.isSuccessful() || fetch.body() == null || fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
            String status = adapterFetchStatus("lever", fetch);
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.LEVER, feedUrl, status, fetch, null);
            increment(errors, status);
            return new AdapterFetchResult(0, 0, errors, false);
        }

        try {
            JsonNode root = objectMapper.readTree(fetch.body());
            if (!root.isArray()) {
                String status = "lever_invalid_payload";
                recordAtsAttempt(crawlRunId, company.companyId(), AtsType.LEVER, feedUrl, status, fetch, "invalid_payload");
                increment(errors, status);
                return new AdapterFetchResult(0, 0, errors, false);
            }
            int extracted = 0;
            List<NormalizedJobPosting> postings = new ArrayList<>();
            for (JsonNode job : root) {
                NormalizedJobPosting posting = normalizeLeverPosting(company, job, feedUrl);
                if (posting == null) {
                    continue;
                }
                postings.add(posting);
            }
            if (!postings.isEmpty()) {
                repository.upsertJobPostingsBatch(company.companyId(), crawlRunId, postings, Instant.now());
                extracted = postings.size();
            }
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.LEVER, feedUrl, "ats_fetch_success", fetch, null);
            return new AdapterFetchResult(extracted, extracted > 0 ? 1 : 0, errors, true);
        } catch (Exception e) {
            log.warn("Failed to parse Lever payload for {}", company.ticker(), e);
            String status = "lever_parse_error";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.LEVER, feedUrl, status, fetch, "parse_error");
            increment(errors, status);
            return new AdapterFetchResult(0, 0, errors, false);
        }
    }

    private AdapterFetchResult ingestFromWorkday(long crawlRunId, CompanyTarget company, String endpointUrl) {
        Map<String, Integer> errors = new LinkedHashMap<>();
        WorkdayEndpoint endpoint = deriveWorkdayEndpoint(endpointUrl);
        if (endpoint == null) {
            increment(errors, "workday_endpoint_parse_failed");
            return new AdapterFetchResult(0, 0, errors, false);
        }

        String cxsUrl = "https://" + endpoint.host() + "/wday/cxs/" + endpoint.tenant() + "/" + endpoint.site() + "/jobs";
        if (!robotsTxtService.isAllowedForAtsAdapter(cxsUrl)) {
            String status = "workday_blocked_by_robots";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, status, null, "blocked_by_robots");
            increment(errors, status);
            return new AdapterFetchResult(0, 0, errors, false);
        }

        int extracted = 0;
        int pagesWithJobs = 0;
        boolean successfulFetch = false;
        java.util.concurrent.atomic.AtomicInteger validationsRemaining = new java.util.concurrent.atomic.AtomicInteger(WORKDAY_URL_VALIDATION_LIMIT);
        int offset = 0;
        for (int page = 0; page < WORKDAY_MAX_PAGES; page++) {
            HttpFetchResult fetch = fetchWorkdayPage(cxsUrl, offset);
            if (!fetch.isSuccessful() || fetch.body() == null || fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
                String status = adapterFetchStatus("workday", fetch);
                recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, status, fetch, null);
                increment(errors, status);
                break;
            }

            try {
                JsonNode root = objectMapper.readTree(fetch.body());
                List<NormalizedJobPosting> postings = parseWorkdayJobPostings(endpoint, root, cxsUrl, errors, validationsRemaining);
                if (postings.isEmpty()) {
                    successfulFetch = true;
                    recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, "ats_fetch_success", fetch, null);
                    break;
                }
                successfulFetch = true;
                pagesWithJobs++;
                repository.upsertJobPostingsBatch(company.companyId(), crawlRunId, postings, Instant.now());
                extracted += postings.size();
                recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, "ats_fetch_success", fetch, null);
                if (postings.size() < WORKDAY_PAGE_SIZE) {
                    break;
                }
                offset += WORKDAY_PAGE_SIZE;
            } catch (Exception e) {
                log.warn("Failed to parse Workday payload for {} endpoint={}", company.ticker(), endpointUrl, e);
                String status = "workday_parse_error";
                recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, status, fetch, "parse_error");
                increment(errors, status);
                break;
            }
        }
        return new AdapterFetchResult(extracted, pagesWithJobs, errors, successfulFetch);
    }

    NormalizedJobPosting normalizeGreenhousePosting(CompanyTarget company, JsonNode job, String sourceUrl) {
        String title = text(job, "title");
        String absoluteUrl = normalizeGreenhouseJobUrl(text(job, "absolute_url"));
        String rawHtml = text(job, "content");
        String description = rawHtml == null ? null : org.jsoup.parser.Parser.unescapeEntities(rawHtml, false);
        String identifier = text(job, "id");
        String location = text(job.path("location"), "name");
        LocalDate datePosted = parseIsoDate(text(job, "updated_at"));
        String employmentType = extractGreenhouseEmploymentType(job.path("metadata"));
        String derivedUrl = buildGreenhouseJobUrl(sourceUrl, identifier);
        String canonical = firstNonBlank(absoluteUrl, derivedUrl);
        return buildPosting(sourceUrl, canonical, title, company.name(), location, employmentType, datePosted, description, identifier);
    }

    private String extractGreenhouseEmploymentType(JsonNode metadata) {
        if (!metadata.isArray()) {
            return null;
        }
        List<String> values = new ArrayList<>();
        for (JsonNode entry : metadata) {
            String name = text(entry, "name");
            if (name == null || !name.toLowerCase(Locale.ROOT).contains("employment")) {
                continue;
            }
            JsonNode valueNode = entry.path("value");
            if (valueNode.isArray()) {
                for (JsonNode item : valueNode) {
                    String val = item.asText(null);
                    if (val != null && !val.isBlank()) {
                        values.add(val.trim());
                    }
                }
            } else {
                String val = valueNode.asText(null);
                if (val != null && !val.isBlank()) {
                    values.add(val.trim());
                }
            }
        }
        return values.isEmpty() ? null : String.join(", ", values);
    }

    private NormalizedJobPosting normalizeLeverPosting(CompanyTarget company, JsonNode job, String sourceUrl) {
        String title = text(job, "text");
        String hostedUrl = text(job, "hostedUrl");
        String applyUrl = text(job, "applyUrl");
        String canonicalUrl = firstNonBlank(hostedUrl, applyUrl);
        String description = htmlToText(firstNonBlank(text(job, "descriptionPlain"), text(job, "description")));
        String identifier = text(job, "id");
        String location = text(job.path("categories"), "location");
        String employmentType = text(job.path("categories"), "commitment");

        LocalDate datePosted = null;
        JsonNode createdAtNode = job.get("createdAt");
        if (createdAtNode != null && createdAtNode.canConvertToLong()) {
            datePosted = Instant.ofEpochMilli(createdAtNode.asLong()).atZone(ZoneOffset.UTC).toLocalDate();
        }

        return buildPosting(sourceUrl, canonicalUrl, title, company.name(), location, employmentType, datePosted, description, identifier);
    }

    private NormalizedJobPosting buildPosting(
        String sourceUrl,
        String canonicalUrl,
        String title,
        String orgName,
        String locationText,
        String employmentType,
        LocalDate datePosted,
        String descriptionText,
        String identifier
    ) {
        String canonical = JobUrlUtils.sanitizeCanonicalUrl(canonicalUrl);
        if (canonical == null || canonical.isBlank()) {
            return null;
        }
        if (title == null || title.isBlank()) {
            return null;
        }

        Map<String, String> stableFields = new TreeMap<>();
        stableFields.put("title", safe(title));
        stableFields.put("org_name", safe(orgName));
        stableFields.put("location_text", safe(locationText));
        stableFields.put("employment_type", safe(employmentType));
        stableFields.put("date_posted", datePosted == null ? "" : datePosted.toString());
        stableFields.put("description", safe(descriptionText));
        stableFields.put("canonical_url", safe(canonical));
        stableFields.put("identifier", safe(identifier));

        String hashPayload;
        try {
            hashPayload = objectMapper.writeValueAsString(stableFields);
        } catch (Exception e) {
            hashPayload = stableFields.toString();
        }

        return new NormalizedJobPosting(
            sourceUrl,
            canonical,
            title,
            orgName,
            locationText,
            employmentType,
            datePosted,
            descriptionText,
            identifier,
            HashUtils.sha256Hex(hashPayload)
        );
    }

    private String extractGreenhouseToken(String endpointUrl) {
        URI uri = safeUri(endpointUrl);
        if (uri == null || uri.getHost() == null) {
            return null;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        List<String> segments = pathSegments(uri.getPath());
        if ((host.contains("api.greenhouse.io") || host.contains("boards-api.greenhouse.io"))
            && segments.size() >= 3
            && "boards".equals(segments.get(1))) {
            return segments.get(2);
        }
        if (host.contains("boards.greenhouse.io") || host.contains("job-boards.greenhouse.io") || host.endsWith("greenhouse.io")) {
            if (!segments.isEmpty()) {
                return segments.getFirst();
            }
        }
        if (host.contains("boards.greenhouse.io") && !segments.isEmpty() && "embed".equals(segments.getFirst())) {
            String token = extractQueryParam(uri, "for");
            if (token != null && !token.isBlank()) {
                return token;
            }
        }
        return null;
    }

    private String buildGreenhouseJobUrl(String endpointUrl, String jobId) {
        if (jobId == null || jobId.isBlank()) {
            return null;
        }
        String token = extractGreenhouseToken(endpointUrl);
        if (token == null || token.isBlank()) {
            return null;
        }
        return normalizeGreenhouseJobUrl("https://boards.greenhouse.io/" + token + "/jobs/" + jobId);
    }

    private String normalizeGreenhouseJobUrl(String raw) {
        URI uri = safeUri(raw);
        if (uri == null || uri.getHost() == null) {
            return raw == null ? null : raw.trim();
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.contains("boards-api.greenhouse.io") || host.contains("api.greenhouse.io")) {
            return null;
        }
        if (host.equals("job-boards.greenhouse.io")) {
            host = "boards.greenhouse.io";
        }
        String path = uri.getRawPath() == null ? "" : uri.getRawPath();
        String query = uri.getRawQuery();
        String normalized = "https://" + host + path;
        if (query != null && !query.isBlank()) {
            normalized = normalized + "?" + query;
        }
        if (normalized.endsWith("/") && normalized.length() > "https://x/".length()) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private String extractLeverAccount(String endpointUrl) {
        URI uri = safeUri(endpointUrl);
        if (uri == null || uri.getHost() == null) {
            return null;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        List<String> segments = pathSegments(uri.getPath());

        if (host.contains("jobs.lever.co") && !segments.isEmpty()) {
            return segments.getFirst();
        }
        if (host.contains("apply.lever.co") && !segments.isEmpty()) {
            return segments.getFirst();
        }
        if (host.contains("api.lever.co") && segments.size() >= 3 && "postings".equals(segments.get(1))) {
            return segments.get(2);
        }
        return null;
    }

    private String extractQueryParam(URI uri, String name) {
        if (uri == null || name == null || name.isBlank()) {
            return null;
        }
        String query = uri.getQuery();
        if (query == null || query.isBlank()) {
            return null;
        }
        String[] parts = query.split("&");
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String key = part;
            String value = "";
            int idx = part.indexOf('=');
            if (idx >= 0) {
                key = part.substring(0, idx);
                value = part.substring(idx + 1);
            }
            if (key.equalsIgnoreCase(name)) {
                return value;
            }
        }
        return null;
    }

    HttpFetchResult fetchWorkdayPage(String cxsUrl, int offset) {
        ObjectNode request = objectMapper.createObjectNode();
        request.put("limit", WORKDAY_PAGE_SIZE);
        request.put("offset", offset);
        request.put("searchText", "");
        request.set("appliedFacets", objectMapper.createObjectNode());

        String payload;
        try {
            payload = objectMapper.writeValueAsString(request);
        } catch (Exception e) {
            payload = "{\"limit\":" + WORKDAY_PAGE_SIZE + ",\"offset\":" + offset + ",\"searchText\":\"\",\"appliedFacets\":{}}";
        }

        HttpFetchResult lastFetch = null;
        for (int attempt = 1; attempt <= WORKDAY_MAX_ATTEMPTS; attempt++) {
            HttpFetchResult fetch = httpClient.postJson(cxsUrl, payload, "application/json,*/*;q=0.8");
            lastFetch = fetch;
            if (fetch.isSuccessful() && fetch.statusCode() >= 200 && fetch.statusCode() < 300) {
                return fetch;
            }
            boolean retryable = fetch.errorCode() != null || fetch.statusCode() == 429 || fetch.statusCode() >= 500;
            if (!retryable || attempt == WORKDAY_MAX_ATTEMPTS) {
                break;
            }
            try {
                Thread.sleep(250L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return lastFetch;
    }

    List<NormalizedJobPosting> parseWorkdayJobPostings(
        WorkdayEndpoint endpoint,
        JsonNode root,
        String sourceUrl,
        Map<String, Integer> errors,
        java.util.concurrent.atomic.AtomicInteger validationsRemaining
    ) {
        List<NormalizedJobPosting> postings = new ArrayList<>();
        JsonNode jobs = root.path("jobPostings");
        if (!jobs.isArray()) {
            return postings;
        }
        for (JsonNode job : jobs) {
            String title = firstNonBlank(text(job, "title"), text(job, "jobTitle"));
            List<String> rawCandidates = collectWorkdayUrlCandidates(job);
            String rawPrimary = rawCandidates.isEmpty() ? null : rawCandidates.getFirst();
            String canonicalUrl = resolveWorkdayCanonicalUrl(endpoint, rawCandidates, errors, validationsRemaining);
            String identifier = firstNonBlank(
                text(job, "bulletinId"),
                text(job, "id"),
                text(job, "jobReqId"),
                extractWorkdayIdFromPath(rawPrimary)
            );
            String location = extractWorkdayLocation(job);
            String employmentType = firstNonBlank(text(job, "timeType"), text(job, "workerSubType"), text(job, "timeTypeLabel"));
            LocalDate datePosted = firstNonBlankDate(
                text(job, "postedOn"),
                text(job, "bulletinDate"),
                text(job, "startDate")
            );
            String description = firstNonBlank(
                htmlToText(text(job, "shortDescription")),
                htmlToText(text(job, "description")),
                htmlToText(text(job, "jobDescription"))
            );

            if (canonicalUrl == null || canonicalUrl.isBlank()) {
                log.debug("Skipping Workday posting without canonical URL. title={} id={}", title, identifier);
                continue;
            }
            if (JobUrlUtils.isInvalidWorkdayUrl(canonicalUrl)) {
                increment(errors, "workday_invalid_canonical_url");
                log.debug("Skipping Workday posting with invalid canonical URL. title={} id={}", title, identifier);
                continue;
            }

            NormalizedJobPosting posting = buildPosting(
                sourceUrl,
                canonicalUrl,
                title,
                null,
                location,
                employmentType,
                datePosted,
                description,
                identifier
            );
            if (posting != null) {
                postings.add(posting);
            }
        }
        return postings;
    }

    private WorkdayEndpoint deriveWorkdayEndpoint(String endpointUrl) {
        URI uri = safeUri(endpointUrl);
        if (uri == null || uri.getHost() == null) {
            return null;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (!host.contains("myworkdayjobs.com")) {
            return null;
        }

        String[] hostParts = host.split("\\.");
        if (hostParts.length < 3) {
            return null;
        }
        String tenant = hostParts[0];
        if (tenant.isBlank()) {
            return null;
        }

        List<String> segments = pathSegments(uri.getPath());
        String site = null;
        if (segments.size() >= 4 && "wday".equals(segments.get(0)) && "cxs".equals(segments.get(1))) {
            site = segments.get(3);
        } else if (segments.size() >= 2 && isLocaleSegment(segments.getFirst())) {
            site = segments.get(1);
        } else if (!segments.isEmpty()) {
            site = segments.getFirst();
        }
        site = stripTrailingPunctuation(site);
        if (site == null || site.isBlank()) {
            return null;
        }
        return new WorkdayEndpoint(host, tenant, site);
    }

    private boolean isLocaleSegment(String segment) {
        return segment != null && segment.matches("^[a-z]{2}-[A-Z]{2}$");
    }

    List<String> buildWorkdayUrlCandidates(String host, String site, String externalPath) {
        if (externalPath == null || externalPath.isBlank()) {
            return List.of();
        }
        String trimmed = externalPath.trim();
        if (trimmed.toLowerCase(Locale.ROOT).contains("invalid-url")) {
            return List.of();
        }

        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        boolean isAbsolute = trimmed.startsWith("http://") || trimmed.startsWith("https://");
        if (isAbsolute) {
            URI uri = safeUri(trimmed);
            if (uri == null || uri.getHost() == null) {
                return List.of();
            }
            String extHost = uri.getHost().toLowerCase(Locale.ROOT);
            if (!extHost.endsWith("myworkdayjobs.com")) {
                return List.of();
            }
            String rawPath = uri.getRawPath();
            String rawQuery = uri.getRawQuery();
            List<String> pathCandidates = buildWorkdayPathCandidates(rawPath, site);
            boolean hasSiteSegment = false;
            List<String> segments = pathSegments(rawPath);
            if (site != null && !site.isBlank() && !segments.isEmpty()) {
                if (segments.getFirst().equalsIgnoreCase(site)) {
                    hasSiteSegment = true;
                } else if (segments.size() > 1 && isLocaleSegment(segments.getFirst()) && segments.get(1).equalsIgnoreCase(site)) {
                    hasSiteSegment = true;
                }
            }
            if (!hasSiteSegment) {
                for (String pathCandidate : pathCandidates) {
                    String candidate = buildWorkdayUrl(extHost, pathCandidate, rawQuery);
                    if (candidate != null) {
                        candidates.add(candidate);
                    }
                }
            }
            String direct = buildWorkdayUrl(extHost, rawPath, rawQuery);
            if (direct != null) {
                candidates.add(direct);
            }
            if (hasSiteSegment) {
                for (String pathCandidate : pathCandidates) {
                    String candidate = buildWorkdayUrl(extHost, pathCandidate, rawQuery);
                    if (candidate != null) {
                        candidates.add(candidate);
                    }
                }
            }
            return new ArrayList<>(candidates);
        }

        String rawPath = stripQueryAndFragment(trimmed);
        for (String pathCandidate : buildWorkdayPathCandidates(rawPath, site)) {
            String candidate = buildWorkdayUrl(host, pathCandidate, null);
            if (candidate != null) {
                candidates.add(candidate);
            }
        }
        return new ArrayList<>(candidates);
    }

    String toCanonicalWorkdayUrl(String host, String site, String externalPath) {
        List<String> candidates = buildWorkdayUrlCandidates(host, site, externalPath);
        return candidates.isEmpty() ? null : candidates.getFirst();
    }

    private String resolveWorkdayCanonicalUrl(
        WorkdayEndpoint endpoint,
        List<String> rawCandidates,
        Map<String, Integer> errors,
        java.util.concurrent.atomic.AtomicInteger validationsRemaining
    ) {
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        for (String rawCandidate : rawCandidates) {
            candidates.addAll(buildWorkdayUrlCandidates(endpoint.host(), endpoint.site(), rawCandidate));
        }
        if (candidates.isEmpty()) {
            increment(errors, "workday_missing_canonical_url");
            return null;
        }

        boolean invalidRedirect = false;
        boolean apiStyleRejected = false;
        boolean fetchFailed = false;
        for (String candidate : candidates) {
            if (isWorkdayApiStyleUrl(candidate)) {
                apiStyleRejected = true;
                continue;
            }
            if (shouldValidateWorkdayUrl(validationsRemaining)) {
                HttpFetchResult fetch = httpClient.get(candidate, HTML_ACCEPT);
                if (fetch == null || !fetch.isSuccessful()) {
                    fetchFailed = true;
                    if (isInvalidWorkdayRedirect(fetch)) {
                        invalidRedirect = true;
                    }
                    continue;
                }
                if (isInvalidWorkdayRedirect(fetch)) {
                    invalidRedirect = true;
                    continue;
                }
            }
            return candidate;
        }

        if (invalidRedirect) {
            increment(errors, "workday_job_url_invalid_redirect");
        }
        if (fetchFailed) {
            increment(errors, "workday_job_url_fetch_failed");
        }
        if (apiStyleRejected) {
            increment(errors, "workday_job_url_api_style_rejected");
        }
        if (!invalidRedirect && !apiStyleRejected && !fetchFailed) {
            increment(errors, "workday_missing_canonical_url");
        }
        return null;
    }

    private List<String> collectWorkdayUrlCandidates(JsonNode job) {
        List<String> candidates = new ArrayList<>();
        addIfPresent(candidates, text(job, "externalPath"));
        addIfPresent(candidates, text(job, "externalUrl"));
        addIfPresent(candidates, text(job, "jobPostingUrl"));
        addIfPresent(candidates, text(job, "jobUrl"));
        addIfPresent(candidates, text(job, "url"));
        addIfPresent(candidates, text(job, "applyUrl"));
        addIfPresent(candidates, text(job, "externalApplyUrl"));
        return candidates;
    }

    private List<String> buildWorkdayPathCandidates(String rawPath, String site) {
        String path = normalizeWorkdayPath(rawPath);
        if (path == null || path.isBlank()) {
            return List.of();
        }
        LinkedHashSet<String> candidates = new LinkedHashSet<>();
        boolean hasLocale = path.matches("^/[a-z]{2}-[A-Z]{2}/.*");
        boolean startsWithSite = site != null
            && path.toLowerCase(Locale.ROOT).startsWith("/" + site.toLowerCase(Locale.ROOT) + "/");
        if (hasLocale) {
            candidates.add(path);
        } else {
            if (site != null && !site.isBlank()) {
                String sitePrefixed = startsWithSite ? path : "/" + site + path;
                candidates.add("/en-US" + sitePrefixed);
                candidates.add(sitePrefixed);
            }
            candidates.add("/en-US" + path);
            candidates.add(path);
        }
        return new ArrayList<>(candidates);
    }

    private String normalizeWorkdayPath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        String cleaned = stripQueryAndFragment(rawPath.trim());
        if (cleaned.isBlank()) {
            return null;
        }
        if (!cleaned.startsWith("/")) {
            cleaned = "/" + cleaned;
        }
        return cleaned;
    }

    private String buildWorkdayUrl(String host, String path, String query) {
        if (host == null || host.isBlank() || path == null || path.isBlank()) {
            return null;
        }
        String normalizedPath = normalizeWorkdayPath(path);
        if (normalizedPath == null) {
            return null;
        }
        String url = "https://" + host + normalizedPath;
        if (query != null && !query.isBlank()) {
            url = url + "?" + query;
        }
        return url;
    }

    private boolean isWorkdayApiStyleUrl(String candidate) {
        if (candidate == null || candidate.isBlank()) {
            return true;
        }
        String lower = candidate.toLowerCase(Locale.ROOT);
        if (lower.contains("/wday/cxs/")) {
            return true;
        }
        URI uri = safeUri(candidate);
        String path = uri == null ? null : uri.getPath();
        if (path == null) {
            return false;
        }
        String trimmedPath = path.toLowerCase(Locale.ROOT);
        return trimmedPath.endsWith("/jobs") || trimmedPath.endsWith("/jobs/");
    }

    private boolean shouldValidateWorkdayUrl(java.util.concurrent.atomic.AtomicInteger remaining) {
        if (remaining == null) {
            return false;
        }
        int current = remaining.get();
        if (current <= 0) {
            return false;
        }
        remaining.decrementAndGet();
        return true;
    }

    private boolean isInvalidWorkdayRedirect(HttpFetchResult fetch) {
        if (fetch == null) {
            return false;
        }
        String finalUrl = fetch.finalUrlOrRequested();
        if (finalUrl == null) {
            return false;
        }
        return JobUrlUtils.isInvalidWorkdayRedirectUrl(finalUrl);
    }

    private String extractWorkdayIdFromPath(String externalPath) {
        if (externalPath == null || externalPath.isBlank()) {
            return null;
        }
        Matcher matcher = Pattern.compile("_([A-Za-z0-9-]+)$").matcher(externalPath.trim());
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String extractWorkdayLocation(JsonNode job) {
        String direct = firstNonBlank(text(job, "locationsText"), text(job, "location"));
        if (direct != null) {
            return direct;
        }
        JsonNode locations = job.get("locations");
        if (locations != null && locations.isArray()) {
            List<String> values = new ArrayList<>();
            for (JsonNode location : locations) {
                String value = firstNonBlank(text(location, "name"), text(location, "city"), text(location, "country"));
                if (value != null) {
                    values.add(value);
                }
            }
            if (!values.isEmpty()) {
                return String.join(" | ", values);
            }
        }
        return null;
    }

    private LocalDate firstNonBlankDate(String... candidates) {
        for (String candidate : candidates) {
            LocalDate parsed = parseIsoDate(candidate);
            if (parsed != null) {
                return parsed;
            }
        }
        return null;
    }

    private URI safeUri(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            String value = raw.trim();
            if (!value.startsWith("http://") && !value.startsWith("https://")) {
                value = "https://" + value;
            }
            return new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    private List<String> pathSegments(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return List.of();
        }
        String[] split = rawPath.split("/");
        List<String> out = new ArrayList<>();
        for (String part : split) {
            if (part != null && !part.isBlank()) {
                out.add(part);
            }
        }
        return out;
    }

    private LocalDate parseIsoDate(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String candidate = value.trim();
        if (candidate.length() >= 10) {
            candidate = candidate.substring(0, 10);
        }
        try {
            return LocalDate.parse(candidate);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String text(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        if (value.isTextual() || value.isNumber() || value.isBoolean()) {
            return value.asText();
        }
        return value.toString();
    }

    private String htmlToText(String html) {
        if (html == null || html.isBlank()) {
            return null;
        }
        return Jsoup.parse(html).text();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    private void addIfPresent(List<String> target, String value) {
        if (target == null || value == null) {
            return;
        }
        String trimmed = value.trim();
        if (!trimmed.isBlank()) {
            target.add(trimmed);
        }
    }

    private String stripQueryAndFragment(String value) {
        if (value == null) {
            return null;
        }
        String result = value;
        int idx = result.indexOf('?');
        if (idx >= 0) {
            result = result.substring(0, idx);
        }
        int hashIdx = result.indexOf('#');
        if (hashIdx >= 0) {
            result = result.substring(0, hashIdx);
        }
        return result;
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private String stripTrailingPunctuation(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        while (!trimmed.isEmpty()) {
            char last = trimmed.charAt(trimmed.length() - 1);
            if (last == '.' || last == ',' || last == ';' || last == ')' || last == ']' || last == '}' || last == '"' || last == '&' || last == '?') {
                trimmed = trimmed.substring(0, trimmed.length() - 1);
                continue;
            }
            break;
        }
        return trimmed;
    }

    private void increment(Map<String, Integer> errors, String key) {
        errors.put(key, errors.getOrDefault(key, 0) + 1);
    }

    private void merge(Map<String, Integer> target, Map<String, Integer> source) {
        source.forEach((key, value) -> target.put(key, target.getOrDefault(key, 0) + value));
    }

    private String adapterFetchStatus(String prefix, HttpFetchResult fetch) {
        if (fetch == null) {
            return prefix + "_unknown_error";
        }
        if (fetch.errorCode() != null && !fetch.errorCode().isBlank()) {
            return prefix + "_" + fetch.errorCode();
        }
        if (fetch.statusCode() > 0) {
            return prefix + "_http_" + fetch.statusCode();
        }
        return prefix + "_unknown_error";
    }

    private void recordAtsAttempt(
        long crawlRunId,
        long companyId,
        AtsType atsType,
        String url,
        String fetchStatus,
        HttpFetchResult fetch,
        String errorOverride
    ) {
        String storedUrl = atsType == AtsType.GREENHOUSE ? normalizeGreenhouseApiUrl(url) : url;
        Integer httpStatus = null;
        if (fetch != null && fetch.statusCode() > 0) {
            httpStatus = fetch.statusCode();
        }
        String errorCode = errorOverride;
        if (errorCode == null && fetch != null) {
            errorCode = fetch.errorCode();
        }
        repository.upsertDiscoveredUrl(
            crawlRunId,
            companyId,
            storedUrl,
            DiscoveredUrlType.ATS_API,
            fetchStatus,
            Instant.now(),
            httpStatus,
            errorCode,
            atsType.name()
        );
    }

    private String normalizeGreenhouseApiUrl(String raw) {
        URI uri = safeUri(raw);
        if (uri == null || uri.getHost() == null) {
            return raw == null ? null : raw.trim();
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.equals("api.greenhouse.io")) {
            host = "boards-api.greenhouse.io";
        }
        String path = uri.getRawPath() == null ? "" : uri.getRawPath();
        String normalized = "https://" + host + path;
        String query = uri.getRawQuery();
        if (query != null && !query.isBlank()) {
            normalized = normalized + "?" + query;
        }
        return normalized;
    }

    record WorkdayEndpoint(String host, String tenant, String site) {
    }

    private record AdapterFetchResult(
        int jobsExtractedCount,
        int jobpostingPagesFoundCount,
        Map<String, Integer> errors,
        boolean successfulFetch
    ) {
    }

    private record GreenhouseAttempt(
        int jobsExtractedCount,
        int jobpostingPagesFoundCount,
        boolean success,
        boolean shouldFallback,
        boolean payloadInvalid
    ) {
    }
}
