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

        int jobsExtractedCount = 0;
        int jobpostingPagesFoundCount = 0;
        boolean successfulFetch = false;
        Map<String, Integer> errors = new LinkedHashMap<>();
        boolean attemptedSupportedAdapter = false;

        for (AtsEndpointRecord endpoint : endpoints) {
            if (endpoint.atsType() == AtsType.GREENHOUSE) {
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

        String primaryUrl = "https://boards-api.greenhouse.io/v1/boards/" + token + "/jobs?content=true";
        String fallbackUrl = "https://api.greenhouse.io/v1/boards/" + token + "/jobs?content=true";

        GreenhouseAttempt primary = fetchGreenhouseFeed(crawlRunId, company, primaryUrl, errors);
        if (primary.success()) {
            return new AdapterFetchResult(primary.jobsExtractedCount(), primary.jobpostingPagesFoundCount(), errors, true);
        }
        if (primary.payloadInvalid()) {
            return new AdapterFetchResult(0, 0, errors, false);
        }
        if (!primary.shouldFallback()) {
            return new AdapterFetchResult(0, 0, errors, false);
        }

        GreenhouseAttempt fallback = fetchGreenhouseFeed(crawlRunId, company, fallbackUrl, errors);
        return new AdapterFetchResult(fallback.jobsExtractedCount(), fallback.jobpostingPagesFoundCount(), errors, fallback.success());
    }

    private GreenhouseAttempt fetchGreenhouseFeed(
        long crawlRunId,
        CompanyTarget company,
        String feedUrl,
        Map<String, Integer> errors
    ) {
        if (!robotsTxtService.isAllowedForAtsAdapter(feedUrl)) {
            String status = "greenhouse_blocked_by_robots";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, feedUrl, status, null, "blocked_by_robots");
            increment(errors, status);
            return new GreenhouseAttempt(0, 0, false, false, false);
        }

        HttpFetchResult fetch = httpClient.get(feedUrl, "application/json,*/*;q=0.8");
        if (!fetch.isSuccessful() || fetch.body() == null || fetch.statusCode() < 200 || fetch.statusCode() >= 300) {
            String status = adapterFetchStatus("greenhouse", fetch);
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, feedUrl, status, fetch, null);
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
            for (JsonNode job : jobs) {
                NormalizedJobPosting posting = normalizeGreenhousePosting(company, job, feedUrl);
                if (posting == null) {
                    continue;
                }
                repository.upsertJobPosting(company.companyId(), crawlRunId, posting, Instant.now());
                extracted++;
            }
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, feedUrl, "ats_fetch_success", fetch, null);
            return new GreenhouseAttempt(extracted, extracted > 0 ? 1 : 0, true, false, false);
        } catch (Exception e) {
            log.warn("Failed to parse Greenhouse payload for {}", company.ticker(), e);
            String status = "greenhouse_parse_error";
            recordAtsAttempt(crawlRunId, company.companyId(), AtsType.GREENHOUSE, feedUrl, status, fetch, "parse_error");
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
            for (JsonNode job : root) {
                NormalizedJobPosting posting = normalizeLeverPosting(company, job, feedUrl);
                if (posting == null) {
                    continue;
                }
                repository.upsertJobPosting(company.companyId(), crawlRunId, posting, Instant.now());
                extracted++;
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
                List<NormalizedJobPosting> postings = parseWorkdayJobPostings(endpoint.host(), root, cxsUrl);
                if (postings.isEmpty()) {
                    successfulFetch = true;
                    recordAtsAttempt(crawlRunId, company.companyId(), AtsType.WORKDAY, cxsUrl, "ats_fetch_success", fetch, null);
                    break;
                }
                successfulFetch = true;
                pagesWithJobs++;
                for (NormalizedJobPosting posting : postings) {
                    repository.upsertJobPosting(company.companyId(), crawlRunId, posting, Instant.now());
                    extracted++;
                }
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

    private NormalizedJobPosting normalizeGreenhousePosting(CompanyTarget company, JsonNode job, String sourceUrl) {
        String title = text(job, "title");
        String absoluteUrl = text(job, "absolute_url");
        String canonicalUrl = absoluteUrl;
        String rawHtml = text(job, "content");
        String description = rawHtml == null ? null : org.jsoup.parser.Parser.unescapeEntities(rawHtml, false);
        String identifier = text(job, "id");
        String location = text(job.path("location"), "name");
        LocalDate datePosted = parseIsoDate(text(job, "updated_at"));
        String employmentType = extractGreenhouseEmploymentType(job.path("metadata"));
        String humanUrl = firstNonBlank(absoluteUrl, sourceUrl);
        String canonical = firstNonBlank(canonicalUrl, humanUrl, sourceUrl);
        return buildPosting(humanUrl, canonical, title, company.name(), location, employmentType, datePosted, description, identifier);
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
        String humanUrl = firstNonBlank(hostedUrl, applyUrl, sourceUrl);
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

        return buildPosting(humanUrl, firstNonBlank(canonicalUrl, humanUrl, sourceUrl), title, company.name(), location, employmentType, datePosted, description, identifier);
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
        if ((title == null || title.isBlank()) && (canonicalUrl == null || canonicalUrl.isBlank())) {
            return null;
        }

        String canonical = firstNonBlank(canonicalUrl, sourceUrl);
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
        if (host.contains("boards.greenhouse.io") || host.endsWith("greenhouse.io")) {
            if (!segments.isEmpty()) {
                return segments.getFirst();
            }
        }
        if (host.contains("api.greenhouse.io") && segments.size() >= 3 && "boards".equals(segments.get(1))) {
            return segments.get(2);
        }
        return null;
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
        if (host.contains("api.lever.co") && segments.size() >= 3 && "postings".equals(segments.get(1))) {
            return segments.get(2);
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

    List<NormalizedJobPosting> parseWorkdayJobPostings(String host, JsonNode root, String sourceUrl) {
        List<NormalizedJobPosting> postings = new ArrayList<>();
        JsonNode jobs = root.path("jobPostings");
        if (!jobs.isArray()) {
            return postings;
        }
        for (JsonNode job : jobs) {
            String title = firstNonBlank(text(job, "title"), text(job, "jobTitle"));
            String externalPath = firstNonBlank(text(job, "externalPath"), text(job, "externalUrl"));
            String canonicalUrl = toCanonicalWorkdayUrl(host, externalPath);
            String identifier = firstNonBlank(
                text(job, "bulletinId"),
                text(job, "id"),
                text(job, "jobReqId"),
                extractWorkdayIdFromPath(externalPath)
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

            String humanUrl = firstNonBlank(canonicalUrl, sourceUrl);
            NormalizedJobPosting posting = buildPosting(
                humanUrl,
                firstNonBlank(canonicalUrl, humanUrl, sourceUrl),
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
        if (segments.size() >= 2 && isLocaleSegment(segments.getFirst())) {
            site = segments.get(1);
        } else if (!segments.isEmpty()) {
            site = segments.getFirst();
        }
        if (site == null || site.isBlank()) {
            return null;
        }
        return new WorkdayEndpoint(host, tenant, site);
    }

    private boolean isLocaleSegment(String segment) {
        return segment != null && segment.matches("^[a-z]{2}-[A-Z]{2}$");
    }

    private String toCanonicalWorkdayUrl(String host, String externalPath) {
        if (externalPath == null || externalPath.isBlank()) {
            return null;
        }
        String trimmed = externalPath.trim();
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return trimmed;
        }
        if (!trimmed.startsWith("/")) {
            trimmed = "/" + trimmed;
        }
        return "https://" + host + trimmed;
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

    private String safe(String value) {
        return value == null ? "" : value.trim();
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
            url,
            DiscoveredUrlType.ATS_API,
            fetchStatus,
            Instant.now(),
            httpStatus,
            errorCode,
            atsType.name()
        );
    }

    private record WorkdayEndpoint(String host, String tenant, String site) {
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
