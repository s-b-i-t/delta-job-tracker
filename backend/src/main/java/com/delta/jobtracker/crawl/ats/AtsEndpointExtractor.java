package com.delta.jobtracker.crawl.ats;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class AtsEndpointExtractor {
    private static final Pattern GREENHOUSE_BOARD = Pattern.compile("(?i)(?:https?:)?//boards\\.greenhouse\\.io/([A-Za-z0-9._-]+)");
    private static final Pattern GREENHOUSE_JOB_BOARDS = Pattern.compile("(?i)(?:https?:)?//job-boards\\.greenhouse\\.io/([A-Za-z0-9._-]+)");
    private static final Pattern GREENHOUSE_API = Pattern.compile("(?i)(?:https?:)?//(?:boards-api|api)\\.greenhouse\\.io/v1/boards/([A-Za-z0-9._-]+)");
    private static final Pattern GREENHOUSE_EMBED = Pattern.compile("(?i)(?:https?:)?//boards\\.greenhouse\\.io/embed/job_board[^\"'\\s>]*");
    private static final Pattern GREENHOUSE_SHORT = Pattern.compile("(?i)(?:https?:)?//grnh\\.se/([A-Za-z0-9._-]+)");
    private static final Pattern LEVER_JOBS = Pattern.compile("(?i)(?:https?:)?//jobs\\.lever\\.co/([A-Za-z0-9._-]+)");
    private static final Pattern LEVER_APPLY = Pattern.compile("(?i)(?:https?:)?//apply\\.lever\\.co/([A-Za-z0-9._-]+)");
    private static final Pattern LEVER_API = Pattern.compile("(?i)(?:https?:)?//api\\.lever\\.co/v0/postings/([A-Za-z0-9._-]+)");
    private static final Pattern WORKDAY = Pattern.compile("(?i)(?:https?:)?//([A-Za-z0-9-]+\\.[A-Za-z0-9.-]*myworkdayjobs\\.com)(/[^\"'\\s<>]*)?");
    private static final Pattern SMARTRECRUITERS_JOBS = Pattern.compile("(?i)(?:https?:)?//jobs\\.smartrecruiters\\.com/([A-Za-z0-9._-]+)");
    private static final Pattern SMARTRECRUITERS_CAREERS = Pattern.compile("(?i)(?:https?:)?//careers\\.smartrecruiters\\.com/([A-Za-z0-9._-]+)");
    private static final Pattern SMARTRECRUITERS_WEB = Pattern.compile("(?i)(?:https?:)?//www\\.smartrecruiters\\.com/([A-Za-z0-9._-]+)");
    private static final Pattern SMARTRECRUITERS_API = Pattern.compile("(?i)(?:https?:)?//api\\.smartrecruiters\\.com/v1/companies/([A-Za-z0-9._-]+)");

    public List<AtsDetectionRecord> extract(String url, String html) {
        Map<String, AtsDetectionRecord> unique = new LinkedHashMap<>();
        extractFromText(url, unique);
        extractFromText(html, unique);
        return new ArrayList<>(unique.values());
    }

    public List<String> extractGreenhouseShortLinks(String html) {
        List<String> links = new ArrayList<>();
        if (html == null || html.isBlank()) {
            return links;
        }
        Matcher matcher = GREENHOUSE_SHORT.matcher(html);
        while (matcher.find()) {
            String token = cleanToken(matcher.group(1));
            if (token != null) {
                links.add("https://grnh.se/" + token);
            }
        }
        return links;
    }

    private void extractFromText(String text, Map<String, AtsDetectionRecord> unique) {
        if (text == null || text.isBlank()) {
            return;
        }
        Matcher boardMatcher = GREENHOUSE_BOARD.matcher(text);
        while (boardMatcher.find()) {
            String token = cleanToken(boardMatcher.group(1));
            if (token == null || "embed".equalsIgnoreCase(token)) {
                continue;
            }
            addEndpoint(unique, AtsType.GREENHOUSE, "https://boards.greenhouse.io/" + token);
        }

        Matcher jobBoardsMatcher = GREENHOUSE_JOB_BOARDS.matcher(text);
        while (jobBoardsMatcher.find()) {
            String token = cleanToken(jobBoardsMatcher.group(1));
            if (token == null || "embed".equalsIgnoreCase(token)) {
                continue;
            }
            addEndpoint(unique, AtsType.GREENHOUSE, "https://boards.greenhouse.io/" + token);
        }

        Matcher apiMatcher = GREENHOUSE_API.matcher(text);
        while (apiMatcher.find()) {
            String token = cleanToken(apiMatcher.group(1));
            if (token != null) {
                addEndpoint(unique, AtsType.GREENHOUSE, "https://boards.greenhouse.io/" + token);
            }
        }

        Matcher embedMatcher = GREENHOUSE_EMBED.matcher(text);
        while (embedMatcher.find()) {
            String token = extractQueryParam(embedMatcher.group(), "for");
            token = cleanToken(token);
            if (token != null) {
                addEndpoint(unique, AtsType.GREENHOUSE, "https://boards.greenhouse.io/" + token);
            }
        }

        Matcher leverMatcher = LEVER_JOBS.matcher(text);
        while (leverMatcher.find()) {
            String account = cleanToken(leverMatcher.group(1));
            if (account != null) {
                addEndpoint(unique, AtsType.LEVER, "https://jobs.lever.co/" + account);
            }
        }

        Matcher leverApplyMatcher = LEVER_APPLY.matcher(text);
        while (leverApplyMatcher.find()) {
            String account = cleanToken(leverApplyMatcher.group(1));
            if (account != null) {
                addEndpoint(unique, AtsType.LEVER, "https://jobs.lever.co/" + account);
            }
        }

        Matcher leverApiMatcher = LEVER_API.matcher(text);
        while (leverApiMatcher.find()) {
            String account = cleanToken(leverApiMatcher.group(1));
            if (account != null) {
                addEndpoint(unique, AtsType.LEVER, "https://jobs.lever.co/" + account);
            }
        }

        Matcher workdayMatcher = WORKDAY.matcher(text);
        while (workdayMatcher.find()) {
            String host = cleanToken(workdayMatcher.group(1));
            String path = workdayMatcher.group(2);
            String normalized = normalizeWorkdayEndpoint(host, path);
            if (normalized != null) {
                addEndpoint(unique, AtsType.WORKDAY, normalized);
            }
        }

        addSmartRecruitersEndpoints(text, unique);
    }

    private void addSmartRecruitersEndpoints(String text, Map<String, AtsDetectionRecord> unique) {
        Matcher jobsMatcher = SMARTRECRUITERS_JOBS.matcher(text);
        while (jobsMatcher.find()) {
            String company = cleanToken(jobsMatcher.group(1));
            String endpoint = smartRecruitersEndpoint(company);
            if (endpoint != null) {
                addEndpoint(unique, AtsType.SMARTRECRUITERS, endpoint);
            }
        }
        Matcher careersMatcher = SMARTRECRUITERS_CAREERS.matcher(text);
        while (careersMatcher.find()) {
            String company = cleanToken(careersMatcher.group(1));
            String endpoint = smartRecruitersEndpoint(company);
            if (endpoint != null) {
                addEndpoint(unique, AtsType.SMARTRECRUITERS, endpoint);
            }
        }
        Matcher webMatcher = SMARTRECRUITERS_WEB.matcher(text);
        while (webMatcher.find()) {
            String company = cleanToken(webMatcher.group(1));
            String endpoint = smartRecruitersEndpoint(company);
            if (endpoint != null) {
                addEndpoint(unique, AtsType.SMARTRECRUITERS, endpoint);
            }
        }
        Matcher apiMatcher = SMARTRECRUITERS_API.matcher(text);
        while (apiMatcher.find()) {
            String company = cleanToken(apiMatcher.group(1));
            String endpoint = smartRecruitersEndpoint(company);
            if (endpoint != null) {
                addEndpoint(unique, AtsType.SMARTRECRUITERS, endpoint);
            }
        }
    }

    private void addEndpoint(Map<String, AtsDetectionRecord> unique, AtsType type, String endpointUrl) {
        if (endpointUrl == null || endpointUrl.isBlank() || type == null || type == AtsType.UNKNOWN) {
            return;
        }
        String normalized = normalizeEndpointUrl(endpointUrl);
        if (normalized == null) {
            return;
        }
        String key = type.name() + "|" + normalized.toLowerCase(Locale.ROOT);
        unique.putIfAbsent(key, new AtsDetectionRecord(type, normalized));
    }

    private String normalizeEndpointUrl(String raw) {
        URI uri = safeUri(raw);
        if (uri == null || uri.getHost() == null) {
            return null;
        }
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        if (host.equals("job-boards.greenhouse.io")) {
            host = "boards.greenhouse.io";
        }
        String path = uri.getPath() == null ? "" : uri.getPath();
        String normalized = "https://" + host + path;
        if (normalized.endsWith("/") && normalized.length() > "https://x/".length()) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private String normalizeWorkdayEndpoint(String host, String rawPath) {
        if (host == null || host.isBlank()) {
            return null;
        }
        String cleanedHost = host.toLowerCase(Locale.ROOT);
        String path = rawPath == null ? "" : stripQueryAndFragment(rawPath);
        path = stripTrailingPunctuation(path);
        List<String> segments = pathSegments(path);
        if (segments.isEmpty()) {
            return null;
        }
        String site;
        String locale = null;
        if (segments.size() >= 4 && "wday".equalsIgnoreCase(segments.get(0)) && "cxs".equalsIgnoreCase(segments.get(1))) {
            site = segments.get(3);
        } else if (segments.size() >= 2 && isLocaleSegment(segments.get(0))) {
            locale = segments.get(0);
            site = segments.get(1);
        } else {
            site = segments.get(0);
        }
        site = stripTrailingPunctuation(site);
        if (site == null || site.isBlank()) {
            return null;
        }
        String normalizedPath = locale == null ? "/" + site : "/" + locale + "/" + site;
        return "https://" + cleanedHost + normalizedPath;
    }

    private String smartRecruitersEndpoint(String company) {
        if (company == null || company.isBlank()) {
            return null;
        }
        return "https://careers.smartrecruiters.com/" + company;
    }

    private boolean isLocaleSegment(String segment) {
        return segment != null && segment.matches("^[a-z]{2}-[A-Z]{2}$");
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

    private String stripQueryAndFragment(String value) {
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

    private String extractQueryParam(String rawUrl, String param) {
        URI uri = safeUri(rawUrl);
        if (uri == null) {
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
            if (key.equalsIgnoreCase(param)) {
                return value;
            }
        }
        return null;
    }

    private String cleanToken(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.trim();
        if (cleaned.isBlank()) {
            return null;
        }
        cleaned = stripTrailingPunctuation(cleaned);
        return cleaned.isBlank() ? null : cleaned;
    }

    private URI safeUri(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        String value = raw.trim();
        if (value.startsWith("//")) {
            value = "https:" + value;
        }
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
    }
}
