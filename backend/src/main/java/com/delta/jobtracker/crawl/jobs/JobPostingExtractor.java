package com.delta.jobtracker.crawl.jobs;

import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.delta.jobtracker.crawl.util.HashUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Component
public class JobPostingExtractor {
    private final ObjectMapper objectMapper;

    public JobPostingExtractor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<NormalizedJobPosting> extract(String html, String sourceUrl) {
        if (html == null || html.isBlank()) {
            return List.of();
        }

        Document document = Jsoup.parse(html);
        List<JsonNode> jobPostingNodes = new ArrayList<>();
        for (Element script : document.select("script[type=application/ld+json]")) {
            String payload = script.data();
            if (payload == null || payload.isBlank()) {
                payload = script.html();
            }
            if (payload == null || payload.isBlank()) {
                continue;
            }
            try {
                JsonNode root = objectMapper.readTree(payload);
                collectJobPostingNodes(root, jobPostingNodes);
            } catch (JsonProcessingException ignored) {
                // Ignore malformed JSON-LD blobs and continue extracting from others.
            }
        }

        List<NormalizedJobPosting> postings = new ArrayList<>();
        for (JsonNode node : jobPostingNodes) {
            postings.add(normalize(node, sourceUrl));
        }
        return postings;
    }

    private void collectJobPostingNodes(JsonNode node, List<JsonNode> out) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isObject()) {
            if (isJobPostingType(node.get("@type"))) {
                out.add(node);
            }
            node.fields().forEachRemaining(entry -> {
                JsonNode value = entry.getValue();
                if (value.isArray() || value.isObject()) {
                    collectJobPostingNodes(value, out);
                }
            });
            return;
        }
        if (node.isArray()) {
            for (JsonNode child : node) {
                collectJobPostingNodes(child, out);
            }
        }
    }

    private boolean isJobPostingType(JsonNode typeNode) {
        if (typeNode == null || typeNode.isNull()) {
            return false;
        }
        if (typeNode.isTextual()) {
            return "jobposting".equalsIgnoreCase(typeNode.asText());
        }
        if (typeNode.isArray()) {
            for (JsonNode child : typeNode) {
                if (child.isTextual() && "jobposting".equalsIgnoreCase(child.asText())) {
                    return true;
                }
            }
        }
        return false;
    }

    private NormalizedJobPosting normalize(JsonNode node, String sourceUrl) {
        String title = firstNonBlank(text(node, "title"), text(node, "name"));
        String orgName = text(node.path("hiringOrganization"), "name");
        String location = extractLocation(node.get("jobLocation"));
        String employmentType = extractEmploymentType(node.get("employmentType"));
        LocalDate datePosted = parseDate(text(node, "datePosted"));
        String description = text(node, "description");
        String identifier = extractIdentifier(node.get("identifier"));
        String canonicalUrl = firstNonBlank(text(node, "url"), sourceUrl);

        Map<String, String> stableFields = new TreeMap<>();
        stableFields.put("title", blankToEmpty(title));
        stableFields.put("org_name", blankToEmpty(orgName));
        stableFields.put("location_text", blankToEmpty(location));
        stableFields.put("employment_type", blankToEmpty(employmentType));
        stableFields.put("date_posted", datePosted == null ? "" : datePosted.toString());
        stableFields.put("description", blankToEmpty(description));
        stableFields.put("canonical_url", blankToEmpty(canonicalUrl));
        stableFields.put("identifier", blankToEmpty(identifier));

        String hashPayload;
        try {
            hashPayload = objectMapper.writeValueAsString(stableFields);
        } catch (JsonProcessingException e) {
            hashPayload = stableFields.toString();
        }

        return new NormalizedJobPosting(
            sourceUrl,
            canonicalUrl,
            title,
            orgName,
            location,
            employmentType,
            datePosted,
            description,
            identifier,
            HashUtils.sha256Hex(hashPayload)
        );
    }

    private String extractIdentifier(JsonNode identifierNode) {
        if (identifierNode == null || identifierNode.isNull()) {
            return null;
        }
        if (identifierNode.isTextual() || identifierNode.isNumber()) {
            return identifierNode.asText();
        }
        if (identifierNode.isObject()) {
            String value = firstNonBlank(
                text(identifierNode, "value"),
                text(identifierNode, "name"),
                text(identifierNode, "propertyID")
            );
            return value;
        }
        return identifierNode.toString();
    }

    private String extractLocation(JsonNode jobLocation) {
        if (jobLocation == null || jobLocation.isNull()) {
            return null;
        }
        LinkedHashSet<String> locations = new LinkedHashSet<>();
        collectLocationStrings(jobLocation, locations);
        if (locations.isEmpty()) {
            return null;
        }
        return String.join(" | ", locations);
    }

    private void collectLocationStrings(JsonNode node, LinkedHashSet<String> out) {
        if (node == null || node.isNull()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                collectLocationStrings(item, out);
            }
            return;
        }
        if (!node.isObject()) {
            if (node.isTextual()) {
                String val = node.asText().trim();
                if (!val.isEmpty()) {
                    out.add(val);
                }
            }
            return;
        }

        JsonNode address = node.has("address") ? node.get("address") : node;
        List<String> parts = new ArrayList<>();
        addIfPresent(parts, text(address, "addressLocality"));
        addIfPresent(parts, text(address, "addressRegion"));
        addIfPresent(parts, text(address, "addressCountry"));

        if (!parts.isEmpty()) {
            out.add(String.join(", ", parts));
            return;
        }

        String fallback = firstNonBlank(
            text(node, "name"),
            text(address, "name")
        );
        if (fallback != null) {
            out.add(fallback);
        }
    }

    private String extractEmploymentType(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isArray()) {
            List<String> values = new ArrayList<>();
            for (JsonNode item : node) {
                if (item.isTextual()) {
                    values.add(item.asText());
                }
            }
            if (!values.isEmpty()) {
                return values.stream().distinct().collect(Collectors.joining(", "));
            }
        }
        return node.toString();
    }

    private LocalDate parseDate(String rawDate) {
        if (rawDate == null || rawDate.isBlank()) {
            return null;
        }
        String candidate = rawDate.trim();
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
            return value.asText().trim();
        }
        return value.toString();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return null;
    }

    private void addIfPresent(List<String> list, String value) {
        if (value != null && !value.isBlank()) {
            list.add(value.trim());
        }
    }

    private String blankToEmpty(String value) {
        return value == null ? "" : value.trim();
    }
}
