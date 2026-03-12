package com.delta.jobtracker.crawl.service;

import java.net.URI;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.springframework.stereotype.Component;

@Component
class CareersLandingLinkExtractor {
  private static final List<String> KEYWORDS =
      List.of(
          "careers",
          "career",
          "jobs",
          "job",
          "openings",
          "opening",
          "search",
          "current-openings",
          "join",
          "talent",
          "opportunit",
          "work-with-us",
          "open-positions");

  List<String> extractRanked(String html, String baseUrl, int maxCandidates) {
    if (html == null
        || html.isBlank()
        || baseUrl == null
        || baseUrl.isBlank()
        || maxCandidates <= 0) {
      return List.of();
    }
    Document doc = Jsoup.parse(html, baseUrl);
    String baseHost = normalizedHost(baseUrl);
    Map<String, CandidateScore> bestByUrl = new LinkedHashMap<>();
    for (Element anchor : doc.select("a[href]")) {
      String href = anchor.attr("abs:href");
      if (href == null || href.isBlank()) {
        continue;
      }
      String normalized = normalizeHttpUrl(href);
      if (normalized == null) {
        continue;
      }
      if (!isSameSiteHost(baseHost, normalizedHost(normalized))) {
        continue;
      }
      String text = anchor.text() == null ? "" : anchor.text();
      int score = scoreCandidate(normalized, text);
      if (score <= 0) {
        continue;
      }
      CandidateScore current = bestByUrl.get(normalized);
      if (current == null || score > current.score()) {
        bestByUrl.put(normalized, new CandidateScore(normalized, score));
      }
    }
    return bestByUrl.values().stream()
        .sorted(
            Comparator.comparingInt(CandidateScore::score)
                .reversed()
                .thenComparing(CandidateScore::url))
        .limit(maxCandidates)
        .map(CandidateScore::url)
        .toList();
  }

  private int scoreCandidate(String normalizedUrl, String anchorText) {
    String urlLower = normalizedUrl.toLowerCase(Locale.ROOT);
    String textLower = anchorText == null ? "" : anchorText.toLowerCase(Locale.ROOT);
    int score = 0;
    for (String keyword : KEYWORDS) {
      if (urlLower.contains(keyword)) {
        score += 6;
      }
      if (textLower.contains(keyword)) {
        score += 4;
      }
    }
    if (urlLower.contains("/careers") || urlLower.contains("/jobs")) {
      score += 4;
    }
    if (urlLower.contains("/openings")
        || urlLower.contains("/search")
        || urlLower.contains("current-openings")) {
      score += 4;
    }
    if (urlLower.contains("jobs.") || urlLower.contains("careers.")) {
      score += 3;
    }
    return score;
  }

  private String normalizeHttpUrl(String raw) {
    try {
      URI uri = URI.create(raw.trim());
      String scheme = uri.getScheme();
      String host = uri.getHost();
      if (scheme == null || host == null) {
        return null;
      }
      String schemeLower = scheme.toLowerCase(Locale.ROOT);
      if (!schemeLower.equals("http") && !schemeLower.equals("https")) {
        return null;
      }
      String path = uri.getPath() == null ? "" : uri.getPath();
      String query = uri.getRawQuery();
      String normalized = schemeLower + "://" + host.toLowerCase(Locale.ROOT) + path;
      if (query != null && !query.isBlank()) {
        normalized += "?" + query;
      }
      return normalized;
    } catch (Exception ignored) {
      return null;
    }
  }

  private String normalizedHost(String rawUrl) {
    if (rawUrl == null || rawUrl.isBlank()) {
      return null;
    }
    try {
      URI uri = URI.create(rawUrl.trim());
      return uri.getHost() == null ? null : uri.getHost().toLowerCase(Locale.ROOT);
    } catch (Exception ignored) {
      return null;
    }
  }

  private boolean isSameSiteHost(String baseHost, String candidateHost) {
    if (baseHost == null || candidateHost == null) {
      return false;
    }
    return candidateHost.equals(baseHost)
        || candidateHost.endsWith("." + baseHost)
        || baseHost.endsWith("." + candidateHost);
  }

  Set<String> defaultFallbackCandidates(String domain) {
    if (domain == null || domain.isBlank()) {
      return Set.of();
    }
    String d = domain.trim().toLowerCase(Locale.ROOT);
    LinkedHashSet<String> out = new LinkedHashSet<>();
    for (String path :
        List.of("/careers", "/careers/", "/jobs", "/jobs/", "/about/careers", "/careers/jobs")) {
      out.add("https://" + d + path);
    }
    out.add("https://careers." + d + "/");
    out.add("https://jobs." + d + "/");
    return out;
  }

  private record CandidateScore(String url, int score) {}
}
