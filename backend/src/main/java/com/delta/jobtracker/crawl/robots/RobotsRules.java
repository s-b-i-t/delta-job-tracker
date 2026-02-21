package com.delta.jobtracker.crawl.robots;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class RobotsRules {
  private final List<Rule> rules;
  private final List<String> sitemapUrls;

  public RobotsRules(List<Rule> rules, List<String> sitemapUrls) {
    this.rules = rules;
    this.sitemapUrls = sitemapUrls;
  }

  public static RobotsRules allowAll() {
    return new RobotsRules(List.of(), List.of());
  }

  public static RobotsRules disallowAll() {
    return new RobotsRules(List.of(new Rule("/", false)), List.of());
  }

  public List<String> getSitemapUrls() {
    return sitemapUrls;
  }

  public boolean isAllowed(String pathAndQuery) {
    if (rules.isEmpty()) {
      return true;
    }

    Rule bestMatch = null;
    int bestMatchLength = -1;
    String subject = pathAndQuery == null || pathAndQuery.isBlank() ? "/" : pathAndQuery;
    for (Rule rule : rules) {
      if (!rule.matches(subject)) {
        continue;
      }
      int length = rule.path().length();
      if (length > bestMatchLength) {
        bestMatch = rule;
        bestMatchLength = length;
      } else if (length == bestMatchLength
          && bestMatch != null
          && rule.allow()
          && !bestMatch.allow()) {
        bestMatch = rule;
      }
    }
    return bestMatch == null || bestMatch.allow();
  }

  public static RobotsRules parse(String robotsText) {
    if (robotsText == null || robotsText.isBlank()) {
      return allowAll();
    }

    List<String> sitemaps = new ArrayList<>();
    List<Rule> parsedRules = new ArrayList<>();

    List<String> currentAgents = new ArrayList<>();
    boolean currentGroupRelevant = false;
    boolean lastDirectiveWasUserAgent = false;

    String[] lines = robotsText.split("\\R");
    for (String rawLine : lines) {
      String noComment = stripComment(rawLine);
      String line = noComment.trim();
      if (line.isEmpty()) {
        currentAgents.clear();
        currentGroupRelevant = false;
        lastDirectiveWasUserAgent = false;
        continue;
      }
      int colonIdx = line.indexOf(':');
      if (colonIdx <= 0) {
        continue;
      }

      String key = line.substring(0, colonIdx).trim().toLowerCase(Locale.ROOT);
      String value = line.substring(colonIdx + 1).trim();

      if ("user-agent".equals(key)) {
        if (!lastDirectiveWasUserAgent) {
          currentAgents.clear();
        }
        currentAgents.add(value.toLowerCase(Locale.ROOT));
        currentGroupRelevant = currentAgents.stream().anyMatch("*"::equals);
        lastDirectiveWasUserAgent = true;
        continue;
      }

      lastDirectiveWasUserAgent = false;
      if ("sitemap".equals(key)) {
        if (!value.isBlank()) {
          sitemaps.add(value);
        }
        continue;
      }

      if (!currentGroupRelevant) {
        continue;
      }
      if (("allow".equals(key) || "disallow".equals(key)) && !value.isBlank()) {
        parsedRules.add(new Rule(value, "allow".equals(key)));
      }
    }

    return new RobotsRules(parsedRules, sitemaps);
  }

  private static String stripComment(String line) {
    int idx = line.indexOf('#');
    return idx >= 0 ? line.substring(0, idx) : line;
  }

  public record Rule(String path, boolean allow) {
    public boolean matches(String testPath) {
      String normalizedPath = path.startsWith("/") ? path : "/" + path;
      if (!normalizedPath.contains("*") && !normalizedPath.contains("$")) {
        return testPath.startsWith(normalizedPath);
      }
      StringBuilder regex = new StringBuilder("^");
      for (int i = 0; i < normalizedPath.length(); i++) {
        char c = normalizedPath.charAt(i);
        if (c == '*') {
          regex.append(".*");
        } else if (c == '$') {
          regex.append("$");
        } else {
          regex.append(Pattern.quote(Character.toString(c)));
        }
      }
      return Pattern.compile(regex.toString()).matcher(testPath).find();
    }
  }
}
