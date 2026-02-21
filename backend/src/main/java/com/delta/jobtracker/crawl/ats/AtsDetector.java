package com.delta.jobtracker.crawl.ats;

import com.delta.jobtracker.crawl.model.AtsType;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import org.springframework.stereotype.Component;

@Component
public class AtsDetector {
  public AtsType detect(String url) {
    if (url == null || url.isBlank()) {
      return AtsType.UNKNOWN;
    }
    String host = extractHost(url);
    if (host == null) {
      return AtsType.UNKNOWN;
    }

    if (host.endsWith("myworkdayjobs.com") || host.contains("workdayjobs")) {
      return AtsType.WORKDAY;
    }
    if (host.contains("boards.greenhouse.io")
        || host.contains("boards-api.greenhouse.io")
        || host.contains("api.greenhouse.io")
        || host.contains("greenhouse.io")
        || host.contains("grnh.se")) {
      return AtsType.GREENHOUSE;
    }
    if (host.contains("jobs.lever.co") || host.contains("api.lever.co")) {
      return AtsType.LEVER;
    }
    if (host.contains("apply.lever.co")) {
      return AtsType.LEVER;
    }
    if (host.contains("smartrecruiters.com")) {
      return AtsType.SMARTRECRUITERS;
    }
    return AtsType.UNKNOWN;
  }

  public AtsType detectFromHtml(String html) {
    if (html == null || html.isBlank()) {
      return AtsType.UNKNOWN;
    }
    String lower = html.toLowerCase(Locale.ROOT);
    if (lower.contains("myworkdayjobs.com")
        || lower.contains("workdayjobs")
        || lower.contains("/wday/cxs/")) {
      return AtsType.WORKDAY;
    }
    if (lower.contains("boards.greenhouse.io")
        || lower.contains("api.greenhouse.io/v1/boards/")
        || lower.contains("boards-api.greenhouse.io/v1/boards/")
        || lower.contains("greenhouse.io")
        || lower.contains("grnh.se/")
        || lower.contains("greenhouse.io/embed/job_board")) {
      return AtsType.GREENHOUSE;
    }
    if (lower.contains("jobs.lever.co")
        || lower.contains("api.lever.co/v0/postings/")
        || lower.contains("apply.lever.co")
        || lower.contains("lever.co")) {
      return AtsType.LEVER;
    }
    if (lower.contains("smartrecruiters.com")
        || lower.contains("api.smartrecruiters.com/v1/companies/")) {
      return AtsType.SMARTRECRUITERS;
    }
    return AtsType.UNKNOWN;
  }

  public AtsType detect(String url, String html) {
    AtsType byUrl = detect(url);
    if (byUrl != AtsType.UNKNOWN) {
      return byUrl;
    }
    return detectFromHtml(html);
  }

  private String extractHost(String url) {
    try {
      URI uri = new URI(url);
      if (uri.getHost() != null) {
        return uri.getHost().toLowerCase(Locale.ROOT);
      }
      URI withHttps = new URI("https://" + url);
      return withHttps.getHost() == null ? null : withHttps.getHost().toLowerCase(Locale.ROOT);
    } catch (URISyntaxException ignored) {
      return null;
    }
  }
}
