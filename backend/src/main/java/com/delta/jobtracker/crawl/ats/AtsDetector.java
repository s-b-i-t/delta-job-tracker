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
    if (host.contains("icims.com")) {
      return AtsType.ICIMS;
    }
    if (host.contains("taleo.net")) {
      return AtsType.TALEO;
    }
    if (host.contains("successfactors") || host.contains("jobs.sap.com")) {
      return AtsType.SUCCESSFACTORS;
    }
    if (host.contains("recruiting.paylocity.com")) {
      return AtsType.PAYLOCITY;
    }
    if (host.contains("brassring.com")) {
      return AtsType.BRASSRING;
    }
    if (host.contains("dayforcehcm.com")) {
      return AtsType.DAYFORCE;
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
    if (lower.contains("icims.com/jobs")
        || lower.contains("careers.icims.com")
        || lower.contains("jobs.icims.com")) {
      return AtsType.ICIMS;
    }
    if (lower.contains("taleo.net/careersection/") || lower.contains("taleo.net/careersection2/")) {
      return AtsType.TALEO;
    }
    if (lower.contains("successfactors")
        || lower.contains("career2.successfactors.eu")
        || lower.contains("jobs.sap.com")) {
      return AtsType.SUCCESSFACTORS;
    }
    if (lower.contains("recruiting.paylocity.com/recruiting/jobs/all")) {
      return AtsType.PAYLOCITY;
    }
    if (lower.contains("brassring.com/tgnewui/search/home/homewithpreload")) {
      return AtsType.BRASSRING;
    }
    if (lower.contains("dayforcehcm.com/candidateportal/")
        || lower.contains("careers.dayforcehcm.com/")
        || lower.contains("dayforcehcm.com/careers/")) {
      return AtsType.DAYFORCE;
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
