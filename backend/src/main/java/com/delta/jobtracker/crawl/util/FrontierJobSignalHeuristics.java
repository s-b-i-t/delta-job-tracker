package com.delta.jobtracker.crawl.util;

import java.net.URI;
import java.util.List;
import java.util.Locale;
import org.springframework.stereotype.Component;

@Component
public class FrontierJobSignalHeuristics {
  private static final List<String> POSITIVE_HINTS =
      List.of(
          "/careers",
          "/career",
          "/jobs",
          "/job",
          "/join-us",
          "/opportunit",
          "/vacanc",
          "/open-positions",
          "/work-with-us");

  private static final List<String> NEGATIVE_EXTENSIONS =
      List.of(
          ".css", ".js", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".pdf", ".zip", ".xml",
          ".gz");

  public boolean isJobLike(String url) {
    URI uri = UrlClassifier.safeUri(url);
    if (uri == null || uri.getHost() == null) {
      return false;
    }
    String full =
        (uri.getHost() + (uri.getPath() == null ? "" : uri.getPath())).toLowerCase(Locale.ROOT);
    for (String ext : NEGATIVE_EXTENSIONS) {
      if (full.endsWith(ext)) {
        return false;
      }
    }
    if (UrlClassifier.isAtsHost(uri.getHost())) {
      return true;
    }
    String path = (uri.getPath() == null ? "" : uri.getPath()).toLowerCase(Locale.ROOT);
    for (String hint : POSITIVE_HINTS) {
      if (path.contains(hint)) {
        return true;
      }
    }
    String query = uri.getQuery() == null ? "" : uri.getQuery().toLowerCase(Locale.ROOT);
    return query.contains("job") || query.contains("career");
  }
}
