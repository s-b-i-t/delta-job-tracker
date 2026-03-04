package com.delta.jobtracker.crawl.sitemap;

import com.delta.jobtracker.crawl.model.FrontierSitemapParseResult;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.springframework.stereotype.Component;

@Component
public class FrontierSitemapParser {

  public String extractXmlPayload(String requestedUrl, HttpFetchResult fetch) throws IOException {
    if (fetch == null) {
      return null;
    }
    byte[] bodyBytes = fetch.bodyBytes();
    if (bodyBytes == null && fetch.body() != null) {
      bodyBytes = fetch.body().getBytes(StandardCharsets.UTF_8);
    }
    if (bodyBytes == null) {
      return fetch.body();
    }
    if (isGzipPayload(requestedUrl, fetch, bodyBytes)) {
      try (GZIPInputStream gzipInputStream =
          new GZIPInputStream(new ByteArrayInputStream(bodyBytes))) {
        return new String(gzipInputStream.readAllBytes(), StandardCharsets.UTF_8);
      }
    }
    return new String(bodyBytes, StandardCharsets.UTF_8);
  }

  public FrontierSitemapParseResult parse(String xmlPayload, int maxUrls) {
    if (xmlPayload == null || xmlPayload.isBlank()) {
      return new FrontierSitemapParseResult(List.of(), List.of());
    }
    int safeMaxUrls = Math.max(1, maxUrls);
    Document xml = Jsoup.parse(xmlPayload, "", Parser.xmlParser());

    Set<String> childSitemaps = new LinkedHashSet<>();
    for (Element loc : xml.select("sitemap > loc")) {
      String url = normalizeUrl(loc.text());
      if (url != null) {
        childSitemaps.add(url);
      }
    }

    List<String> urls = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    for (Element loc : xml.select("url > loc")) {
      if (urls.size() >= safeMaxUrls) {
        break;
      }
      String url = normalizeUrl(loc.text());
      if (url == null || !seen.add(url)) {
        continue;
      }
      urls.add(url);
    }

    return new FrontierSitemapParseResult(new ArrayList<>(childSitemaps), urls);
  }

  private String normalizeUrl(String input) {
    if (input == null || input.isBlank()) {
      return null;
    }
    String value = input.trim();
    if (!value.startsWith("http://") && !value.startsWith("https://")) {
      value = "https://" + value;
    }
    if (!(value.toLowerCase(Locale.ROOT).startsWith("http://")
        || value.toLowerCase(Locale.ROOT).startsWith("https://"))) {
      return null;
    }
    return value;
  }

  private boolean isGzipPayload(String requestedUrl, HttpFetchResult fetch, byte[] bodyBytes) {
    String requested = requestedUrl == null ? "" : requestedUrl.toLowerCase(Locale.ROOT);
    String finalUrl =
        fetch.finalUrlOrRequested() == null
            ? ""
            : fetch.finalUrlOrRequested().toLowerCase(Locale.ROOT);
    if (requested.endsWith(".gz") || finalUrl.endsWith(".gz")) {
      return true;
    }
    String contentEncoding = fetch.contentEncoding();
    if (contentEncoding != null && contentEncoding.toLowerCase(Locale.ROOT).contains("gzip")) {
      return true;
    }
    return bodyBytes.length >= 2 && (bodyBytes[0] & 0xFF) == 0x1f && (bodyBytes[1] & 0xFF) == 0x8b;
  }
}
