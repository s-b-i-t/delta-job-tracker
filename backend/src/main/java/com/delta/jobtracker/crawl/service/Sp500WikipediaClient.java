package com.delta.jobtracker.crawl.service;

import java.io.IOException;
import org.jsoup.nodes.Document;

public interface Sp500WikipediaClient {
  Document fetchConstituentsPage(String url, String userAgent, int timeoutMs) throws IOException;
}
