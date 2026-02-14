package com.delta.jobtracker.crawl.service;

import org.jsoup.nodes.Document;

import java.io.IOException;

public interface Sp500WikipediaClient {
    Document fetchConstituentsPage(String url, String userAgent, int timeoutMs) throws IOException;
}
