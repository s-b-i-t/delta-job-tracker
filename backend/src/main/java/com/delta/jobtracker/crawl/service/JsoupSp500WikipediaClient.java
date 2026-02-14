package com.delta.jobtracker.crawl.service;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class JsoupSp500WikipediaClient implements Sp500WikipediaClient {
    @Override
    public Document fetchConstituentsPage(String url, String userAgent, int timeoutMs) throws IOException {
        return Jsoup.connect(url)
            .userAgent(userAgent)
            .timeout(timeoutMs)
            .get();
    }
}
