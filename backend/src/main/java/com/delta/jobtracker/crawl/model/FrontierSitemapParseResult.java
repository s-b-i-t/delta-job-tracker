package com.delta.jobtracker.crawl.model;

import java.util.List;

public record FrontierSitemapParseResult(List<String> childSitemaps, List<String> urls) {}
