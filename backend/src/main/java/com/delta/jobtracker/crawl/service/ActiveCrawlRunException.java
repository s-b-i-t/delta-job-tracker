package com.delta.jobtracker.crawl.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class ActiveCrawlRunException extends RuntimeException {
    public ActiveCrawlRunException(String message) {
        super(message);
    }
}
