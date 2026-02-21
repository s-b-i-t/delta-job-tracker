package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.service.ActiveCrawlRunException;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class CrawlExceptionHandler {

  @ExceptionHandler(ActiveCrawlRunException.class)
  public ResponseEntity<Map<String, String>> handleActiveRun(ActiveCrawlRunException ex) {
    return ResponseEntity.status(HttpStatus.CONFLICT)
        .body(Map.of("error", "active_crawl_run", "message", ex.getMessage()));
  }
}
