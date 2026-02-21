package com.delta.jobtracker.crawl.http;

public class CanaryAbortException extends RuntimeException {
  public CanaryAbortException(String message) {
    super(message);
  }
}
