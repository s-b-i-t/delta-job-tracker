package com.delta.jobtracker.crawl;

import com.delta.jobtracker.config.CrawlerProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrawlerPropertiesGuardrailTest {

    @Test
    void userAgentFallsBackToSafeDefault() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.setUserAgent("   ");
        assertTrue(properties.getUserAgent().startsWith("delta-job-tracker/0.1"));
    }

    @Test
    void concurrencyAndDelayAreClamped() {
        CrawlerProperties properties = new CrawlerProperties();
        properties.setGlobalConcurrency(0);
        properties.setPerHostDelayMs(-10);
        assertEquals(1, properties.getGlobalConcurrency());
        assertEquals(1, properties.getPerHostDelayMs());
    }
}
