package com.delta.jobtracker.crawl;

import com.delta.jobtracker.crawl.ats.AtsDetector;
import com.delta.jobtracker.crawl.model.AtsType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AtsDetectorTest {
    private final AtsDetector detector = new AtsDetector();

    @Test
    void detectsWorkday() {
        assertEquals(AtsType.WORKDAY, detector.detect("https://company.wd5.myworkdayjobs.com/en-US/External"));
    }

    @Test
    void detectsGreenhouse() {
        assertEquals(AtsType.GREENHOUSE, detector.detect("https://boards.greenhouse.io/example/jobs/123"));
    }

    @Test
    void detectsLever() {
        assertEquals(AtsType.LEVER, detector.detect("https://jobs.lever.co/example/abc"));
    }

    @Test
    void unknownForRegularCompanySite() {
        assertEquals(AtsType.UNKNOWN, detector.detect("https://example.com/careers"));
    }

    @Test
    void detectsFromHtmlMarkers() {
        String html = "<html><script>var x='https://api.lever.co/v0/postings/example?mode=json';</script></html>";
        assertEquals(AtsType.LEVER, detector.detect("https://example.com/careers", html));
    }
}
