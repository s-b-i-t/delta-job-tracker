package com.delta.jobtracker.crawl.ats;

import com.delta.jobtracker.crawl.model.AtsDetectionRecord;
import com.delta.jobtracker.crawl.model.AtsType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AtsFingerprintFromSitemapsDetectorTest {
    private final AtsFingerprintFromSitemapsDetector detector =
        new AtsFingerprintFromSitemapsDetector(new AtsEndpointExtractor());

    @Test
    void detectsEndpointsFromSitemapUrls() {
        List<String> urls = List.of(
            "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/job/123",
            "https://boards.greenhouse.io/embed/job_board?for=rocket"
        );

        Map<AtsDetectionRecord, String> endpoints = detector.detect(urls);

        assertThat(endpoints)
            .containsEntry(
                new AtsDetectionRecord(AtsType.WORKDAY, "https://acme.wd5.myworkdayjobs.com/External"),
                "https://acme.wd5.myworkdayjobs.com/wday/cxs/acme/External/job/123"
            )
            .containsEntry(
                new AtsDetectionRecord(AtsType.GREENHOUSE, "https://boards.greenhouse.io/rocket"),
                "https://boards.greenhouse.io/embed/job_board?for=rocket"
            );
    }
}
