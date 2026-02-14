package com.delta.jobtracker.crawl;

import com.delta.jobtracker.crawl.jobs.JobPostingExtractor;
import com.delta.jobtracker.crawl.model.NormalizedJobPosting;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobPostingExtractorTest {
    private final JobPostingExtractor extractor = new JobPostingExtractor(new ObjectMapper());

    @Test
    void extractsJobPostingFromJsonLd() {
        String html =
            """
                <html>
                <head>
                  <script type="application/ld+json">
                    {
                      "@context":"https://schema.org",
                      "@type":"JobPosting",
                      "title":"Senior Engineer",
                      "hiringOrganization":{"@type":"Organization","name":"Example Corp"},
                      "jobLocation":{"@type":"Place","address":{"addressLocality":"Austin","addressRegion":"TX","addressCountry":"US"}},
                      "employmentType":"FULL_TIME",
                      "datePosted":"2026-01-05",
                      "description":"Build services",
                      "identifier":{"@type":"PropertyValue","value":"REQ-123"},
                      "url":"https://example.com/jobs/req-123"
                    }
                  </script>
                </head>
                <body></body>
                </html>
                """;

        List<NormalizedJobPosting> jobs = extractor.extract(html, "https://example.com/jobs/req-123");
        assertEquals(1, jobs.size());
        NormalizedJobPosting posting = jobs.getFirst();
        assertEquals("Senior Engineer", posting.title());
        assertEquals("Example Corp", posting.orgName());
        assertEquals("Austin, TX, US", posting.locationText());
        assertEquals("FULL_TIME", posting.employmentType());
        assertNotNull(posting.datePosted());
        assertFalse(posting.contentHash().isBlank());
    }
}
