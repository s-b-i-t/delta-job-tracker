package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.delta.jobtracker.crawl.robots.RobotsTxtService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class WorkdayUrlHelperTest {

    @Mock
    private PoliteHttpClient httpClient;
    @Mock
    private RobotsTxtService robotsTxtService;
    @Mock
    private CrawlJdbcRepository repository;

    private AtsAdapterIngestionService service;

    @BeforeEach
    void setUp() {
        service = new AtsAdapterIngestionService(httpClient, robotsTxtService, repository, new ObjectMapper());
    }

    @Test
    void toCanonicalWorkdayUrlAcceptsAbsoluteWorkdayUrl() {
        String url = service.toCanonicalWorkdayUrl(
            "acme.wd5.myworkdayjobs.com",
            "External",
            "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Engineer_R123"
        );
        assertThat(url).isEqualTo("https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Engineer_R123");
    }

    @Test
    void toCanonicalWorkdayUrlRejectsInvalidHost() {
        String url = service.toCanonicalWorkdayUrl(
            "acme.wd5.myworkdayjobs.com",
            "External",
            "https://community.workday.com/invalid-url"
        );
        assertThat(url).isNull();
    }

    @Test
    void buildWorkdayUrlCandidatesInjectsLocaleAndSite() {
        List<String> candidates = service.buildWorkdayUrlCandidates(
            "acme.wd5.myworkdayjobs.com",
            "External",
            "External/job/Remote/Engineer_R123"
        );
        assertThat(candidates).isNotEmpty();
        assertThat(candidates.getFirst()).isEqualTo(
            "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Engineer_R123"
        );
        assertThat(candidates).contains(
            "https://acme.wd5.myworkdayjobs.com/External/job/Remote/Engineer_R123"
        );
    }

    @Test
    void buildWorkdayUrlCandidatesUsesSiteWhenMissing() {
        List<String> candidates = service.buildWorkdayUrlCandidates(
            "acme.wd5.myworkdayjobs.com",
            "External",
            "job/Remote/Engineer_R123"
        );
        assertThat(candidates).isNotEmpty();
        assertThat(candidates.getFirst()).isEqualTo(
            "https://acme.wd5.myworkdayjobs.com/en-US/External/job/Remote/Engineer_R123"
        );
    }
}
