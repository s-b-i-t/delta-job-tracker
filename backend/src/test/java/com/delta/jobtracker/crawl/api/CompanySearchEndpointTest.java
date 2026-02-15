package com.delta.jobtracker.crawl.api;

import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class CompanySearchEndpointTest {

    @Autowired
    private CrawlController controller;

    @Autowired
    private CrawlJdbcRepository repository;

    @Test
    void companySearchOrdersExactTickerFirst() {
        String suffix = String.valueOf(System.nanoTime());
        String searchTerm = "air" + suffix;
        long companyAir = repository.upsertCompany(searchTerm.toUpperCase(), "Air Inc " + searchTerm, "Technology");
        long companyAirbnb = repository.upsertCompany("AB" + suffix.substring(0, 4), "Airbnb " + searchTerm, "Technology");

        repository.upsertCompanyDomain(companyAir, "air.example.com", null, "test", 1.0, Instant.now());
        repository.upsertCompanyDomain(companyAirbnb, "airbnb.com", null, "test", 1.0, Instant.now());

        var results = controller.searchCompanies(searchTerm, 20);
        assertEquals(2, results.size());
        assertEquals(companyAir, results.getFirst().id());
        assertEquals(searchTerm.toUpperCase(), results.getFirst().ticker());
    }
}
