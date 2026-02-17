package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.AtsType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class AtsEndpointNormalizationTest {

    @Autowired
    private CrawlJdbcRepository repository;

    @Autowired
    private NamedParameterJdbcTemplate jdbc;

    @Test
    void greenhouseApiHostIsCanonicalizedAndDeduped() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long companyId = repository.upsertCompany("GN" + suffix, "Greenhouse Norm " + suffix, "Technology");

        String apiUrl = "https://api.greenhouse.io/v1/boards/airbnb/jobs?content=true";
        String canonicalUrl = "https://boards-api.greenhouse.io/v1/boards/airbnb/jobs?content=true";

        repository.upsertAtsEndpoint(companyId, AtsType.GREENHOUSE, apiUrl, null, 0.9, Instant.now(), "test", true);
        repository.upsertAtsEndpoint(companyId, AtsType.GREENHOUSE, canonicalUrl, null, 0.9, Instant.now(), "test", true);

        String stored = jdbc.queryForObject(
            """
                SELECT ats_url
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                LIMIT 1
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            String.class
        );

        Integer count = jdbc.queryForObject(
            """
                SELECT COUNT(*)
                FROM ats_endpoints
                WHERE company_id = :companyId
                  AND ats_type = 'GREENHOUSE'
                """,
            new MapSqlParameterSource().addValue("companyId", companyId),
            Integer.class
        );

        assertEquals(canonicalUrl, stored);
        assertEquals(1, count);
    }
}
