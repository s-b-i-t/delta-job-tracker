package com.delta.jobtracker.crawl.persistence;

import com.delta.jobtracker.crawl.model.AtsType;
import com.delta.jobtracker.crawl.model.CompanyTarget;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
class CrawlTargetSelectionTest {

    @Autowired
    private CrawlJdbcRepository repository;

    @Test
    void atsOnlySelectionReturnsOnlyAtsBackedCompanies() {
        String suffix = UUID.randomUUID().toString().substring(0, 6).toUpperCase();
        long atsCompanyId = repository.upsertCompany("AT" + suffix, "ATS Co " + suffix, "Tech");
        long noAtsCompanyId = repository.upsertCompany("NA" + suffix, "No ATS Co " + suffix, "Tech");

        repository.upsertCompanyDomain(atsCompanyId, "ats" + suffix.toLowerCase() + ".example.com", null);
        repository.upsertCompanyDomain(noAtsCompanyId, "noats" + suffix.toLowerCase() + ".example.com", null);

        repository.upsertAtsEndpoint(
            atsCompanyId,
            AtsType.GREENHOUSE,
            "https://boards.greenhouse.io/ats" + suffix.toLowerCase(),
            "https://ats" + suffix.toLowerCase() + ".example.com/careers",
            0.9,
            Instant.now(),
            "test",
            true
        );

        List<CompanyTarget> targets = repository.findCompanyTargetsWithAts(List.of(), 10);
        assertThat(targets)
            .extracting(CompanyTarget::ticker)
            .contains("AT" + suffix)
            .doesNotContain("NA" + suffix);
    }
}
