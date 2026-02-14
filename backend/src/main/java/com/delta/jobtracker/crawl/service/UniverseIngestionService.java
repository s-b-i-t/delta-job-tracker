package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

@Service
public class UniverseIngestionService {
    private static final Logger log = LoggerFactory.getLogger(UniverseIngestionService.class);

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;

    public UniverseIngestionService(CrawlerProperties properties, CrawlJdbcRepository repository) {
        this.properties = properties;
        this.repository = repository;
    }

    public IngestionSummary ingest() {
        Path sp500Path = resolvePath(properties.getData().getSp500Csv());
        Path domainsPath = resolvePath(properties.getData().getDomainsCsv());

        int companiesUpserted = 0;
        int domainsUpserted = 0;
        Map<String, Long> companyIdsByTicker = new HashMap<>();

        try (Reader reader = Files.newBufferedReader(sp500Path, StandardCharsets.UTF_8);
             CSVParser parser = csvParser(reader)) {
            for (CSVRecord record : parser) {
                String ticker = getColumn(record, "ticker", "symbol");
                String name = getColumn(record, "company_name", "security", "name");
                String sector = getColumn(record, "sector", "gics sector");
                if (ticker == null || name == null) {
                    continue;
                }
                ticker = ticker.toUpperCase(Locale.ROOT);
                long companyId = repository.upsertCompany(ticker, name, sector);
                companyIdsByTicker.put(ticker, companyId);
                companiesUpserted++;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to ingest S&P universe CSV at " + sp500Path, e);
        }

        try (Reader reader = Files.newBufferedReader(domainsPath, StandardCharsets.UTF_8);
             CSVParser parser = csvParser(reader)) {
            for (CSVRecord record : parser) {
                String ticker = getColumn(record, "ticker");
                String companyName = getColumn(record, "company_name");
                String domain = getColumn(record, "domain");
                String careersHint = getColumn(record, "optional_careers_hint_url");
                if (ticker == null || domain == null) {
                    continue;
                }
                ticker = ticker.toUpperCase(Locale.ROOT);
                String finalTicker = ticker;
                String finalCompanyName = companyName;
                long companyId = companyIdsByTicker.computeIfAbsent(
                    finalTicker,
                    ignored -> repository.upsertCompany(
                        finalTicker,
                        finalCompanyName != null ? finalCompanyName : finalTicker,
                        null
                    )
                );
                repository.upsertCompanyDomain(companyId, domain.toLowerCase(Locale.ROOT), blankToNull(careersHint));
                domainsUpserted++;
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to ingest domain mapping CSV at " + domainsPath, e);
        }

        log.info("Universe ingestion complete. companiesUpserted={}, domainsUpserted={}", companiesUpserted, domainsUpserted);
        return new IngestionSummary(companiesUpserted, domainsUpserted);
    }

    private CSVParser csvParser(Reader reader) throws IOException {
        CSVFormat format = CSVFormat.DEFAULT.builder()
            .setHeader()
            .setSkipHeaderRecord(true)
            .setIgnoreSurroundingSpaces(true)
            .build();
        return format.parse(reader);
    }

    private String getColumn(CSVRecord record, String... names) {
        for (String name : names) {
            for (String header : record.toMap().keySet()) {
                if (header == null) {
                    continue;
                }
                if (header.trim().equalsIgnoreCase(name)) {
                    String value = record.get(header).trim();
                    return value.isEmpty() ? null : value;
                }
            }
        }
        return null;
    }

    private Path resolvePath(String configuredPath) {
        Path path = Paths.get(configuredPath);
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return Paths.get("").toAbsolutePath().resolve(path).normalize();
    }

    private String blankToNull(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }
}
