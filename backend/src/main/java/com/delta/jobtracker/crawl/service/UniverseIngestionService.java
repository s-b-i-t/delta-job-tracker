package com.delta.jobtracker.crawl.service;

import com.delta.jobtracker.config.CrawlerProperties;
import com.delta.jobtracker.crawl.http.PoliteHttpClient;
import com.delta.jobtracker.crawl.model.HttpFetchResult;
import com.delta.jobtracker.crawl.model.IngestionSummary;
import com.delta.jobtracker.crawl.persistence.CrawlJdbcRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class UniverseIngestionService {
    private static final Logger log = LoggerFactory.getLogger(UniverseIngestionService.class);
    private static final Pattern FOOTNOTE_PATTERN = Pattern.compile("\\[[^\\]]+\\]");
    private static final int MAX_ERROR_SAMPLES = 10;

    private final CrawlerProperties properties;
    private final CrawlJdbcRepository repository;
    private final Sp500WikipediaClient sp500WikipediaClient;
    private final PoliteHttpClient httpClient;
    private final ObjectMapper objectMapper;

    public UniverseIngestionService(
        CrawlerProperties properties,
        CrawlJdbcRepository repository,
        Sp500WikipediaClient sp500WikipediaClient,
        PoliteHttpClient httpClient,
        ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.repository = repository;
        this.sp500WikipediaClient = sp500WikipediaClient;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public IngestionSummary ingest() {
        return ingest("wiki");
    }

    public IngestionSummary ingest(String source) {
        Path sp500Path = resolvePath(properties.getData().getSp500Csv());
        Path domainsPath = resolvePath(properties.getData().getDomainsCsv());
        String wikiUrl = properties.getData().getSp500WikipediaUrl();

        MutableCounts counts = new MutableCounts();
        ErrorCollector errors = new ErrorCollector();
        Map<String, Long> companyIdsByTicker = new HashMap<>();

        IngestSource ingestSource = IngestSource.fromRaw(source, errors);
        boolean companiesIngested;
        if (ingestSource == IngestSource.SEC) {
            String secUrl = properties.getData().getSecCompanyTickersUrl();
            companiesIngested = ingestCompaniesFromSec(secUrl, companyIdsByTicker, counts, errors);
        } else if (ingestSource == IngestSource.FILE) {
            companiesIngested = ingestCompaniesFromCsv(sp500Path, companyIdsByTicker, counts, errors);
        } else {
            companiesIngested = ingestCompaniesFromWikipedia(wikiUrl, companyIdsByTicker, counts, errors);
            if (!companiesIngested) {
                errors.add("wikipedia source unavailable, falling back to file " + sp500Path);
                companiesIngested = ingestCompaniesFromCsv(sp500Path, companyIdsByTicker, counts, errors);
            }
        }

        if (!companiesIngested) {
            errors.add("no companies ingested from selected source");
        }

        if (ingestSource != IngestSource.SEC) {
            ingestDomainSeeds(domainsPath, companyIdsByTicker, counts, errors);
        }

        log.info(
            "Universe ingestion complete. source={}, companiesUpserted={}, domainsSeeded={}, errorsCount={}",
            ingestSource.name().toLowerCase(Locale.ROOT),
            counts.companiesUpserted,
            counts.domainsSeeded,
            errors.totalCount()
        );
        return new IngestionSummary(
            counts.companiesUpserted,
            counts.domainsSeeded,
            errors.totalCount(),
            errors.sampleErrors()
        );
    }

    private boolean ingestCompaniesFromWikipedia(
        String wikiUrl,
        Map<String, Long> companyIdsByTicker,
        MutableCounts counts,
        ErrorCollector errors
    ) {
        if (wikiUrl == null || wikiUrl.isBlank()) {
            errors.add("wikipedia URL is blank");
            return false;
        }

        Document document;
        try {
            int timeoutMs = Math.max(1, properties.getRequestTimeoutSeconds()) * 1000;
            document = sp500WikipediaClient.fetchConstituentsPage(wikiUrl, properties.getUserAgent(), timeoutMs);
        } catch (Exception e) {
            errors.add("failed to fetch wikipedia constituents page: " + rootMessage(e));
            return false;
        }

        Element table = findConstituentsTable(document);
        if (table == null) {
            errors.add("wikipedia constituents table not found");
            return false;
        }

        Map<String, Integer> headerIndex = buildHeaderIndex(table);
        Integer tickerIndex = findHeaderIndex(headerIndex, "symbol", "ticker");
        Integer nameIndex = findHeaderIndex(headerIndex, "security", "name");
        Integer sectorIndex = findHeaderIndex(headerIndex, "gics sector", "sector");
        Integer cikIndex = findHeaderIndex(headerIndex, "cik");
        if (tickerIndex == null || nameIndex == null) {
            errors.add("wikipedia table missing required columns (symbol/security)");
            return false;
        }

        int before = counts.companiesUpserted;
        int rowNumber = 0;
        for (Element row : table.select("tr")) {
            Elements cells = row.select("td");
            if (cells.isEmpty()) {
                continue;
            }
            rowNumber++;
            String ticker = normalizeTicker(readCell(cells, tickerIndex));
            Element nameCell = nameIndex == null || nameIndex < 0 || nameIndex >= cells.size()
                ? null
                : cells.get(nameIndex);
            String name = normalizeText(nameCell == null ? null : nameCell.text());
            String sector = normalizeText(readCell(cells, sectorIndex));
            String cik = normalizeText(readCell(cells, cikIndex));
            if (ticker == null || name == null) {
                errors.add("wiki row " + rowNumber + " missing required fields");
                continue;
            }

            String wikipediaTitle = extractWikipediaTitle(nameCell);
            long companyId = repository.upsertCompany(ticker, name, sector, wikipediaTitle);
            companyIdsByTicker.put(ticker, companyId);
            counts.companiesUpserted++;

            if (cik == null) {
                log.debug("wiki row missing CIK for ticker {}", ticker);
            }
        }
        return counts.companiesUpserted > before;
    }

    private boolean ingestCompaniesFromCsv(
        Path sp500Path,
        Map<String, Long> companyIdsByTicker,
        MutableCounts counts,
        ErrorCollector errors
    ) {
        int before = counts.companiesUpserted;
        try (Reader reader = Files.newBufferedReader(sp500Path, StandardCharsets.UTF_8);
             CSVParser parser = csvParser(reader)) {
            for (CSVRecord record : parser) {
                String ticker = normalizeTicker(getColumn(record, "ticker", "symbol"));
                String name = normalizeText(getColumn(record, "company_name", "security", "name"));
                String sector = normalizeText(getColumn(record, "sector", "gics sector"));
                if (ticker == null || name == null) {
                    errors.add("csv row " + record.getRecordNumber() + " missing required fields");
                    continue;
                }
                long companyId = repository.upsertCompany(ticker, name, sector);
                companyIdsByTicker.put(ticker, companyId);
                counts.companiesUpserted++;
            }
        } catch (IOException e) {
            errors.add("failed to ingest S&P CSV at " + sp500Path + ": " + rootMessage(e));
            return false;
        }
        return counts.companiesUpserted > before;
    }

    private boolean ingestCompaniesFromSec(
        String secUrl,
        Map<String, Long> companyIdsByTicker,
        MutableCounts counts,
        ErrorCollector errors
    ) {
        if (secUrl == null || secUrl.isBlank()) {
            errors.add("sec company tickers URL is blank");
            return false;
        }
        int before = counts.companiesUpserted;
        HttpFetchResult fetch = httpClient.get(secUrl, "application/json,*/*;q=0.8");
        if (!fetch.isSuccessful() || fetch.body() == null || fetch.body().isBlank()) {
            String status = fetch.errorCode() == null ? "http_" + fetch.statusCode() : fetch.errorCode();
            errors.add("failed to fetch SEC tickers JSON: " + status);
            return false;
        }

        try {
            JsonNode root = objectMapper.readTree(fetch.body());
            if (root == null || !root.isObject()) {
                errors.add("SEC tickers payload was not a JSON object");
                return false;
            }
            for (var entry : iterable(root.fields())) {
                JsonNode value = entry.getValue();
                String ticker = normalizeTicker(value.path("ticker").asText(null));
                String name = normalizeText(value.path("title").asText(null));
                if (ticker == null) {
                    ticker = syntheticTickerFromName(name);
                }
                if (ticker == null || name == null) {
                    errors.add("SEC record " + entry.getKey() + " missing ticker/name");
                    continue;
                }
                long companyId = repository.upsertCompany(ticker, name, null);
                companyIdsByTicker.put(ticker, companyId);
                counts.companiesUpserted++;
            }
        } catch (Exception e) {
            errors.add("failed to parse SEC tickers JSON: " + rootMessage(e));
            return false;
        }
        return counts.companiesUpserted > before;
    }

    private void ingestDomainSeeds(
        Path domainsPath,
        Map<String, Long> companyIdsByTicker,
        MutableCounts counts,
        ErrorCollector errors
    ) {
        try (Reader reader = Files.newBufferedReader(domainsPath, StandardCharsets.UTF_8);
             CSVParser parser = csvParser(reader)) {
            for (CSVRecord record : parser) {
                String ticker = normalizeTicker(getColumn(record, "ticker"));
                String companyName = normalizeText(getColumn(record, "company_name"));
                String domain = normalizeText(getColumn(record, "domain"));
                String careersHint = normalizeText(getColumn(record, "optional_careers_hint_url"));
                if (ticker == null || domain == null) {
                    errors.add("domains row " + record.getRecordNumber() + " missing required fields");
                    continue;
                }
                long companyId = companyIdsByTicker.computeIfAbsent(
                    ticker,
                    ignored -> repository.upsertCompany(
                        ticker,
                        companyName != null ? companyName : ticker,
                        null
                    )
                );
                repository.upsertCompanyDomain(companyId, domain.toLowerCase(Locale.ROOT), blankToNull(careersHint));
                counts.domainsSeeded++;
            }
        } catch (IOException e) {
            errors.add("failed to ingest domains CSV at " + domainsPath + ": " + rootMessage(e));
        }
    }

    private Element findConstituentsTable(Document document) {
        if (document == null) {
            return null;
        }
        Element byId = document.selectFirst("table#constituents");
        if (byId != null) {
            return byId;
        }
        for (Element table : document.select("table")) {
            Map<String, Integer> headerIndex = buildHeaderIndex(table);
            boolean hasSymbol = findHeaderIndex(headerIndex, "symbol", "ticker") != null;
            boolean hasSecurity = findHeaderIndex(headerIndex, "security", "name") != null;
            boolean hasSector = findHeaderIndex(headerIndex, "gics sector", "sector") != null;
            if (hasSymbol && hasSecurity && hasSector) {
                return table;
            }
        }
        return null;
    }

    private Map<String, Integer> buildHeaderIndex(Element table) {
        Map<String, Integer> index = new LinkedHashMap<>();
        Element headerRow = table.selectFirst("tr:has(th)");
        if (headerRow == null) {
            return index;
        }
        int i = 0;
        for (Element th : headerRow.select("th")) {
            String normalizedHeader = normalizeHeader(th.text());
            if (!normalizedHeader.isEmpty()) {
                index.put(normalizedHeader, i);
            }
            i++;
        }
        return index;
    }

    private Integer findHeaderIndex(Map<String, Integer> headerIndex, String... names) {
        for (String name : names) {
            for (Map.Entry<String, Integer> entry : headerIndex.entrySet()) {
                String header = entry.getKey();
                if (header.equals(name) || header.contains(name)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    private String normalizeHeader(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace('\u00A0', ' ')
            .trim()
            .replaceAll("\\s+", " ")
            .toLowerCase(Locale.ROOT);
    }

    private String readCell(Elements cells, Integer index) {
        if (index == null || index < 0 || index >= cells.size()) {
            return null;
        }
        return cells.get(index).text();
    }

    private String normalizeTicker(String ticker) {
        String normalized = normalizeText(ticker);
        return normalized == null ? null : normalized.toUpperCase(Locale.ROOT);
    }

    private String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String cleaned = value.replace('\u00A0', ' ').trim();
        cleaned = FOOTNOTE_PATTERN.matcher(cleaned).replaceAll("").trim();
        return cleaned.isEmpty() ? null : cleaned;
    }

    private String syntheticTickerFromName(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        String normalized = name.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "");
        if (normalized.isBlank()) {
            return null;
        }
        String prefix = normalized.length() >= 7 ? normalized.substring(0, 7) : normalized;
        String hash = Integer.toHexString(normalized.hashCode()).toUpperCase(Locale.ROOT);
        hash = String.format("%8s", hash).replace(' ', '0');
        String candidate = "N" + prefix + hash;
        if (candidate.length() > 16) {
            candidate = candidate.substring(0, 16);
        }
        return candidate;
    }

    private String extractWikipediaTitle(Element cell) {
        if (cell == null) {
            return null;
        }
        Element link = cell.selectFirst("a[href]");
        if (link == null) {
            return null;
        }
        String href = link.attr("href");
        if (href == null || href.isBlank()) {
            return null;
        }
        String normalized = href.trim();
        if (normalized.startsWith("//")) {
            normalized = "https:" + normalized;
        }
        String title = null;
        if (normalized.startsWith("/wiki/")) {
            title = normalized.substring("/wiki/".length());
        } else if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            int idx = normalized.indexOf("/wiki/");
            if (idx != -1) {
                title = normalized.substring(idx + "/wiki/".length());
            }
        }
        if (title == null || title.isBlank()) {
            return null;
        }
        int hashIdx = title.indexOf('#');
        if (hashIdx > 0) {
            title = title.substring(0, hashIdx);
        }
        int queryIdx = title.indexOf('?');
        if (queryIdx > 0) {
            title = title.substring(0, queryIdx);
        }
        return title.isBlank() ? null : title;
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

    private String rootMessage(Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.toString() : current.getMessage();
    }

    private <T> Iterable<T> iterable(java.util.Iterator<T> iterator) {
        return () -> iterator;
    }

    private enum IngestSource {
        WIKI,
        FILE,
        SEC;

        private static IngestSource fromRaw(String raw, ErrorCollector errors) {
            if (raw == null || raw.isBlank()) {
                return WIKI;
            }
            String normalized = raw.trim().toUpperCase(Locale.ROOT);
            try {
                return IngestSource.valueOf(normalized);
            } catch (IllegalArgumentException e) {
                errors.add("unsupported ingest source '" + raw + "', defaulting to wiki");
                return WIKI;
            }
        }
    }

    private static class MutableCounts {
        private int companiesUpserted;
        private int domainsSeeded;
    }

    private static class ErrorCollector {
        private int totalCount;
        private final List<String> sampleErrors = new ArrayList<>();

        private void add(String message) {
            totalCount++;
            if (sampleErrors.size() < MAX_ERROR_SAMPLES) {
                sampleErrors.add(message);
            }
        }

        private int totalCount() {
            return totalCount;
        }

        private List<String> sampleErrors() {
            return List.copyOf(sampleErrors);
        }
    }
}
