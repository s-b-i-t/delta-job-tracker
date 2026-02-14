package com.delta.jobtracker.crawl;

import com.delta.jobtracker.crawl.service.Sp500WikipediaClient;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@ActiveProfiles("test")
@Import(IngestWikipediaMockedTest.MockWikiConfig.class)
class IngestWikipediaMockedTest {

    @Autowired
    private WebApplicationContext context;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void ingestDefaultsToWikipediaAndStillSeedsDomains() throws Exception {
        mockMvc.perform(post("/api/ingest"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.companiesUpserted").value(greaterThanOrEqualTo(450)))
            .andExpect(jsonPath("$.domainsSeeded").value(greaterThanOrEqualTo(1)));
    }

    @TestConfiguration
    static class MockWikiConfig {
        @Bean
        @Primary
        Sp500WikipediaClient sp500WikipediaClient() {
            return (url, userAgent, timeoutMs) -> Jsoup.parse(buildConstituentsHtml(), url);
        }

        private static String buildConstituentsHtml() throws IOException {
            Path csvPath = Path.of("../data/sp500_constituents.csv").toAbsolutePath().normalize();
            StringBuilder html = new StringBuilder();
            html.append("<html><body><table id=\"constituents\">")
                .append("<thead><tr>")
                .append("<th>Symbol</th><th>Security</th><th>GICS Sector</th><th>CIK</th>")
                .append("</tr></thead><tbody>");

            try (Reader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8);
                 CSVParser parser = CSVFormat.DEFAULT.builder()
                     .setHeader()
                     .setSkipHeaderRecord(true)
                     .setIgnoreSurroundingSpaces(true)
                     .build()
                     .parse(reader)) {
                for (CSVRecord record : parser) {
                    html.append("<tr>")
                        .append("<td>").append(escapeHtml(record.get("ticker"))).append("</td>")
                        .append("<td>").append(escapeHtml(record.get("company_name"))).append("</td>")
                        .append("<td>").append(escapeHtml(record.get("sector"))).append("</td>")
                        .append("<td>0000000000</td>")
                        .append("</tr>");
                }
            }
            html.append("</tbody></table></body></html>");
            return html.toString();
        }

        private static String escapeHtml(String value) {
            return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
        }
    }
}
