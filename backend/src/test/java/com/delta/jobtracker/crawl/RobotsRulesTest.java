package com.delta.jobtracker.crawl;

import com.delta.jobtracker.crawl.robots.RobotsRules;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RobotsRulesTest {

    @Test
    void parsesSitemapAndDisallowForWildcardUserAgent() {
        String robots =
            """
                User-agent: *
                Disallow: /private
                Allow: /private/public
                Sitemap: https://example.com/sitemap.xml
                """;

        RobotsRules rules = RobotsRules.parse(robots);
        assertTrue(rules.getSitemapUrls().contains("https://example.com/sitemap.xml"));
        assertFalse(rules.isAllowed("/private/page"));
        assertTrue(rules.isAllowed("/private/public/page"));
        assertTrue(rules.isAllowed("/other"));
    }
}
