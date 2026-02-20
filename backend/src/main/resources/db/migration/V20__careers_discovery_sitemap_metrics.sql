ALTER TABLE careers_discovery_runs
    ADD COLUMN sitemaps_scanned INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN sitemap_urls_checked INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN endpoints_found_sitemap_json TEXT;
