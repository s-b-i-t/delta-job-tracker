ALTER TABLE discovered_urls
    ADD COLUMN IF NOT EXISTS http_status INTEGER;

ALTER TABLE discovered_urls
    ADD COLUMN IF NOT EXISTS error_code VARCHAR(64);

ALTER TABLE discovered_urls
    ADD COLUMN IF NOT EXISTS adapter VARCHAR(32);
