ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS companies_attempted INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS companies_succeeded INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS companies_failed INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS jobs_extracted_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE crawl_runs
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMP WITH TIME ZONE;

UPDATE crawl_runs
SET last_heartbeat_at = started_at
WHERE last_heartbeat_at IS NULL;
