ALTER TABLE careers_discovery_runs
    ADD COLUMN companies_considered INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN homepage_scanned INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN endpoints_found_homepage_json TEXT;

ALTER TABLE careers_discovery_runs
    ADD COLUMN endpoints_found_vendor_json TEXT;

ALTER TABLE careers_discovery_runs
    ADD COLUMN careers_paths_checked INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN robots_blocked_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN fetch_failed_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN time_budget_exceeded_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS careers_discovery_state (
    company_id BIGINT PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    last_reason_code TEXT,
    last_candidate_url TEXT,
    consecutive_failures INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_careers_discovery_state_next_attempt
    ON careers_discovery_state(next_attempt_at);
