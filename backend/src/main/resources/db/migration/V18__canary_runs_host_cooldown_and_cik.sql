CREATE TABLE canary_runs (
    id BIGSERIAL PRIMARY KEY,
    type TEXT NOT NULL,
    requested_limit INTEGER,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32) NOT NULL,
    summary_json TEXT,
    error_summary_json TEXT
);

CREATE INDEX idx_canary_runs_type_status ON canary_runs(type, status);

CREATE TABLE host_crawl_state (
    host TEXT PRIMARY KEY,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_error_category TEXT,
    last_attempt_at TIMESTAMP WITH TIME ZONE,
    next_allowed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_host_crawl_state_next_allowed ON host_crawl_state(next_allowed_at);

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS cik TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain_resolution_method TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain_resolution_status TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain_resolution_error TEXT;

ALTER TABLE companies
    ADD COLUMN IF NOT EXISTS domain_resolution_attempted_at TIMESTAMP WITH TIME ZONE;

CREATE INDEX IF NOT EXISTS idx_companies_cik ON companies(cik);
