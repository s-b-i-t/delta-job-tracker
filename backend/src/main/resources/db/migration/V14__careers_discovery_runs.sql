CREATE TABLE IF NOT EXISTS careers_discovery_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    status TEXT NOT NULL,
    company_limit INT NOT NULL,
    processed_count INT NOT NULL DEFAULT 0,
    succeeded_count INT NOT NULL DEFAULT 0,
    failed_count INT NOT NULL DEFAULT 0,
    endpoints_added INT NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS careers_discovery_company_results (
    id BIGSERIAL PRIMARY KEY,
    discovery_run_id BIGINT NOT NULL REFERENCES careers_discovery_runs(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    reason_code TEXT,
    stage TEXT NOT NULL,
    found_endpoints_count INT NOT NULL DEFAULT 0,
    duration_ms BIGINT,
    http_status INT,
    error_detail TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    UNIQUE (discovery_run_id, company_id)
);

CREATE INDEX IF NOT EXISTS idx_careers_discovery_company_results_run_status
    ON careers_discovery_company_results(discovery_run_id, status);

CREATE INDEX IF NOT EXISTS idx_careers_discovery_company_results_run_company
    ON careers_discovery_company_results(discovery_run_id, company_id);
