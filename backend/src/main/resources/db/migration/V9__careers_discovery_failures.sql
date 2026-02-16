CREATE TABLE IF NOT EXISTS careers_discovery_failures (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    reason_code VARCHAR(64) NOT NULL,
    candidate_url TEXT,
    detail TEXT,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_careers_discovery_failures_company_observed
    ON careers_discovery_failures(company_id, observed_at DESC);
