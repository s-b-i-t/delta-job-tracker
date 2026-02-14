ALTER TABLE company_domains
    ADD COLUMN source VARCHAR(32) NOT NULL DEFAULT 'MANUAL';

ALTER TABLE company_domains
    ADD COLUMN confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0;

ALTER TABLE company_domains
    ADD COLUMN resolved_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE ats_endpoints
    ADD COLUMN discovered_from_url TEXT;

ALTER TABLE ats_endpoints
    ADD COLUMN confidence DOUBLE PRECISION NOT NULL DEFAULT 1.0;

CREATE INDEX idx_company_domains_source_resolved_at ON company_domains(source, resolved_at);
CREATE INDEX idx_ats_endpoints_company_detected ON ats_endpoints(company_id, detected_at DESC);
