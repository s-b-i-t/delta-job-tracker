ALTER TABLE careers_discovery_company_results
    ADD COLUMN careers_url_found BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN careers_url_initial TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN careers_url_final TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN careers_discovery_method TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN careers_discovery_stage_failure TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN vendor_detected BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN vendor_name TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN endpoint_extracted BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN endpoint_url TEXT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN http_status_first_failure INT;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN request_count INT;

CREATE INDEX IF NOT EXISTS idx_careers_discovery_company_results_run_funnel
    ON careers_discovery_company_results(discovery_run_id, careers_url_found, vendor_detected, endpoint_extracted);
