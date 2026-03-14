ALTER TABLE ats_endpoints
    ADD COLUMN last_revalidated_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE ats_endpoints
    ADD COLUMN promoted_from_vendor_probe_at TIMESTAMP WITH TIME ZONE;

ALTER TABLE careers_discovery_runs
    ADD COLUMN endpoints_promoted INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN endpoints_confirmed INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN endpoints_promoted INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_company_results
    ADD COLUMN endpoints_confirmed INT NOT NULL DEFAULT 0;
