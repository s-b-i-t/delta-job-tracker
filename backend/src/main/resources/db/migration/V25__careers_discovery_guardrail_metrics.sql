ALTER TABLE careers_discovery_runs
    ADD COLUMN request_budget INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN request_count_total INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN hard_timeout_seconds INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN stop_reason TEXT;

ALTER TABLE careers_discovery_runs
    ADD COLUMN host_failure_cutoff_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN host_failure_cutoff_skips INT NOT NULL DEFAULT 0;
