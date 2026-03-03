ALTER TABLE careers_discovery_runs
    ADD COLUMN selection_eligible_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN selection_returned_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN companies_input_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN companies_attempted_count INT NOT NULL DEFAULT 0;

ALTER TABLE careers_discovery_runs
    ADD COLUMN cached_skip_count INT NOT NULL DEFAULT 0;
