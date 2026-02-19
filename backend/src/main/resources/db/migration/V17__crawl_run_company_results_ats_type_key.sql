-- Verification query:
-- select company_id, stage, ats_type, ats_type_key, stop_reason, truncated, total_jobs_available
-- from crawl_run_company_results
-- where crawl_run_id = :runId
-- order by company_id, stage, ats_type_key;

ALTER TABLE crawl_run_company_results
    ADD COLUMN IF NOT EXISTS ats_type_key TEXT NOT NULL DEFAULT 'NONE';

UPDATE crawl_run_company_results
SET ats_type_key = COALESCE(ats_type, 'NONE');

ALTER TABLE crawl_run_company_results
    DROP CONSTRAINT IF EXISTS crawl_run_company_results_crawl_run_id_company_id_stage_endpoint_url_key;

ALTER TABLE crawl_run_company_results
    ADD CONSTRAINT crawl_run_company_results_run_company_stage_ats_key_unique
    UNIQUE (crawl_run_id, company_id, stage, ats_type_key);
