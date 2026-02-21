ALTER TABLE crawl_run_company_results
    ADD COLUMN IF NOT EXISTS total_jobs_available INT;

ALTER TABLE crawl_run_company_results
    ADD COLUMN IF NOT EXISTS stop_reason TEXT;
