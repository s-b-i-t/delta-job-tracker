ALTER TABLE job_postings
    ADD COLUMN crawl_run_id BIGINT REFERENCES crawl_runs(id) ON DELETE SET NULL;

CREATE INDEX idx_job_postings_crawl_run_id ON job_postings(crawl_run_id);
CREATE INDEX idx_job_postings_company_crawl_run_id ON job_postings(company_id, crawl_run_id);
