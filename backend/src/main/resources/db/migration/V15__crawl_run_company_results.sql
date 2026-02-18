CREATE TABLE IF NOT EXISTS crawl_run_company_results (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    ats_type TEXT,
    endpoint_url TEXT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE,
    duration_ms BIGINT,
    jobs_extracted INT NOT NULL DEFAULT 0,
    truncated BOOLEAN NOT NULL DEFAULT FALSE,
    reason_code TEXT,
    http_status INT,
    error_detail TEXT,
    retryable BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (crawl_run_id, company_id, stage, endpoint_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_run_company_results_run_status
    ON crawl_run_company_results(crawl_run_id, status);

CREATE INDEX IF NOT EXISTS idx_crawl_run_company_results_run_company
    ON crawl_run_company_results(crawl_run_id, company_id);
