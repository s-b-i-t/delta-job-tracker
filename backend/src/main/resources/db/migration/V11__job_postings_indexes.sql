CREATE INDEX IF NOT EXISTS idx_job_postings_company_last_seen_desc
    ON job_postings (company_id, last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_postings_last_seen_desc
    ON job_postings (last_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_job_postings_company_active_last_seen_desc
    ON job_postings (company_id, is_active, last_seen_at DESC);
