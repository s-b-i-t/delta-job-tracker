ALTER TABLE job_postings
    ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_job_postings_company_active ON job_postings(company_id, is_active);
CREATE INDEX IF NOT EXISTS idx_job_postings_company_first_seen ON job_postings(company_id, first_seen_at);
CREATE INDEX IF NOT EXISTS idx_job_postings_company_last_seen ON job_postings(company_id, last_seen_at);
