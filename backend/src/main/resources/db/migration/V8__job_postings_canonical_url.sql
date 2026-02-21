ALTER TABLE job_postings
    ADD COLUMN IF NOT EXISTS canonical_url TEXT;
