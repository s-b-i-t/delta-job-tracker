CREATE TABLE IF NOT EXISTS crawl_queue (
    company_id BIGINT PRIMARY KEY REFERENCES companies(id) ON DELETE CASCADE,
    next_run_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_until TIMESTAMP WITH TIME ZONE,
    lock_owner TEXT,
    lock_count INT NOT NULL DEFAULT 0,
    last_started_at TIMESTAMP WITH TIME ZONE,
    last_finished_at TIMESTAMP WITH TIME ZONE,
    last_success_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    consecutive_failures INT NOT NULL DEFAULT 0,
    total_runs INT NOT NULL DEFAULT 0,
    total_successes INT NOT NULL DEFAULT 0,
    total_failures INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawl_queue_next_run_at ON crawl_queue(next_run_at);
CREATE INDEX IF NOT EXISTS idx_crawl_queue_locked_until ON crawl_queue(locked_until);
CREATE INDEX IF NOT EXISTS idx_crawl_queue_next_run_locked ON crawl_queue(next_run_at, locked_until);
