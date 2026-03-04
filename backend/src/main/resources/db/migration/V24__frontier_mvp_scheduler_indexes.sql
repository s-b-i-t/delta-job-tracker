CREATE INDEX IF NOT EXISTS idx_crawl_urls_kind_status_due_lock
    ON crawl_urls (url_kind, status, next_fetch_at, locked_until);
