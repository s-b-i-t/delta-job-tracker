CREATE TABLE IF NOT EXISTS crawl_hosts (
    host TEXT PRIMARY KEY,
    next_allowed_at TIMESTAMP WITH TIME ZONE,
    backoff_state INTEGER NOT NULL DEFAULT 0,
    last_status_bucket TEXT,
    inflight_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crawl_hosts_next_allowed_at ON crawl_hosts(next_allowed_at);

CREATE TABLE IF NOT EXISTS crawl_urls (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    host TEXT NOT NULL REFERENCES crawl_hosts(host) ON DELETE CASCADE,
    canonical_url TEXT NOT NULL,
    url_kind VARCHAR(32) NOT NULL DEFAULT 'CANDIDATE',
    priority INTEGER NOT NULL DEFAULT 0,
    next_fetch_at TIMESTAMP WITH TIME ZONE NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'QUEUED',
    last_fetch_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    locked_until TIMESTAMP WITH TIME ZONE,
    lock_owner TEXT,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    CONSTRAINT uq_crawl_urls_canonical_url UNIQUE (canonical_url)
);

CREATE INDEX IF NOT EXISTS idx_crawl_urls_next_fetch_at ON crawl_urls(next_fetch_at);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_host ON crawl_urls(host);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_status ON crawl_urls(status);
CREATE INDEX IF NOT EXISTS idx_crawl_urls_kind_status_due ON crawl_urls(url_kind, status, next_fetch_at);

CREATE TABLE IF NOT EXISTS crawl_url_attempts (
    id BIGSERIAL PRIMARY KEY,
    url_id BIGINT NOT NULL REFERENCES crawl_urls(id) ON DELETE CASCADE,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    http_status INTEGER,
    elapsed_ms BIGINT,
    error_bucket TEXT
);

CREATE INDEX IF NOT EXISTS idx_crawl_url_attempts_url_id ON crawl_url_attempts(url_id);
CREATE INDEX IF NOT EXISTS idx_crawl_url_attempts_fetched_at ON crawl_url_attempts(fetched_at);
