CREATE TABLE companies (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(16) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    sector TEXT
);

CREATE TABLE company_domains (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    domain TEXT NOT NULL,
    careers_hint_url TEXT,
    UNIQUE (company_id, domain)
);

CREATE TABLE crawl_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE,
    status VARCHAR(32) NOT NULL,
    notes TEXT
);

CREATE TABLE discovered_sitemaps (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    sitemap_url TEXT NOT NULL,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    url_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE discovered_urls (
    id BIGSERIAL PRIMARY KEY,
    crawl_run_id BIGINT NOT NULL REFERENCES crawl_runs(id) ON DELETE CASCADE,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    url_type VARCHAR(32) NOT NULL,
    fetch_status VARCHAR(64),
    last_fetched_at TIMESTAMP WITH TIME ZONE,
    UNIQUE (crawl_run_id, company_id, url)
);

CREATE TABLE ats_endpoints (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    ats_type VARCHAR(32) NOT NULL,
    ats_url TEXT NOT NULL,
    detected_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (company_id, ats_type, ats_url)
);

CREATE TABLE job_postings (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    source_url TEXT NOT NULL,
    title TEXT,
    org_name TEXT,
    location_text TEXT,
    employment_type TEXT,
    date_posted DATE,
    description_text TEXT,
    external_identifier TEXT,
    content_hash CHAR(64) NOT NULL,
    first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (company_id, content_hash)
);

CREATE INDEX idx_discovered_urls_company_url ON discovered_urls(company_id, url);
CREATE INDEX idx_job_postings_company_last_seen ON job_postings(company_id, last_seen_at);
CREATE INDEX idx_company_domains_domain ON company_domains(domain);
