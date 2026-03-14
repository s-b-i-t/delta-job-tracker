CREATE TABLE domain_resolution_attempts (
    id BIGSERIAL PRIMARY KEY,
    company_id BIGINT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    finished_at TIMESTAMP WITH TIME ZONE NOT NULL,
    selection_mode TEXT NOT NULL,
    final_method TEXT NOT NULL,
    final_status TEXT NOT NULL,
    error_category TEXT,
    resolved_domain TEXT,
    resolved_source TEXT,
    resolved_method TEXT,
    step_trace_json TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_domain_resolution_attempts_company_finished
    ON domain_resolution_attempts(company_id, finished_at DESC);

CREATE INDEX idx_domain_resolution_attempts_selection_finished
    ON domain_resolution_attempts(selection_mode, finished_at DESC);
