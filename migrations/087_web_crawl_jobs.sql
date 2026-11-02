-- Migration: 087_web_crawl_jobs
-- Agentic web crawl: persistent job table for background worker lifecycle.
-- Octo-only (NOT applied to fr_koi / gv_koi / cv_koi baselines).

CREATE TABLE IF NOT EXISTS web_crawl_jobs (
    id BIGSERIAL PRIMARY KEY,
    start_url TEXT NOT NULL,
    goal TEXT,
    submitted_by TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'queued',
        'running',
        'done',
        'committed',
        'partially_committed',
        'failed',
        'cancelled',
        'interrupted'
    )),
    claimed_by TEXT,
    progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    cost_usd NUMERIC(8,4) NOT NULL DEFAULT 0,
    proposal_version TEXT,
    ontology_version TEXT,
    commit_history JSONB NOT NULL DEFAULT '[]'::jsonb,
    budget_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_web_crawl_jobs_status_created
    ON web_crawl_jobs (status, created_at);

CREATE INDEX IF NOT EXISTS idx_web_crawl_jobs_submitted_status
    ON web_crawl_jobs (submitted_by, status);

CREATE INDEX IF NOT EXISTS idx_web_crawl_jobs_submitted_created
    ON web_crawl_jobs (submitted_by, created_at);

CREATE INDEX IF NOT EXISTS idx_web_crawl_jobs_start_url_status
    ON web_crawl_jobs (start_url, status);

-- Atomic-enqueue safety net: a second concurrent INSERT for same
-- (submitted_by, start_url) while the first is still in-flight raises
-- unique_violation, which the endpoint catches to return the existing job_id.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_inflight_per_user_url
    ON web_crawl_jobs (submitted_by, start_url)
    WHERE status IN ('queued', 'running');
