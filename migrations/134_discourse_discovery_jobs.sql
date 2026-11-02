-- =============================================================================
-- 134: ingestion-time discourse discovery: a closed run kind and a job outbox
-- =============================================================================
-- Status:   applied only to throwaway databases (2026-10-20). Apply in one
--           transaction: psql -X -1 -v ON_ERROR_STOP=1 -f <this file>.
--           Backout of a run: operational rollback (discourse_links_canary.py
--           rollback), which keeps history. 134_..._down.sql is destructive
--           schema removal, a separate operation.
-- Depends:  133_discourse_link_candidates (applied on personal_koi 2026-10-19).
--           Does not edit 133 or any row it holds.
-- Issue:    gaiaaiagent/koi-processor#60
--
-- 1. discourse_link_runs.kind widens from 'canary' to the closed set
--    ('canary', 'ingestion_discovery'). The existing canary row is unchanged
--    and stays valid. A run kind names the operation that wrote a run's rows.
--    It grants no admission, review or publication authority: judgments stay
--    behind KOI_DISCOURSE_JUDGMENTS and a minted AdmissionAuthority.
-- 2. discourse_discovery_jobs: one row per newly extracted source revision to
--    discover from. The extractor writes it after its extraction bindings
--    commit (KOI_DISCOURSE_INGEST_DISCOVERY=1); scripts/
--    discourse_discovery_worker.py drains it. Same lease shape as
--    terminusdb_outbox (048): status, attempts, claimed_at/claimed_by,
--    next_attempt_at. A completed job names the run that wrote its
--    candidates; the run and the job's completion commit together.
-- =============================================================================

-- Named, not IF EXISTS: if the constraint were named differently, this must fail
-- rather than leave the old 'canary'-only check in place beside the new one.
ALTER TABLE discourse_link_runs DROP CONSTRAINT discourse_link_runs_kind_check;
ALTER TABLE discourse_link_runs ADD CONSTRAINT discourse_link_runs_kind_check
    CHECK (kind IN ('canary', 'ingestion_discovery'));

CREATE TABLE IF NOT EXISTS discourse_discovery_jobs (
    id                       bigserial PRIMARY KEY,
    job_key                  text NOT NULL UNIQUE,  -- sha256 of everything below that binds the job
    source_rid               text NOT NULL,
    source_version           text NOT NULL,         -- captured with the extraction input
    extraction_run_id        text NOT NULL,
    extraction_input_sha256  text NOT NULL,
    moves                    jsonb NOT NULL,        -- [[move_id, representation_sha256], ...] bound by that run
    context                  jsonb NOT NULL,        -- CallerContext.describe() of the configured context
    profile                  jsonb NOT NULL,        -- embedding and retrieval profile
    limits                   jsonb NOT NULL,
    status                   text NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'running', 'completed', 'failed', 'stale')),
    attempts                 integer NOT NULL DEFAULT 0,
    next_attempt_at          timestamptz NOT NULL DEFAULT now(),
    claimed_at               timestamptz,           -- lease: set on claim, NULL when not running
    claimed_by               text,
    run_id                   text REFERENCES discourse_link_runs(run_id),
    last_error               text,
    created_at               timestamptz NOT NULL DEFAULT now(),
    updated_at               timestamptz NOT NULL DEFAULT now(),
    finished_at              timestamptz,
    CHECK ((status = 'running') = (claimed_at IS NOT NULL)),
    CHECK ((status = 'completed') = (run_id IS NOT NULL)),
    CHECK ((status IN ('completed', 'failed', 'stale')) = (finished_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_ddj_claimable ON discourse_discovery_jobs (next_attempt_at, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_ddj_source ON discourse_discovery_jobs (source_rid);

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('134_discourse_discovery_jobs', 'v1_ingestion_discovery')
ON CONFLICT (migration_id) DO NOTHING;
