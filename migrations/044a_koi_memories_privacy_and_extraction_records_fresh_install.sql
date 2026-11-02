\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 044a: objects personal code needs from two skipped RegenAI-era migrations
--
-- On personal installs the runner skips 010_jena_integration and
-- 015_add_privacy_column (RegenAI-era; they fail on a personal database). Two parts
-- of them are used by personal code, so they are created here, after 044 creates
-- koi_memories, with DDL copied from those files:
--   015: koi_memories.is_private and access_source, their indexes and comments
--        (api/read_scope.py, the sensors, scripts/ingest_document.py)
--   010: koi_extraction_records and its indexes
--        (api/routers/admin_router.py, api/merge_reversal.py)
-- 015's notion backfill and views, and 010's Jena objects, have no users and are left out.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
-- from 015_add_privacy_column
ALTER TABLE koi_memories ADD COLUMN IF NOT EXISTS is_private BOOLEAN DEFAULT FALSE;
ALTER TABLE koi_memories ADD COLUMN IF NOT EXISTS access_source VARCHAR(100);

-- Index for efficient privacy filtering (critical for query performance)
CREATE INDEX IF NOT EXISTS idx_koi_memories_is_private ON koi_memories(is_private);
CREATE INDEX IF NOT EXISTS idx_koi_memories_access_source ON koi_memories(access_source);

-- Composite index for common query pattern: non-superseded + privacy filter
CREATE INDEX IF NOT EXISTS idx_koi_memories_active_privacy
ON koi_memories(superseded_at, is_private)
WHERE superseded_at IS NULL;

COMMENT ON COLUMN koi_memories.is_private IS 'If TRUE, requires OAuth authentication with @regen.network email to access';
COMMENT ON COLUMN koi_memories.access_source IS 'Identifies which configuration determined privacy level (e.g., notion-main-workspace, notion-regentokenomics)';

-- from 010_jena_integration
CREATE TABLE IF NOT EXISTS koi_extraction_records (
    extraction_rid VARCHAR PRIMARY KEY,
    source_rid VARCHAR NOT NULL REFERENCES koi_memories(rid),
    extraction_type VARCHAR NOT NULL CHECK (extraction_type IN ('entity', 'relation', 'jsonld', 'claim', 'evidence')),
    jena_uri VARCHAR NOT NULL,
    jena_graph VARCHAR,
    subject_uri VARCHAR,  -- For relations
    predicate VARCHAR,     -- For relations
    object_uri VARCHAR,    -- For relations
    confidence FLOAT,
    extractor_model VARCHAR,
    extraction_metadata JSONB,
    cat_receipt_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    synced_to_jena BOOLEAN DEFAULT FALSE,
    jena_sync_timestamp TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_extraction_source ON koi_extraction_records(source_rid);
CREATE INDEX IF NOT EXISTS idx_extraction_type ON koi_extraction_records(extraction_type);
CREATE INDEX IF NOT EXISTS idx_extraction_jena ON koi_extraction_records(jena_uri);
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
