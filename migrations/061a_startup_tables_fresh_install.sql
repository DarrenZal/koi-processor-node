\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 061a: tables the API creates at startup, created first on fresh installs
--
-- api/personal_ingest_api.py creates document_entity_links and entity_rid_mappings
-- (with CREATE TABLE IF NOT EXISTS) when it starts. 062_visibility_scope,
-- 086_deep_extraction_schema and 132_document_entity_links_live_entity_uri alter
-- those tables, so on a new database they failed until the API had started once.
-- The DDL below is copied verbatim from that startup code, which is unchanged and
-- becomes a no-op on these tables.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
CREATE TABLE IF NOT EXISTS document_entity_links (
    id SERIAL PRIMARY KEY,
    document_rid TEXT NOT NULL,
    entity_uri TEXT NOT NULL,
    mention_count INT DEFAULT 1,
    context TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(document_rid, entity_uri)
);

CREATE TABLE IF NOT EXISTS entity_rid_mappings (
    id SERIAL PRIMARY KEY,
    vault_rid TEXT UNIQUE NOT NULL,
    vault_path TEXT NOT NULL,
    canonical_uri TEXT NOT NULL,
    entity_type TEXT,
    name TEXT,
    content_hash TEXT,
    sync_status TEXT DEFAULT 'linked',
    last_synced TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT valid_sync_status CHECK (
        sync_status IN ('linked', 'local_only', 'pending_sync', 'conflict')
    )
);

CREATE INDEX IF NOT EXISTS idx_rid_mappings_canonical
ON entity_rid_mappings(canonical_uri);

CREATE INDEX IF NOT EXISTS idx_rid_mappings_vault_path
ON entity_rid_mappings(vault_path);

CREATE INDEX IF NOT EXISTS idx_rid_mappings_sync_status
ON entity_rid_mappings(sync_status);
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
