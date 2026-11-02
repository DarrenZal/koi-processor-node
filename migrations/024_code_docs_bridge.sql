-- Migration: Code ↔ Docs Bridge Tables
-- Created: 2025-12-22
-- Description: Adds relational bridge tables to connect the semantic KG (Postgres/RDF)
--              with the code graph (Apache AGE + tree-sitter pipeline).

-- Canonical artifact table: one row per code entity/artifact.
-- Population source (initial): code_entity_provenance view (from CAT receipts).
CREATE TABLE IF NOT EXISTS koi_code_artifacts (
    -- Canonical identifier shared across systems (can be a URI string).
    -- Recommended initial value: code_entity_provenance.entity_rid (rid://code/...)
    code_uri TEXT PRIMARY KEY,

    -- What it is
    kind TEXT NOT NULL, -- e.g., Function, Class, Keeper, Message

    -- Where it is (canonical join keys)
    repo_key TEXT NOT NULL,   -- e.g. github.com/regen-network/regen-ledger
    file_path TEXT NOT NULL,  -- repo-relative path, as stored by provenance
    symbol TEXT,              -- e.g., MsgCreateBatch / BasketKeeper / main
    language TEXT,

    -- Optional code versioning (future-proofing; can be NULL for "current")
    commit_sha TEXT,

    -- Optional direct mapping to Apache AGE node identity (nullable until implemented)
    age_graph TEXT,
    age_id BIGINT,

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT koi_code_artifacts_age_uniq UNIQUE (age_graph, age_id)
);

CREATE INDEX IF NOT EXISTS koi_code_artifacts_repo_idx
    ON koi_code_artifacts(repo_key);

CREATE INDEX IF NOT EXISTS koi_code_artifacts_repo_file_idx
    ON koi_code_artifacts(repo_key, file_path);

CREATE INDEX IF NOT EXISTS koi_code_artifacts_symbol_idx
    ON koi_code_artifacts(symbol);

CREATE INDEX IF NOT EXISTS koi_code_artifacts_kind_idx
    ON koi_code_artifacts(kind);

CREATE INDEX IF NOT EXISTS koi_code_artifacts_metadata_gin
    ON koi_code_artifacts USING GIN (metadata);

-- Document → code links: captures mentions in docs and ties them to canonical code_uri.
CREATE TABLE IF NOT EXISTS koi_doc_code_links (
    memory_rid TEXT NOT NULL,
    code_uri TEXT NOT NULL REFERENCES koi_code_artifacts(code_uri) ON DELETE CASCADE,

    mention_text TEXT NOT NULL,
    confidence REAL,

    -- Optional code versioning for "docs refer to this commit"
    commit_sha TEXT,

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT koi_doc_code_links_uniq UNIQUE (memory_rid, code_uri, mention_text)
);

CREATE INDEX IF NOT EXISTS koi_doc_code_links_code_uri_idx
    ON koi_doc_code_links(code_uri);

CREATE INDEX IF NOT EXISTS koi_doc_code_links_memory_rid_idx
    ON koi_doc_code_links(memory_rid);

-- If you store code_uri in entity_registry.metadata, this makes reverse lookups fast.
DO $$
BEGIN
    IF to_regclass('public.entity_registry') IS NOT NULL THEN
        EXECUTE 'CREATE INDEX IF NOT EXISTS entity_registry_metadata_code_uri_idx ON entity_registry ((metadata->>''code_uri''));';
    END IF;
END $$;
