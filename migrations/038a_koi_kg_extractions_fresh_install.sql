\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 038a: koi_kg_extractions for fresh personal installs
--
-- 039_code_docstring_extractions references koi_kg_extractions, which
-- 013_create_kg_tables creates. 013 is a RegenAI-era migration that the runner skips
-- on personal installs, so this creates the table and its indexes first, copied from
-- 013 (indexes gain IF NOT EXISTS). Scripts that use it: extract_docstring_semantics.py,
-- backfill_entity_registry.py.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
CREATE TABLE IF NOT EXISTS koi_kg_extractions (
    id SERIAL PRIMARY KEY,

    -- RID References (follows existing RID pattern)
    memory_rid VARCHAR(500) NOT NULL,
    extraction_rid VARCHAR(500) UNIQUE NOT NULL,

    -- Extraction metadata
    extraction_type VARCHAR(50) NOT NULL CHECK (
        extraction_type IN (
            'passA',
            'passB',
            'entity_resolution',
            'nanopub_creation',
            'contradiction_detection'
        )
    ),

    -- KG data stored as JSONB for flexibility
    entities JSONB DEFAULT '[]',
    statements JSONB DEFAULT '[]',
    relations JSONB DEFAULT '[]',
    nanopubs JSONB DEFAULT '[]',

    -- Quality metrics
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),

    -- Ontology tracking
    ontology_version VARCHAR(20) DEFAULT 'op-v1.1',
    extractor_version VARCHAR(20),

    -- Cost tracking
    tokens_consumed INTEGER DEFAULT 0,
    cost_usd DECIMAL(10,6) DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_kg_memory_rid ON koi_kg_extractions(memory_rid);
CREATE INDEX IF NOT EXISTS idx_kg_extraction_rid ON koi_kg_extractions(extraction_rid);
CREATE INDEX IF NOT EXISTS idx_kg_extraction_type ON koi_kg_extractions(extraction_type);
CREATE INDEX IF NOT EXISTS idx_kg_confidence ON koi_kg_extractions(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_kg_created_at ON koi_kg_extractions(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_kg_ontology_version ON koi_kg_extractions(ontology_version);
CREATE INDEX IF NOT EXISTS idx_kg_entities_gin ON koi_kg_extractions USING gin(entities);
CREATE INDEX IF NOT EXISTS idx_kg_statements_gin ON koi_kg_extractions USING gin(statements);
CREATE INDEX IF NOT EXISTS idx_kg_relations_gin ON koi_kg_extractions USING gin(relations);
CREATE INDEX IF NOT EXISTS idx_kg_nanopubs_gin ON koi_kg_extractions USING gin(nanopubs);
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
