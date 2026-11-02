-- Migration 039: Code Docstring Extractions
-- Purpose: Provenance table for docstring-to-KG pipeline + shadow koi_memories rows
-- Date: 2026-02-18
-- Part of: Docstring Semantic Extraction Feature
--
-- Architecture:
--   Tree-sitter extracts docstrings from code → docstring_filter.py filters meaningful ones →
--   OpenAI extractor produces semantic entities → stored in koi_kg_extractions (passA) →
--   This table adds provenance (file_hash, commit_sha, batch_index, model, prompt_version).
--   Shadow koi_memories rows make docstring batches visible to analytics scripts.

-- ============================================================================
-- Part A: Provenance Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS koi_code_docstring_extractions (
    id SERIAL PRIMARY KEY,

    -- Link to koi_kg_extractions (CASCADE: delete provenance when extraction deleted)
    extraction_rid TEXT NOT NULL,

    -- Source identification
    repo TEXT NOT NULL,
    file_path TEXT NOT NULL,
    commit_sha TEXT,                        -- git commit at extraction time
    file_hash TEXT NOT NULL,                -- SHA-256 of source file content

    -- Batch tracking (files with many docstrings produce multiple batches)
    batch_index INTEGER NOT NULL DEFAULT 0,

    -- Extraction versioning
    model TEXT NOT NULL,                    -- effective model (post env-var resolution)
    prompt_version TEXT NOT NULL,           -- e.g. "v1.0"

    -- Entity provenance
    source_entity_ids JSONB NOT NULL DEFAULT '[]',  -- tree-sitter entity_ids in this batch
    input_chars INTEGER,                    -- characters sent to LLM

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- FK with cascade: when kg_extraction is deleted, provenance follows
    CONSTRAINT koi_cde_extraction_fk FOREIGN KEY (extraction_rid)
        REFERENCES koi_kg_extractions(extraction_rid) ON DELETE CASCADE,

    -- Deterministic idempotency: same inputs always produce same key
    CONSTRAINT koi_cde_idempotent UNIQUE (repo, file_path, file_hash, batch_index, prompt_version, model)
);

-- Indexes
CREATE INDEX IF NOT EXISTS koi_cde_repo_file_idx
    ON koi_code_docstring_extractions(repo, file_path);
CREATE INDEX IF NOT EXISTS koi_cde_extraction_rid_idx
    ON koi_code_docstring_extractions(extraction_rid);

-- ============================================================================
-- Part B: Documentation
-- ============================================================================

COMMENT ON TABLE koi_code_docstring_extractions IS
    'Provenance for docstring-to-KG extractions: links koi_kg_extractions to source code files';
COMMENT ON COLUMN koi_code_docstring_extractions.extraction_rid IS
    'FK to koi_kg_extractions.extraction_rid (format: code_docstring:{repo}:{file_hash}:{batch}:{prompt_ver}:{model})';
COMMENT ON COLUMN koi_code_docstring_extractions.file_hash IS
    'SHA-256 of the source file content at extraction time — used for idempotency';
COMMENT ON COLUMN koi_code_docstring_extractions.model IS
    'Effective model string after env-var resolution (e.g., gpt-4o-mini), not the CLI argument';
COMMENT ON COLUMN koi_code_docstring_extractions.prompt_version IS
    'Prompt template version — bump when prompt_builder code_docstring rules change';
COMMENT ON COLUMN koi_code_docstring_extractions.source_entity_ids IS
    'Array of tree-sitter entity_id hashes included in this batch';

-- ============================================================================
-- Schema Migration Tracking
-- ============================================================================

INSERT INTO schema_migrations (version, applied_at)
VALUES ('039_code_docstring_extractions', NOW())
ON CONFLICT (version) DO NOTHING;

-- Note: Shadow koi_memories rows (source_sensor='code_docstring') are inserted
-- by scripts/extract_docstring_semantics.py at runtime, not by this migration.
-- This keeps the migration idempotent and side-effect-free.
