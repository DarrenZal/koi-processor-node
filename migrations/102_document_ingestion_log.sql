-- Migration 102: document ingestion tracking (log + window checkpoints + dead-letter)
--
-- Tracking tables for the unified document-ingestion pipeline (Phase 1 of
-- an internal planning document). NEW TABLES ONLY — no
-- existing table is altered, so the daily session deep-extraction pipeline is
-- completely untouched. The one production-table change (generalizing
-- session_discourse_moves to carry document discourse) is a SEPARATE later
-- migration (103), applied only when Phase 3 (discourse) begins. Numbering keeps
-- numeric order = phase order: applying through 102 does NOT pull in 103.
--
-- Run: psql personal_koi -v ON_ERROR_STOP=1 --single-transaction -f migrations/102_document_ingestion_log.sql

-- 1. Per-document ingestion ledger. Idempotency spine = document_rid
--    ('document:<sha256(converted-markdown)>'). One row per ingested document;
--    re-ingesting identical bytes upserts this row.
CREATE TABLE IF NOT EXISTS document_ingestion_log (
    document_rid                  TEXT PRIMARY KEY,
    source_path                   TEXT,
    source_url                    TEXT,
    title                         TEXT,
    content_hash                  TEXT NOT NULL,
    char_count                    INT,
    chunk_count                   INT,        -- total RAG chunks (the turn-coordinate space)
    window_count                  INT,
    group_id                      TEXT DEFAULT 'personal',
    tier                          TEXT CHECK (tier IS NULL OR tier IN ('rag','standard','thorough')),
    rag_chunked_at                TIMESTAMPTZ, -- RAG step complete (gates extraction ordering)
    deep_extracted_at             TIMESTAMPTZ, -- NULL=never; '1970-01-01'=retry-exhausted sentinel
    deep_extraction_attempts      INT DEFAULT 0,
    deep_extraction_last_error    TEXT,
    deep_extraction_next_retry_at TIMESTAMPTZ,
    claims_extracted_at           TIMESTAMPTZ,
    last_run_id                   TEXT,
    last_ingested_at              TIMESTAMPTZ DEFAULT NOW(),
    created_at                    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_doc_ingest_retry
    ON document_ingestion_log (deep_extraction_next_retry_at)
    WHERE deep_extraction_next_retry_at IS NOT NULL;

-- 2. Per-window extraction checkpoint. Lets a long document resume mid-extraction
--    (window 5 of 7) without re-calling the LLM for already-extracted windows:
--    each window's validated JSON is cached in raw_json once status='extracted'.
CREATE TABLE IF NOT EXISTS document_window_extractions (
    document_rid     TEXT NOT NULL REFERENCES document_ingestion_log(document_rid) ON DELETE CASCADE,
    window_index     INT  NOT NULL,
    char_start       INT  NOT NULL,
    char_end         INT  NOT NULL,
    chunk_index_base INT  NOT NULL,   -- global RAG-chunk offset (window-boundary metadata only)
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending','extracted','imported','failed')),
    route_used       TEXT,            -- 'claude_p' | 'telus_gemma'
    raw_json         JSONB,           -- cached validated per-window extraction (resume + merge source)
    last_error       TEXT,
    run_id           TEXT,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (document_rid, window_index)
);
CREATE INDEX IF NOT EXISTS idx_docwin_status
    ON document_window_extractions (document_rid, status);

-- 3. Per-item dead-letter, doc-specific. The session dead-letter
--    deep_extraction_item_errors is intentionally NOT generalized/touched.
CREATE TABLE IF NOT EXISTS document_extraction_item_errors (
    id           SERIAL PRIMARY KEY,
    document_rid TEXT NOT NULL,
    window_index INT,
    item_type    TEXT,    -- 'entity' | 'fact' | 'merge' | 'episode_post' | 'type_mismatch'
    payload      JSONB,
    error        TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_doc_item_errors_rid
    ON document_extraction_item_errors (document_rid);
