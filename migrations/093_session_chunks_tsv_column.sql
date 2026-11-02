-- Migration 093: add generated tsvector column to session_chunks for hybrid BM25+dense+RRF
-- See: an internal planning document §3.0 + P2
--
-- Companion: 093b_session_chunks_tsv_index.sql (CREATE INDEX CONCURRENTLY GIN, outside txn)
--
-- The GENERATED ALWAYS AS ... STORED auto-computes chunk_tsv on insert/update;
-- no application code change needed for the column itself. The hybrid retrieval
-- code change (api/routers/knowledge_router.py sessions surface) ships with P2.

BEGIN;

ALTER TABLE session_chunks
  ADD COLUMN IF NOT EXISTS chunk_tsv tsvector
  GENERATED ALWAYS AS (to_tsvector('english', coalesce(chunk_text, ''))) STORED;

COMMENT ON COLUMN session_chunks.chunk_tsv IS
  'Generated tsvector (English) for hybrid BM25+dense+RRF retrieval on sessions surface. Used in api/routers/knowledge_router.py unified_search lexical leg. Per BKC Octo Pattern B6.';

COMMIT;
