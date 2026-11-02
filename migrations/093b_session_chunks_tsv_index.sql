-- Migration 093b: GIN index on session_chunks.chunk_tsv for fast tsvector lookup
--
-- Must run OUTSIDE a transaction for CREATE INDEX CONCURRENTLY.
-- Apply with: psql personal_koi -f migrations/093b_session_chunks_tsv_index.sql
--
-- Companion to 093_session_chunks_tsv_column.sql.
-- Safe to run immediately after 093 (chunk_tsv is GENERATED STORED so populates with column add).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_session_chunks_chunk_tsv_gin
  ON session_chunks
  USING GIN (chunk_tsv);
