-- Migration 073: Add tsvector column + GIN index on koi_memory_chunks for BM25 keyword search (B6)
-- The tsvector is GENERATED ALWAYS from the JSONB text field, so it auto-updates on INSERT/UPDATE.

ALTER TABLE koi_memory_chunks
  ADD COLUMN IF NOT EXISTS tsv tsvector
  GENERATED ALWAYS AS (to_tsvector('english', COALESCE(content->>'text', ''))) STORED;

CREATE INDEX IF NOT EXISTS idx_chunks_tsv ON koi_memory_chunks USING GIN(tsv);
