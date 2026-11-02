-- Migration 074: Update tsvector to include contextual retrieval context (B8)
-- The tsvector now indexes both the LLM-generated context and the original chunk text.
-- Until context is populated, COALESCE(NULL, '') preserves existing BM25 behavior.

DROP INDEX IF EXISTS idx_chunks_tsv;
ALTER TABLE koi_memory_chunks DROP COLUMN IF EXISTS tsv;

ALTER TABLE koi_memory_chunks ADD COLUMN tsv tsvector
  GENERATED ALWAYS AS (
    to_tsvector('english',
      COALESCE(content->>'context', '') || ' ' || COALESCE(content->>'text', '')
    )
  ) STORED;

CREATE INDEX idx_chunks_tsv ON koi_memory_chunks USING GIN(tsv);
