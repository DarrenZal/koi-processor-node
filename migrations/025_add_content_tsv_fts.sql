-- Migration: Add tsvector column for full-text search
-- This enables BM25/FTS keyword search in hybrid RAG

-- Add tsvector column (idempotent)
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS content_tsv tsvector;

-- Create trigger function to auto-update tsvector with weighted fields
-- Weight A (highest): title - matches here are most relevant
-- Weight B: text body - the main content
CREATE OR REPLACE FUNCTION koi_memories_content_tsv_trigger()
RETURNS trigger AS $$
BEGIN
  NEW.content_tsv :=
    setweight(to_tsvector('english', COALESCE(NEW.content->>'title', '')), 'A') ||
    setweight(to_tsvector('english', COALESCE(NEW.content->>'text', '')), 'B');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger (only fires on content column changes)
DROP TRIGGER IF EXISTS koi_memories_content_tsv_update ON koi_memories;
CREATE TRIGGER koi_memories_content_tsv_update
  BEFORE INSERT OR UPDATE OF content ON koi_memories
  FOR EACH ROW
  EXECUTE FUNCTION koi_memories_content_tsv_trigger();

-- NOTE: Index should be created separately with CONCURRENTLY to avoid locks:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS koi_memories_content_tsv_gin
--   ON koi_memories USING GIN(content_tsv);
--
-- Backfill should also be done separately in batches. See scripts/backfill-fts.sql
