-- Backfill content_tsv for existing rows
-- Run this AFTER applying migration 025_add_content_tsv_fts.sql
-- Execute in batches to avoid long locks

-- Step 1: Batch backfill (repeat until 0 rows updated)
-- Use deterministic ordering to avoid processing same rows twice
UPDATE koi_memories
SET content_tsv =
  setweight(to_tsvector('english', COALESCE(content->>'title', '')), 'A') ||
  setweight(to_tsvector('english', COALESCE(content->>'text', '')), 'B')
WHERE id IN (
  SELECT id FROM koi_memories
  WHERE content_tsv IS NULL
  ORDER BY id
  LIMIT 5000
);
-- Check: SELECT COUNT(*) FROM koi_memories WHERE content_tsv IS NULL;
-- Repeat Step 1 until count is 0

-- Step 2: Update statistics after backfill completes
ANALYZE koi_memories;

-- Step 3: Create GIN index (run outside transaction, uses CONCURRENTLY)
-- This must be run as a separate statement, not in a transaction block
CREATE INDEX CONCURRENTLY IF NOT EXISTS koi_memories_content_tsv_gin
ON koi_memories USING GIN(content_tsv);

-- Verification queries:
-- SELECT COUNT(*) FROM koi_memories WHERE content_tsv IS NOT NULL;
-- SELECT indexname FROM pg_indexes
-- WHERE tablename = 'koi_memories' AND indexname = 'koi_memories_content_tsv_gin';
-- SELECT rid, ts_rank_cd(content_tsv, to_tsquery('english', 'claims & engine')) as rank
-- FROM koi_memories WHERE content_tsv @@ to_tsquery('english', 'claims & engine')
-- ORDER BY rank DESC LIMIT 5;
