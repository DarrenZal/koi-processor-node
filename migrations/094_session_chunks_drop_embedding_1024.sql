-- Migration 094: drop legacy 1024-dim embedding column from session_chunks
--
-- Prereqs (must all be true before applying):
--   1. Migration 092 applied (embedding_3072 column exists)
--   2. Migration 092b applied (HNSW halfvec index on embedding_3072 exists)
--   3. Reembed pipeline complete: SELECT COUNT(*) FROM session_chunks WHERE embedding_3072 IS NULL = 0
--   4. api/routers/knowledge_router.py updated to use embedding_3072::halfvec(3072) for sessions surface
--   5. Code deployed + smoke test passing (sessions surface returns non-empty for representative queries)
--   6. embedding_1024_backup column populated as additional safety (per ## Rollback in plan)
--
-- This migration is the final cleanup; if any prereq isn't met, defer.

BEGIN;

-- Verify prereq inline before dropping (fails the migration if any embedding_3072 is NULL)
DO $$
DECLARE
  unmigrated_count integer;
BEGIN
  SELECT COUNT(*) INTO unmigrated_count
  FROM session_chunks
  WHERE embedding_3072 IS NULL;

  IF unmigrated_count > 0 THEN
    RAISE EXCEPTION 'Migration 094 aborted: % rows still have NULL embedding_3072. Reembed must complete first.', unmigrated_count;
  END IF;
END $$;

-- Safe to drop now
ALTER TABLE session_chunks DROP COLUMN IF EXISTS embedding;
ALTER TABLE session_chunks DROP COLUMN IF EXISTS embedding_1024_backup;

-- HNSW index on the legacy column auto-drops with the column.

COMMIT;
