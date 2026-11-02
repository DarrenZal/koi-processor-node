-- Migration 023: Content Hash Deduplication Index
-- Purpose: Add index on content_hash column for fast duplicate detection
-- Related: DUPLICATE_CONTENT_FIX_PLAN.md Phase 2

-- Ensure pgcrypto extension is available for SHA-256 in SQL
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Add content_hash column if it doesn't exist
-- (It should already exist from create_new_version(), but ensure it's there)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'koi_memories' AND column_name = 'content_hash'
    ) THEN
        ALTER TABLE koi_memories ADD COLUMN content_hash TEXT;
    END IF;
END $$;

-- Create index on content_hash for fast duplicate lookups
-- Only index non-superseded memories since that's what we query
CREATE INDEX IF NOT EXISTS idx_memories_content_hash_active
ON koi_memories(content_hash)
WHERE superseded_at IS NULL AND content_hash IS NOT NULL;

-- Backfill content_hash for existing records that don't have it
-- Uses SHA-256 to match Python's hashlib.sha256().hexdigest()
-- Note: This may take a while on large tables - run in batches if needed
UPDATE koi_memories
SET content_hash = encode(digest(content->>'text', 'sha256'), 'hex')
WHERE content_hash IS NULL
  AND content->>'text' IS NOT NULL
  AND superseded_at IS NULL;

-- Add comment for documentation
COMMENT ON COLUMN koi_memories.content_hash IS
'SHA-256 hash of content text for cross-RID deduplication. Populated by event bridge.';

-- Show stats after migration
DO $$
DECLARE
    total_count INTEGER;
    with_hash_count INTEGER;
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_count FROM koi_memories WHERE superseded_at IS NULL;
    SELECT COUNT(*) INTO with_hash_count FROM koi_memories WHERE superseded_at IS NULL AND content_hash IS NOT NULL;

    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT content_hash, COUNT(*) as cnt
        FROM koi_memories
        WHERE superseded_at IS NULL AND content_hash IS NOT NULL
        GROUP BY content_hash
        HAVING COUNT(*) > 1
    ) duplicates;

    RAISE NOTICE 'Migration 023 complete:';
    RAISE NOTICE '  Total active memories: %', total_count;
    RAISE NOTICE '  With content_hash: %', with_hash_count;
    RAISE NOTICE '  Duplicate content groups: %', duplicate_count;
END $$;
