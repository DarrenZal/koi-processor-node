-- =============================================================================
-- Migration 036: Ensure aliases column is TEXT[] (not JSONB)
-- =============================================================================
-- Date: 2026-02-02 (revised 2026-03-11)
-- Purpose: Align new environments with production database where aliases is TEXT[]
--          Migration 030 defined aliases as JSONB but live DB uses TEXT[]
-- Database: personal_koi
-- Notes: Revised to handle dependent views (ledger_entities,
--        entity_resolution_stats) and subquery limitation in USING clause.
--        Original version failed on prod due to both issues.
-- =============================================================================

-- Only alter if column exists and is JSONB
DO $$
DECLARE
    _ledger_entities_def TEXT;
    _stats_def TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'entity_registry'
        AND column_name = 'aliases'
        AND data_type = 'jsonb'
    ) THEN
        RAISE NOTICE 'aliases column is already TEXT[] or does not exist — skipping';
        RETURN;
    END IF;

    -- Save dependent view definitions (if they exist) before dropping
    SELECT pg_get_viewdef('ledger_entities'::regclass, true) INTO _ledger_entities_def
    FROM pg_class WHERE relname = 'ledger_entities' AND relkind = 'v';

    SELECT pg_get_viewdef('entity_resolution_stats'::regclass, true) INTO _stats_def
    FROM pg_class WHERE relname = 'entity_resolution_stats' AND relkind = 'v';

    -- Drop dependent views
    DROP VIEW IF EXISTS ledger_entities;
    DROP VIEW IF EXISTS entity_resolution_stats;

    -- Drop default (may be jsonb-typed, blocking ALTER TYPE)
    ALTER TABLE entity_registry ALTER COLUMN aliases DROP DEFAULT;

    -- Convert JSONB to TEXT[] using simple cast (avoids subquery limitation)
    -- Safe because all known prod data is '[]'::jsonb; non-empty arrays use
    -- the array_agg path as fallback
    ALTER TABLE entity_registry
    ALTER COLUMN aliases TYPE TEXT[]
    USING CASE
        WHEN aliases IS NULL THEN '{}'::TEXT[]
        WHEN aliases = 'null'::jsonb THEN '{}'::TEXT[]
        WHEN aliases = '[]'::jsonb THEN '{}'::TEXT[]
        ELSE ARRAY(SELECT jsonb_array_elements_text(aliases))
    END;

    RAISE NOTICE 'Converted aliases column from JSONB to TEXT[]';

    -- Recreate views with TEXT[]-compatible functions
    IF _ledger_entities_def IS NOT NULL THEN
        EXECUTE 'CREATE VIEW ledger_entities AS ' || _ledger_entities_def;
        RAISE NOTICE 'Recreated ledger_entities view';
    END IF;

    IF _stats_def IS NOT NULL THEN
        -- The old view used jsonb_array_length — replace with array_length
        _stats_def := replace(_stats_def, 'jsonb_array_length', 'array_length');
        -- array_length needs dimension arg; jsonb_array_length had 1 arg
        -- Replace array_length(aliases) with array_length(aliases, 1)
        -- and array_length(COALESCE(aliases, ''[]''::jsonb)) similarly
        _stats_def := replace(_stats_def, 'array_length(entity_registry.aliases)', 'array_length(entity_registry.aliases, 1)');
        _stats_def := replace(_stats_def, '''[]''::jsonb', '''{}''::text[]');
        _stats_def := replace(_stats_def, 'COALESCE(entity_registry.aliases, ''{}''::text[])', 'entity_registry.aliases');
        EXECUTE 'CREATE VIEW entity_resolution_stats AS ' || _stats_def;
        RAISE NOTICE 'Recreated entity_resolution_stats view (updated for TEXT[])';
    END IF;
END $$;

-- Set default to empty array (avoid nulls, makes COALESCE unnecessary)
ALTER TABLE entity_registry
ALTER COLUMN aliases SET DEFAULT '{}';

-- Backfill any existing nulls
UPDATE entity_registry SET aliases = '{}' WHERE aliases IS NULL;

-- Ensure GIN index exists for TEXT[] (different syntax than JSONB)
-- Drop JSONB GIN index if exists, create TEXT[] GIN index
DROP INDEX IF EXISTS idx_entity_registry_aliases_gin;
CREATE INDEX IF NOT EXISTS idx_entity_aliases_gin
ON entity_registry USING GIN (aliases);

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON COLUMN entity_registry.aliases IS 'Array of alternative names for alias matching (TEXT[] for efficient ANY() queries)';
