\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 035a: convert entity_registry.aliases to TEXT[] on fresh personal installs
--
-- 030 creates entity_registry.aliases as JSONB; live personal_koi databases use
-- TEXT[], and 036_aliases_text_array aligns new databases. On a new database 036
-- fails: its ALTER ... TYPE ... USING contains a subquery, which PostgreSQL rejects
-- ("cannot use subquery in transform expression"), so the column stays JSONB and
-- 036's later SET DEFAULT '{}' gives it a JSONB object default.
--
-- This runs the same conversion first, with the subquery moved into a function
-- (functions are allowed in USING), and recreates the two dependent views exactly
-- as 036 does. 036 then finds TEXT[] and takes its own "already TEXT[]" branch.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker),
-- and a no-op when aliases is already TEXT[].

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
CREATE OR REPLACE FUNCTION pg_temp.koi_jsonb_aliases_to_text_array(j JSONB)
RETURNS TEXT[] LANGUAGE sql IMMUTABLE AS $fn$
    SELECT CASE
        WHEN j IS NULL OR jsonb_typeof(j) <> 'array' THEN '{}'::TEXT[]
        ELSE coalesce((SELECT array_agg(x) FROM jsonb_array_elements_text(j) AS x), '{}'::TEXT[])
    END
$fn$;

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
        RAISE NOTICE 'aliases column is already TEXT[] or does not exist: skipping';
        RETURN;
    END IF;

    SELECT pg_get_viewdef('ledger_entities'::regclass, true) INTO _ledger_entities_def
    FROM pg_class WHERE relname = 'ledger_entities' AND relkind = 'v';
    SELECT pg_get_viewdef('entity_resolution_stats'::regclass, true) INTO _stats_def
    FROM pg_class WHERE relname = 'entity_resolution_stats' AND relkind = 'v';

    DROP VIEW IF EXISTS ledger_entities;
    DROP VIEW IF EXISTS entity_resolution_stats;

    ALTER TABLE entity_registry ALTER COLUMN aliases DROP DEFAULT;
    ALTER TABLE entity_registry
        ALTER COLUMN aliases TYPE TEXT[]
        USING pg_temp.koi_jsonb_aliases_to_text_array(aliases);

    -- The view rewrites of 036_aliases_text_array. 036 adds the array_length
    -- dimension before it strips the COALESCE, which leaves a one-argument
    -- array_length(text[]) that does not exist; here the COALESCE goes first.
    IF _ledger_entities_def IS NOT NULL THEN
        EXECUTE 'CREATE VIEW ledger_entities AS ' || _ledger_entities_def;
    END IF;
    IF _stats_def IS NOT NULL THEN
        _stats_def := replace(_stats_def, 'jsonb_array_length', 'array_length');
        _stats_def := replace(_stats_def, '''[]''::jsonb', '''{}''::text[]');
        _stats_def := replace(_stats_def, 'COALESCE(entity_registry.aliases, ''{}''::text[])', 'entity_registry.aliases');
        _stats_def := replace(_stats_def, 'array_length(entity_registry.aliases)', 'array_length(entity_registry.aliases, 1)');
        EXECUTE 'CREATE VIEW entity_resolution_stats AS ' || _stats_def;
    END IF;
    RAISE NOTICE 'Converted entity_registry.aliases from JSONB to TEXT[]';
END $$;
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
