-- =============================================================================
-- Migration 111 DOWN: revert the entity type registry scaffold
-- =============================================================================
-- Pairs with: 111_entity_type_registry.sql
-- Run:        psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/111_entity_type_registry_down.sql
--
-- SAME ATOMICITY FLAGS AS THE UP-MIGRATION, for the same reason: a rollback that
-- half-applies leaves the operator in a state neither file describes. Every
-- statement uses IF EXISTS, so this file is idempotent and re-runnable -- if it
-- aborts, fix the cause and run it again rather than hand-repairing.
--
-- =============================================================================
-- ORDER OF OPERATIONS — REVERT CODE BEFORE RUNNING THIS
-- =============================================================================
-- Dropping resolution_tier while deployed code still writes it makes every
-- entity INSERT fail with:
--     column "resolution_tier" of relation "entity_registry" does not exist
-- turning a cosmetic rollback into a total write outage.
--
--   1. git revert <step-5 sha> in EVERY checkout that serves the API or runs
--      ingestion, then restart the service
--   2. verify the new PID is serving and /health is healthy
--   3. only then run this file
--
-- =============================================================================
-- DROP ORDER — dependency-correct
-- =============================================================================
--   trigger -> CHECK constraints -> function -> views -> indexes
--           -> columns -> allowed_facets -> allowed_entity_types
--
-- Constraints BEFORE the function: chk_entity_facets_shape DEPENDS ON
-- koi_facets_well_formed(), so DROP FUNCTION ahead of it fails with a
-- dependency error.
-- allowed_facets BEFORE allowed_entity_types: the FK points that way.
--
-- No CASCADE anywhere. CASCADE on a rollback silently removes objects this file
-- never enumerated, which is precisely what a rollback must not do.
--
-- =============================================================================
-- DATA SAFETY
-- =============================================================================
-- entity_facets and resolution_tier were both added empty/NULL; no pre-existing
-- row carried a value. Dropping them loses only values written AFTER 111
-- applied -- acceptable, because rollback only fires while the feature is failing.
--
-- NOTHING here touches entity_type, entity_text, merged_into or any pre-existing
-- column. 111 added no constraint to entity_type, so there is none to drop.
-- =============================================================================

-- 1. trigger (before its function)
DROP TRIGGER IF EXISTS tr_entity_facets_registered ON entity_registry;

-- 2. CHECK constraints (before the function they depend on)
ALTER TABLE entity_registry DROP CONSTRAINT IF EXISTS chk_entity_facets_shape;
ALTER TABLE entity_registry DROP CONSTRAINT IF EXISTS chk_entity_resolution_tier;

-- 3. functions
DROP FUNCTION IF EXISTS entity_facets_registered_guard();
DROP FUNCTION IF EXISTS koi_facets_well_formed(TEXT[]);

-- 4. views
DROP VIEW IF EXISTS edge_type_violations;
DROP VIEW IF EXISTS entity_type_drift;

-- 5. indexes
DROP INDEX IF EXISTS idx_entity_registry_resolution_tier;
DROP INDEX IF EXISTS idx_entity_registry_facets;

-- 6. columns
ALTER TABLE entity_registry
    DROP COLUMN IF EXISTS entity_facets,
    DROP COLUMN IF EXISTS resolution_tier;

-- 7. tables (facets first -- FK points at types)
DROP TABLE IF EXISTS allowed_facets;
DROP TABLE IF EXISTS allowed_entity_types;

-- 8. bookkeeping
DELETE FROM koi_migrations WHERE migration_id = '111_entity_type_registry';

-- =============================================================================
-- VERIFY AFTER ROLLBACK
-- =============================================================================
--   \d entity_registry
--     -> NO entity_facets, NO resolution_tier
--   SELECT to_regclass('allowed_entity_types'), to_regclass('allowed_facets');
--     -> both NULL
--   SELECT count(*), count(DISTINCT entity_type) FROM entity_registry;
--     -> unchanged from the pre-111 baseline (record both counts before applying 111)
--   SELECT count(*) FROM koi_migrations WHERE migration_id='111_entity_type_registry';
--     -> 0
-- =============================================================================
