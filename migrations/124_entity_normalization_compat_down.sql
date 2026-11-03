-- =============================================================================
-- 124_entity_normalization_compat_down — reverse 124.
-- =============================================================================
-- Safe to run at any time. 124 is additive: it creates a function, an index and a
-- view, and changes no existing row and no existing column. Reversing it therefore
-- destroys no data — unlike 123's down file, there is nothing to export first.
--
-- ORDER MATTERS: the view and the index both DEPEND on koi_normalize_entity_text,
-- so the function is dropped last. Dropping it first would either fail or, with
-- CASCADE, silently take the view and index with it — which is why CASCADE is not
-- used here: a dependency that has grown since 124 shipped should make this file
-- FAIL loudly rather than be removed without anyone noticing.
--
-- Reversing this removes the normalizer-drift compatibility read. Any caller that queries
-- through koi_normalize_entity_text() must be reverted in the same deploy, or it
-- will error with "function does not exist" on its next call. As of 2026-09-13 the
-- callers are: api/ingest_identity.py and the document-ingest preflight that uses it.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

DROP VIEW IF EXISTS entity_current_norm_duplicates;

DROP INDEX IF EXISTS idx_entity_registry_current_norm;
DROP INDEX IF EXISTS idx_entity_registry_current_norm_label;

-- No CASCADE, deliberately: if something added since 124 still depends on this
-- function, this statement must fail and stop the rollback.
DROP FUNCTION IF EXISTS koi_normalize_entity_text(TEXT);

DELETE FROM koi_migrations
 WHERE migration_id = 'personal:124_entity_normalization_compat';

COMMIT;
