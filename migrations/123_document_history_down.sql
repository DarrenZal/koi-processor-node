-- =============================================================================
-- Rollback for migration 123: document history
-- =============================================================================
-- The journal is the ONLY record of what was superseded and why, and the claims are the
-- only record of sticky retention policy. NEITHER IS RE-DERIVABLE.
--
-- The export is NOT a comment any more -- see the EXPORT block below line 22. It used to
-- live here, commented out, under the word "EXPORT FIRST:". Run as shipped, the file
-- dropped the ownership rows and journal edges, printed only
-- "DROP TABLE / DROP TABLE / DELETE 1", and exited 0 with "down assertion PASSED".
-- An instruction in a comment is not a step.
--
-- This drops exactly FOUR objects and one ledger row:
--   view  document_source_projection
--   table document_supersession
--   table document_source_claim
--   column session_discourse_moves.superseded_at   <- and, with it, leg E's stamps
--
-- It touches NOTHING on koi_memories or koi_memory_chunks. koi_memories.superseded_at and
-- koi_memories.previous_version_id are PRE-EXISTING -- created by
-- migrations/003_create_isolated_koi_tables.sql's own CREATE TABLE, together with
-- idx_koi_memories_superseded -- and are neither created by 123 nor dropped here. Reading
-- "the nullable column" as koi_memories.superseded_at would be catastrophic; it is
-- session_discourse_moves.superseded_at and nothing else.
-- =============================================================================
\set ON_ERROR_STOP on

-- =============================================================================
-- EXPORT -- runs, and MUST be below \set ON_ERROR_STOP on. Placement is the whole
-- point, not style. Measured both ways against this database:
--   \copy ABOVE \set ON_ERROR_STOP  ->  failed export prints one error, psql CONTINUES
--                                       to the DROPs, and EXITS 0.        (exit=0, reached=1)
--   \copy BELOW \set ON_ERROR_STOP  ->  failed export ABORTS before anything drops.
--                                                                          (exit=3, reached=0)
-- So the obvious fix -- lifting the commented lines to the header -- reproduces the
-- original defect with the appearance of having repaired it.
--
-- The paths are LITERAL, deliberately. psql's \copy does NOT interpolate :'var' in a
-- filename -- it passes the variable name through as a literal string. Measured:
--   \copy (SELECT 1) TO :'sup_path' CSV
--   -> ERROR: syntax error at or near "'sup_path'" ... TO STDOUT 'sup_path' CSV HEADER
-- so a timestamped-variable version LOOKS parameterised and writes nothing. If you want a
-- timestamp, rename the files afterwards; do not reintroduce the variable.
-- psql 14 does expand a leading ~ in a \copy path (verified).
-- A re-run overwrites these two files -- acceptable, because a second run has nothing to
-- export (the tables are already gone) and aborts at the first \copy instead of dropping.
-- =============================================================================
\copy document_supersession TO '~/koi-backups/123_down_supersession.csv' CSV HEADER
\copy document_source_claim TO '~/koi-backups/123_down_claims.csv'       CSV HEADER
\echo 'exported journal + claims before dropping anything'

\if :{?lock_timeout}
\else
  \set lock_timeout 3s
\endif

BEGIN;
SET LOCAL lock_timeout = :'lock_timeout';

-- The view depends on koi_memories only; drop it before the tables for readability, not
-- necessity. CREATE OR REPLACE VIEW in the up migration means a re-run is safe either way.
DROP VIEW IF EXISTS document_source_projection;

-- No CASCADE, deliberately: if a view, index or dependent object has appeared on this
-- column since 123 was applied, fail loudly rather than silently dropping it. (The up
-- migration creates no index on this column, so there is nothing of its own to cascade.)
-- Dropping the column also discards leg E's stamps and every runtime stamp; that data is
-- a projection of koi_memories.superseded_at plus the journal, both of which survive
-- until the next two statements.
ALTER TABLE session_discourse_moves DROP COLUMN IF EXISTS superseded_at;

-- Drops each table's owned indexes, and for document_supersession its bigserial sequence.
-- Order is free: neither table has a foreign key in either direction, by design (a claim
-- and a journal row must outlive a history_policy='drop' hard delete of the row they name).
DROP TABLE IF EXISTS document_supersession;
DROP TABLE IF EXISTS document_source_claim;

DELETE FROM koi_migrations WHERE migration_id = '123_document_history';

-- Post-condition, asserted before COMMIT rather than discovered afterwards. Paired with a
-- positive control so a pass cannot mean "the catalog query matched nothing".
DO $$
DECLARE left_over text;
BEGIN
  SELECT string_agg(x, '; ') INTO left_over FROM (
    SELECT 'table document_source_claim'      AS x WHERE to_regclass('document_source_claim') IS NOT NULL
    UNION ALL SELECT 'table document_supersession'  WHERE to_regclass('document_supersession') IS NOT NULL
    UNION ALL SELECT 'view document_source_projection' WHERE to_regclass('document_source_projection') IS NOT NULL
    UNION ALL SELECT 'column session_discourse_moves.superseded_at' WHERE EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='session_discourse_moves' AND column_name='superseded_at')
    UNION ALL SELECT 'ledger row 123_document_history' WHERE EXISTS (
      SELECT 1 FROM koi_migrations WHERE migration_id='123_document_history')
    -- the down migration must not have touched the PRE-EXISTING koi_memories columns
    UNION ALL SELECT 'koi_memories.superseded_at was dropped -- THIS MUST NEVER HAPPEN' WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='koi_memories' AND column_name='superseded_at')
    UNION ALL SELECT 'koi_memories.previous_version_id was dropped -- THIS MUST NEVER HAPPEN' WHERE NOT EXISTS (
      SELECT 1 FROM information_schema.columns WHERE table_schema='public'
        AND table_name='koi_memories' AND column_name='previous_version_id')
  ) t;
  IF left_over IS NOT NULL THEN
    RAISE EXCEPTION 'migration 123 down failed: %', left_over;
  END IF;
  -- POSITIVE CONTROL: the catalog queries above must be able to SEE session_discourse_moves
  -- at all, or every "absent" arm is vacuous.
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public'
                   AND table_name='session_discourse_moves' AND column_name='source_rid') THEN
    RAISE EXCEPTION 'migration 123 down: catalog positive control failed -- '
                    'session_discourse_moves.source_rid is not visible, so the absence '
                    'assertions above proved nothing';
  END IF;
  RAISE NOTICE 'migration 123 down assertion PASSED';
END $$;

COMMIT;

-- WHAT THIS DOWN MIGRATION DOES NOT UNDO, stated rather than implied:
--   * rows already stamped in koi_memories.superseded_at  -> use the kill switch
--     (POST /history/read-filter {"read_filter":"off"}) to make them visible again;
--   * koi_memory_chunks already deleted by an apply       -> re-ingest from the git archive;
--   * rows already hard-deleted under history_policy='drop' -> the archive is the only copy,
--     which is why the --no-hardlinks mirror is a hard predecessor of any chunk-deleting run;
--   * document_ingestion_log                              -> never written or deleted by 123.
-- Those are runtime effects, recovered by the rollback STEPS, not by this DDL.