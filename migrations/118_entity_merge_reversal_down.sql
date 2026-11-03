-- Rollback for migration 118.
-- NOTE: dropping `reversal` makes every merge recorded since 2026-09-02
-- permanently irreversible, exactly as the 263 pre-118 merges already are.
-- Export first if any unreverted reversible merges remain:
--   \copy (SELECT * FROM entity_merge_log WHERE reversal IS NOT NULL AND reverted_at IS NULL) TO 'merge_reversal_backup.csv' CSV HEADER

DROP INDEX IF EXISTS idx_entity_merge_log_reversible;
ALTER TABLE entity_merge_log DROP COLUMN IF EXISTS reversal;

DELETE FROM koi_migrations WHERE migration_id = '118_entity_merge_reversal';
