-- =============================================================================
-- Migration 118: make entity merges reversible
-- =============================================================================
-- Date:     2026-09-02
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/118_entity_merge_reversal.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/118_entity_merge_reversal_down.sql
--
-- =============================================================================
-- WHY THE EXISTING LOG CANNOT REVERSE A MERGE
-- =============================================================================
-- entity_merge_log has carried a `reverted_at` column since migration 101 and
-- NOTHING HAS EVER SET IT -- a correctly-built column with no writer, the same
-- shape as the rest of this week's findings.
--
-- The deeper problem is that `rewired` records COUNTS, not identities:
--
--     {"plain": {"knowledge_facts.object_uri": 1},
--      "document_entity_links": {"rewired": 2, "deduped": 0}, ...}
--
-- "2 document links were rewired" does not say WHICH 2. After loser->survivor,
-- the loser's rows are indistinguishable from rows the survivor already held.
-- A merge is a blind UPDATE; the information needed to undo it is destroyed by
-- the act of doing it, unless captured first.
--
-- So: all 263 historical merges are IRREVERSIBLE and must stay that way. An
-- unmerge that guessed would silently manufacture wrong provenance, which is
-- worse than refusing. `reversal IS NULL` is the marker for "cannot be undone",
-- and unmerge refuses on it rather than doing its best.
--
-- =============================================================================
-- WHAT THIS ADDS
-- =============================================================================
-- `reversal` holds a pre-merge snapshot captured INSIDE the merge transaction,
-- before any rewiring:
--
--   {"schema": 1,
--    "loser": "orn:...",
--    "survivor": "orn:...",
--    "refs": {"knowledge_facts.subject_uri": [12,88], ...},   <- exact row ids
--    "aliases_added": ["corin ashgrove"],                       <- only the NEW ones
--    "deletions": {"entity_relationships": [{...full row...}]} <- restorable
--   }
--
-- Row ids are sufficient because every affected table has an `id` primary key
-- (verified 2026-09-02 across all 9). Deletions need full row content -- the
-- merge's dedupe and self-loop steps DELETE rows, and an id alone cannot
-- resurrect one.
--
-- `aliases_added` records only aliases the survivor did not already have, so
-- unmerge removes exactly what the merge contributed and does not strip an
-- alias the survivor legitimately owned beforehand.
-- =============================================================================

ALTER TABLE entity_merge_log
    ADD COLUMN IF NOT EXISTS reversal JSONB;

COMMENT ON COLUMN entity_merge_log.reversal IS
  'Pre-merge snapshot enabling exact reversal: affected row ids per table.column, aliases actually added, and full content of any deleted rows. Captured inside the merge transaction before rewiring. NULL means the merge is NOT reversible -- true for all 263 merges predating migration 118 (2026-09-02), because the prior log recorded only counts, and a blind UPDATE destroys the identities needed to undo it. unmerge REFUSES when this is NULL rather than guessing.';

COMMENT ON COLUMN entity_merge_log.reverted_at IS
  'Set by unmerge. Existed unwritten from migration 101 until 2026-09-02.';

CREATE INDEX IF NOT EXISTS idx_entity_merge_log_reversible
    ON entity_merge_log (id)
    WHERE reversal IS NOT NULL AND reverted_at IS NULL;

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('118_entity_merge_reversal', 'v1_merge_reversal')
ON CONFLICT (migration_id) DO NOTHING;
