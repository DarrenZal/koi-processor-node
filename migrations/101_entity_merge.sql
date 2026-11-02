-- Migration 101: entity_merge
-- Tombstone columns on entity_registry + an audit log for POST /entities/merge
-- (api/routers/admin_router.py). A merge rewires every entity-URI reference from
-- a loser onto a survivor in ONE transaction, then TOMBSTONES the loser
-- (merged_into = survivor) instead of hard-deleting it: the ON DELETE CASCADE
-- FKs on entity_relationships / pending_relationships would otherwise wipe the
-- very rows the merge just rewired.
--
-- See plan an internal planning document; task #3146.

-- 1. Tombstone columns. merged_into points a loser at its survivor (NULL = live);
--    self-referential FK to entity_registry.fuseki_uri (UNIQUE, so a valid FK
--    target). No ON DELETE action — entities are never hard-deleted.
ALTER TABLE entity_registry
    ADD COLUMN IF NOT EXISTS merged_into text REFERENCES entity_registry(fuseki_uri),
    ADD COLUMN IF NOT EXISTS merged_at   timestamptz,
    ADD COLUMN IF NOT EXISTS merged_by   text;

-- Partial index: redirect resolution and "who absorbed this entity" lookups
-- only ever touch tombstoned (non-NULL) rows.
CREATE INDEX IF NOT EXISTS idx_entity_registry_merged_into
    ON entity_registry (merged_into)
    WHERE merged_into IS NOT NULL;

-- 2. Audit + idempotency + reversibility log. One row per APPLIED merge.
--    rewired = per-table {rewired, deduped, merged, self_loops_deleted} counts.
--    reverted_at is reserved for a future un-merge path (not yet implemented).
CREATE TABLE IF NOT EXISTS entity_merge_log (
    id           SERIAL PRIMARY KEY,
    survivor_uri text        NOT NULL,
    loser_uri    text        NOT NULL,
    rewired      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    merged_by    text,
    merged_at    timestamptz NOT NULL DEFAULT NOW(),
    reverted_at  timestamptz
);

CREATE INDEX IF NOT EXISTS idx_entity_merge_log_loser
    ON entity_merge_log (loser_uri);
CREATE INDEX IF NOT EXISTS idx_entity_merge_log_survivor
    ON entity_merge_log (survivor_uri);

COMMENT ON TABLE entity_merge_log IS
    'Audit/idempotency/reversibility log for POST /entities/merge. One row per applied merge; rewired = per-table rewire+dedup counts. See task #3146.';
COMMENT ON COLUMN entity_registry.merged_into IS
    'If set, this entity was merged INTO the referenced fuseki_uri (tombstone). NULL = live. Resolve redirects by following this column.';
