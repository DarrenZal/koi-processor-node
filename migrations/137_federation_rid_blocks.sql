-- =============================================================================
-- Migration 137: federation_rid_blocks, which stops exact records from being sent to peers again
-- =============================================================================
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/137_federation_rid_blocks.sql
-- Rollback: not a backout. The down file refuses while any record is blocked (see it).
--
-- Date: 2026-10-25
-- Record: an internal record (E2, NP6(b))
--
-- Why: a peer upserts what it receives, so a record removed on a peer comes back with the next event
-- this node queues for it. Each row here is the sha256 of one exact identifier (a task key, an entity
-- URI, a doclink RID). api/federation_blocks.py withholds any event whose RID or any string value in its
-- contents or manifest hashes to a row: when it is queued, polled, peeked for push, served by
-- /koi-net/bundles/fetch or /koi-net/manifests/fetch, and in the /koi-net/rids/fetch listing. Rows hold
-- hashes only, because some identifiers carry a name.
--
-- The guard fails closed: without this table nothing is queued or served, so apply it before the code.

CREATE TABLE IF NOT EXISTS federation_rid_blocks (
    rid_sha256 TEXT PRIMARY KEY CHECK (rid_sha256 ~ '^[0-9a-f]{64}$'),
    kind       TEXT NOT NULL,
    reason     TEXT NOT NULL,
    blocked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE federation_rid_blocks IS
    'sha256 of exact record identifiers that are never sent to a peer again. Written from a hash manifest by scripts/federation/block_rids.py.';

DO $$
BEGIN
    IF to_regclass('koi_migrations') IS NOT NULL THEN
        INSERT INTO koi_migrations (migration_id, checksum)
        VALUES ('137_federation_rid_blocks', 'v1_federation_rid_blocks')
        ON CONFLICT (migration_id) DO NOTHING;
    END IF;
END $$;
