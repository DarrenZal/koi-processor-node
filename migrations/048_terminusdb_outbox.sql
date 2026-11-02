-- Migration 048: TerminusDB outbox table for eventual-consistency sync
-- PostgreSQL remains authoritative; outbox drains to TerminusDB asynchronously.

CREATE TABLE IF NOT EXISTS terminusdb_outbox (
    id BIGSERIAL PRIMARY KEY,
    operation TEXT NOT NULL,          -- 'entity_upsert', 'assertion_upsert', 'assertion_retract'
    payload JSONB NOT NULL,           -- Full document to write to TerminusDB
    payload_hash TEXT NOT NULL,       -- SHA256 of (operation + rid + payload) for dedup
    rid TEXT,                         -- Entity/assertion RID
    source_rid TEXT,                  -- Vault file that triggered this
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT DEFAULT 0,
    claimed_at TIMESTAMPTZ,                    -- Worker lease: set on claim, NULL when idle
    claimed_by TEXT,                            -- Worker ID (hostname:pid) for debugging
    next_attempt_at TIMESTAMPTZ DEFAULT NOW(),  -- Exponential backoff scheduling
    created_at TIMESTAMPTZ DEFAULT NOW(),
    applied_at TIMESTAMPTZ,
    error TEXT,

    CONSTRAINT valid_status CHECK (status IN ('pending', 'processing', 'applied', 'dead_letter'))
);

-- Dedup: only block re-enqueue while row is still pending (applied/dead_letter don't block future re-enqueue)
CREATE UNIQUE INDEX IF NOT EXISTS idx_outbox_dedup ON terminusdb_outbox(payload_hash)
    WHERE status IN ('pending', 'processing');

-- Worker polling: FOR UPDATE SKIP LOCKED safe
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON terminusdb_outbox(status, next_attempt_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_rid ON terminusdb_outbox(rid);
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letter ON terminusdb_outbox(status)
    WHERE status = 'dead_letter';
