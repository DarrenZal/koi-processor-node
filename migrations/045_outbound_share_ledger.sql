-- Migration 045: Outbound share ledger for reliable FORGET retraction
--
-- The /share endpoint creates events in koi_net_events, but those rows
-- expire via TTL. For reliable retraction during peer offboarding,
-- we need a durable ledger of what was shared with whom.

CREATE TABLE IF NOT EXISTS koi_outbound_shares (
    id SERIAL PRIMARY KEY,
    document_rid TEXT NOT NULL,
    target_node TEXT NOT NULL,
    shared_at TIMESTAMPTZ DEFAULT NOW(),
    retracted_at TIMESTAMPTZ,
    UNIQUE(document_rid, target_node)
);

CREATE INDEX IF NOT EXISTS idx_outbound_shares_target
    ON koi_outbound_shares(target_node);

CREATE INDEX IF NOT EXISTS idx_outbound_shares_active
    ON koi_outbound_shares(target_node)
    WHERE retracted_at IS NULL;
