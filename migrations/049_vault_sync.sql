-- Migration 049: Vault Sync tables for bidirectional markdown sync between KOI-net peers
-- Phase Sync-1: two peers, poll-based (~60s), conflict copies

-- Track sync state per file
CREATE TABLE IF NOT EXISTS vault_sync_state (
    id SERIAL PRIMARY KEY,
    relative_path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    origin_node TEXT NOT NULL,
    origin_seq INTEGER NOT NULL DEFAULT 1,
    file_size_bytes INTEGER NOT NULL DEFAULT 0,
    last_synced_at TIMESTAMPTZ,
    last_modified_at TIMESTAMPTZ NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(relative_path)
);

CREATE INDEX IF NOT EXISTS idx_vault_sync_deleted ON vault_sync_state(is_deleted) WHERE is_deleted = TRUE;

-- Per-peer sync configuration (Phase Sync-1: exactly one peer)
CREATE TABLE IF NOT EXISTS vault_sync_peers (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    peer_node_rid TEXT NOT NULL REFERENCES koi_net_nodes(node_rid),
    shared_folder TEXT NOT NULL DEFAULT 'Shared',
    enabled BOOLEAN DEFAULT TRUE,
    last_full_sync_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Idempotency guard: prevent redelivered events from creating duplicate conflict copies
CREATE TABLE IF NOT EXISTS vault_sync_applied_events (
    source_node TEXT NOT NULL,
    event_id UUID NOT NULL,
    rid TEXT NOT NULL,
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(source_node, event_id, rid)
);

CREATE INDEX IF NOT EXISTS idx_vault_sync_applied_age ON vault_sync_applied_events(applied_at);
