-- Migration 050: Vault sync metrics persistence
-- Phase Sync-1.5a: Observability — counters survive restarts

CREATE TABLE IF NOT EXISTS vault_sync_metrics (
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    metrics_json JSONB NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
