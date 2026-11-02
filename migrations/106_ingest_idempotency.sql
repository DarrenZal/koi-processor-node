-- 106_ingest_idempotency.sql
--
-- Idempotency ledger for write endpoints that accept an optional caller-supplied
-- `request_id`. When a request carries a request_id that is already recorded,
-- the endpoint returns the stored response (with `idempotent_replay: true`)
-- instead of performing the writes a second time. The response row is inserted
-- INSIDE the same transaction as the writes, so a rolled-back request never
-- leaves a stale idempotency entry.
--
-- Currently written by POST /knowledge/episodes and POST /ingest.

CREATE TABLE IF NOT EXISTS ingest_idempotency (
    request_id TEXT PRIMARY KEY,
    endpoint   TEXT NOT NULL,
    response   JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
