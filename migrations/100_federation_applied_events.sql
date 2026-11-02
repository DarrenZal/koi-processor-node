-- Migration 100: federation_applied_events
-- Idempotency tracking for koi-net domain federation.
-- Subscriber handlers (knowledge_episode, knowledge_fact, document_entity_link)
-- record successfully-applied events here to dedupe poll redelivery.
--
-- Pattern by domain:
--   knowledge_episode / knowledge_fact (idempotent ON CONFLICT):
--     apply first, INSERT idempotency row on success.
--   document_entity_link (additive mention_count, NOT idempotent):
--     INSERT idempotency row first (inside txn); only proceed with
--     additive UPDATE if the insert succeeded (txn-rolls-back-on-fail).
--
-- Cleanup: events expire from koi_net_events at 72h TTL; idempotency rows
-- past 6 months are pruned by a monthly cron (see plan §Risks).

CREATE TABLE IF NOT EXISTS federation_applied_events (
    domain      TEXT NOT NULL,
    event_id    UUID NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT NOW(),
    source_node TEXT,
    PRIMARY KEY (domain, event_id)
);

CREATE INDEX IF NOT EXISTS idx_fae_applied_at
    ON federation_applied_events (applied_at);

COMMENT ON TABLE federation_applied_events IS
    'Idempotency tracking for koi-net domain federation (knowledge_episode, knowledge_fact, document_entity_link). See plan koi-graph-graceful-toucan.';
