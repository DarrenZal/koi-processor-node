-- Migration 075: Intent Match Proposals
-- Coordinator-vetted matching workflow for MVIS pilot

CREATE TABLE IF NOT EXISTS intent_match_proposals (
    id SERIAL PRIMARY KEY,
    proposal_rid TEXT UNIQUE NOT NULL,        -- orn:koi-net.match:<hash>
    offer_intent_rid TEXT NOT NULL,           -- intent_registry.intent_rid
    want_intent_rid TEXT NOT NULL,            -- intent_registry.intent_rid
    match_type TEXT NOT NULL
        CHECK (match_type IN ('local', 'cross_landscape')),
    status TEXT NOT NULL DEFAULT 'candidate'
        CHECK (status IN ('candidate', 'introduced', 'accepted', 'declined', 'expired')),
    score FLOAT,                             -- min(offer.priority, want.priority)
    coordinator_notes TEXT,
    proposed_at TIMESTAMPTZ DEFAULT NOW(),
    introduced_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    resolved_by TEXT                          -- coordinator name
);

-- Deduplication: only one unresolved proposal per intent pair at a time.
-- Resolved proposals (accepted/declined/expired) don't block new candidates.
CREATE UNIQUE INDEX IF NOT EXISTS idx_match_pair_unresolved
    ON intent_match_proposals (offer_intent_rid, want_intent_rid)
    WHERE status IN ('candidate', 'introduced');

CREATE INDEX IF NOT EXISTS idx_match_offer ON intent_match_proposals(offer_intent_rid);
CREATE INDEX IF NOT EXISTS idx_match_want ON intent_match_proposals(want_intent_rid);
CREATE INDEX IF NOT EXISTS idx_match_status ON intent_match_proposals(status);

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('075_intent_match_proposals', 'v1_coordinator_matching')
ON CONFLICT (migration_id) DO NOTHING;
