-- Migration 054: Commons merge candidate queue
--
-- When the ingest worker finds ambiguous entity matches (0.85-0.95 confidence),
-- it records them here for admin review instead of auto-merging.
-- Non-ambiguous entities from the same share are ingested immediately.

CREATE TABLE IF NOT EXISTS koi_commons_merge_candidates (
    id SERIAL PRIMARY KEY,
    share_id INT NOT NULL REFERENCES koi_shared_documents(id),
    remote_entity_label TEXT NOT NULL,
    remote_entity_type TEXT,
    local_entity_uri TEXT NOT NULL,
    local_entity_label TEXT NOT NULL,
    confidence FLOAT NOT NULL,
    resolution TEXT CHECK (resolution IN ('merge', 'keep_separate', 'cross_ref')),
    resolved_by TEXT,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (share_id, remote_entity_label, local_entity_uri)
);

CREATE INDEX IF NOT EXISTS idx_merge_candidates_share
    ON koi_commons_merge_candidates(share_id);

CREATE INDEX IF NOT EXISTS idx_merge_candidates_unresolved
    ON koi_commons_merge_candidates(share_id)
    WHERE resolution IS NULL;
