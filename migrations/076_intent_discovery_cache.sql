-- Migration 076: Intent Discovery Cache
-- Remote intent discovery cache — receives federated discovery projections
-- Separate from intent_registry to avoid null-constraint violations
-- (intent_registry requires intent_key, publisher_name which discovery payloads don't carry)

CREATE TABLE IF NOT EXISTS intent_discovery_cache (
    id SERIAL PRIMARY KEY,
    intent_rid TEXT UNIQUE NOT NULL,
    source_node TEXT NOT NULL,              -- which peer sent this
    intent_type TEXT NOT NULL,
    status TEXT NOT NULL,
    landscape_group TEXT NOT NULL,
    visibility TEXT NOT NULL,
    asset_offered TEXT,
    asset_wanted TEXT,
    quantity TEXT,
    received_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discovery_cache_rid ON intent_discovery_cache(intent_rid);
CREATE INDEX IF NOT EXISTS idx_discovery_cache_status ON intent_discovery_cache(status);
CREATE INDEX IF NOT EXISTS idx_discovery_cache_asset ON intent_discovery_cache(asset_offered, asset_wanted);

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('076_intent_discovery_cache', 'v1_federation_cache')
ON CONFLICT (migration_id) DO NOTHING;
