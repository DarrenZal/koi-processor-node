-- Migration 074: Intent Registry (MVIS)
-- Intents as first-class KOI graph entities for Cascadia pilot
-- Follows task_registry (flat, CHECK) + claims_engine (state log, entity link) patterns

-- Landscape group configuration (centralized governance parameters)
CREATE TABLE IF NOT EXISTS landscape_group_config (
    id SERIAL PRIMARY KEY,
    group_key TEXT UNIQUE NOT NULL,           -- e.g. 'olympic_peninsula'
    display_name TEXT NOT NULL,               -- e.g. 'Olympic Peninsula'
    decay_lambda FLOAT DEFAULT 0.023,         -- ~30-day half-life (ln(2)/30)
    coordinator_name TEXT,
    coordinator_contact TEXT,                 -- email (never exposed via API)
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Controlled vocabulary for asset types
CREATE TABLE IF NOT EXISTS intent_asset_vocabulary (
    id SERIAL PRIMARY KEY,
    asset_key TEXT UNIQUE NOT NULL,           -- e.g. 'tractor_repair'
    display_name TEXT NOT NULL,               -- e.g. 'Tractor Repair'
    category TEXT,                            -- e.g. 'equipment', 'food', 'labor'
    landscape_group TEXT,                     -- NULL = global, otherwise group-specific
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Core intent registry
CREATE TABLE IF NOT EXISTS intent_registry (
    id SERIAL PRIMARY KEY,
    intent_rid TEXT UNIQUE NOT NULL,          -- orn:koi-net.intent:<hash> (graph identity)
    intent_key TEXT UNIQUE NOT NULL,          -- client-side idempotency key
    entity_uri TEXT,                          -- entity_registry.fuseki_uri (intent as graph entity)

    intent_type TEXT NOT NULL
        CHECK (intent_type IN ('OFFER', 'WANT', 'SWAP')),
    status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'active', 'fulfilled', 'stale', 'archived')),

    -- Publisher (resolved to entity when possible)
    publisher_name TEXT NOT NULL,
    publisher_contact TEXT,                   -- NEVER exposed in list/match API responses
    publisher_uri TEXT,                       -- entity_registry.fuseki_uri

    -- Landscape & visibility
    landscape_group TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'local'
        CHECK (visibility IN ('local', 'regional')),

    -- What is offered / wanted (controlled vocabulary)
    asset_offered TEXT,                       -- from intent_asset_vocabulary (NULL for WANT)
    asset_wanted TEXT,                        -- from intent_asset_vocabulary (NULL for OFFER)
    quantity TEXT,                            -- freeform: "10 hours", "40 kg"
    description TEXT,                         -- human-readable context

    -- Priority & decay
    priority FLOAT NOT NULL DEFAULT 100.0,
    decay_rate TEXT NOT NULL DEFAULT 'normal'
        CHECK (decay_rate IN ('normal', 'urgent')),
    last_refreshed_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at DATE,

    -- Provenance (hybrid intake support)
    capture_method TEXT DEFAULT 'manual'
        CHECK (capture_method IN ('manual', 'transcript_extraction', 'process_note', 'agent')),
    source_document TEXT,                    -- reference to originating note/transcript
    source_excerpt TEXT,                     -- quote from source (for review)
    entered_by TEXT,                         -- scribe or system
    reviewed_by TEXT,                        -- who promoted draft → active
    ai_confidence FLOAT,                    -- NULL if manually created

    -- Metadata
    notes TEXT,                              -- coordinator/reviewer notes
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    fulfilled_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ
);

-- Intent state transitions (insert-only audit log)
CREATE TABLE IF NOT EXISTS intent_state_log (
    id SERIAL PRIMARY KEY,
    intent_rid TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    actor TEXT,                               -- coordinator, scribe, or 'system'
    reason TEXT,                              -- e.g. 'review_approved', 'coordinator_match', 'decay_archive'
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_intent_rid ON intent_registry(intent_rid);
CREATE INDEX IF NOT EXISTS idx_intent_status ON intent_registry(status);
CREATE INDEX IF NOT EXISTS idx_intent_type ON intent_registry(intent_type);
CREATE INDEX IF NOT EXISTS idx_intent_landscape ON intent_registry(landscape_group);
CREATE INDEX IF NOT EXISTS idx_intent_visibility ON intent_registry(visibility);
CREATE INDEX IF NOT EXISTS idx_intent_asset_offered ON intent_registry(asset_offered);
CREATE INDEX IF NOT EXISTS idx_intent_asset_wanted ON intent_registry(asset_wanted);
CREATE INDEX IF NOT EXISTS idx_intent_publisher_uri ON intent_registry(publisher_uri);
CREATE INDEX IF NOT EXISTS idx_intent_expires ON intent_registry(expires_at);
CREATE INDEX IF NOT EXISTS idx_intent_capture ON intent_registry(capture_method);
CREATE INDEX IF NOT EXISTS idx_intent_state_log_rid ON intent_state_log(intent_rid);
CREATE INDEX IF NOT EXISTS idx_vocab_category ON intent_asset_vocabulary(category);
CREATE INDEX IF NOT EXISTS idx_landscape_key ON landscape_group_config(group_key);

-- Predicates for entity relationships
-- object_types includes 'Intent' because intents are first-class graph entities
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
    ('publishes_intent', 'Person or org publishes an intent', ARRAY['Person', 'Organization'], ARRAY['Intent']),
    ('fulfills_intent', 'Person or org fulfills a matched intent', ARRAY['Person', 'Organization'], ARRAY['Intent'])
ON CONFLICT (predicate) DO NOTHING;

-- Migration bookkeeping (per 066_attestations.sql pattern — literal checksum, not pg_read_file)
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('074_intent_registry', 'v1_mvis_pilot')
ON CONFLICT (migration_id) DO NOTHING;
