-- =============================================================================
-- Migration 026: Anchored Metadata Records for Off-chain Metrics
-- =============================================================================
-- Date: 2025-12-31
-- Purpose: Cache resolved Regen metadata IRIs with provenance for derivations
-- Scope: Session E - Off-chain Metadata Resolver + KOI Caching + Derivations
-- =============================================================================

-- Create anchored metadata records table
-- Stores resolved metadata from Regen's metadata resolver API
CREATE TABLE IF NOT EXISTS anchored_metadata_records (
    id SERIAL PRIMARY KEY,

    -- Identity (the metadata IRI is the unique key)
    iri TEXT UNIQUE NOT NULL,           -- e.g., regen:13toVg...rdf

    -- Resolution provenance
    resolver_url TEXT NOT NULL,         -- e.g., https://api.regen.network/data/v2/metadata-graph/{iri}
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Content integrity
    content_hash TEXT NOT NULL,         -- SHA-256 of raw response payload for integrity
    payload_size_bytes INTEGER,         -- Size guard: reject overly large payloads

    -- Cached payload
    payload_jsonb JSONB NOT NULL,       -- Full resolved JSON-LD payload

    -- Metadata about the resolution
    http_status INTEGER,                -- Response HTTP status (200, 404, etc.)
    resolution_time_ms INTEGER,         -- How long the resolution took

    -- TTL / refresh tracking
    last_refresh_at TIMESTAMPTZ,        -- When was this last refreshed (NULL = never refreshed)
    refresh_count INTEGER DEFAULT 0,    -- How many times refreshed

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- Indexes for anchored_metadata_records
-- =============================================================================

-- Primary lookup by IRI (unique constraint provides this implicitly, but be explicit)
CREATE INDEX IF NOT EXISTS idx_anchored_metadata_iri
ON anchored_metadata_records (iri);

-- Lookup by content hash (for deduplication / integrity checks)
CREATE INDEX IF NOT EXISTS idx_anchored_metadata_content_hash
ON anchored_metadata_records (content_hash);

-- Find stale records for refresh
CREATE INDEX IF NOT EXISTS idx_anchored_metadata_resolved_at
ON anchored_metadata_records (resolved_at DESC);

-- =============================================================================
-- Derived Metrics Table
-- =============================================================================
-- Stores derived metrics extracted from anchored metadata records
-- Each derivation links back to its source record with a JSON pointer path

CREATE TABLE IF NOT EXISTS metadata_derivations (
    id SERIAL PRIMARY KEY,

    -- Link to source metadata record
    metadata_record_id INTEGER NOT NULL REFERENCES anchored_metadata_records(id) ON DELETE CASCADE,

    -- Metric identification
    metric_id TEXT NOT NULL,            -- e.g., 'hectares', 'tco2e_claimed'

    -- Derivation path (JSON pointer or property path)
    json_pointer TEXT NOT NULL,         -- e.g., '/regen:projectSize/qudt:numericValue'

    -- Extracted value
    numeric_value NUMERIC,              -- The extracted numeric value
    string_value TEXT,                  -- For non-numeric derivations
    unit TEXT,                          -- e.g., 'unit:HA', 'unit:TCO2E'

    -- Normalization / conversion
    raw_value TEXT,                     -- Original value before normalization
    unit_source TEXT,                   -- Where the unit came from (e.g., JSON pointer to unit field)
    normalized_to TEXT,                 -- If we normalized to a canonical unit
    conversion_factor NUMERIC,          -- Factor used for conversion (if any)

    -- Validation
    is_valid BOOLEAN DEFAULT TRUE,      -- Did the derivation pass validation?
    validation_errors JSONB,            -- Any validation issues

    -- Audit
    derived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Ensure one derivation per metric per record
    CONSTRAINT unique_metric_per_record UNIQUE (metadata_record_id, metric_id)
);

-- =============================================================================
-- Indexes for metadata_derivations
-- =============================================================================

-- Find all derivations for a metric type
CREATE INDEX IF NOT EXISTS idx_derivations_metric_id
ON metadata_derivations (metric_id);

-- Find derivations by source record
CREATE INDEX IF NOT EXISTS idx_derivations_metadata_record
ON metadata_derivations (metadata_record_id);

-- Find valid/invalid derivations
CREATE INDEX IF NOT EXISTS idx_derivations_is_valid
ON metadata_derivations (is_valid) WHERE is_valid = TRUE;

-- =============================================================================
-- Derivation Allowlist Table
-- =============================================================================
-- Defines which metrics can be derived and their extraction rules
-- This is the "allowlist" that restricts what fields can become metrics

CREATE TABLE IF NOT EXISTS derivation_allowlist (
    id SERIAL PRIMARY KEY,

    -- Metric identification
    metric_id TEXT UNIQUE NOT NULL,     -- e.g., 'hectares', 'tco2e_impact'
    metric_label TEXT NOT NULL,         -- Human-readable label

    -- Extraction rule
    json_pointer TEXT NOT NULL,         -- JSON pointer to extract value
    unit_pointer TEXT,                  -- JSON pointer to extract unit (optional)
    expected_unit TEXT,                 -- Expected unit value for validation

    -- Type and validation
    value_type TEXT NOT NULL DEFAULT 'numeric',  -- 'numeric', 'string', 'boolean'
    min_value NUMERIC,                  -- Minimum valid value
    max_value NUMERIC,                  -- Maximum valid value

    -- Status
    is_active BOOLEAN DEFAULT TRUE,     -- Can be disabled without deletion
    requires_review BOOLEAN DEFAULT FALSE, -- Flag metrics needing human review

    -- Documentation
    description TEXT,                   -- What this metric represents
    data_source TEXT,                   -- Where in the metadata this comes from

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT                     -- Who added this rule
);

-- =============================================================================
-- Initial Derivation Allowlist Seeds
-- =============================================================================
-- Per plan: start with hectares only, block tCO2e until explicit unit-bearing derivation exists

INSERT INTO derivation_allowlist (metric_id, metric_label, json_pointer, unit_pointer, expected_unit, value_type, description, data_source)
VALUES
    ('hectares', 'Project Size (Hectares)',
     '/regen:projectSize/qudt:numericValue',
     '/regen:projectSize/qudt:unit',
     'unit:HA',
     'numeric',
     'Total project area in hectares from Regen JSON-LD metadata',
     'regen:projectSize.qudt:numericValue with regen:projectSize.qudt:unit == unit:HA')
ON CONFLICT (metric_id) DO NOTHING;

-- tCO2e is explicitly NOT added - per plan: "do not emit until we have an explicit unit-bearing field"

-- =============================================================================
-- Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW anchored_metadata_stats AS
SELECT
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE http_status = 200) as successful_resolutions,
    COUNT(*) FILTER (WHERE http_status != 200 OR http_status IS NULL) as failed_resolutions,
    AVG(resolution_time_ms) as avg_resolution_time_ms,
    AVG(payload_size_bytes) as avg_payload_size_bytes,
    MIN(resolved_at) as oldest_resolution,
    MAX(resolved_at) as newest_resolution,
    COUNT(*) FILTER (WHERE resolved_at < NOW() - INTERVAL '7 days') as stale_records_7d
FROM anchored_metadata_records;

CREATE OR REPLACE VIEW derivation_stats AS
SELECT
    d.metric_id,
    a.metric_label,
    COUNT(*) as total_derivations,
    COUNT(*) FILTER (WHERE d.is_valid) as valid_derivations,
    COUNT(*) FILTER (WHERE NOT d.is_valid) as invalid_derivations,
    AVG(d.numeric_value) as avg_value,
    MIN(d.numeric_value) as min_value,
    MAX(d.numeric_value) as max_value,
    SUM(d.numeric_value) as total_value
FROM metadata_derivations d
JOIN derivation_allowlist a ON d.metric_id = a.metric_id
GROUP BY d.metric_id, a.metric_label;

-- =============================================================================
-- Update trigger for updated_at
-- =============================================================================

CREATE OR REPLACE FUNCTION update_anchored_metadata_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS anchored_metadata_updated_at_trigger ON anchored_metadata_records;
CREATE TRIGGER anchored_metadata_updated_at_trigger
    BEFORE UPDATE ON anchored_metadata_records
    FOR EACH ROW
    EXECUTE FUNCTION update_anchored_metadata_updated_at();

DROP TRIGGER IF EXISTS derivation_allowlist_updated_at_trigger ON derivation_allowlist;
CREATE TRIGGER derivation_allowlist_updated_at_trigger
    BEFORE UPDATE ON derivation_allowlist
    FOR EACH ROW
    EXECUTE FUNCTION update_anchored_metadata_updated_at();
