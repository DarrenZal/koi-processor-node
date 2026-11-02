-- =============================================================================
-- Migration 030: Ledger Entity Fields for Entity Resolution
-- =============================================================================
-- Date: 2026-01-20
-- Purpose: Add ledger-specific fields to entity_registry for credit classes,
--          projects, and organizations indexed from Regen Ledger.
-- Database: eliza (same as entity_registry)
-- =============================================================================

-- Add ledger-specific fields to entity_registry
-- These fields support the Ledger Sensor's entity indexing pipeline

-- ledger_id: The on-chain identifier (e.g., C02, C02-003)
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS ledger_id VARCHAR(50);

-- metadata_iri: The Regen metadata IRI for off-chain data
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS metadata_iri TEXT;

-- admin_address: The blockchain address that administers this entity
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS admin_address VARCHAR(100);

-- aliases: JSON array of alternative names for fuzzy matching
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS aliases JSONB DEFAULT '[]'::jsonb;

-- jurisdiction: For projects, the geographic jurisdiction code
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS jurisdiction VARCHAR(50);

-- class_id: For projects, the parent credit class ID
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS class_id VARCHAR(50);

-- source: Source of the entity (e.g., 'regen_ledger', 'kg_extraction')
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS source VARCHAR(50);

-- =============================================================================
-- Indexes for Ledger Entity Lookup
-- =============================================================================

-- Index for ledger ID lookup (fast exact match on credit class/project IDs)
CREATE INDEX IF NOT EXISTS idx_entity_registry_ledger_id
ON entity_registry(ledger_id) WHERE ledger_id IS NOT NULL;

-- Index for admin address lookup (find all entities by admin)
CREATE INDEX IF NOT EXISTS idx_entity_registry_admin
ON entity_registry(admin_address) WHERE admin_address IS NOT NULL;

-- Index for class_id lookup (find all projects in a credit class)
CREATE INDEX IF NOT EXISTS idx_entity_registry_class_id
ON entity_registry(class_id) WHERE class_id IS NOT NULL;

-- Index for source lookup (filter by data source)
CREATE INDEX IF NOT EXISTS idx_entity_registry_source
ON entity_registry(source) WHERE source IS NOT NULL;

-- GIN index for aliases JSONB containment queries
-- Enables queries like: WHERE aliases @> '["Urban Forest"]'::jsonb
CREATE INDEX IF NOT EXISTS idx_entity_registry_aliases_gin
ON entity_registry USING GIN (aliases);

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON COLUMN entity_registry.ledger_id IS 'On-chain identifier (e.g., C02 for credit class, C02-003 for project)';
COMMENT ON COLUMN entity_registry.metadata_iri IS 'Regen metadata IRI for off-chain data (e.g., regen:13toVf...)';
COMMENT ON COLUMN entity_registry.admin_address IS 'Blockchain address that administers this entity';
COMMENT ON COLUMN entity_registry.aliases IS 'JSON array of alternative names for fuzzy matching';
COMMENT ON COLUMN entity_registry.jurisdiction IS 'Geographic jurisdiction code for projects (e.g., US-PA)';
COMMENT ON COLUMN entity_registry.class_id IS 'Parent credit class ID for projects';
COMMENT ON COLUMN entity_registry.source IS 'Source of entity data (regen_ledger, kg_extraction, etc.)';

-- =============================================================================
-- View: Ledger Entities
-- =============================================================================

CREATE OR REPLACE VIEW ledger_entities AS
SELECT
    id,
    fuseki_uri,
    entity_text,
    entity_type,
    ledger_id,
    metadata_iri,
    admin_address,
    aliases,
    jurisdiction,
    class_id,
    occurrence_count,
    first_seen_at,
    last_seen_at
FROM entity_registry
WHERE source = 'regen_ledger'
ORDER BY
    CASE entity_type
        WHEN 'CREDIT_CLASS' THEN 1
        WHEN 'PROJECT' THEN 2
        WHEN 'ORGANIZATION' THEN 3
        ELSE 4
    END,
    ledger_id;

-- =============================================================================
-- View: Entity Resolution Statistics
-- =============================================================================

CREATE OR REPLACE VIEW entity_resolution_stats AS
SELECT
    source,
    entity_type,
    COUNT(*) as entity_count,
    COUNT(DISTINCT admin_address) as unique_admins,
    COUNT(*) FILTER (WHERE aliases IS NOT NULL AND jsonb_array_length(aliases) > 0) as entities_with_aliases,
    AVG(jsonb_array_length(COALESCE(aliases, '[]'::jsonb)))::NUMERIC(5,2) as avg_aliases_per_entity,
    MAX(last_seen_at) as last_updated
FROM entity_registry
WHERE source IS NOT NULL
GROUP BY source, entity_type
ORDER BY source, entity_count DESC;
