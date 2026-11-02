-- =============================================================================
-- Migration 020: Entity Registry for pgvector-based Deduplication
-- =============================================================================
-- Date: 2025-12-09
-- Purpose: Centralized entity registry with semantic deduplication
-- Strategy: Exact match → Vector similarity → Create new
-- Database: eliza (same as koi_kg_extractions)
-- =============================================================================

-- Create entity registry table
CREATE TABLE IF NOT EXISTS entity_registry (
    id SERIAL PRIMARY KEY,

    -- Identity
    fuseki_uri TEXT UNIQUE NOT NULL,           -- Canonical Fuseki URI
    entity_text TEXT NOT NULL,                 -- Original entity name
    entity_type TEXT NOT NULL,                 -- PERSON, ORGANIZATION, etc.
    normalized_text TEXT NOT NULL,             -- Lowercase, trimmed for matching

    -- Semantic matching
    embedding VECTOR(1536) NOT NULL,           -- OpenAI ada-002 embedding

    -- Provenance
    first_seen_at TIMESTAMP DEFAULT NOW(),
    last_seen_at TIMESTAMP DEFAULT NOW(),
    occurrence_count INTEGER DEFAULT 1,        -- How many times seen

    -- Metadata
    metadata JSONB DEFAULT '{}'::jsonb,        -- Additional properties

    -- Constraints
    -- CRITICAL: This UNIQUE constraint prevents race conditions!
    -- If two threads try to insert same entity simultaneously, DB rejects duplicate
    CONSTRAINT entity_registry_text_type_key UNIQUE (normalized_text, entity_type)
);

-- =============================================================================
-- Indexes: Three-tier lookup optimization
-- =============================================================================

-- Tier 1: Exact Match (B-Tree, fastest)
CREATE INDEX IF NOT EXISTS idx_entity_exact
ON entity_registry (normalized_text, entity_type);

-- Tier 2: Semantic Match (HNSW, vector similarity)
CREATE INDEX IF NOT EXISTS idx_entity_vector
ON entity_registry
USING hnsw (embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- Support queries by URI
CREATE INDEX IF NOT EXISTS idx_entity_uri
ON entity_registry (fuseki_uri);

-- Support provenance queries
CREATE INDEX IF NOT EXISTS idx_entity_first_seen
ON entity_registry (first_seen_at DESC);

-- =============================================================================
-- Optional: Fuzzy Trigram Index (for typos)
-- =============================================================================
-- Uncomment if you want Tier 1.5: fuzzy string matching

-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX idx_entity_trigram
-- ON entity_registry
-- USING gin (normalized_text gin_trgm_ops);

-- =============================================================================
-- Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW entity_registry_stats AS
SELECT
    COUNT(*) as total_entities,
    COUNT(DISTINCT entity_type) as unique_types,
    SUM(occurrence_count) as total_occurrences,
    AVG(occurrence_count) as avg_occurrences_per_entity,
    MAX(occurrence_count) as max_occurrences,
    MIN(first_seen_at) as oldest_entity,
    MAX(last_seen_at) as newest_entity
FROM entity_registry;

-- =============================================================================
-- Type Distribution View
-- =============================================================================

CREATE OR REPLACE VIEW entity_registry_by_type AS
SELECT
    entity_type,
    COUNT(*) as count,
    SUM(occurrence_count) as total_occurrences,
    AVG(occurrence_count)::NUMERIC(10,2) as avg_occurrences
FROM entity_registry
GROUP BY entity_type
ORDER BY count DESC;
