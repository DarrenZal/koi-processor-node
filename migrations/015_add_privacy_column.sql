-- Migration: Add privacy column for access control
-- Date: 2025-12-03
-- Purpose: Enable private data that requires OAuth authentication
-- Related: Plan in regen-koi-mcp/docs for private Notion data

-- Add privacy columns to koi_memories
ALTER TABLE koi_memories ADD COLUMN IF NOT EXISTS is_private BOOLEAN DEFAULT FALSE;
ALTER TABLE koi_memories ADD COLUMN IF NOT EXISTS access_source VARCHAR(100);

-- Index for efficient privacy filtering (critical for query performance)
CREATE INDEX IF NOT EXISTS idx_koi_memories_is_private ON koi_memories(is_private);
CREATE INDEX IF NOT EXISTS idx_koi_memories_access_source ON koi_memories(access_source);

-- Composite index for common query pattern: non-superseded + privacy filter
CREATE INDEX IF NOT EXISTS idx_koi_memories_active_privacy
ON koi_memories(superseded_at, is_private)
WHERE superseded_at IS NULL;

-- Backfill existing Notion data from main Regen workspace as PRIVATE
-- The main NOTION_API_KEY workspace uses 'notion-sensor' and RIDs containing 'notion'
-- This catches existing data before the sensor was updated with is_private metadata
UPDATE koi_memories
SET is_private = TRUE, access_source = 'notion-main-workspace-backfill'
WHERE source_sensor = 'notion-sensor'
  AND rid LIKE '%notion%'
  AND (
    -- Exclude regentokenomics data which should be public
    metadata->>'workspace_id' IS NULL
    OR metadata->>'workspace_id' != 'regentokenomics'
  )
  AND is_private IS NULL;

-- Ensure any regentokenomics data is explicitly PUBLIC
UPDATE koi_memories
SET is_private = FALSE, access_source = 'notion-regentokenomics-backfill'
WHERE source_sensor = 'notion-sensor'
  AND metadata->>'workspace_id' = 'regentokenomics'
  AND access_source IS NULL;

-- Set default for non-Notion data (already public)
UPDATE koi_memories
SET is_private = FALSE, access_source = 'default-public'
WHERE is_private IS NULL;

-- Update views to include privacy columns
CREATE OR REPLACE VIEW current_koi_memories AS
SELECT
    km.*,
    ke.dim_768 IS NOT NULL as has_768_embedding,
    ke.dim_1024 IS NOT NULL as has_bge_embedding,
    ke.dim_1536 IS NOT NULL as has_openai_embedding
FROM koi_memories km
LEFT JOIN koi_embeddings ke ON km.id = ke.memory_id
WHERE km.superseded_at IS NULL;

-- Add view for public data only (useful for unauthenticated queries)
CREATE OR REPLACE VIEW public_koi_memories AS
SELECT
    km.*,
    ke.dim_768 IS NOT NULL as has_768_embedding,
    ke.dim_1024 IS NOT NULL as has_bge_embedding,
    ke.dim_1536 IS NOT NULL as has_openai_embedding
FROM koi_memories km
LEFT JOIN koi_embeddings ke ON km.id = ke.memory_id
WHERE km.superseded_at IS NULL
  AND (km.is_private = FALSE OR km.is_private IS NULL);

-- Update statistics view to include privacy breakdown
CREATE OR REPLACE VIEW koi_pipeline_stats AS
SELECT
    COUNT(DISTINCT km.rid) as unique_documents,
    COUNT(km.id) as total_versions,
    COUNT(CASE WHEN km.event_type = 'NEW' THEN 1 END) as new_events,
    COUNT(CASE WHEN km.event_type = 'UPDATE' THEN 1 END) as update_events,
    COUNT(CASE WHEN km.event_type = 'FORGET' THEN 1 END) as forget_events,
    COUNT(DISTINCT km.source_sensor) as active_sensors,
    COUNT(ke.dim_1024) as bge_embeddings,
    COUNT(ke.dim_768) as gemma_embeddings,
    COUNT(CASE WHEN km.is_private = TRUE THEN 1 END) as private_documents,
    COUNT(CASE WHEN km.is_private = FALSE OR km.is_private IS NULL THEN 1 END) as public_documents,
    MAX(km.created_at) as latest_event
FROM koi_memories km
LEFT JOIN koi_embeddings ke ON km.id = ke.memory_id;

-- Add comments for documentation
COMMENT ON COLUMN koi_memories.is_private IS 'If TRUE, requires OAuth authentication with @regen.network email to access';
COMMENT ON COLUMN koi_memories.access_source IS 'Identifies which configuration determined privacy level (e.g., notion-main-workspace, notion-regentokenomics)';
COMMENT ON VIEW public_koi_memories IS 'View of current memories filtered to only public (non-private) data';
