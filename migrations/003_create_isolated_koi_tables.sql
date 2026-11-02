-- Migration: Create isolated KOI tables for sensor pipeline
-- Purpose: Separate sensor-generated content from scraped/backfilled data
-- Date: 2025-09-09

-- Create isolated table for KOI sensor memories
CREATE TABLE IF NOT EXISTS koi_memories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rid VARCHAR(500) NOT NULL,
    cid VARCHAR(500),
    version INTEGER DEFAULT 1,
    previous_version_id UUID REFERENCES koi_memories(id),
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('NEW', 'UPDATE', 'FORGET')),
    source_sensor VARCHAR(200) NOT NULL,
    agent_id UUID,
    content JSONB NOT NULL,
    metadata JSONB DEFAULT '{}',
    superseded_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure unique RID per version
    UNIQUE(rid, version)
);

-- Create isolated table for KOI embeddings
CREATE TABLE IF NOT EXISTS koi_embeddings (
    id SERIAL PRIMARY KEY,
    memory_id UUID NOT NULL REFERENCES koi_memories(id) ON DELETE CASCADE,
    dim_384 vector(384),
    dim_512 vector(512),
    dim_768 vector(768),
    dim_1024 vector(1024),
    dim_1536 vector(1536),
    dim_3072 vector(3072),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- One embedding per memory
    UNIQUE(memory_id)
);

-- Indexes for efficient querying
CREATE INDEX idx_koi_memories_rid ON koi_memories(rid);
CREATE INDEX idx_koi_memories_cid ON koi_memories(cid);
CREATE INDEX idx_koi_memories_source_sensor ON koi_memories(source_sensor);
CREATE INDEX idx_koi_memories_agent_id ON koi_memories(agent_id);
CREATE INDEX idx_koi_memories_created_at ON koi_memories(created_at DESC);
CREATE INDEX idx_koi_memories_event_type ON koi_memories(event_type);
CREATE INDEX idx_koi_memories_version ON koi_memories(rid, version DESC);
CREATE INDEX idx_koi_memories_superseded ON koi_memories(superseded_at) WHERE superseded_at IS NOT NULL;

-- Indexes for vector similarity search
CREATE INDEX idx_koi_embeddings_dim_768 ON koi_embeddings USING ivfflat (dim_768 vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_koi_embeddings_dim_1024 ON koi_embeddings USING ivfflat (dim_1024 vector_cosine_ops) WITH (lists = 100);
CREATE INDEX idx_koi_embeddings_dim_1536 ON koi_embeddings USING ivfflat (dim_1536 vector_cosine_ops) WITH (lists = 100);

-- Function to get the latest version of a RID
CREATE OR REPLACE FUNCTION get_latest_koi_memory(p_rid VARCHAR)
RETURNS TABLE (
    id UUID,
    rid VARCHAR,
    version INTEGER,
    content JSONB,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        km.id,
        km.rid,
        km.version,
        km.content,
        km.created_at
    FROM koi_memories km
    WHERE km.rid = p_rid
    AND km.superseded_at IS NULL
    ORDER BY km.version DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to get version history for a RID
CREATE OR REPLACE FUNCTION get_koi_memory_history(p_rid VARCHAR)
RETURNS TABLE (
    id UUID,
    version INTEGER,
    event_type VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE,
    superseded_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        km.id,
        km.version,
        km.event_type,
        km.created_at,
        km.superseded_at
    FROM koi_memories km
    WHERE km.rid = p_rid
    ORDER BY km.version DESC;
END;
$$ LANGUAGE plpgsql;

-- View for current (non-superseded) memories
CREATE OR REPLACE VIEW current_koi_memories AS
SELECT 
    km.*,
    ke.dim_768 IS NOT NULL as has_768_embedding,
    ke.dim_1024 IS NOT NULL as has_bge_embedding,
    ke.dim_1536 IS NOT NULL as has_openai_embedding
FROM koi_memories km
LEFT JOIN koi_embeddings ke ON km.id = ke.memory_id
WHERE km.superseded_at IS NULL;

-- Statistics view
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
    MAX(km.created_at) as latest_event
FROM koi_memories km
LEFT JOIN koi_embeddings ke ON km.id = ke.memory_id;

-- Add comments for documentation
COMMENT ON TABLE koi_memories IS 'Isolated storage for KOI sensor pipeline data with versioning support';
COMMENT ON TABLE koi_embeddings IS 'Embeddings for KOI memories, supporting multiple dimensions';
COMMENT ON COLUMN koi_memories.rid IS 'Resource Identifier - unique identifier for content';
COMMENT ON COLUMN koi_memories.cid IS 'Content Identifier - IPFS/content-addressed hash';
COMMENT ON COLUMN koi_memories.version IS 'Version number for this RID';
COMMENT ON COLUMN koi_memories.previous_version_id IS 'Link to previous version for audit trail';
COMMENT ON COLUMN koi_memories.superseded_at IS 'Timestamp when this version was replaced by newer version';
COMMENT ON FUNCTION get_latest_koi_memory IS 'Returns the current (non-superseded) version of a document by RID';
COMMENT ON FUNCTION get_koi_memory_history IS 'Returns complete version history for a document';