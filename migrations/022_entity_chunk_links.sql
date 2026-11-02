-- Migration 022: Entity-Chunk Links Table
-- Materializes the Hybrid RAG bridge for fast retrieval

-- Entity-to-chunk links (the bridge between Graph and Chunks)
CREATE TABLE IF NOT EXISTS koi_entity_chunk_links (
    id SERIAL PRIMARY KEY,
    entity_name TEXT NOT NULL,
    entity_name_lower TEXT NOT NULL,  -- For case-insensitive search
    entity_type TEXT NOT NULL,
    entity_uri TEXT,  -- The GAIA URI from the knowledge graph
    chunk_rid TEXT NOT NULL,
    chunk_index INTEGER,
    document_rid TEXT NOT NULL,
    char_offset INTEGER,
    confidence FLOAT DEFAULT 0.8,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Indexes for fast lookup
    UNIQUE(entity_name_lower, chunk_rid, char_offset)
);

-- Index for entity name lookups
CREATE INDEX IF NOT EXISTS idx_entity_links_name
ON koi_entity_chunk_links(entity_name_lower);

-- Index for chunk lookups (find all entities in a chunk)
CREATE INDEX IF NOT EXISTS idx_entity_links_chunk
ON koi_entity_chunk_links(chunk_rid);

-- Index for document lookups
CREATE INDEX IF NOT EXISTS idx_entity_links_document
ON koi_entity_chunk_links(document_rid);

-- Entity type index
CREATE INDEX IF NOT EXISTS idx_entity_links_type
ON koi_entity_chunk_links(entity_type);

-- Composite index for graph-to-chunk bridge
CREATE INDEX IF NOT EXISTS idx_entity_links_uri
ON koi_entity_chunk_links(entity_uri);

-- Add comment
COMMENT ON TABLE koi_entity_chunk_links IS
'Hybrid RAG Bridge: Links knowledge graph entities to source text chunks';
