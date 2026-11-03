-- Migration: Add Jena Graph Database Integration
-- Purpose: Cross-reference PostgreSQL records with Apache Jena URIs for unified provenance tracking
-- Date: 2025-09-26

-- 1. Add Jena URI references to existing tables
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS jena_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_graph VARCHAR,
ADD COLUMN IF NOT EXISTS jena_sync_status VARCHAR DEFAULT 'pending',
ADD COLUMN IF NOT EXISTS jena_synced_at TIMESTAMP;

ALTER TABLE koi_embeddings
ADD COLUMN IF NOT EXISTS jena_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_embedding_uri VARCHAR;

ALTER TABLE koi_content
ADD COLUMN IF NOT EXISTS jena_artifact_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_provenance_graph VARCHAR;

ALTER TABLE koi_memory_chunks
ADD COLUMN IF NOT EXISTS jena_chunk_uri VARCHAR;

-- 2. Create extraction records table for entities/relations
CREATE TABLE IF NOT EXISTS koi_extraction_records (
    extraction_rid VARCHAR PRIMARY KEY,
    source_rid VARCHAR NOT NULL REFERENCES koi_memories(rid),
    extraction_type VARCHAR NOT NULL CHECK (extraction_type IN ('entity', 'relation', 'jsonld', 'claim', 'evidence')),
    jena_uri VARCHAR NOT NULL,
    jena_graph VARCHAR,
    subject_uri VARCHAR,  -- For relations
    predicate VARCHAR,     -- For relations
    object_uri VARCHAR,    -- For relations
    confidence FLOAT,
    extractor_model VARCHAR,
    extraction_metadata JSONB,
    cat_receipt_id VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    synced_to_jena BOOLEAN DEFAULT FALSE,
    jena_sync_timestamp TIMESTAMP
);

-- 3. Create table for tracking Jena named graphs
CREATE TABLE IF NOT EXISTS koi_jena_graphs (
    graph_uri VARCHAR PRIMARY KEY,
    graph_type VARCHAR NOT NULL,  -- 'knowledge', 'provenance', 'extraction'
    source_type VARCHAR,          -- 'discourse', 'twitter', 'medium', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    triple_count INTEGER DEFAULT 0,
    last_updated TIMESTAMP,
    metadata JSONB
);

-- 4. Enhanced CAT receipts with Jena references
ALTER TABLE koi_transformation_receipts
ADD COLUMN IF NOT EXISTS jena_activity_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_receipt_uri VARCHAR,
ADD COLUMN IF NOT EXISTS input_artifact_uri VARCHAR,
ADD COLUMN IF NOT EXISTS output_artifact_uri VARCHAR,
ADD COLUMN IF NOT EXISTS stored_in_graph VARCHAR;

-- 5. Create provenance linkage table
CREATE TABLE IF NOT EXISTS koi_provenance_links (
    id SERIAL PRIMARY KEY,
    from_rid VARCHAR NOT NULL,
    to_rid VARCHAR NOT NULL,
    relationship_type VARCHAR NOT NULL, -- 'derived_from', 'generated_by', 'used', 'attributed_to'
    jena_from_uri VARCHAR,
    jena_to_uri VARCHAR,
    jena_predicate VARCHAR,
    activity_uri VARCHAR,  -- The transformation activity
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(from_rid, to_rid, relationship_type)
);

-- 6. Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_memories_jena_uri ON koi_memories(jena_uri);
CREATE INDEX IF NOT EXISTS idx_memories_jena_sync ON koi_memories(jena_sync_status);
CREATE INDEX IF NOT EXISTS idx_extraction_source ON koi_extraction_records(source_rid);
CREATE INDEX IF NOT EXISTS idx_extraction_type ON koi_extraction_records(extraction_type);
CREATE INDEX IF NOT EXISTS idx_extraction_jena ON koi_extraction_records(jena_uri);
CREATE INDEX IF NOT EXISTS idx_provenance_from ON koi_provenance_links(from_rid);
CREATE INDEX IF NOT EXISTS idx_provenance_to ON koi_provenance_links(to_rid);
CREATE INDEX IF NOT EXISTS idx_receipts_jena ON koi_transformation_receipts(jena_receipt_uri);

-- 7. Create view for pending Jena synchronization
CREATE OR REPLACE VIEW koi_jena_sync_queue AS
SELECT
    'memory' as record_type,
    rid,
    jena_sync_status,
    created_at
FROM koi_memories
WHERE jena_sync_status = 'pending'
UNION ALL
SELECT
    'extraction' as record_type,
    extraction_rid as rid,
    CASE WHEN synced_to_jena THEN 'synced' ELSE 'pending' END as jena_sync_status,
    created_at
FROM koi_extraction_records
WHERE NOT synced_to_jena
ORDER BY created_at;

-- 8. Create function to generate Jena URIs
CREATE OR REPLACE FUNCTION generate_jena_uri(
    artifact_type VARCHAR,
    identifier VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    RETURN CONCAT(
        'https://regen.network/koi/',
        artifact_type,
        '/',
        REPLACE(identifier, ':', '/')
    );
END;
$$ LANGUAGE plpgsql;

-- 9. Create function to track provenance relationships
CREATE OR REPLACE FUNCTION add_provenance_link(
    p_from_rid VARCHAR,
    p_to_rid VARCHAR,
    p_relationship VARCHAR,
    p_activity_uri VARCHAR DEFAULT NULL,
    p_confidence FLOAT DEFAULT 1.0
) RETURNS VOID AS $$
BEGIN
    INSERT INTO koi_provenance_links (
        from_rid, to_rid, relationship_type,
        jena_from_uri, jena_to_uri,
        activity_uri, confidence
    ) VALUES (
        p_from_rid, p_to_rid, p_relationship,
        generate_jena_uri('artifact', p_from_rid),
        generate_jena_uri('artifact', p_to_rid),
        p_activity_uri, p_confidence
    )
    ON CONFLICT (from_rid, to_rid, relationship_type)
    DO UPDATE SET
        confidence = GREATEST(EXCLUDED.confidence, koi_provenance_links.confidence),
        activity_uri = COALESCE(EXCLUDED.activity_uri, koi_provenance_links.activity_uri);
END;
$$ LANGUAGE plpgsql;

-- 10. Comments for documentation
COMMENT ON TABLE koi_extraction_records IS 'Records of entities, relations, and structured data extracted from documents';
COMMENT ON TABLE koi_jena_graphs IS 'Named graphs in Apache Jena for organizing knowledge and provenance';
COMMENT ON TABLE koi_provenance_links IS 'Provenance relationships between artifacts using PROV-O vocabulary';
COMMENT ON COLUMN koi_memories.jena_uri IS 'URI of this document in Apache Jena knowledge graph';
COMMENT ON COLUMN koi_embeddings.jena_embedding_uri IS 'URI of the embedding artifact in Jena provenance graph';
COMMENT ON FUNCTION generate_jena_uri IS 'Generate consistent URIs for Jena artifacts';
COMMENT ON FUNCTION add_provenance_link IS 'Create provenance relationship between two artifacts';