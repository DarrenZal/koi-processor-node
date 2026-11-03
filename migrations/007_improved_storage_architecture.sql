-- Migration 007: Improved Storage Architecture
-- Purpose: Implement three-layer storage pattern for raw artifacts, documents, and chunks

-- 0. Ensure koi_memories.rid has unique constraint (required for foreign keys)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'koi_memories'::regclass
        AND contype = 'u'
        AND conname = 'koi_memories_rid_key'
    ) THEN
        ALTER TABLE koi_memories ADD CONSTRAINT koi_memories_rid_key UNIQUE (rid);
    END IF;
END$$;

-- 1. Enhance koi_content table for raw artifact storage
ALTER TABLE koi_content
ADD COLUMN IF NOT EXISTS raw_content TEXT,
ADD COLUMN IF NOT EXISTS content_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMP DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS extracted_at TIMESTAMP,
ADD COLUMN IF NOT EXISTS extraction_version VARCHAR(20) DEFAULT 'v1';

-- Add index for content type filtering
CREATE INDEX IF NOT EXISTS idx_koi_content_type ON koi_content(content_type);
CREATE INDEX IF NOT EXISTS idx_koi_content_scraped ON koi_content(scraped_at);

-- 2. Create new table for storing chunks separately
CREATE TABLE IF NOT EXISTS koi_memory_chunks (
    id SERIAL PRIMARY KEY,
    chunk_rid VARCHAR UNIQUE NOT NULL,
    document_rid VARCHAR NOT NULL,
    source_content_rid VARCHAR REFERENCES koi_content(rid),
    chunk_index INTEGER NOT NULL,
    total_chunks INTEGER NOT NULL,
    content JSONB NOT NULL,
    embedding vector(1024),
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),

    -- Foreign key to parent document
    CONSTRAINT fk_document_rid
        FOREIGN KEY (document_rid)
        REFERENCES koi_memories(rid)
        ON DELETE CASCADE
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_chunks_document ON koi_memory_chunks(document_rid);
CREATE INDEX IF NOT EXISTS idx_chunks_content_source ON koi_memory_chunks(source_content_rid);
CREATE INDEX IF NOT EXISTS idx_chunks_created ON koi_memory_chunks(created_at DESC);

-- 3. Add columns to koi_memories for better document tracking
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS source_content_rid VARCHAR REFERENCES koi_content(rid),
ADD COLUMN IF NOT EXISTS is_chunk BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS parent_document_rid VARCHAR REFERENCES koi_memories(rid);

-- Mark existing entries as chunks (they have :chunk_ in their RID)
UPDATE koi_memories
SET is_chunk = TRUE
WHERE rid LIKE '%:chunk_%';

-- 4. Create table for tracking processing status
CREATE TABLE IF NOT EXISTS koi_processing_status (
    id SERIAL PRIMARY KEY,
    content_rid VARCHAR REFERENCES koi_content(rid),
    stage VARCHAR(50) NOT NULL, -- 'scraped', 'extracted', 'chunked', 'embedded'
    status VARCHAR(20) NOT NULL, -- 'pending', 'processing', 'completed', 'failed'
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    metadata JSONB DEFAULT '{}',

    UNIQUE(content_rid, stage)
);

-- 5. Create view for easy querying of complete documents with their chunks
CREATE OR REPLACE VIEW v_documents_with_chunks AS
SELECT
    d.rid as document_rid,
    d.source_sensor,
    d.content->>'title' as title,
    d.metadata->>'url' as url,
    d.published_at,
    d.created_at as document_created,
    COUNT(c.id) as chunk_count,
    array_agg(c.chunk_rid ORDER BY c.chunk_index) as chunk_rids
FROM koi_memories d
LEFT JOIN koi_memory_chunks c ON c.document_rid = d.rid
WHERE d.is_chunk = FALSE OR d.is_chunk IS NULL
GROUP BY d.rid;

-- 6. Create function for content deduplication
CREATE OR REPLACE FUNCTION check_content_exists(p_content_hash VARCHAR)
RETURNS TABLE(content_exists BOOLEAN, content_rid VARCHAR, last_scraped TIMESTAMP) AS $$
BEGIN
    RETURN QUERY
    SELECT
        CASE WHEN c.rid IS NOT NULL THEN TRUE ELSE FALSE END::BOOLEAN as content_exists,
        c.rid::VARCHAR as content_rid,
        c.scraped_at as last_scraped
    FROM koi_content c
    WHERE c.content_hash = p_content_hash
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- 7. Create function to reprocess content from raw artifacts
CREATE OR REPLACE FUNCTION reprocess_content(p_content_rid VARCHAR)
RETURNS VOID AS $$
DECLARE
    v_raw_content TEXT;
    v_content_type VARCHAR;
BEGIN
    -- Get raw content
    SELECT raw_content, content_type
    INTO v_raw_content, v_content_type
    FROM koi_content
    WHERE rid = p_content_rid;

    -- Mark for reprocessing
    UPDATE koi_processing_status
    SET status = 'pending',
        started_at = NULL,
        completed_at = NULL
    WHERE content_rid = p_content_rid
    AND stage IN ('extracted', 'chunked', 'embedded');

    -- Log the reprocessing request
    INSERT INTO koi_processing_status (content_rid, stage, status, started_at)
    VALUES (p_content_rid, 'reprocess_requested', 'pending', NOW())
    ON CONFLICT (content_rid, stage)
    DO UPDATE SET status = 'pending', started_at = NOW();

    RAISE NOTICE 'Content % marked for reprocessing', p_content_rid;
END;
$$ LANGUAGE plpgsql;

-- 8. Add comment documentation
COMMENT ON TABLE koi_content IS 'Stores raw, immutable artifacts from web scraping and other sources';
COMMENT ON TABLE koi_memories IS 'Stores processed documents with extracted metadata (one per document, not chunks)';
COMMENT ON TABLE koi_memory_chunks IS 'Stores searchable chunks derived from documents';
COMMENT ON TABLE koi_processing_status IS 'Tracks processing pipeline status for each content item';

COMMENT ON COLUMN koi_content.raw_content IS 'Original HTML/JSON/text as scraped';
COMMENT ON COLUMN koi_content.content_hash IS 'SHA256 hash for deduplication';
COMMENT ON COLUMN koi_memories.source_content_rid IS 'Reference to raw artifact in koi_content';
COMMENT ON COLUMN koi_memory_chunks.document_rid IS 'Reference to parent document in koi_memories';