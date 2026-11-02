-- Migration: Add publication date tracking to KOI tables
-- Purpose: Track when content was originally published vs when it was ingested
-- Date: 2025-09-11

-- Add publication date fields to koi_memories table
ALTER TABLE koi_memories 
ADD COLUMN IF NOT EXISTS published_at TIMESTAMP WITH TIME ZONE,
ADD COLUMN IF NOT EXISTS published_confidence FLOAT DEFAULT 1.0 CHECK (published_confidence >= 0 AND published_confidence <= 1),
ADD COLUMN IF NOT EXISTS content_hash VARCHAR(64),
ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;

-- Add index for efficient date-based queries
CREATE INDEX IF NOT EXISTS idx_koi_memories_published_at ON koi_memories(published_at DESC) 
WHERE published_at IS NOT NULL;

-- Add index for content deduplication
CREATE INDEX IF NOT EXISTS idx_koi_memories_content_hash ON koi_memories(content_hash)
WHERE content_hash IS NOT NULL;

-- Add composite index for daily curator queries
CREATE INDEX IF NOT EXISTS idx_koi_memories_daily_curator 
ON koi_memories(published_at DESC, source_sensor, event_type)
WHERE published_at IS NOT NULL AND superseded_at IS NULL;

-- Update metadata to include publication tracking
COMMENT ON COLUMN koi_memories.published_at IS 'Original publication date of the content';
COMMENT ON COLUMN koi_memories.published_confidence IS 'Confidence score for extracted publication date (0-1)';
COMMENT ON COLUMN koi_memories.content_hash IS 'SHA-256 hash of content for deduplication';
COMMENT ON COLUMN koi_memories.last_seen_at IS 'Last time this content was observed by sensors';

-- Create view for daily curator queries
CREATE OR REPLACE VIEW v_daily_content AS
SELECT 
    km.id,
    km.rid,
    km.cid,
    km.source_sensor,
    km.content,
    km.metadata,
    km.published_at,
    km.published_confidence,
    km.created_at as ingested_at,
    CASE 
        WHEN km.published_at >= NOW() - INTERVAL '24 hours' THEN 'new'
        WHEN km.published_at >= NOW() - INTERVAL '48 hours' THEN 'recent'
        WHEN km.published_at >= NOW() - INTERVAL '7 days' THEN 'this_week'
        ELSE 'older'
    END as recency_category,
    EXTRACT(EPOCH FROM (NOW() - km.published_at)) / 3600 as hours_old
FROM koi_memories km
WHERE km.superseded_at IS NULL
  AND km.event_type != 'FORGET'
ORDER BY km.published_at DESC NULLS LAST;

-- Grant permissions
GRANT SELECT ON v_daily_content TO PUBLIC;