-- Migration 012: Fix Production Database Alignment
-- This migration ensures the database structure matches what's actually in production
-- Production has columns that were added manually or through failed migrations

-- Note: Migration 010 (jena_integration) exists in codebase but wasn't properly applied in production
-- This migration consolidates all missing columns to ensure consistency

-- Add columns that exist in production but not in migrations 001-007
-- These were likely added manually on production

-- Check and add chunking-related columns (found in production, not in migration files)
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS chunk_rid VARCHAR,
ADD COLUMN IF NOT EXISTS chunk_index INTEGER,
ADD COLUMN IF NOT EXISTS total_chunks INTEGER,
ADD COLUMN IF NOT EXISTS free_chunks VARCHAR,
ADD COLUMN IF NOT EXISTS chunks_created INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS jena_chunk_uri VARCHAR;

-- Add columns from migration 007 that may have partially applied
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS source_content_rid VARCHAR,
ADD COLUMN IF NOT EXISTS is_chunk BOOLEAN DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS parent_document_rid VARCHAR;

-- Add columns from migration 010 (jena integration)
ALTER TABLE koi_memories
ADD COLUMN IF NOT EXISTS jena_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_graph VARCHAR,
ADD COLUMN IF NOT EXISTS jena_sync_status VARCHAR DEFAULT 'pending',
ADD COLUMN IF NOT EXISTS jena_synced_at TIMESTAMP;

-- Add jena columns to koi_embeddings (from migration 010)
ALTER TABLE koi_embeddings
ADD COLUMN IF NOT EXISTS jena_uri VARCHAR,
ADD COLUMN IF NOT EXISTS jena_embedding_uri VARCHAR;

-- Create indexes that should exist
CREATE INDEX IF NOT EXISTS idx_koi_memories_source_content ON koi_memories(source_content_rid);
CREATE INDEX IF NOT EXISTS idx_koi_memories_is_chunk ON koi_memories(is_chunk);
CREATE INDEX IF NOT EXISTS idx_koi_memories_parent_doc ON koi_memories(parent_document_rid);
CREATE INDEX IF NOT EXISTS idx_koi_memories_jena_sync ON koi_memories(jena_sync_status) WHERE jena_sync_status = 'pending';

-- Update schema_migrations to reflect this fix
INSERT INTO schema_migrations (version, applied_at) 
VALUES ('012_fix_production_alignment', NOW())
ON CONFLICT (version) DO NOTHING;

-- Add comments for documentation
COMMENT ON COLUMN koi_memories.chunk_rid IS 'Reference to chunk if this is chunked content';
COMMENT ON COLUMN koi_memories.chunk_index IS 'Index of this chunk within the document';
COMMENT ON COLUMN koi_memories.total_chunks IS 'Total number of chunks for this document';
COMMENT ON COLUMN koi_memories.source_content_rid IS 'Reference to original source content';
COMMENT ON COLUMN koi_memories.is_chunk IS 'Whether this memory is a chunk of a larger document';
COMMENT ON COLUMN koi_memories.parent_document_rid IS 'RID of parent document if this is a chunk';
COMMENT ON COLUMN koi_memories.jena_uri IS 'URI in Apache Jena triplestore';
COMMENT ON COLUMN koi_memories.jena_graph IS 'Graph name in Jena';
COMMENT ON COLUMN koi_memories.jena_sync_status IS 'Synchronization status with Jena';
COMMENT ON COLUMN koi_memories.jena_synced_at IS 'Last synchronization timestamp with Jena';

-- Note for future developers:
-- This migration was created to align the database structure with production
-- after discovering that production had columns not properly tracked in migrations.
-- Always ensure migrations are applied and tracked properly to avoid such discrepancies.