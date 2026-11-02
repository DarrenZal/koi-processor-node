-- Migration: Create email sensor tables
-- Date: 2026-01-31
-- Purpose: Store email-specific metadata and attachments for the email sensor

-- ============================================================================
-- Email Metadata Table
-- ============================================================================
-- Stores structured email headers and metadata, linked to koi_memories via memory_id
-- Uses memory_id FK (UUID) which is always unique, safer than rid-based joins

CREATE TABLE IF NOT EXISTS email_metadata (
    id SERIAL PRIMARY KEY,
    memory_id UUID NOT NULL,                    -- FK to koi_memories.id (safer than rid)
    rid TEXT UNIQUE NOT NULL,                   -- orn:gmail.message:{hash}
    gmail_msg_id TEXT UNIQUE,                   -- X-GM-MSGID for dedup (nullable - may not be available)
    message_id TEXT NOT NULL,                   -- RFC 5322 Message-ID header
    thread_id TEXT,                             -- Gmail thread grouping (hash of References)
    from_address TEXT NOT NULL,
    from_name TEXT,
    to_addresses TEXT[],
    cc_addresses TEXT[],
    subject TEXT,
    date_sent TIMESTAMP WITH TIME ZONE,
    labels TEXT[],                              -- Aggregated Gmail labels/folders
    has_attachments BOOLEAN DEFAULT FALSE,
    attachment_count INT DEFAULT 0,
    content_hash TEXT,                          -- SHA256 of body for change detection
    folder TEXT,                                -- Source Maildir folder
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- FK to koi_memories.id (cascade on delete)
    CONSTRAINT email_metadata_memory_fk
        FOREIGN KEY (memory_id) REFERENCES koi_memories(id) ON DELETE CASCADE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_email_metadata_date ON email_metadata(date_sent DESC);
CREATE INDEX IF NOT EXISTS idx_email_metadata_from ON email_metadata(from_address);
CREATE INDEX IF NOT EXISTS idx_email_metadata_thread ON email_metadata(thread_id);
CREATE INDEX IF NOT EXISTS idx_email_metadata_message_id ON email_metadata(message_id);
CREATE INDEX IF NOT EXISTS idx_email_metadata_memory_id ON email_metadata(memory_id);

-- Composite index for finding emails by sender and date
CREATE INDEX IF NOT EXISTS idx_email_metadata_from_date
    ON email_metadata(from_address, date_sent DESC);

-- ============================================================================
-- Email Attachments Table
-- ============================================================================
-- Tracks attachments separately, linked to parent email via parent_memory_id

CREATE TABLE IF NOT EXISTS email_attachments (
    id SERIAL PRIMARY KEY,
    rid TEXT UNIQUE NOT NULL,                   -- orn:gmail.attachment:{hash}/{index}_{hash}
    parent_memory_id UUID NOT NULL,             -- FK to koi_memories.id (the parent email)
    filename TEXT,
    content_type TEXT,
    size_bytes BIGINT,
    content_hash TEXT,                          -- SHA256 of attachment content
    storage_path TEXT,                          -- Local path if stored
    extracted_text_rid TEXT,                    -- RID of extracted text in koi_memories (if any)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- FK for cascade deletes
    CONSTRAINT email_attachments_parent_fk
        FOREIGN KEY (parent_memory_id) REFERENCES koi_memories(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_email_attachments_parent ON email_attachments(parent_memory_id);
CREATE INDEX IF NOT EXISTS idx_email_attachments_content_type ON email_attachments(content_type);

-- ============================================================================
-- Ensure IVFFLAT index on koi_memory_chunks.embedding
-- ============================================================================
-- This enables vector search on chunk embeddings

-- Check if index exists and create if not
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE tablename = 'koi_memory_chunks'
        AND indexname = 'idx_koi_memory_chunks_embedding'
    ) THEN
        -- Create IVFFLAT index with 100 lists (matches existing patterns)
        CREATE INDEX idx_koi_memory_chunks_embedding
            ON koi_memory_chunks USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100);
        RAISE NOTICE 'Created IVFFLAT index on koi_memory_chunks.embedding';
    ELSE
        RAISE NOTICE 'Index idx_koi_memory_chunks_embedding already exists';
    END IF;
END$$;

-- ============================================================================
-- Add Comments
-- ============================================================================
COMMENT ON TABLE email_metadata IS 'Email-specific metadata linked to koi_memories for email sensor';
COMMENT ON COLUMN email_metadata.memory_id IS 'UUID FK to koi_memories.id - safer than rid-based joins';
COMMENT ON COLUMN email_metadata.gmail_msg_id IS 'Gmail X-GM-MSGID if available from IMAP (may be NULL for Maildir)';
COMMENT ON COLUMN email_metadata.message_id IS 'RFC 5322 Message-ID header - always available';
COMMENT ON COLUMN email_metadata.thread_id IS 'Hash of first message in References header for thread grouping';
COMMENT ON COLUMN email_metadata.content_hash IS 'SHA256 of email body for change detection during sync';

COMMENT ON TABLE email_attachments IS 'Attachment metadata linked to parent emails';
COMMENT ON COLUMN email_attachments.extracted_text_rid IS 'RID of extracted text stored in koi_memories (for PDFs, DOCX, etc.)';
