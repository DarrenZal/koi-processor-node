-- Migration 055: Session Schema Governance
-- Namespace: personal:055
--
-- Brings session tables (created inline by koi-sensors claude_session_sensor)
-- under migration governance. All statements are idempotent (CREATE IF NOT EXISTS)
-- so this won't break existing data.

-- Enable pgvector if not already enabled
CREATE EXTENSION IF NOT EXISTS vector;

-- Table 1: Session ingestion tracking
CREATE TABLE IF NOT EXISTS session_ingestion_log (
    session_id TEXT PRIMARY KEY,
    transcript_path TEXT NOT NULL,
    project_path TEXT,
    summary TEXT,
    first_prompt TEXT,
    message_count INT DEFAULT 0,
    chunk_count INT DEFAULT 0,
    file_mtime DOUBLE PRECISION,
    last_ingested_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    -- Metadata columns (added by sensor _ensure_schema)
    tools_used TEXT[],
    tool_counts JSONB,
    mcp_servers TEXT[],
    files_accessed TEXT[],
    model TEXT,
    cwd TEXT,
    git_branch TEXT
);

-- Table 2: Session chunks with embeddings
CREATE TABLE IF NOT EXISTS session_chunks (
    id SERIAL PRIMARY KEY,
    session_rid TEXT NOT NULL,
    session_id TEXT NOT NULL,
    chunk_index INT NOT NULL,
    chunk_text TEXT NOT NULL,
    role TEXT,
    timestamp TIMESTAMP,
    embedding vector(1536),
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(session_id, chunk_index)
);

-- Indexes for session_chunks
CREATE INDEX IF NOT EXISTS idx_session_chunks_session_id
    ON session_chunks(session_id);

CREATE INDEX IF NOT EXISTS idx_session_chunks_rid
    ON session_chunks(session_rid);

-- HNSW vector similarity index
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE indexname = 'idx_session_chunks_embedding'
    ) THEN
        CREATE INDEX idx_session_chunks_embedding
            ON session_chunks USING hnsw (embedding vector_cosine_ops);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'HNSW index creation skipped: %', SQLERRM;
END $$;

-- Table 3: Tool usage detail table
CREATE TABLE IF NOT EXISTS session_tool_usage (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES session_ingestion_log(session_id) ON DELETE CASCADE,
    tool_name TEXT NOT NULL,
    call_count INT DEFAULT 1,
    is_mcp BOOLEAN DEFAULT FALSE,
    mcp_server TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(session_id, tool_name)
);

-- Indexes for tool usage queries
CREATE INDEX IF NOT EXISTS idx_session_tool_usage_session
    ON session_tool_usage(session_id);

CREATE INDEX IF NOT EXISTS idx_session_tool_usage_tool
    ON session_tool_usage(tool_name);

CREATE INDEX IF NOT EXISTS idx_session_tool_usage_mcp
    ON session_tool_usage(mcp_server) WHERE mcp_server IS NOT NULL;

-- Registration: run `python scripts/stamp_baseline.py` to register in koi_migrations
