-- Migration: Create KOI Query Log Table
-- Purpose: Track hybrid search queries for analytics and improvement
-- Date: 2025-09-30

-- Create koi_query_log table for tracking all queries to the hybrid search API
CREATE TABLE IF NOT EXISTS koi_query_log (
    id SERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    user_id VARCHAR(100) DEFAULT 'web-user',
    agent_id VARCHAR(100) DEFAULT 'koi-interface',
    confidence_score FLOAT,
    triggered_extraction BOOLEAN DEFAULT FALSE,
    total_results INTEGER DEFAULT 0,
    execution_time_ms FLOAT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_koi_query_log_user ON koi_query_log(user_id);
CREATE INDEX IF NOT EXISTS idx_koi_query_log_agent ON koi_query_log(agent_id);
CREATE INDEX IF NOT EXISTS idx_koi_query_log_created ON koi_query_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_koi_query_log_confidence ON koi_query_log(confidence_score);
CREATE INDEX IF NOT EXISTS idx_koi_query_log_extraction ON koi_query_log(triggered_extraction);

-- Add comment for documentation
COMMENT ON TABLE koi_query_log IS 'Logs all queries to the KOI hybrid search API for analytics and continuous improvement';

