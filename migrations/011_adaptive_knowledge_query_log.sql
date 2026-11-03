-- Migration 011: Adaptive Knowledge Query Logging
-- Implements query tracking for confidence monitoring and active learning

-- Create query log table for adaptive knowledge system
CREATE TABLE IF NOT EXISTS koi_query_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_text TEXT NOT NULL,
    query_embedding vector(1024),
    user_id UUID,
    agent_id UUID,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    confidence_score FLOAT,
    triggered_extraction BOOLEAN DEFAULT FALSE,
    extraction_receipt_rid TEXT,
    response_time_ms INTEGER,
    feedback_provided BOOLEAN DEFAULT FALSE,
    feedback_type TEXT, -- 'correction', 'missing_info', 'quality', 'wrong_relationship'
    feedback_rating INTEGER, -- 1-5 rating for quality
    feedback_text TEXT,
    feedback_timestamp TIMESTAMPTZ,
    
    -- Additional metadata
    source_type TEXT, -- 'mcp', 'api', 'agent', 'test'
    session_id UUID,
    parent_query_id UUID REFERENCES koi_query_log(id),
    
    -- Results tracking
    result_count INTEGER,
    top_result_score FLOAT,
    extraction_count INTEGER DEFAULT 0,
    
    -- Performance metrics
    vector_search_ms INTEGER,
    sparql_search_ms INTEGER,
    total_processing_ms INTEGER
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_query_confidence ON koi_query_log(confidence_score);
CREATE INDEX IF NOT EXISTS idx_query_timestamp ON koi_query_log(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_query_extraction ON koi_query_log(triggered_extraction) 
  WHERE triggered_extraction = TRUE;
CREATE INDEX IF NOT EXISTS idx_query_feedback ON koi_query_log(feedback_provided) 
  WHERE feedback_provided = FALSE;
CREATE INDEX IF NOT EXISTS idx_query_agent ON koi_query_log(agent_id);
CREATE INDEX IF NOT EXISTS idx_query_user ON koi_query_log(user_id);
CREATE INDEX IF NOT EXISTS idx_query_session ON koi_query_log(session_id);
CREATE INDEX IF NOT EXISTS idx_query_low_confidence ON koi_query_log(confidence_score) 
  WHERE confidence_score < 0.7;

-- Create view for query analytics
CREATE OR REPLACE VIEW koi_query_analytics AS
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as query_count,
    AVG(confidence_score) as avg_confidence,
    COUNT(CASE WHEN triggered_extraction THEN 1 END) as extraction_count,
    COUNT(CASE WHEN feedback_provided THEN 1 END) as feedback_count,
    AVG(response_time_ms) as avg_response_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY confidence_score) as median_confidence,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_response_ms
FROM koi_query_log
GROUP BY hour
ORDER BY hour DESC;

-- Create view for problematic queries (low confidence, high frequency)
CREATE OR REPLACE VIEW koi_problematic_queries AS
SELECT 
    query_text,
    COUNT(*) as frequency,
    AVG(confidence_score) as avg_confidence,
    MIN(confidence_score) as min_confidence,
    COUNT(CASE WHEN triggered_extraction THEN 1 END) as extraction_count,
    COUNT(CASE WHEN feedback_provided THEN 1 END) as feedback_count,
    AVG(CASE WHEN feedback_rating IS NOT NULL THEN feedback_rating END) as avg_rating
FROM koi_query_log
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY query_text
HAVING COUNT(*) > 2 AND AVG(confidence_score) < 0.7
ORDER BY frequency DESC, avg_confidence ASC;

-- Table for tracking extracted knowledge from adaptive system
CREATE TABLE IF NOT EXISTS koi_adaptive_extractions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    query_log_id UUID REFERENCES koi_query_log(id),
    document_rid TEXT NOT NULL,
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    
    -- Extraction details
    model_used TEXT NOT NULL, -- 'gpt-4o-mini', 'gpt-4', etc.
    extraction_type TEXT NOT NULL, -- 'entity', 'relationship', 'fact', 'concept'
    extraction_prompt TEXT,
    extraction_cost_usd DECIMAL(10, 6),
    
    -- Extracted content
    extracted_content JSONB NOT NULL,
    triples_generated INTEGER,
    entities_extracted INTEGER,
    relationships_extracted INTEGER,
    
    -- Quality metrics
    confidence_before FLOAT,
    confidence_after FLOAT,
    confidence_improvement FLOAT GENERATED ALWAYS AS (confidence_after - confidence_before) STORED,
    
    -- CAT receipt tracking
    cat_receipt_rid TEXT NOT NULL,
    provenance_data JSONB
);

CREATE INDEX IF NOT EXISTS idx_extraction_query ON koi_adaptive_extractions(query_log_id);
CREATE INDEX IF NOT EXISTS idx_extraction_document ON koi_adaptive_extractions(document_rid);
CREATE INDEX IF NOT EXISTS idx_extraction_timestamp ON koi_adaptive_extractions(extraction_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_extraction_improvement ON koi_adaptive_extractions(confidence_improvement DESC);

-- Table for active learning document selection
CREATE TABLE IF NOT EXISTS koi_active_learning_pool (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_rid TEXT NOT NULL UNIQUE,
    added_timestamp TIMESTAMPTZ DEFAULT NOW(),
    
    -- IDDS scoring components
    informativeness_score FLOAT,
    diversity_score FLOAT,
    combined_score FLOAT,
    
    -- Selection metadata
    selection_round INTEGER,
    selected BOOLEAN DEFAULT FALSE,
    selected_timestamp TIMESTAMPTZ,
    
    -- Document metadata
    document_type TEXT,
    document_source TEXT,
    document_length INTEGER,
    has_embedding BOOLEAN DEFAULT FALSE,
    
    -- Processing status
    processing_status TEXT DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    processing_attempts INTEGER DEFAULT 0,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_learning_pool_status ON koi_active_learning_pool(processing_status);
CREATE INDEX IF NOT EXISTS idx_learning_pool_score ON koi_active_learning_pool(combined_score DESC);
CREATE INDEX IF NOT EXISTS idx_learning_pool_selected ON koi_active_learning_pool(selected) WHERE selected = FALSE;

-- Function to calculate query confidence trend
CREATE OR REPLACE FUNCTION koi_confidence_trend(
    query_pattern TEXT,
    days_back INTEGER DEFAULT 7
) RETURNS TABLE (
    day DATE,
    avg_confidence FLOAT,
    query_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        DATE(timestamp) as day,
        AVG(confidence_score) as avg_confidence,
        COUNT(*) as query_count
    FROM koi_query_log
    WHERE 
        query_text ILIKE '%' || query_pattern || '%'
        AND timestamp > NOW() - (days_back || ' days')::INTERVAL
    GROUP BY DATE(timestamp)
    ORDER BY day DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to identify queries needing extraction
CREATE OR REPLACE FUNCTION koi_queries_needing_extraction(
    confidence_threshold FLOAT DEFAULT 0.7,
    min_frequency INTEGER DEFAULT 3
) RETURNS TABLE (
    query_text TEXT,
    frequency BIGINT,
    avg_confidence FLOAT,
    last_seen TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        ql.query_text,
        COUNT(*) as frequency,
        AVG(ql.confidence_score) as avg_confidence,
        MAX(ql.timestamp) as last_seen
    FROM koi_query_log ql
    LEFT JOIN koi_adaptive_extractions ae ON ql.id = ae.query_log_id
    WHERE 
        ql.confidence_score < confidence_threshold
        AND ae.id IS NULL -- Not yet extracted
        AND ql.timestamp > NOW() - INTERVAL '30 days'
    GROUP BY ql.query_text
    HAVING COUNT(*) >= min_frequency
    ORDER BY COUNT(*) DESC, AVG(ql.confidence_score) ASC
    LIMIT 100;
END;
$$ LANGUAGE plpgsql;

-- Add comments for documentation
COMMENT ON TABLE koi_query_log IS 'Tracks all queries to the adaptive knowledge system for monitoring and learning';
COMMENT ON TABLE koi_adaptive_extractions IS 'Records knowledge extracted through the adaptive extraction system';
COMMENT ON TABLE koi_active_learning_pool IS 'Manages documents selected for active learning and extraction';
COMMENT ON VIEW koi_query_analytics IS 'Hourly analytics of query performance and confidence';
COMMENT ON VIEW koi_problematic_queries IS 'Identifies frequently asked queries with low confidence scores';
COMMENT ON FUNCTION koi_confidence_trend IS 'Shows confidence score trends for specific query patterns over time';
COMMENT ON FUNCTION koi_queries_needing_extraction IS 'Identifies queries that would benefit from adaptive extraction';