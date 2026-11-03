-- Migration 013: Create Knowledge Graph Tables
-- Purpose: Add tables to support Knowledge Graph extraction and provenance tracking
-- Date: 2025-10-04
-- Part of: KG Integration Phase 1

-- ============================================================================
-- KG Extractions Table
-- ============================================================================
-- Stores Knowledge Graph extraction results linked to memories
-- Tracks entities, statements, relations, and nanopublications

CREATE TABLE IF NOT EXISTS koi_kg_extractions (
    id SERIAL PRIMARY KEY,

    -- RID References (follows existing RID pattern)
    memory_rid VARCHAR(500) NOT NULL,
    extraction_rid VARCHAR(500) UNIQUE NOT NULL,

    -- Extraction metadata
    extraction_type VARCHAR(50) NOT NULL CHECK (
        extraction_type IN (
            'passA',
            'passB',
            'entity_resolution',
            'nanopub_creation',
            'contradiction_detection'
        )
    ),

    -- KG data stored as JSONB for flexibility
    entities JSONB DEFAULT '[]',
    statements JSONB DEFAULT '[]',
    relations JSONB DEFAULT '[]',
    nanopubs JSONB DEFAULT '[]',

    -- Quality metrics
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),

    -- Ontology tracking
    ontology_version VARCHAR(20) DEFAULT 'op-v1.1',
    extractor_version VARCHAR(20),

    -- Cost tracking
    tokens_consumed INTEGER DEFAULT 0,
    cost_usd DECIMAL(10,6) DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX idx_kg_memory_rid ON koi_kg_extractions(memory_rid);
CREATE INDEX idx_kg_extraction_rid ON koi_kg_extractions(extraction_rid);
CREATE INDEX idx_kg_extraction_type ON koi_kg_extractions(extraction_type);
CREATE INDEX idx_kg_confidence ON koi_kg_extractions(confidence_score DESC);
CREATE INDEX idx_kg_created_at ON koi_kg_extractions(created_at DESC);
CREATE INDEX idx_kg_ontology_version ON koi_kg_extractions(ontology_version);

-- JSONB GIN indexes for entity and statement queries
CREATE INDEX idx_kg_entities_gin ON koi_kg_extractions USING gin(entities);
CREATE INDEX idx_kg_statements_gin ON koi_kg_extractions USING gin(statements);
CREATE INDEX idx_kg_relations_gin ON koi_kg_extractions USING gin(relations);
CREATE INDEX idx_kg_nanopubs_gin ON koi_kg_extractions USING gin(nanopubs);

-- ============================================================================
-- KG Contradictions Table
-- ============================================================================
-- Tracks contradictions between statements for conflict resolution

CREATE TABLE IF NOT EXISTS koi_kg_contradictions (
    id SERIAL PRIMARY KEY,

    -- Statement 1 references
    statement1_rid VARCHAR(500) NOT NULL,
    statement1_url TEXT,
    statement1_extraction_id INTEGER REFERENCES koi_kg_extractions(id) ON DELETE CASCADE,

    -- Statement 2 references
    statement2_rid VARCHAR(500) NOT NULL,
    statement2_url TEXT,
    statement2_extraction_id INTEGER REFERENCES koi_kg_extractions(id) ON DELETE CASCADE,

    -- Contradiction metadata
    contradiction_type VARCHAR(50) NOT NULL,
    contradiction_details JSONB DEFAULT '{}',

    -- Provenance link to CAT receipt
    cat_receipt_id VARCHAR(64) REFERENCES koi_transformation_receipts(receipt_id) ON DELETE SET NULL,

    -- Resolution tracking
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(200),

    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Ensure we don't track same contradiction twice
    UNIQUE(statement1_rid, statement2_rid)
);

-- Indexes for efficient contradiction queries
CREATE INDEX idx_contradictions_stmt1_rid ON koi_kg_contradictions(statement1_rid);
CREATE INDEX idx_contradictions_stmt2_rid ON koi_kg_contradictions(statement2_rid);
CREATE INDEX idx_contradictions_type ON koi_kg_contradictions(contradiction_type);
CREATE INDEX idx_contradictions_resolved ON koi_kg_contradictions(resolved);
CREATE INDEX idx_contradictions_created_at ON koi_kg_contradictions(created_at DESC);
CREATE INDEX idx_contradictions_cat_receipt ON koi_kg_contradictions(cat_receipt_id);

-- GIN index for contradiction details
CREATE INDEX idx_contradictions_details_gin ON koi_kg_contradictions USING gin(contradiction_details);

-- ============================================================================
-- Helper Functions
-- ============================================================================

-- Function to get all KG extractions for a memory
CREATE OR REPLACE FUNCTION get_kg_extractions_for_memory(p_memory_rid VARCHAR)
RETURNS TABLE (
    extraction_id INTEGER,
    extraction_rid VARCHAR,
    extraction_type VARCHAR,
    entities JSONB,
    statements JSONB,
    confidence_score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        kg.id,
        kg.extraction_rid,
        kg.extraction_type,
        kg.entities,
        kg.statements,
        kg.confidence_score,
        kg.created_at
    FROM koi_kg_extractions kg
    WHERE kg.memory_rid = p_memory_rid
    ORDER BY kg.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to get unresolved contradictions for a statement
CREATE OR REPLACE FUNCTION get_unresolved_contradictions(p_statement_rid VARCHAR)
RETURNS TABLE (
    contradiction_id INTEGER,
    other_statement_rid VARCHAR,
    other_statement_url TEXT,
    contradiction_type VARCHAR,
    created_at TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id,
        CASE
            WHEN c.statement1_rid = p_statement_rid THEN c.statement2_rid
            ELSE c.statement1_rid
        END as other_statement_rid,
        CASE
            WHEN c.statement1_rid = p_statement_rid THEN c.statement2_url
            ELSE c.statement1_url
        END as other_statement_url,
        c.contradiction_type,
        c.created_at
    FROM koi_kg_contradictions c
    WHERE (c.statement1_rid = p_statement_rid OR c.statement2_rid = p_statement_rid)
    AND c.resolved = FALSE
    ORDER BY c.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Views for Analytics
-- ============================================================================

-- View for KG extraction statistics
CREATE OR REPLACE VIEW kg_extraction_stats AS
SELECT
    extraction_type,
    COUNT(*) as total_extractions,
    AVG(confidence_score) as avg_confidence,
    SUM(tokens_consumed) as total_tokens,
    SUM(cost_usd) as total_cost_usd,
    MIN(created_at) as first_extraction,
    MAX(created_at) as latest_extraction
FROM koi_kg_extractions
GROUP BY extraction_type;

-- View for contradiction statistics
CREATE OR REPLACE VIEW kg_contradiction_stats AS
SELECT
    contradiction_type,
    COUNT(*) as total_contradictions,
    COUNT(CASE WHEN resolved = TRUE THEN 1 END) as resolved_count,
    COUNT(CASE WHEN resolved = FALSE THEN 1 END) as unresolved_count,
    MIN(created_at) as first_detected,
    MAX(created_at) as latest_detected
FROM koi_kg_contradictions
GROUP BY contradiction_type;

-- ============================================================================
-- Documentation Comments
-- ============================================================================

COMMENT ON TABLE koi_kg_extractions IS 'Knowledge Graph extractions from KOI memories with entity/statement tracking';
COMMENT ON TABLE koi_kg_contradictions IS 'Tracks contradictions between KG statements for resolution';

COMMENT ON COLUMN koi_kg_extractions.memory_rid IS 'RID of source memory content';
COMMENT ON COLUMN koi_kg_extractions.extraction_rid IS 'Unique RID for this extraction (e.g., memory_rid:kg:passA:v1.1)';
COMMENT ON COLUMN koi_kg_extractions.extraction_type IS 'Type of KG extraction performed';
COMMENT ON COLUMN koi_kg_extractions.entities IS 'Extracted entities as JSONB array';
COMMENT ON COLUMN koi_kg_extractions.statements IS 'Extracted statements/claims as JSONB array';
COMMENT ON COLUMN koi_kg_extractions.relations IS 'Extracted relations as JSONB array';
COMMENT ON COLUMN koi_kg_extractions.nanopubs IS 'Nanopublications as JSONB array';
COMMENT ON COLUMN koi_kg_extractions.confidence_score IS 'Average confidence score (0.0-1.0)';
COMMENT ON COLUMN koi_kg_extractions.ontology_version IS 'Ontology version used for extraction';
COMMENT ON COLUMN koi_kg_extractions.tokens_consumed IS 'LLM tokens consumed during extraction';
COMMENT ON COLUMN koi_kg_extractions.cost_usd IS 'Cost in USD for this extraction';

COMMENT ON COLUMN koi_kg_contradictions.statement1_rid IS 'RID of first contradicting statement';
COMMENT ON COLUMN koi_kg_contradictions.statement2_rid IS 'RID of second contradicting statement';
COMMENT ON COLUMN koi_kg_contradictions.statement1_url IS 'Source URL for first statement (provenance)';
COMMENT ON COLUMN koi_kg_contradictions.statement2_url IS 'Source URL for second statement (provenance)';
COMMENT ON COLUMN koi_kg_contradictions.contradiction_type IS 'Type of contradiction detected';
COMMENT ON COLUMN koi_kg_contradictions.cat_receipt_id IS 'CAT receipt linking to contradiction detection transformation';
COMMENT ON COLUMN koi_kg_contradictions.resolved IS 'Whether contradiction has been resolved';

COMMENT ON FUNCTION get_kg_extractions_for_memory IS 'Returns all KG extractions for a given memory RID';
COMMENT ON FUNCTION get_unresolved_contradictions IS 'Returns unresolved contradictions involving a statement RID';

-- ============================================================================
-- Schema Migration Tracking
-- ============================================================================

INSERT INTO schema_migrations (version, applied_at)
VALUES ('013_create_kg_tables', NOW())
ON CONFLICT (version) DO NOTHING;

-- Note: This migration creates the foundation for Knowledge Graph extraction
-- It integrates with existing RID/CAT receipt infrastructure
-- CAT receipts track transformations, while these tables store the KG data itself
