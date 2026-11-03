-- Migration: Add Code Graph Provenance Support
-- Created: 2025-11-26
-- Description: Creates views and indexes for code entity provenance tracking

-- ============================================================================
-- PART 1: code_entity_provenance View
-- ============================================================================
-- This view provides easy access to all code entity provenance information
-- by joining CAT receipts with entity metadata

CREATE OR REPLACE VIEW code_entity_provenance AS
SELECT
    -- Entity identifiers
    r.output_rid AS entity_rid,
    r.metadata->>'entity_name' AS entity_name,
    r.metadata->>'entity_type' AS entity_type,

    -- Source information
    r.metadata->>'repo' AS repo,
    r.metadata->>'file_path' AS file_path,
    (r.metadata->>'line_number')::int AS line_number,

    -- Git provenance
    r.metadata->>'commit_sha' AS commit_sha,
    r.metadata->>'commit_date' AS commit_date,
    r.metadata->>'branch' AS branch,
    r.metadata->>'file_hash' AS file_hash,

    -- CAT receipt tracking
    r.receipt_id AS cat_receipt_id,
    r.input_rid AS source_event_rid,
    r.input_cid AS commit_sha_verified,
    r.output_cid AS entity_hash,

    -- Processing metadata
    r.processor_name,
    r.processor_version,
    r.metadata->>'extraction_method' AS extraction_method,
    r.metadata->>'language' AS language,

    -- Timestamps
    r.created_at AS extracted_at,

    -- GitHub link for verification
    r.metadata->>'github_url' AS github_url,

    -- Entity details
    r.metadata->>'signature' AS signature,
    r.metadata->>'docstring' AS docstring,

    -- Complete metadata for advanced queries
    r.metadata AS full_metadata

FROM koi_transformation_receipts r
WHERE r.transformation_type = 'github_to_code_entity'
ORDER BY r.created_at DESC;

COMMENT ON VIEW code_entity_provenance IS
'Provides complete provenance tracking for all code entities extracted from GitHub.
Each row represents one extracted entity (Function, Class, Keeper, Message, etc.)
with full traceability to source file, commit, and extraction process.';

-- ============================================================================
-- PART 2: code_relationship_provenance View
-- ============================================================================
-- Tracks provenance of relationships between code entities

CREATE OR REPLACE VIEW code_relationship_provenance AS
SELECT
    -- Relationship identifiers
    r.output_rid AS edge_rid,
    r.metadata->>'relationship_type' AS relationship_type,

    -- Connected entities
    r.metadata->>'from_entity' AS from_entity,
    r.metadata->>'to_entity' AS to_entity,
    r.metadata->>'from_rid' AS from_rid,
    r.metadata->>'to_rid' AS to_rid,

    -- Relationship quality
    (r.metadata->>'inferred')::boolean AS inferred,
    (r.metadata->>'confidence')::float AS confidence,

    -- CAT receipt tracking
    r.receipt_id AS cat_receipt_id,
    r.input_rid AS source_entity_rid,

    -- Processing metadata
    r.processor_name,
    r.processor_version,
    r.created_at AS extracted_at,

    -- Complete metadata
    r.metadata AS full_metadata

FROM koi_transformation_receipts r
WHERE r.transformation_type = 'code_entity_to_relationship'
ORDER BY r.created_at DESC;

COMMENT ON VIEW code_relationship_provenance IS
'Tracks provenance of relationships (HANDLES, CALLS, etc.) between code entities.
Includes confidence scores and whether relationship was inferred or explicit.';

-- ============================================================================
-- PART 3: Entity Verification Functions
-- ============================================================================

-- Function to verify an entity still exists at its source location
CREATE OR REPLACE FUNCTION verify_entity_at_source(
    p_entity_rid TEXT
) RETURNS TABLE (
    entity_rid TEXT,
    entity_name TEXT,
    still_exists BOOLEAN,
    current_commit_sha TEXT,
    extraction_commit_sha TEXT,
    commits_behind INT,
    github_url TEXT,
    message TEXT
) AS $$
BEGIN
    -- This function can be enhanced to actually check GitHub API
    -- For now, it returns metadata for manual verification
    RETURN QUERY
    SELECT
        p.entity_rid,
        p.entity_name,
        NULL::BOOLEAN AS still_exists,  -- Would need GitHub API call
        NULL::TEXT AS current_commit_sha,
        p.commit_sha AS extraction_commit_sha,
        NULL::INT AS commits_behind,
        p.github_url,
        'Manual verification required - visit GitHub URL' AS message
    FROM code_entity_provenance p
    WHERE p.entity_rid = p_entity_rid;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION verify_entity_at_source IS
'Verify if a code entity still exists at its original source location.
Returns metadata for manual verification via GitHub URL.
Future enhancement: integrate with GitHub API for automated checks.';

-- ============================================================================
-- PART 4: Provenance Chain Functions
-- ============================================================================

-- Get complete provenance chain for an entity
CREATE OR REPLACE FUNCTION get_entity_provenance_chain(
    p_entity_rid TEXT
) RETURNS TABLE (
    step_number INT,
    transformation_type TEXT,
    input_rid TEXT,
    output_rid TEXT,
    processor_name TEXT,
    processor_version TEXT,
    created_timestamp TIMESTAMP,
    metadata JSONB
) AS $$
BEGIN
    -- Return the transformation chain leading to this entity
    RETURN QUERY
    WITH RECURSIVE prov_chain AS (
        -- Base case: the entity itself
        SELECT
            1 AS depth,
            r.transformation_type,
            r.input_rid,
            r.output_rid,
            r.processor_name,
            r.processor_version,
            r.created_at,
            r.metadata
        FROM koi_transformation_receipts r
        WHERE r.output_rid = p_entity_rid

        UNION ALL

        -- Recursive case: parent transformations
        SELECT
            pc.depth + 1,
            r.transformation_type,
            r.input_rid,
            r.output_rid,
            r.processor_name,
            r.processor_version,
            r.created_at,
            r.metadata
        FROM koi_transformation_receipts r
        JOIN prov_chain pc ON r.output_rid = pc.input_rid
        WHERE pc.depth < 10  -- Limit recursion depth
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY depth DESC) AS step_number,
        *
    FROM prov_chain
    ORDER BY depth DESC;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_entity_provenance_chain IS
'Returns complete provenance chain for an entity, from source sensor event
through all transformation steps to final code entity.';

-- ============================================================================
-- PART 5: Indexes for Performance
-- ============================================================================

-- Indexes on koi_transformation_receipts for code graph queries
CREATE INDEX IF NOT EXISTS idx_transform_receipts_code_entity
    ON koi_transformation_receipts(transformation_type)
    WHERE transformation_type = 'github_to_code_entity';

CREATE INDEX IF NOT EXISTS idx_transform_receipts_code_relationship
    ON koi_transformation_receipts(transformation_type)
    WHERE transformation_type = 'code_entity_to_relationship';

-- GIN index on metadata for fast JSON queries
CREATE INDEX IF NOT EXISTS idx_transform_receipts_metadata_gin
    ON koi_transformation_receipts USING GIN (metadata);

-- Index for finding entities by repo
CREATE INDEX IF NOT EXISTS idx_transform_receipts_repo
    ON koi_transformation_receipts ((metadata->>'repo'))
    WHERE transformation_type = 'github_to_code_entity';

-- Index for finding entities by file
CREATE INDEX IF NOT EXISTS idx_transform_receipts_file
    ON koi_transformation_receipts ((metadata->>'file_path'))
    WHERE transformation_type = 'github_to_code_entity';

-- Index for finding entities by commit
CREATE INDEX IF NOT EXISTS idx_transform_receipts_commit
    ON koi_transformation_receipts (input_cid)
    WHERE transformation_type = 'github_to_code_entity';

-- ============================================================================
-- PART 6: Sample Queries Documentation
-- ============================================================================

COMMENT ON TABLE koi_transformation_receipts IS
'CAT (Content Addressable Transformation) receipts for all data transformations.

Common queries for code graph provenance:

-- 1. Find all entities in a repository:
SELECT * FROM code_entity_provenance
WHERE repo = ''regen-ledger''
ORDER BY file_path, line_number;

-- 2. Find specific entity by name:
SELECT * FROM code_entity_provenance
WHERE entity_name = ''MsgCreateBatch'';

-- 3. Find all entities from a specific commit:
SELECT * FROM code_entity_provenance
WHERE commit_sha = ''abc123...'';

-- 4. Find all Keepers:
SELECT * FROM code_entity_provenance
WHERE entity_type = ''Keeper'';

-- 5. Find entities extracted recently:
SELECT * FROM code_entity_provenance
WHERE extracted_at > NOW() - INTERVAL ''1 day'';

-- 6. Find all relationships for an entity:
SELECT * FROM code_relationship_provenance
WHERE from_entity = ''BasketKeeper''
   OR to_entity = ''BasketKeeper'';

-- 7. Get complete provenance chain:
SELECT * FROM get_entity_provenance_chain(
    ''rid://code/regen-ledger/x/ecocredit/keeper/keeper.go:42#Keeper''
);

-- 8. Verify entity at source:
SELECT * FROM verify_entity_at_source(
    ''rid://code/regen-ledger/x/ecocredit/msg.go:10#MsgCreateBatch''
);

-- 9. Find entities with low confidence relationships:
SELECT DISTINCT from_entity
FROM code_relationship_provenance
WHERE confidence < 0.8;

-- 10. Statistics by repository:
SELECT
    repo,
    COUNT(*) as entity_count,
    COUNT(DISTINCT entity_type) as type_count,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction
FROM code_entity_provenance
GROUP BY repo
ORDER BY entity_count DESC;
';

-- ============================================================================
-- Verify migration
-- ============================================================================

-- Test queries
DO $$
BEGIN
    RAISE NOTICE '✓ Migration completed successfully';
    RAISE NOTICE '';
    RAISE NOTICE 'Created views:';
    RAISE NOTICE '  - code_entity_provenance';
    RAISE NOTICE '  - code_relationship_provenance';
    RAISE NOTICE '';
    RAISE NOTICE 'Created functions:';
    RAISE NOTICE '  - verify_entity_at_source(entity_rid)';
    RAISE NOTICE '  - get_entity_provenance_chain(entity_rid)';
    RAISE NOTICE '';
    RAISE NOTICE 'Created indexes for performance';
    RAISE NOTICE '';
    RAISE NOTICE 'Sample query:';
    RAISE NOTICE '  SELECT * FROM code_entity_provenance LIMIT 5;';
END $$;
