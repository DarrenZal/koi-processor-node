-- =============================================================================
-- Migration 032: Entity Relationships for Knowledge Graph Context
-- =============================================================================
-- Date: 2026-01-30
-- Purpose: Store relationships between entities from vault YAML frontmatter
-- Use Case: Relationship-aware entity resolution (e.g., Paul != Polly based on org affiliations)
-- Database: personal_koi
-- =============================================================================

-- Enable pg_trgm extension for fuzzy matching in pending resolution
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- Predicate Allow-list (must be created first - FK target)
-- =============================================================================
-- Prevents bad mappings and provides type constraints

CREATE TABLE IF NOT EXISTS allowed_predicates (
    predicate TEXT PRIMARY KEY,
    description TEXT,
    subject_types TEXT[],  -- Allowed subject entity types (NULL = any)
    object_types TEXT[]    -- Allowed object entity types (NULL = any)
);

-- Seed with canonical predicates
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
    ('affiliated_with', 'Person belongs to organization', ARRAY['Person'], ARRAY['Organization']),
    ('founded', 'Person founded org/project', ARRAY['Person'], ARRAY['Organization', 'Project']),
    ('has_founder', 'Org/project was founded by', ARRAY['Organization', 'Project'], ARRAY['Person']),
    ('knows', 'Person knows person (symmetric)', ARRAY['Person'], ARRAY['Person']),
    ('collaborates_with', 'Person collaborates with person (symmetric)', ARRAY['Person'], ARRAY['Person']),
    ('involves_person', 'Project involves person', ARRAY['Project', 'Meeting'], ARRAY['Person']),
    ('involves_organization', 'Project involves organization', ARRAY['Project'], ARRAY['Organization']),
    ('has_project', 'Organization has project', ARRAY['Organization'], ARRAY['Project']),
    ('attended', 'Person attended meeting', ARRAY['Person'], ARRAY['Meeting']),
    ('located_in', 'Entity is located in place', NULL, ARRAY['Location'])
ON CONFLICT (predicate) DO NOTHING;

-- =============================================================================
-- Entity Relationships Table (Resolved Relationships)
-- =============================================================================
-- Stores relationships where BOTH subject and object are in entity_registry

CREATE TABLE IF NOT EXISTS entity_relationships (
    id SERIAL PRIMARY KEY,
    subject_uri TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_uri TEXT NOT NULL,
    confidence FLOAT DEFAULT 1.0,
    source TEXT DEFAULT 'vault',  -- 'vault', 'extracted', 'manual'
    source_rid TEXT,              -- Which vault file this came from
    source_field TEXT,            -- Original YAML field name (e.g., 'affiliation')
    raw_value TEXT,               -- Original value before parsing (for debugging)
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    -- Prevent duplicates
    UNIQUE(subject_uri, predicate, object_uri),

    -- No self-references
    CHECK (subject_uri != object_uri),

    -- Predicate format: lowercase with underscores only
    CHECK (predicate ~ '^[a-z][a-z0-9_]*$'),

    -- FK to allowed_predicates
    CONSTRAINT fk_rel_predicate FOREIGN KEY (predicate)
        REFERENCES allowed_predicates(predicate),

    -- FK to entity_registry (subject)
    CONSTRAINT fk_rel_subject FOREIGN KEY (subject_uri)
        REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE,

    -- FK to entity_registry (object)
    CONSTRAINT fk_rel_object FOREIGN KEY (object_uri)
        REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
);

-- =============================================================================
-- Pending Relationships Table (Unresolved Targets)
-- =============================================================================
-- Stores relationships where ONE side hasn't been registered yet
-- These get promoted to entity_relationships when the target is registered

CREATE TABLE IF NOT EXISTS pending_relationships (
    id SERIAL PRIMARY KEY,
    -- One of these will be NULL (the unresolved side)
    subject_uri TEXT,
    object_uri TEXT,
    predicate TEXT NOT NULL,
    -- The raw label for the unresolved side
    raw_unknown_label TEXT NOT NULL,
    unknown_side TEXT NOT NULL CHECK (unknown_side IN ('subject', 'object')),
    target_type_hint TEXT,
    source TEXT DEFAULT 'vault',
    source_rid TEXT,
    source_field TEXT,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Exactly one URI must be set
    CHECK ((subject_uri IS NOT NULL AND object_uri IS NULL) OR
           (subject_uri IS NULL AND object_uri IS NOT NULL)),

    -- Tie unknown_side to the NULL side
    CHECK (
        (unknown_side = 'subject' AND subject_uri IS NULL AND object_uri IS NOT NULL) OR
        (unknown_side = 'object' AND object_uri IS NULL AND subject_uri IS NOT NULL)
    ),

    -- FK to allowed_predicates
    CONSTRAINT fk_pending_predicate FOREIGN KEY (predicate)
        REFERENCES allowed_predicates(predicate),

    -- FK to entity_registry (subject) - NULL allowed
    CONSTRAINT fk_pending_subject FOREIGN KEY (subject_uri)
        REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE,

    -- FK to entity_registry (object) - NULL allowed
    CONSTRAINT fk_pending_object FOREIGN KEY (object_uri)
        REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
);

-- Unique index for pending edges (uses expression index with COALESCE since UNIQUE constraint can't use COALESCE)
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_unique_edge
ON pending_relationships (
    COALESCE(subject_uri, ''),
    COALESCE(object_uri, ''),
    predicate,
    raw_unknown_label,
    unknown_side
);

-- =============================================================================
-- Indexes for entity_relationships
-- =============================================================================

-- Lookup by subject (find all relationships FROM an entity)
CREATE INDEX IF NOT EXISTS idx_rel_subject
ON entity_relationships(subject_uri);

-- Lookup by object (find all relationships TO an entity)
CREATE INDEX IF NOT EXISTS idx_rel_object
ON entity_relationships(object_uri);

-- Lookup by subject + predicate (e.g., "who is affiliated with?")
CREATE INDEX IF NOT EXISTS idx_rel_subject_predicate
ON entity_relationships(subject_uri, predicate);

-- Lookup by object + predicate (e.g., "who founded this?")
CREATE INDEX IF NOT EXISTS idx_rel_object_predicate
ON entity_relationships(object_uri, predicate);

-- For sync/cleanup: find all relationships from a source file
CREATE INDEX IF NOT EXISTS idx_rel_source_rid
ON entity_relationships(source_rid);

-- =============================================================================
-- Indexes for pending_relationships
-- =============================================================================

-- For fuzzy matching when resolving pending (exact match first)
CREATE INDEX IF NOT EXISTS idx_pending_unknown_label
ON pending_relationships(raw_unknown_label);

-- GIN trigram index for similarity() scans (fuzzy matching)
CREATE INDEX IF NOT EXISTS idx_pending_unknown_label_trgm
ON pending_relationships USING GIN (raw_unknown_label gin_trgm_ops);

-- For sync/cleanup
CREATE INDEX IF NOT EXISTS idx_pending_source_rid
ON pending_relationships(source_rid);

-- =============================================================================
-- Statistics View
-- =============================================================================

CREATE OR REPLACE VIEW relationship_stats AS
SELECT
    COUNT(*) as total_relationships,
    COUNT(DISTINCT predicate) as unique_predicates,
    COUNT(DISTINCT subject_uri) as unique_subjects,
    COUNT(DISTINCT object_uri) as unique_objects,
    COUNT(DISTINCT source_rid) as unique_source_files,
    (SELECT COUNT(*) FROM pending_relationships) as pending_count
FROM entity_relationships;

-- Predicate distribution view
CREATE OR REPLACE VIEW relationships_by_predicate AS
SELECT
    predicate,
    COUNT(*) as count,
    COUNT(DISTINCT subject_uri) as unique_subjects,
    COUNT(DISTINCT object_uri) as unique_objects
FROM entity_relationships
GROUP BY predicate
ORDER BY count DESC;

-- =============================================================================
-- Comments
-- =============================================================================

COMMENT ON TABLE entity_relationships IS 'Stores resolved relationships between entities from vault YAML frontmatter';
COMMENT ON TABLE pending_relationships IS 'Stores relationships with unresolved targets, promoted when target is registered';
COMMENT ON TABLE allowed_predicates IS 'Allow-list of canonical predicates with type constraints';

COMMENT ON COLUMN entity_relationships.source_rid IS 'Vault file path (e.g., People/Shawn Anderson.md) for sync/cleanup';
COMMENT ON COLUMN entity_relationships.source_field IS 'Original YAML field name before predicate mapping';
COMMENT ON COLUMN entity_relationships.raw_value IS 'Original YAML value before parsing (for debugging)';

COMMENT ON COLUMN pending_relationships.unknown_side IS 'Which side is unresolved: subject or object';
COMMENT ON COLUMN pending_relationships.raw_unknown_label IS 'The raw name/label that needs resolution';
COMMENT ON COLUMN pending_relationships.target_type_hint IS 'Entity type hint for the unresolved side';
