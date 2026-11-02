-- Migration: 063_mediawiki_import
-- Adds tables for MediaWiki wiki import pipeline
-- Part of: Salish Sea Wiki Import (graph densification)

-- Wiki registry
CREATE TABLE IF NOT EXISTS mediawiki_wikis (
    id SERIAL PRIMARY KEY,
    base_url TEXT NOT NULL UNIQUE,
    api_url TEXT NOT NULL,
    wiki_name TEXT,
    status TEXT DEFAULT 'active',
    sync_mode TEXT DEFAULT 'dump',
    last_scan_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error_at TIMESTAMPTZ,
    page_count INT DEFAULT 0,
    entity_count INT DEFAULT 0,
    config JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Per-page state tracking
CREATE TABLE IF NOT EXISTS mediawiki_page_state (
    id SERIAL PRIMARY KEY,
    wiki_id INT NOT NULL REFERENCES mediawiki_wikis(id),
    page_id INT NOT NULL,
    title TEXT NOT NULL,
    normalized_title TEXT,
    source_rid TEXT UNIQUE,
    namespace INT DEFAULT 0,
    template_type TEXT,
    bkc_entity_type TEXT,
    page_class TEXT DEFAULT 'source_only',
    is_redirect BOOLEAN DEFAULT FALSE,
    redirect_target TEXT,
    content_hash TEXT,
    revision_id INT,
    word_count INT DEFAULT 0,
    wikilink_count INT DEFAULT 0,
    template_field_count INT DEFAULT 0,
    entity_density_score FLOAT DEFAULT 0.0,
    ingest_confidence FLOAT DEFAULT 0.0,
    promotion_priority FLOAT DEFAULT 0.0,
    parse_confidence FLOAT DEFAULT 1.0,
    ambiguity_score FLOAT DEFAULT 0.0,
    parse_version TEXT,
    status TEXT DEFAULT 'pending',
    review_status TEXT DEFAULT 'unreviewed',
    error_message TEXT,
    entities_created INT DEFAULT 0,
    relationships_created INT DEFAULT 0,
    entity_uri TEXT,
    scanned_at TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ,
    last_run_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(wiki_id, page_id)
);

-- Import run tracking
CREATE TABLE IF NOT EXISTS mediawiki_import_runs (
    id SERIAL PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    wiki_id INT NOT NULL REFERENCES mediawiki_wikis(id),
    parse_version TEXT NOT NULL,
    mode TEXT NOT NULL,
    page_limit INT,
    pages_processed INT DEFAULT 0,
    entities_created INT DEFAULT 0,
    entities_matched INT DEFAULT 0,
    edges_promoted INT DEFAULT 0,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running'
);

-- Source-native page links (edges with provenance)
CREATE TABLE IF NOT EXISTS mediawiki_page_links (
    id SERIAL PRIMARY KEY,
    wiki_id INT NOT NULL REFERENCES mediawiki_wikis(id),
    source_page_id INT NOT NULL REFERENCES mediawiki_page_state(id),
    target_title TEXT NOT NULL,
    normalized_target_title TEXT,
    raw_target_text TEXT,
    target_page_id INT,
    predicate TEXT DEFAULT 'related_to',
    edge_class TEXT DEFAULT 'editorial',
    field_name TEXT,
    confidence FLOAT DEFAULT 0.6,
    source_section TEXT,
    source_revision_id INT,
    resolution_status TEXT DEFAULT 'unresolved',
    resolved_target_uri TEXT,
    target_match_confidence FLOAT
);

-- Unique index with COALESCE for nullable columns
CREATE UNIQUE INDEX IF NOT EXISTS idx_mw_links_unique
    ON mediawiki_page_links (wiki_id, source_page_id, target_title, predicate, COALESCE(field_name, ''), COALESCE(source_section, ''));

-- Indexes
CREATE INDEX IF NOT EXISTS idx_mw_page_status ON mediawiki_page_state(wiki_id, status);
CREATE INDEX IF NOT EXISTS idx_mw_page_priority ON mediawiki_page_state(promotion_priority DESC);
CREATE INDEX IF NOT EXISTS idx_mw_page_type ON mediawiki_page_state(template_type);
CREATE INDEX IF NOT EXISTS idx_mw_page_rid ON mediawiki_page_state(source_rid);
CREATE INDEX IF NOT EXISTS idx_mw_page_run ON mediawiki_page_state(last_run_id);
CREATE INDEX IF NOT EXISTS idx_mw_links_target ON mediawiki_page_links(normalized_target_title);
CREATE INDEX IF NOT EXISTS idx_mw_links_resolution ON mediawiki_page_links(resolution_status);

-- Record migration
INSERT INTO koi_migrations (migration_id, checksum, applied_at)
VALUES ('bkc:063_mediawiki_import', '', NOW())
ON CONFLICT (migration_id) DO NOTHING;
