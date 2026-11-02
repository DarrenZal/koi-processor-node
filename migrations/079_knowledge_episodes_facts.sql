-- Migration 079: Knowledge episodes and temporal facts
-- Adds episode grouping and fact-level storage with semantic search,
-- replacing Graphiti/Neo4j with KOI-native knowledge graph extension.
-- Episodes group facts extracted from a single source (meeting, document, etc.).
-- Facts are natural-language sentences with subject/object entity references,
-- temporal validity, and pgvector embeddings for semantic search.

-- ── knowledge_episodes ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS knowledge_episodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    content TEXT,
    source_description TEXT,
    source_document TEXT,
    group_id TEXT DEFAULT 'personal',
    valid_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_episodes_valid_at ON knowledge_episodes(valid_at);
CREATE INDEX IF NOT EXISTS idx_episodes_group_id ON knowledge_episodes(group_id);

-- ── knowledge_facts ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS knowledge_facts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id UUID REFERENCES knowledge_episodes(id) ON DELETE CASCADE,
    subject_uri TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_uri TEXT,
    object_literal TEXT,
    fact_text TEXT NOT NULL,
    fact_embedding VECTOR(1536),
    valid_from TIMESTAMPTZ,
    valid_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    group_id TEXT DEFAULT 'personal',
    source_node_rid TEXT
);

CREATE INDEX IF NOT EXISTS idx_facts_subject ON knowledge_facts(subject_uri);
CREATE INDEX IF NOT EXISTS idx_facts_object ON knowledge_facts(object_uri);
CREATE INDEX IF NOT EXISTS idx_facts_predicate ON knowledge_facts(predicate);
CREATE INDEX IF NOT EXISTS idx_facts_embedding ON knowledge_facts USING hnsw (fact_embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
CREATE INDEX IF NOT EXISTS idx_facts_valid ON knowledge_facts(valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_facts_episode ON knowledge_facts(episode_id);
