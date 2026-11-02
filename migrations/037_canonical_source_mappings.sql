-- Migration 037: Canonical Source Mappings
-- Maps entities to their authoritative source URLs (GitHub, Ledger, Discourse, Notion)
-- Addresses PR #2 feedback: Notion URLs used as canonical when content has moved to GitHub

CREATE TABLE IF NOT EXISTS canonical_source_mappings (
  id SERIAL PRIMARY KEY,
  entity_uri TEXT NOT NULL,            -- fuseki_uri from entity_registry
  entity_text TEXT NOT NULL,           -- human-readable name
  source_type VARCHAR(50) NOT NULL,    -- 'github', 'notion', 'discourse', 'ledger'
  source_url TEXT NOT NULL,            -- full URL
  is_canonical BOOLEAN DEFAULT false,
  priority INTEGER DEFAULT 0,          -- higher = preferred
  notes TEXT,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE (entity_uri, source_url)
);

-- Enforce at most one canonical source per entity at the DB level
CREATE UNIQUE INDEX IF NOT EXISTS idx_csm_one_canonical_per_entity
  ON canonical_source_mappings(entity_uri)
  WHERE is_canonical = true;

CREATE INDEX IF NOT EXISTS idx_csm_entity_uri ON canonical_source_mappings(entity_uri);

-- Source preference policy (documented here as canonical reference):
-- priority 10: GitHub (specs, code, agent definitions), Regen Ledger (on-chain entities)
-- priority 5:  Discourse/Forum (discussions, proposals)
-- priority 0:  Notion (legacy, deprecated)
--
-- Canonical selection tiebreak: ORDER BY priority DESC, updated_at DESC, source_url ASC
