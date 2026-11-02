-- =============================================================================
-- Migration 068: Add missing entity_registry columns (phonetic_code, first_seen_rid)
-- =============================================================================
-- Date: 2026-03-11
-- Purpose: The codebase references phonetic_code and first_seen_rid in INSERT
--          statements but no prior migration adds them. They were added manually
--          to production on 2026-03-11 during the aliases type fix deployment.
--          This migration ensures new environments get them automatically.
-- Context: Also documents the entity_resolution_stats view recreation needed
--          when converting aliases from jsonb → TEXT[] (migration 036).
-- =============================================================================

-- phonetic_code: used by fuzzy entity resolution (Tier 1.x)
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS phonetic_code TEXT;

-- first_seen_rid: tracks which document first introduced an entity
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS first_seen_rid TEXT;
