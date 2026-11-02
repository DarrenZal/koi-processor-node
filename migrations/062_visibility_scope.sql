-- 062_visibility_scope.sql
-- Add visibility_scope to entity_rid_mappings and node_private flag to entity_registry
-- for hiding community_only entities from public API endpoints.

ALTER TABLE entity_rid_mappings
    ADD COLUMN IF NOT EXISTS visibility_scope TEXT DEFAULT 'public';

ALTER TABLE entity_registry
    ADD COLUMN IF NOT EXISTS node_private BOOLEAN DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_entity_registry_node_private
    ON entity_registry(node_private) WHERE node_private = true;
