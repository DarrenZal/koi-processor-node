-- Rollback for migration 132. See that file for rationale.
-- Apply: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/132_document_entity_links_live_entity_uri_down.sql
--
-- Drops the trigger and its function only. Rows the trigger already rewrote stay on
-- their live survivor (that was the correct value). After rollback, writers that name
-- a merged-away URI create tombstone links again; find them with
--   SELECT del.* FROM document_entity_links del
--   JOIN entity_registry er ON er.fuseki_uri = del.entity_uri
--   WHERE er.merged_into IS NOT NULL;

DROP TRIGGER IF EXISTS trg_document_entity_links_live_entity_uri ON document_entity_links;
DROP FUNCTION IF EXISTS document_entity_links_live_entity_uri();

DELETE FROM koi_migrations WHERE migration_id = '132_document_entity_links_live_entity_uri';
