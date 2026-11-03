-- Rollback for 133_discourse_link_candidates.sql.
-- Drops only the eight tables that migration creates, in dependency order, and
-- its koi_migrations row. It touches no existing table: session_discourse_moves,
-- document_ingestion_log, koi_memories and entity_relationships are unchanged
-- by the up migration.
-- Export first if any judgment or review event should be kept:
--   \copy discourse_relations TO 'discourse_relations.csv' CSV HEADER
--   \copy discourse_review_events TO 'discourse_review_events.csv' CSV HEADER

BEGIN;
DROP TABLE IF EXISTS discourse_link_run_effects;
DROP TABLE IF EXISTS discourse_link_runs;
DROP TABLE IF EXISTS discourse_review_events;
DROP TABLE IF EXISTS discourse_relations;
DROP TABLE IF EXISTS discourse_link_candidates;
DROP TABLE IF EXISTS discourse_move_embeddings;
DROP TABLE IF EXISTS discourse_move_extraction_bindings;
DROP TABLE IF EXISTS discourse_claim_representations;
DELETE FROM koi_migrations WHERE migration_id = '133_discourse_link_candidates';
COMMIT;
