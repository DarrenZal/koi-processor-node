-- Migration 089: add embedding_3072 columns for OpenAI text-embedding-3-large
-- See: an internal planning document
--
-- Companion: 090_add_embedding_3072_indexes.sql (CREATE INDEX CONCURRENTLY, outside txn)
-- 1024-dim `embedding` column retained for rollback; DO NOT drop.

BEGIN;

ALTER TABLE koi_memory_chunks
  ADD COLUMN IF NOT EXISTS embedding_3072 vector(3072);

ALTER TABLE entity_registry
  ADD COLUMN IF NOT EXISTS embedding_3072 vector(3072);

COMMENT ON COLUMN koi_memory_chunks.embedding_3072 IS
  'OpenAI text-embedding-3-large @ 3072-dim. Primary as of migration 089; embedding col (1024) retained for rollback.';
COMMENT ON COLUMN entity_registry.embedding_3072 IS
  'OpenAI text-embedding-3-large @ 3072-dim. Primary as of migration 089.';

COMMIT;
