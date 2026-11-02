-- Migration 092: add embedding_3072 column for OpenAI text-embedding-3-large on session_chunks
-- See: an internal planning document
--
-- Parallels migration 089 pattern (koi_memory_chunks + entity_registry).
-- Companion: 092b_session_chunks_embedding_3072_index.sql (CREATE INDEX CONCURRENTLY, outside txn)
-- Cleanup: 094_session_chunks_drop_embedding_1024.sql (after reembed verified, drops legacy column)
--
-- Bridges to: pgvector caps `vector` indexes at 2000 dims; HNSW index in 092b uses halfvec(3072) cast.
-- Storage remains full-precision vector(3072); index uses half-precision internally.

BEGIN;

ALTER TABLE session_chunks
  ADD COLUMN IF NOT EXISTS embedding_3072 vector(3072);

COMMENT ON COLUMN session_chunks.embedding_3072 IS
  'OpenAI text-embedding-3-large @ 3072-dim. Primary as of migration 092; legacy embedding col (1024-dim Qwen) retained until reembed verified, dropped via migration 094.';

COMMIT;
