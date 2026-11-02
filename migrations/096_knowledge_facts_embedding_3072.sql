-- Migration 096: add embedding_3072 column to knowledge_facts
-- Companion to 089/090 which migrated entity_registry + koi_memory_chunks.
-- Sibling deferred from the Apr 23 batch — see CLAUDE.md "knowledge_facts +
-- session_chunks cleanup PR" (routine trig_01Y61KRLNeg3tvHksNN8TFY6, May 1).
--
-- knowledge_facts.fact_embedding (1024) was left untouched in 089 because no
-- write path used it for new data after the migration. Discovered Apr 28 that
-- the add_knowledge endpoint (knowledge_router.create_episode) still writes
-- fact embeddings — those writes have been crashing with "different vector
-- dimensions 1024 and 3072" since Apr 23.
--
-- Companion: 097_knowledge_facts_embedding_3072_index.sql (CREATE INDEX
-- CONCURRENTLY, must run outside txn).
-- 1024-dim `fact_embedding` column retained for rollback; DO NOT drop until a
-- followup migration after the cleanup PR.

BEGIN;

ALTER TABLE knowledge_facts
  ADD COLUMN IF NOT EXISTS fact_embedding_3072 vector(3072);

COMMENT ON COLUMN knowledge_facts.fact_embedding_3072 IS
  'OpenAI text-embedding-3-large @ 3072-dim. Primary as of migration 096; fact_embedding (1024) retained for rollback.';

COMMIT;
