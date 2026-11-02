-- Migration 097: HNSW halfvec index on knowledge_facts.fact_embedding_3072
--
-- pgvector caps full-precision `vector` indexes at 2000 dims; halfvec supports
-- up to 4000. Query-path must cast the query vector to halfvec(3072) too.
--
-- Must run OUTSIDE a transaction for CREATE INDEX CONCURRENTLY.
-- Apply with: psql ... -f 097_...sql
--
-- Companion to 096_knowledge_facts_embedding_3072.sql.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_knowledge_facts_embedding_3072_hnsw
  ON knowledge_facts
  USING hnsw ((fact_embedding_3072::halfvec(3072)) halfvec_cosine_ops)
  WITH (m = 16, ef_construction = 64);
