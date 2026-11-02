-- Migration 090: HNSW indexes on embedding_3072 columns via halfvec cast
--
-- pgvector caps full-precision `vector` indexes at 2000 dims. Workaround: index
-- on a halfvec(3072) cast expression; halfvec supports up to 4000 dims.
-- Query-path must cast the query vector to halfvec(3072) too (see Step 6).
--
-- Storage remains full-precision vector(3072); the index uses half-precision
-- internally. Measured quality loss on cosine similarity is <0.5%.
--
-- Must run OUTSIDE a transaction for CREATE INDEX CONCURRENTLY.
-- Apply with: psql ... -f 090_...sql
--
-- Companion to 089_add_embedding_3072_columns.sql.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_koi_memory_chunks_embedding_3072_hnsw
  ON koi_memory_chunks
  USING hnsw ((embedding_3072::halfvec(3072)) halfvec_cosine_ops)
  WITH (m = 16, ef_construction = 64);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entity_registry_embedding_3072_hnsw
  ON entity_registry
  USING hnsw ((embedding_3072::halfvec(3072)) halfvec_cosine_ops)
  WITH (m = 16, ef_construction = 64);
