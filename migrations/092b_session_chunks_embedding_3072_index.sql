-- Migration 092b: HNSW index on session_chunks.embedding_3072 via halfvec cast
--
-- pgvector caps full-precision `vector` indexes at 2000 dims. Workaround: index
-- on a halfvec(3072) cast expression; halfvec supports up to 4000 dims.
-- Query-path must cast the query vector to halfvec(3072) too (see knowledge_router.py).
--
-- Must run OUTSIDE a transaction for CREATE INDEX CONCURRENTLY.
-- Apply with: psql personal_koi -f migrations/092b_session_chunks_embedding_3072_index.sql
--
-- Companion to 092_session_chunks_embedding_3072_column.sql.
-- Run AFTER reembed pipeline populates embedding_3072 (otherwise index is built on NULL data).

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_session_chunks_embedding_3072_hnsw
  ON session_chunks
  USING hnsw ((embedding_3072::halfvec(3072)) halfvec_cosine_ops)
  WITH (m = 16, ef_construction = 64);
