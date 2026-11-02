-- Migration 105: document_field_membership (Option B multi-field RAG membership)
--
-- Lets ONE content-addressed document (document_rid = 'document:<sha256(content)>')
-- belong to MULTIPLE learning-field RAG namespaces at once, and lets retrieval scope
-- to a field — WITHOUT re-embedding. Chunks in koi_memory_chunks stay field-agnostic
-- (content-addressed, DELETE+reinsert-by-rid on re-ingest is harmless for identical
-- bytes); this table is the separate, additive, authoritative field-membership layer.
--
-- Today group_id lives only in koi_memory_chunks.metadata->>'group_id' (no column), so
-- a doc effectively lives in ONE field at a time. This table decouples membership from
-- the chunk rows so a doc can be in field A AND field B simultaneously.
--
-- NEW TABLE ONLY — no existing table is altered. Retrieval scoping is opt-in (the
-- text_search `fields=` param); when unset, retrieval is unchanged (globally visible).
--
-- Run: psql personal_koi -v ON_ERROR_STOP=1 --single-transaction -f migrations/105_document_field_membership.sql

CREATE TABLE IF NOT EXISTS document_field_membership (
    document_rid TEXT        NOT NULL,
    field_id     TEXT        NOT NULL,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    added_by     TEXT,
    PRIMARY KEY (document_rid, field_id)
);

CREATE INDEX IF NOT EXISTS idx_dfm_field_id ON document_field_membership (field_id);

-- ── Backfill (idempotent) ──────────────────────────────────────────────────────────
-- One membership row per distinct (document_rid, group_id) for chunks that carry a
-- non-null group_id, plus one row per document_rid whose chunks have NULL group_id
-- mapped to the '__global__' sentinel field so those ~143k legacy null-group chunks
-- stay field-scopeable (and discoverable via fields=['__global__']).

-- Non-null group_id → field_id verbatim.
INSERT INTO document_field_membership (document_rid, field_id, added_by)
SELECT DISTINCT document_rid, metadata->>'group_id', 'backfill'
FROM koi_memory_chunks
WHERE metadata->>'group_id' IS NOT NULL
ON CONFLICT DO NOTHING;

-- NULL group_id → '__global__' sentinel.
INSERT INTO document_field_membership (document_rid, field_id, added_by)
SELECT DISTINCT document_rid, '__global__', 'backfill'
FROM koi_memory_chunks
WHERE metadata->>'group_id' IS NULL
ON CONFLICT DO NOTHING;

INSERT INTO schema_migrations (version) VALUES ('105_document_field_membership')
ON CONFLICT DO NOTHING;
