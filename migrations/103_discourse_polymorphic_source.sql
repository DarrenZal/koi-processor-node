-- Migration 103: generalize session_discourse_moves to carry document discourse
--
-- Phase 3 of an internal planning document, and the ONLY
-- production-table change in the unified document-ingest arc — migration 102's header
-- reserves this exact number for it. Generalizes session_discourse_moves IN PLACE
-- (vs a parallel doc_discourse_moves table) so one set of query/embedding/index paths
-- serves both producers.
--
-- The daily session deep-extraction pipeline is UNAFFECTED:
--   * source_type defaults to 'session', so every existing row backfills in place;
--   * the session importer's INSERT (extract_deep_sessions.py) omits source_type and
--     source_rid — the DEFAULT and nullable column keep it working byte-for-byte;
--   * no session column is dropped or renamed.
--
-- Additive + idempotent: ADD COLUMN IF NOT EXISTS, guarded CONSTRAINT adds, idempotent
-- DROP NOT NULL, CREATE INDEX IF NOT EXISTS. Safe to re-run.
--
-- Run: psql personal_koi -v ON_ERROR_STOP=1 --single-transaction \
--        -f migrations/103_discourse_polymorphic_source.sql
--      (then record in koi_migrations — see scripts/stamp_baseline.py)

-- ── Preamble guards (mirror 086: refuse wrong DB / non-local connection) ───────────
DO $$
DECLARE
  db TEXT := current_database();
  test_mode TEXT := current_setting('deep_extract.test_mode', true);
BEGIN
  IF NOT (db = 'personal_koi' OR (db = 'personal_koi_test' AND test_mode = '1')) THEN
    RAISE EXCEPTION 'Refusing: wrong DB % (test_mode=%)', db, test_mode;
  END IF;
  IF inet_server_addr() IS NOT NULL
     AND host(inet_server_addr()) NOT IN ('127.0.0.1','::1') THEN
    RAISE EXCEPTION 'Refusing: non-local inet_server_addr %', inet_server_addr();
  END IF;
END$$;

-- ── 1. Polymorphic source columns ─────────────────────────────────────────────────
-- source_type: which producer wrote the move. DEFAULT 'session' backfills every
-- existing row in place (all current rows are session moves) and lets the unchanged
-- session importer keep inserting without naming the column. A constant default is a
-- metadata-only ADD COLUMN on PG11+ (no table rewrite).
ALTER TABLE session_discourse_moves
  ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'session';

-- source_rid: the document RID ('document:<sha256>') for document moves; NULL for
-- session moves (which are keyed by session_id).
ALTER TABLE session_discourse_moves
  ADD COLUMN IF NOT EXISTS source_rid TEXT;

-- ── 2. Relax the session-only NOT NULL so document moves can omit session_id ────────
-- (Idempotent: DROP NOT NULL on an already-nullable column is a no-op.)
ALTER TABLE session_discourse_moves
  ALTER COLUMN session_id DROP NOT NULL;

-- ── 3. Source-aware integrity CHECKs (guarded for idempotency) ─────────────────────
DO $$
BEGIN
  -- enum: only the two known producers
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_discourse_source_type') THEN
    ALTER TABLE session_discourse_moves
      ADD CONSTRAINT chk_discourse_source_type
      CHECK (source_type IN ('session','document'));
  END IF;
  -- each producer supplies its own identity column. Existing rows satisfy this:
  -- source_type='session' (default) AND session_id IS NOT NULL (pre-existing data).
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_discourse_source_identity') THEN
    ALTER TABLE session_discourse_moves
      ADD CONSTRAINT chk_discourse_source_identity
      CHECK (
        (source_type = 'session'  AND session_id IS NOT NULL) OR
        (source_type = 'document' AND source_rid IS NOT NULL)
      );
  END IF;
END$$;

-- ── 4. Lookup indexes for the document path ────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_discourse_source_rid
  ON session_discourse_moves(source_rid) WHERE source_rid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_discourse_source_type
  ON session_discourse_moves(source_type);

-- ── 5. Document the dual coordinate semantics carried by the existing range columns ─
-- Document moves reuse turn_range_* as a global RAG-chunk range (the document's
-- position coordinate space), parallel to a session's turn range. Queries MUST scope
-- by source_type before interpreting these as turns vs chunks.
COMMENT ON COLUMN session_discourse_moves.turn_range_start IS
  'source_type=session: conversation turn index. source_type=document: global RAG chunk index (range start).';
COMMENT ON COLUMN session_discourse_moves.turn_range_end IS
  'source_type=session: conversation turn index. source_type=document: global RAG chunk index (range end).';
COMMENT ON COLUMN session_discourse_moves.source_type IS
  'Producer of the move: ''session'' (daily deep-extraction) or ''document'' (unified document-ingest, Phase 3).';
COMMENT ON COLUMN session_discourse_moves.source_rid IS
  'document:<sha256> RID for document moves; NULL for session moves (keyed by session_id).';
