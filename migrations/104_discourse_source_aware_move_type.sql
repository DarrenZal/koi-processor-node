-- Migration 104: source-type-aware move_type + status on session_discourse_moves
--
-- Completes the polymorphic discourse design (plan §Q2). Migration 103 added
-- source_type/source_rid but left move_type + status as the FLAT SESSION enums, which
-- forced document moves into session vocabulary (problem/observation/...). 104 makes
-- BOTH constraints source-aware:
--   * session rows keep the session enums (unchanged behavior);
--   * document rows use the document-ARGUMENT taxonomy
--       move_type ∈ thesis|premise|evidence|claim|counterpoint|open_question|definition|implication
--       status    ∈ asserted|supported|contested|speculative|open|deferred   (or NULL)
--
-- PRECONDITION: any pre-existing DOCUMENT discourse rows written under the old session
-- move_types violate the new document CHECK and MUST be deleted first (re-extracted
-- under the new taxonomy). The single BioHubs document's 57 session-typed moves are
-- removed as a one-time operational step BEFORE this migration; the migration then
-- validates cleanly. Session rows are never touched. A fresh DB (NUC) has no document
-- rows, so 104 applies with no precondition there.
--
-- Reverse: drop the two new constraints, restore the original flat CHECKs (the
-- session enum) — see the commented block at the foot.
--
-- Run: psql personal_koi -v ON_ERROR_STOP=1 --single-transaction \
--        -f migrations/104_discourse_source_aware_move_type.sql

-- ── Preamble guards (mirror 086/103) ───────────────────────────────────────────────
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

-- Precondition guard: any document row under the OLD session move_types would fail the
-- new document CHECK below and hang the ADD CONSTRAINT. Fail fast with an actionable
-- message instead (delete / re-extract those rows first — see the header). A fresh DB
-- (NUC) and a DB whose document rows already use the argument taxonomy both pass with 0.
DO $$
DECLARE bad INT;
BEGIN
  SELECT count(*) INTO bad FROM session_discourse_moves
   WHERE source_type = 'document' AND move_type NOT IN
     ('thesis','premise','evidence','claim','counterpoint','open_question','definition','implication');
  IF bad > 0 THEN
    RAISE EXCEPTION 'Refusing migration 104: % document discourse row(s) use a non-argument move_type. '
      'Delete or re-extract them under the argument taxonomy first (header lines 12-17).', bad;
  END IF;
END$$;

-- ── 1. move_type: replace the flat session CHECK with a source-aware one ────────────
ALTER TABLE session_discourse_moves DROP CONSTRAINT IF EXISTS session_discourse_moves_move_type_check;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_discourse_move_type_by_source') THEN
    ALTER TABLE session_discourse_moves ADD CONSTRAINT chk_discourse_move_type_by_source CHECK (
      (source_type = 'session' AND move_type IN
        ('decision','problem','question','resolution','observation','next_step','learning'))
      OR
      (source_type = 'document' AND move_type IN
        ('thesis','premise','evidence','claim','counterpoint','open_question','definition','implication'))
    );
  END IF;
END$$;

-- ── 2. status: replace the flat session CHECK with a source-aware one (NULL allowed) ─
ALTER TABLE session_discourse_moves DROP CONSTRAINT IF EXISTS session_discourse_moves_status_check;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_discourse_status_by_source') THEN
    ALTER TABLE session_discourse_moves ADD CONSTRAINT chk_discourse_status_by_source CHECK (
      status IS NULL
      OR (source_type = 'session' AND status IN
        ('open','resolved','superseded','deferred','made','reverted','pending','done','cancelled'))
      OR (source_type = 'document' AND status IN
        ('asserted','supported','contested','speculative','open','deferred'))
    );
  END IF;
END$$;

-- ── Reverse migration (manual) ──────────────────────────────────────────────────────
-- ALTER TABLE session_discourse_moves DROP CONSTRAINT IF EXISTS chk_discourse_move_type_by_source;
-- ALTER TABLE session_discourse_moves DROP CONSTRAINT IF EXISTS chk_discourse_status_by_source;
-- ALTER TABLE session_discourse_moves ADD CONSTRAINT session_discourse_moves_move_type_check
--   CHECK (move_type IN ('decision','problem','question','resolution','observation','next_step','learning'));
-- ALTER TABLE session_discourse_moves ADD CONSTRAINT session_discourse_moves_status_check
--   CHECK (status IS NULL OR status IN
--     ('open','resolved','superseded','deferred','made','reverted','pending','done','cancelled'));
