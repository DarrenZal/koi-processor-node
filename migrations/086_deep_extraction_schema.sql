-- 086_deep_extraction_schema.sql
-- Deep session extraction v2: discourse + continuity + provenance layers.
-- See an internal planning document for full Decision log (1-143).
-- Idempotent: all CREATE use IF NOT EXISTS; triggers use DROP-then-CREATE.
-- Run via: psql personal_koi -v ON_ERROR_STOP=1 --single-transaction -f migrations/086_deep_extraction_schema.sql

-- =====================================================================
-- Preamble guards (Decisions 96/106/125/140)
-- =====================================================================
DO $$
DECLARE
  db TEXT := current_database();
  test_mode TEXT := current_setting('deep_extract.test_mode', true);
  data_dir TEXT;
BEGIN
  -- Hard: refuse wrong DB (allow personal_koi_test when deep_extract.test_mode=1)
  IF NOT (db = 'personal_koi' OR (db = 'personal_koi_test' AND test_mode = '1')) THEN
    RAISE EXCEPTION 'Refusing: wrong DB % (test_mode=%)', db, test_mode;
  END IF;
  -- Hard: refuse non-local TCP connection
  IF inet_server_addr() IS NOT NULL
     AND host(inet_server_addr()) NOT IN ('127.0.0.1','::1') THEN
    RAISE EXCEPTION 'Refusing: non-local inet_server_addr %', inet_server_addr();
  END IF;
  -- Hard: pgvector required for VECTOR(1024) columns + hnsw indexes
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    RAISE EXCEPTION 'Refusing: pgvector extension not installed. Run CREATE EXTENSION vector; before this migration.';
  END IF;
  -- Soft: warn on data_directory mismatch (orchestrator enforces via EXPECTED_PG_DATA_DIR)
  SELECT setting INTO data_dir FROM pg_settings WHERE name='data_directory';
  IF data_dir != '/opt/homebrew/var/postgresql@14' THEN
    RAISE WARNING 'data_directory % does not match expected /opt/homebrew/var/postgresql@14 — ensure EXPECTED_PG_DATA_DIR env matches', data_dir;
  END IF;
END$$;

-- =====================================================================
-- session_discourse_moves — decisions, problems, questions, resolutions
-- =====================================================================
-- Resolutions are stored as separate rows with move_type='resolution' and
-- resolves_move_id -> the problem's id (Decision 11).
-- status is a flat-union CHECK (Decision 10); per-type legal subsets enforced
-- by importer + JSON schema.
CREATE TABLE IF NOT EXISTS session_discourse_moves (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    episode_id UUID REFERENCES knowledge_episodes(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL,
    move_type TEXT NOT NULL CHECK (move_type IN ('decision','problem','question','resolution','observation','next_step','learning')),
    title TEXT NOT NULL,
    detail TEXT,
    status TEXT CHECK (status IS NULL OR status IN (
        'open','resolved','superseded','deferred',
        'made','reverted',
        'pending','done','cancelled'
    )),
    resolves_move_id UUID REFERENCES session_discourse_moves(id),
    turn_range_start INT,
    turn_range_end INT,
    embedding VECTOR(1024),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discourse_session ON session_discourse_moves(session_id);
CREATE INDEX IF NOT EXISTS idx_discourse_episode ON session_discourse_moves(episode_id);
CREATE INDEX IF NOT EXISTS idx_discourse_type ON session_discourse_moves(move_type);
CREATE INDEX IF NOT EXISTS idx_discourse_status ON session_discourse_moves(status) WHERE status IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_discourse_embedding ON session_discourse_moves USING hnsw (embedding vector_cosine_ops);

-- Decision 50: next_step.target_date
ALTER TABLE session_discourse_moves
  ADD COLUMN IF NOT EXISTS target_date DATE;
CREATE INDEX IF NOT EXISTS idx_discourse_target_date
  ON session_discourse_moves(target_date) WHERE target_date IS NOT NULL;

-- =====================================================================
-- session_continuity_links — session → session relationships
-- =====================================================================
CREATE TABLE IF NOT EXISTS session_continuity_links (
    id SERIAL PRIMARY KEY,
    from_session_id TEXT NOT NULL,
    to_session_id TEXT NOT NULL,
    link_type TEXT NOT NULL,
    detail TEXT,
    extracted_from_turn INT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (from_session_id, to_session_id, link_type)
);

CREATE INDEX IF NOT EXISTS idx_continuity_from ON session_continuity_links(from_session_id);
CREATE INDEX IF NOT EXISTS idx_continuity_to ON session_continuity_links(to_session_id);

-- =====================================================================
-- knowledge_facts — add turn-range provenance columns
-- =====================================================================
ALTER TABLE knowledge_facts
  ADD COLUMN IF NOT EXISTS turn_range_start INT,
  ADD COLUMN IF NOT EXISTS turn_range_end INT;

-- =====================================================================
-- session_ingestion_log — deep-extraction tracking columns
-- =====================================================================
-- NULL = never deep-extracted. Eligibility: deep_extracted_at IS NULL OR deep_extracted_at < last_ingested_at
-- (and not equal to sentinel '1970-01-01' — retry-exhausted, Decision 35).
ALTER TABLE session_ingestion_log
  ADD COLUMN IF NOT EXISTS deep_extracted_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS deep_extraction_attempts INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS deep_extraction_last_error TEXT,
  ADD COLUMN IF NOT EXISTS deep_extraction_next_retry_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS deep_extraction_last_run_id TEXT,
  ADD COLUMN IF NOT EXISTS deep_extraction_mode TEXT
    CHECK (deep_extraction_mode IS NULL OR deep_extraction_mode IN ('layers_only','full'));

-- =====================================================================
-- deep_extraction_item_errors — per-item dead-letter table (Decision 23)
-- =====================================================================
CREATE TABLE IF NOT EXISTS deep_extraction_item_errors (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    item_type TEXT NOT NULL CHECK (item_type IN ('fact','continuity_link','discourse_move','entity')),
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    extraction_run_id TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_item_errors_session ON deep_extraction_item_errors(session_id);
CREATE INDEX IF NOT EXISTS idx_item_errors_reason ON deep_extraction_item_errors(reason);

-- =====================================================================
-- layers_only DB-level hard guard (Decision 141)
-- =====================================================================
-- Orchestrator sets `application_name = 'deep-extract:layers_only:<run_id>'`
-- before transaction when mode=layers_only. Trigger blocks writes to v1 tables
-- EXCEPT the minimal-episode auto-create exception (Decision 68).
CREATE OR REPLACE FUNCTION deep_extract_layers_only_guard() RETURNS trigger AS $fn$
DECLARE app_name TEXT := current_setting('application_name', true);
BEGIN
  IF app_name LIKE 'deep-extract:layers_only:%' THEN
    -- Exception: allow minimal-episode auto-create for missing-episode recovery
    IF TG_TABLE_NAME = 'knowledge_episodes'
       AND TG_OP = 'INSERT'
       AND (NEW.metadata ? 'minimal_episode') THEN
      RETURN NEW;
    END IF;
    RAISE EXCEPTION 'layers_only mode cannot write to % (op=%)', TG_TABLE_NAME, TG_OP;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$fn$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tr_layers_only_guard_facts ON knowledge_facts;
DROP TRIGGER IF EXISTS tr_layers_only_guard_links ON document_entity_links;
DROP TRIGGER IF EXISTS tr_layers_only_guard_episodes ON knowledge_episodes;
CREATE TRIGGER tr_layers_only_guard_facts
  BEFORE INSERT OR UPDATE OR DELETE ON knowledge_facts
  FOR EACH ROW EXECUTE FUNCTION deep_extract_layers_only_guard();
CREATE TRIGGER tr_layers_only_guard_links
  BEFORE INSERT OR UPDATE OR DELETE ON document_entity_links
  FOR EACH ROW EXECUTE FUNCTION deep_extract_layers_only_guard();
CREATE TRIGGER tr_layers_only_guard_episodes
  BEFORE INSERT OR UPDATE OR DELETE ON knowledge_episodes
  FOR EACH ROW EXECUTE FUNCTION deep_extract_layers_only_guard();

-- =====================================================================
-- Diagnostic: report pre-existing knowledge_episodes duplicates (Decision 19)
-- =====================================================================
DO $$
DECLARE
    dup_count INT;
BEGIN
    SELECT COUNT(*) INTO dup_count FROM (
        SELECT source_document FROM knowledge_episodes
        WHERE source_description = 'claude_session'
        GROUP BY source_document HAVING COUNT(*) > 1
    ) d;
    RAISE NOTICE 'deep-extraction: % source_document values have >1 knowledge_episodes row. Importer picks most-recent by created_at.', dup_count;
END$$;

-- =====================================================================
-- deep_extraction_config — v2 ship-date + future config keys (Decision 42)
-- =====================================================================
CREATE TABLE IF NOT EXISTS deep_extraction_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO deep_extraction_config (key, value)
  VALUES ('v2_ship_date', NOW()::text)
  ON CONFLICT (key) DO NOTHING;
