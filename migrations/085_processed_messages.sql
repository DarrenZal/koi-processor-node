-- Migration 085: daily_note_processed_lines → processed_messages + v2 columns
--
-- Phase J of agent-inbox-pipeline-v2 (an internal planning document).
--
-- Renames the v1 idempotency table so v2 can accept messages from multiple
-- sources (daily-note + Telegram + future Email) via a unified MessageSource
-- abstraction. Adds:
--   - source_type           ('daily_note' | 'telegram')
--   - source_id             (line_hash for daily-note; chat:thread:msg for Telegram)
--   - response_channel      (annotation | telegram-thread | telegram-edit | inline-only)
--   - processing_started_at (crash-recovery for Telegram long handlers)
--
-- Extends the status CHECK constraint with v2 values (incl. EXTRACTION_ZERO_FACTS
-- for "subprocess exit 0 but 0 facts written" visibility — prevents silently-
-- useless OK status).
--
-- Extends the verb CHECK to allow Track B/D verbs (research, question).
--
-- Creates a compat view `daily_note_processed_lines` so v1 read-only callers
-- (daily-note-investigator.sh, quickcheck.sh, observability) keep working
-- without code change. Writes must go through the new `processed_messages` name.
--
-- Safety: requires writers stopped per canonical cutover sequence. Run NUC
-- first, MacBook within 60s (federation schema match). v1 rows get
-- source_type='daily_note' + source_id=line_hash via backfill.

BEGIN;

-- Pre-check guard: abort if any (note_path, line_hash, url_hash) tripled exists.
-- Validated pre-migration (0 rows); guard is defensive.
DO $$
DECLARE
    dup_count int;
BEGIN
    SELECT COUNT(*) INTO dup_count FROM (
        SELECT note_path, line_hash, url_hash, COUNT(*)
        FROM daily_note_processed_lines
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 1
    ) d;
    IF dup_count > 0 THEN
        RAISE EXCEPTION 'Migration 085 aborted: % duplicate (note_path, line_hash, url_hash) triples in daily_note_processed_lines', dup_count;
    END IF;
END $$;

-- Drop constraints we'll re-add with new semantics
ALTER TABLE daily_note_processed_lines DROP CONSTRAINT IF EXISTS chk_dnpl_status;
ALTER TABLE daily_note_processed_lines DROP CONSTRAINT IF EXISTS chk_dnpl_verb;
ALTER TABLE daily_note_processed_lines DROP CONSTRAINT IF EXISTS chk_dnpl_source_tier;

-- Rename
ALTER TABLE daily_note_processed_lines RENAME TO processed_messages;

-- Rename old auto-generated indexes + PK to match the new table name (cosmetic)
ALTER INDEX IF EXISTS daily_note_processed_lines_pkey RENAME TO processed_messages_pkey;
ALTER INDEX IF EXISTS idx_dnpl_status RENAME TO idx_pm_status;
ALTER INDEX IF EXISTS idx_dnpl_ack RENAME TO idx_pm_ack;
ALTER INDEX IF EXISTS idx_dnpl_url_hash RENAME TO idx_pm_url_hash;

-- New columns (NOT NULL defaults for v1 rows)
ALTER TABLE processed_messages ADD COLUMN IF NOT EXISTS source_type text NOT NULL DEFAULT 'daily_note';
ALTER TABLE processed_messages ADD COLUMN IF NOT EXISTS source_id text;
ALTER TABLE processed_messages ADD COLUMN IF NOT EXISTS response_channel text;
ALTER TABLE processed_messages ADD COLUMN IF NOT EXISTS processing_started_at timestamptz;

-- Backfill source_id from line_hash for v1 rows
UPDATE processed_messages SET source_id = line_hash WHERE source_id IS NULL;
ALTER TABLE processed_messages ALTER COLUMN source_id SET NOT NULL;

-- Swap PK: (note_path, line_hash, url_hash) → (source_type, source_id, url_hash)
ALTER TABLE processed_messages DROP CONSTRAINT processed_messages_pkey;
ALTER TABLE processed_messages ADD CONSTRAINT processed_messages_pkey
    PRIMARY KEY (source_type, source_id, url_hash);

-- Re-add CHECK constraints with expanded v2 vocabulary
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_status CHECK (status IN (
    -- v1 statuses (preserved)
    'OK', 'AMBIGUOUS', 'FAILED', 'DEFERRED', 'INTERACTIVE_FLAGGED',
    -- v2 additions
    'LINKED_SEMANTIC',
    'RESEARCH_COMPLETE', 'RESEARCH_FAILED', 'RESEARCH_BUDGET_DEFERRED',
    'QUESTION_ANSWERED', 'QUESTION_FAILED',
    'BARE_URL_ASSUMED',
    'PATH_RESOLVED', 'AMBIGUOUS_PATH_NOT_FOUND',
    'AMBIGUOUS_THIN_CONTENT',
    'BRIEF_REPLY_HANDLED',
    'EXTRACTION_ZERO_FACTS'
));
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_verb CHECK (verb IN (
    'ingest', 'index', 'look at', 'read', 'idea',
    'research', 'question'
));
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_source_tier CHECK (
    source_tier IS NULL OR source_tier IN ('aiohttp', 'playwright', 'scrapling', 'none')
);
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_source_type CHECK (
    source_type IN ('daily_note', 'telegram')
);
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_response_channel CHECK (
    response_channel IS NULL OR response_channel IN (
        'annotation', 'telegram-thread', 'telegram-edit', 'telegram-new', 'inline-only'
    )
);

-- v2 indexes
CREATE INDEX IF NOT EXISTS idx_pm_source ON processed_messages (source_type, source_id);
CREATE INDEX IF NOT EXISTS idx_pm_status_unack ON processed_messages (status) WHERE ack_at IS NULL;

-- Compat view for v1 read-only callers (daily-note-investigator.sh, quickcheck.sh)
-- Writes MUST go through processed_messages directly — view is not writable.
CREATE OR REPLACE VIEW daily_note_processed_lines AS
  SELECT note_path, line_hash, url_hash, verb, status, reason,
         episode_id, memory_id, source_tier, processed_at, ack_at
  FROM processed_messages
  WHERE source_type = 'daily_note';

COMMIT;
