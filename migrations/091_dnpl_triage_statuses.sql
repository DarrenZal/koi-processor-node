-- Migration 091: extend processed_messages constraints for the triage verb.
--
-- Plan §so-i-think-we-deep-crystal originally believed daily_note_processed_lines
-- was free-form text (no CHECK constraint). That check ran against the VIEW;
-- views don't carry constraints. The base table `processed_messages` DOES have
-- two CHECK constraints (chk_pm_status, chk_pm_verb) that need widening before
-- the new triage verb + TRIAGE_* statuses can be persisted.
--
-- This widens both. Fully backwards-compatible — existing rows already pass.

BEGIN;

-- Widen status set with the six TRIAGE_* values that process_daily_note.py
-- emits as DB rows (TRIAGE_BUDGET_DEFERRED is JSONL-only by design and is
-- intentionally NOT included).
ALTER TABLE processed_messages DROP CONSTRAINT IF EXISTS chk_pm_status;
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_status CHECK (
    status = ANY (ARRAY[
        'OK',
        'AMBIGUOUS',
        'FAILED',
        'DEFERRED',
        'INTERACTIVE_FLAGGED',
        'LINKED_SEMANTIC',
        'RESEARCH_COMPLETE',
        'RESEARCH_FAILED',
        'RESEARCH_BUDGET_DEFERRED',
        'QUESTION_ANSWERED',
        'QUESTION_FAILED',
        'BARE_URL_ASSUMED',
        'PATH_RESOLVED',
        'AMBIGUOUS_PATH_NOT_FOUND',
        'AMBIGUOUS_THIN_CONTENT',
        'BRIEF_REPLY_HANDLED',
        'EXTRACTION_ZERO_FACTS',
        -- Triage statuses (plan §Phase 1)
        'TRIAGE_RELEVANT',
        'TRIAGE_RELEVANT_DUP',
        'TRIAGE_MAYBE',
        'TRIAGE_NOT_RELEVANT',
        'TRIAGE_PARSE_FAILED',
        'TRIAGE_FAILED'
    ]::text[])
);

-- Widen verb set with 'triage'.
ALTER TABLE processed_messages DROP CONSTRAINT IF EXISTS chk_pm_verb;
ALTER TABLE processed_messages ADD CONSTRAINT chk_pm_verb CHECK (
    verb = ANY (ARRAY[
        'ingest', 'index', 'look at', 'read', 'idea', 'research', 'question',
        'triage'
    ]::text[])
);

COMMIT;

-- Note: TRIAGE_BUDGET_DEFERRED is intentionally NOT in the status set.
-- Budget-exhausted triages are logged to ~/.config/dobby/triage-deferred.jsonl
-- and produce NO DB row, so the next nightshift sees the line as new and
-- re-processes from scratch (plan P3-1 / P4-1). The status name is reserved
-- in case a future revision changes that strategy.
--
-- Rollback (if needed):
--   ALTER TABLE processed_messages DROP CONSTRAINT chk_pm_status;
--   ALTER TABLE processed_messages ADD  CONSTRAINT chk_pm_status CHECK (
--       status = ANY (ARRAY['OK','AMBIGUOUS','FAILED','DEFERRED',
--           'INTERACTIVE_FLAGGED','LINKED_SEMANTIC','RESEARCH_COMPLETE',
--           'RESEARCH_FAILED','RESEARCH_BUDGET_DEFERRED','QUESTION_ANSWERED',
--           'QUESTION_FAILED','BARE_URL_ASSUMED','PATH_RESOLVED',
--           'AMBIGUOUS_PATH_NOT_FOUND','AMBIGUOUS_THIN_CONTENT',
--           'BRIEF_REPLY_HANDLED','EXTRACTION_ZERO_FACTS']::text[]));
--   ALTER TABLE processed_messages DROP CONSTRAINT chk_pm_verb;
--   ALTER TABLE processed_messages ADD  CONSTRAINT chk_pm_verb CHECK (
--       verb = ANY (ARRAY['ingest','index','look at','read','idea','research',
--           'question']::text[]));
--   But this requires deleting any existing TRIAGE_* rows first — would
--   need: DELETE FROM processed_messages WHERE status LIKE 'TRIAGE_%';
