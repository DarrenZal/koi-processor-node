-- Migration 082: source_host + extraction-status columns on session_ingestion_log
--
-- Phase 5c (cross-device sessions) + Phase 2c.1 (persisted extraction retry state).
-- All columns nullable/defaulted so the change is zero-downtime: the running
-- sensor keeps writing the old set of columns until its code is upgraded, and
-- existing readers that don't know about the new columns continue to work.

ALTER TABLE session_ingestion_log
  ADD COLUMN IF NOT EXISTS source_host TEXT,
  ADD COLUMN IF NOT EXISTS entities_extracted_at TIMESTAMP,
  ADD COLUMN IF NOT EXISTS extraction_attempts INT DEFAULT 0,
  ADD COLUMN IF NOT EXISTS extraction_last_error TEXT;

-- Backfill: every row already in the table predates cross-device sync, so it
-- originated on the MacBook. Phase 5a's rsync will land NUC-sourced sessions
-- in a separate base_path and the sensor will stamp those as 'dobby'.
UPDATE session_ingestion_log
   SET source_host = 'macbook'
 WHERE source_host IS NULL;

-- No index needed until the table grows beyond ~10K rows — GROUP BY source_host
-- does a seq scan in sub-100ms for the current corpus (4.7K rows).
