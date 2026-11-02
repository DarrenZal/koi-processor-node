-- =============================================================================
-- Migration 136: session_exclusions, which keeps excluded Claude sessions out of user-facing endpoints
-- =============================================================================
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/136_session_exclusions.sql
-- Rollback: not a backout. The down file refuses while any session is marked (see it).
--
-- Numbered 136: 133-135 are taken (133 and 134 are applied here, 135 is in koi-processor-write-log).
--
-- Date: 2026-10-25
-- Record: an internal record (D4)
--
-- Why: excluding a session in the claude-sessions sensor only stops future ingestion. Its
-- session_ingestion_log row (summary, first prompt, files accessed) stays, because exclusion and audit
-- need it, and /search-sessions, /session-stats, /session-tools, /session-files and
-- /knowledge/discourse-search?source_type=session returned it. The sensor writes one row here per
-- excluded session, from an exact-session manifest (koi-sensors claude_session_sensor.py
-- --mode exclusion-mark, and quarantine-apply). Those readers filter on it (api/session_exclusions.py)
-- and fail closed with a 503 while the table is missing, so apply this migration before the code.
-- Only this migration creates the table. The sensor refuses to mark or quarantine without it.

CREATE TABLE IF NOT EXISTS session_exclusions (
    session_id  TEXT PRIMARY KEY,
    excluded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reason      TEXT
);

COMMENT ON TABLE session_exclusions IS
    'Claude sessions excluded by the claude-sessions sensor; user-facing session endpoints skip them. Written from an exact-session manifest.';

DO $$
BEGIN
    IF to_regclass('koi_migrations') IS NOT NULL THEN
        INSERT INTO koi_migrations (migration_id, checksum)
        VALUES ('136_session_exclusions', 'v1_session_exclusions')
        ON CONFLICT (migration_id) DO NOTHING;
    END IF;
END $$;
