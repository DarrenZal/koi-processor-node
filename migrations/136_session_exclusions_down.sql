-- Rollback for migration 136. See that file for rationale.
-- Apply: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/136_session_exclusions_down.sql
--
-- This is not a backout. The session readers fail closed without the table, and deleting the marks
-- would expose excluded sessions' summaries, first prompts, file lists and discourse moves again. The
-- backout is KOI_SESSION_READERS_DISABLED=true (api/session_exclusions.py). This file refuses while any
-- session is marked, so it only removes an empty table.

DO $$
BEGIN
    IF to_regclass('session_exclusions') IS NOT NULL
       AND EXISTS (SELECT 1 FROM session_exclusions) THEN
        RAISE EXCEPTION 'session_exclusions still marks % session(s); refusing to drop it',
            (SELECT count(*) FROM session_exclusions);
    END IF;
END $$;

DROP TABLE IF EXISTS session_exclusions;

DO $$
BEGIN
    IF to_regclass('koi_migrations') IS NOT NULL THEN
        DELETE FROM koi_migrations WHERE migration_id = '136_session_exclusions';
    END IF;
END $$;
