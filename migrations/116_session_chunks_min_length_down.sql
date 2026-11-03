-- Rollback for migration 116. See that file for rationale.
-- Apply: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/116_session_chunks_min_length_down.sql

ALTER TABLE session_chunks DROP CONSTRAINT IF EXISTS chk_session_chunks_min_length;

DELETE FROM koi_migrations WHERE migration_id = '116_session_chunks_min_length';
