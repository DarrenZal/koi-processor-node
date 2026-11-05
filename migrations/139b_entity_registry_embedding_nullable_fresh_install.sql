\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 139b: entity_registry.embedding is nullable on fresh personal installs
--
-- 020 creates entity_registry.embedding as VECTOR(1536) NOT NULL; live personal_koi
-- databases allow null. store_new_entity and the other creators write embedding_3072
-- and never this column, so on a fresh build POST /ingest could not create any entity
-- (NotNullViolationError). This drops the constraint.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).
-- Applied any other way, for example psql -f against a live database, it does nothing.

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
ALTER TABLE entity_registry ALTER COLUMN embedding DROP NOT NULL;
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
