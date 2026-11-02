\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 123a: entity_registry.revoked_at for fresh personal installs
--
-- 124_entity_normalization_compat creates a view that filters on
-- entity_registry.revoked_at, but no migration and no API code adds that column:
-- live personal_koi databases gained it outside the migration chain. This adds it
-- with the live column's type (timestamp with time zone, nullable, no default).
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).
-- Applied any other way, for example psql -f against a live database, it does nothing.

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
