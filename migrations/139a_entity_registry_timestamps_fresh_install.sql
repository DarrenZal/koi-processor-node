\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 139a: entity_registry.created_at and updated_at for fresh personal installs
--
-- The API reads entity_registry.created_at (GET /entities and GET /entity-search order
-- by it) and entity_registry.updated_at (/health freshness, the wallet-address update),
-- but the migration chain creates entity_registry with first_seen_at and last_seen_at
-- instead: live personal_koi databases gained created_at and updated_at outside the
-- chain. On a fresh build both routes returned 500 (UndefinedColumnError). This adds the
-- two columns with the live columns' type and default (timestamp without time zone,
-- nullable, DEFAULT now()).
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).
-- Applied any other way, for example psql -f against a live database, it does nothing.

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now();
ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now();
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
