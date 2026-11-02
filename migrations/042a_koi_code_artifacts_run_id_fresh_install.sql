\set ON_ERROR_STOP on
-- Set first, so that a psql older than 10 (no \if) stops here instead of running the body.

-- Migration 042a: koi_code_artifacts.extraction_run_id for fresh personal installs
--
-- 024_code_docs_bridge creates koi_code_artifacts without extraction_run_id.
-- 043_github_code_graph's CREATE TABLE IF NOT EXISTS is then skipped and its index
-- on extraction_run_id fails. This adds the column (nullable TEXT, as 043 defines it)
-- before 043 runs.
--
-- Fresh-install only: a no-op unless scripts/run_migrations.sh is building an empty
-- personal_koi database (koi.fresh_install=on and the koi_fresh_install_in_progress marker).

SELECT (coalesce(current_setting('koi.fresh_install', true), '') = 'on'
        AND current_database() = 'personal_koi'
        AND to_regclass('public.koi_fresh_install_in_progress') IS NOT NULL) AS koi_fresh_install \gset
\if :koi_fresh_install
ALTER TABLE koi_code_artifacts ADD COLUMN IF NOT EXISTS extraction_run_id TEXT;
\else
\echo 'not a fresh personal_koi install in progress: nothing to do'
\endif
