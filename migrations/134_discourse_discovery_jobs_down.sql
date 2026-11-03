-- DESTRUCTIVE SCHEMA REMOVAL for 134_discourse_discovery_jobs.sql. Not a rollback step.
--
-- The supported backout for an ingestion discovery run is operational:
--   scripts/discourse_links_canary.py rollback --dsn <dsn> --run-id ingest:<job>:<attempt> --apply
-- It undoes the run's owned outputs (candidates, vectors, unreferenced claim
-- texts), keeps the job, run and effect rows as history (run status
-- rolled_back), and leaves the ingested document and its extraction bindings
-- alone. Nothing here is needed for that.
--
-- This file drops the job table and restores the 'canary'-only run kind. It
-- refuses while any non-canary run row exists, because removing the kind would
-- mean discarding those runs' audit records. Doing that is a separate,
-- explicitly approved destructive operation.
BEGIN;

DO $$
DECLARE n integer;
BEGIN
    SELECT count(*) INTO n FROM discourse_link_runs WHERE kind <> 'canary';
    IF n > 0 THEN
        RAISE EXCEPTION 'refusing: % non-canary run row(s) exist in discourse_link_runs; removing the run kind would discard their audit history', n;
    END IF;
END $$;

DROP TABLE IF EXISTS discourse_discovery_jobs;
ALTER TABLE discourse_link_runs DROP CONSTRAINT discourse_link_runs_kind_check;
ALTER TABLE discourse_link_runs ADD CONSTRAINT discourse_link_runs_kind_check CHECK (kind = 'canary');
DELETE FROM koi_migrations WHERE migration_id = '134_discourse_discovery_jobs';

COMMIT;
