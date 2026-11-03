-- Rollback for migration 115. See that file for rationale.
-- Apply: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/115_predicate_raw_and_resolver_decisions_down.sql

DROP TABLE IF EXISTS resolver_decisions;

ALTER TABLE knowledge_facts DROP COLUMN IF EXISTS predicate_raw;
ALTER TABLE knowledge_facts DROP COLUMN IF EXISTS confidence;

DELETE FROM koi_migrations WHERE migration_id = '115_predicate_raw_and_resolver_decisions';
