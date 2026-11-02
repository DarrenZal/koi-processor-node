-- =============================================================================
-- Migration 126: register Document and Event as document-extractable types
-- =============================================================================
-- Date:     2026-09-14
-- Issue:    #68 — align the deep-document extraction type contract
-- Database: personal_koi (PostgreSQL 14.15)
--
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -f migrations/126_document_extraction_type_registry.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -f migrations/126_document_extraction_type_registry_down.sql
--
-- TRANSACTION AND LEDGER (house pattern of 124/125, added 2026-09-14 after review
-- finding M8). This file carries its own BEGIN; … COMMIT;, so `-1` is not needed —
-- and without an in-file transaction a bare `psql -f` would autocommit the upsert
-- below BEFORE the assertion ran, leaving a half-applied state that neither this
-- file nor its down file describes. It also records itself in koi_migrations as
-- 'personal:126_document_extraction_type_registry' (after the assertion, so a
-- recorded row means the postcondition held). Without that row a database rebuilt
-- from migrations/ has no way to tell 126-applied from 126-never-applied.
--
-- =============================================================================
-- THIS IS A NO-OP ON THE LAPTOP AND THAT IS THE POINT
-- =============================================================================
-- Verified read-only 2026-09-14 against live personal_koi:
--
--   SELECT entity_type, extractable FROM allowed_entity_types
--    WHERE entity_type IN ('Document','Event');
--   -- Document | t
--   -- Event    | t
--
-- Both rows already exist with extractable = true. They were INSERTed directly,
-- not by any migration — `docs/operations/two-node-topology.md` records that the
-- laptop's allowed_entity_types holds 32 rows against the NUC's 28, and that the
-- four extra ones appear in no migration file anywhere.
--
-- So this migration does not change the laptop's designations. It exists because
-- the REGISTRY was right and the CODE was wrong, and a database rebuilt from
-- migrations/ would reproduce the wrong half: migration 111 seeds 28 types with
-- `extractable = true` for exactly seven, and its own comment says "exactly the 7
-- the deep-extraction enum can emit". That enum is now nine. Without this file, a
-- fresh personal_koi would fail tests/test_document_extraction_type_contract.py
-- ::test_the_live_registry_and_the_contract_designate_the_same_extractable_set —
-- correctly, because it would genuinely disagree with the extractor.
--
-- Written as an idempotent upsert so it is safe on a node that already has the
-- rows (no-op) and correct on one that does not (insert). It touches nothing in
-- entity_registry, knowledge_facts, or any other graph table.
--
-- Dry-run + negative controls: tests/test_migration_126.py (scratch DB, rolled back).
-- =============================================================================

BEGIN;

INSERT INTO allowed_entity_types (entity_type, description, extractable) VALUES
    ('Document',
     'A discrete written work ingested from a corpus: paper, guide, report, or article.',
     true),
    ('Event',
     'A dated happening that is not a Meeting: a conference, launch, incident, or milestone occurrence.',
     true)
ON CONFLICT (entity_type) DO UPDATE
    SET extractable = true,
        deprecated_at = NULL;


-- -----------------------------------------------------------------------------
-- Assertion. A migration that reports success without establishing its
-- postcondition is the failure mode this repository keeps re-learning.
--
-- This is an exact SET check, not a count. The first draft asserted `count = 9`,
-- which 'Meeting' wrongly extractable plus 'CaseStudy' wrongly missing also
-- satisfies. The extractable, non-deprecated set must EQUAL the contract set
-- (api/document_extraction_contract.py, doc-entity-types-v2-2026-09-14), and the
-- error names both what is missing and what should not be there.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
    expected   TEXT[] := ARRAY['Person', 'Organization', 'Project', 'Concept',
                               'Location', 'Protocol', 'CaseStudy',
                               'Document', 'Event'];
    actual     TEXT[];
    missing    TEXT[];
    unexpected TEXT[];
BEGIN
    SELECT coalesce(array_agg(entity_type ORDER BY entity_type), '{}')
      INTO actual
      FROM allowed_entity_types
     WHERE extractable AND deprecated_at IS NULL;

    SELECT coalesce(array_agg(x ORDER BY x), '{}') INTO missing
      FROM unnest(expected) AS x
     WHERE NOT (x = ANY(actual));

    SELECT coalesce(array_agg(x ORDER BY x), '{}') INTO unexpected
      FROM unnest(actual) AS x
     WHERE NOT (x = ANY(expected));

    IF cardinality(missing) > 0 OR cardinality(unexpected) > 0 THEN
        RAISE EXCEPTION
            '126: the extractable entity-type set does not equal the extractor''s '
            'contract set. missing=% unexpected=% actual=%. The contract '
            '(api/document_extraction_contract.py, doc-entity-types-v2-2026-09-14) '
            'admits exactly: %.',
            missing, unexpected, actual, expected;
    END IF;

    RAISE NOTICE '126: assertion PASSED (extractable entity types = %)', actual;
END $$;

INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('personal:126_document_extraction_type_registry', 'v1_extractable_document_event')
ON CONFLICT (migration_id) DO NOTHING;

COMMIT;
