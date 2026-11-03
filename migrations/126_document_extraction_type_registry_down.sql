-- =============================================================================
-- Migration 126 DOWN: un-designate Document and Event as extractable
-- =============================================================================
-- Date:     2026-09-14
--
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -f migrations/126_document_extraction_type_registry_down.sql
--
-- Transactional (in-file BEGIN; … COMMIT;, `\set ON_ERROR_STOP on` first) and
-- removes the koi_migrations ledger row 126 recorded — house pattern of 124/125,
-- added 2026-09-14 after review finding M8. The assertion is an exact SET check
-- for migration 111's seven, for the same reason the up file's is: a count of 7
-- cannot tell "the seven" from "six of them plus a stray".
--
-- =============================================================================
-- READ THIS BEFORE RUNNING IT
-- =============================================================================
-- This does NOT delete the Document or Event rows, and must not: live entities
-- carry those two types, with relationship edges and document links between
-- them. Deleting the rows would also break allowed_facets' foreign key.
--
-- It clears only the `extractable` designation, returning the registry to
-- migration 111's seven. That is the correct inverse of what 126 asserts.
--
-- Running this while the extractor still admits nine types puts the registry and
-- the code back into exactly the disagreement migration 126 resolved, and
-- tests/test_document_extraction_type_contract.py will say so. Roll the code back
-- too, or do not roll this back.
-- =============================================================================

\set ON_ERROR_STOP on

BEGIN;

UPDATE allowed_entity_types
   SET extractable = false
 WHERE entity_type IN ('Document', 'Event');

DO $$
DECLARE
    expected   TEXT[] := ARRAY['Person', 'Organization', 'Project', 'Concept',
                               'Location', 'Protocol', 'CaseStudy'];
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
            '126 rollback: the extractable entity-type set does not equal migration '
            '111''s seven. missing=% unexpected=% actual=%. The registry holds a '
            'designation this migration did not create (or lacks one it did not '
            'remove); inspect before proceeding.',
            missing, unexpected, actual;
    END IF;

    RAISE NOTICE '126 rollback: assertion PASSED (extractable entity types = %)', actual;
END $$;

DELETE FROM koi_migrations
 WHERE migration_id = 'personal:126_document_extraction_type_registry';

COMMIT;
