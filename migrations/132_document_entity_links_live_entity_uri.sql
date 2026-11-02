-- =============================================================================
-- Migration 132: writes to document_entity_links.entity_uri are redirected to the live entity
-- =============================================================================
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/132_document_entity_links_live_entity_uri.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/132_document_entity_links_live_entity_uri_down.sql
--
-- Numbered 132, not 119: 119-122 and 124-131 are taken in sibling worktrees
-- (koi-document-ingest-integrity-20260913, koi-federated-retractions-20260915 and
-- others), and 124/125 are already in this database's koi_migrations ledger.
--
-- Date: 2026-10-12
-- Plan: an internal planning document
--       registration-of-new-notes-optional-differentfrom-veto.md, Phase 1b step 11d.
--
-- Why: `/entities/merge` tombstones the loser (sets merged_into, keeps the row) and
-- rewires the loser's links to the survivor once, at merge time. Every later write
-- that names the loser's URI re-creates a link to a dead entity. Found 2026-10-12:
-- 81 such links, written by /ingest after a create collided with a tombstone's
-- deterministic URI (fixed in api/personal_ingest_api.py) and reachable from other
-- writers too: federation apply (api/domain_event_handlers.py) writes the payload's
-- entity_uri as received, and deep extraction, mediawiki, web and github ingest
-- each INSERT directly. Fixing each writer is necessary but not sufficient; this
-- trigger is the backstop for the STORED row, from every writer including ones not
-- written yet.
--
-- What it does NOT cover:
--   * The writer's own federation emit. A writer that emits the URI it passed in,
--     not the stored one, still announces the tombstone. Writers must resolve first
--     (/ingest, register-entity, knowledge-add) or emit the RETURNING value (github).
--   * Links that were live when written and whose entity is tombstoned LATER. The
--     trigger fires on document_entity_links writes only, not on
--     entity_registry.merged_into. /entities/merge rewires at merge time; the
--     resurrected-survivor branch of merge_reversal.unmerge re-tombstones a row
--     without moving links it gained after the retype. Those remain a repair query
--     (see the rollback file).
--   * A multi-row INSERT ... ON CONFLICT DO UPDATE whose source names two URIs with
--     one survivor: the trigger folds both onto one key and Postgres rejects the
--     statement. Such writers must resolve before grouping (deep extraction Flag 2).
--
-- What it does: BEFORE INSERT, and BEFORE UPDATE OF entity_uri, rewrite
-- NEW.entity_uri to its live survivor by following entity_registry.merged_into.
--   * Live URI, or a URI not in entity_registry at all: unchanged. entity_uri has no
--     FK to entity_registry and some writers link before registering.
--   * Chain broken (a hop names a URI with no registry row), a cycle, or more than
--     10 hops (MAX_MERGE_CHAIN in api/resolution_primitives.py): unchanged, with a
--     WARNING. A guessed URI is worse than a known tombstone, which a repair can find.
--
-- Interaction with ON CONFLICT: a BEFORE ROW INSERT trigger runs before the
-- arbiter-index check, so /ingest's
--   INSERT ... ON CONFLICT (document_rid, entity_uri) DO UPDATE SET mention_count + 1
-- tests the REWRITTEN uri and folds into an existing survivor link instead of
-- adding a second row. The DO UPDATE branch sets only mention_count, so it does not
-- fire the UPDATE OF entity_uri arm.
--
-- An UPDATE that points a link at a tombstone whose survivor is already linked to the
-- same document now raises a unique violation instead of creating a tombstone link.
-- That is intended: the writer must fold the counts, as _do_merge does.
--
-- Ordering requirement for unmerge: api/merge_reversal.py::unmerge repoints links back
-- to the loser, so it must un-tombstone the loser FIRST, or this trigger sends the
-- links straight back to the survivor. Reordered in the same change as this file.
--
-- Trigger name order: Postgres fires same-event BEFORE ROW triggers alphabetically,
-- so tr_layers_only_guard_links (deep-extract layers_only guard) fires before this
-- one and still refuses those writes first.
-- =============================================================================

CREATE OR REPLACE FUNCTION document_entity_links_live_entity_uri()
RETURNS trigger AS $$
DECLARE
    cur    TEXT := NEW.entity_uri;
    nxt    TEXT;
    hops   INT  := 0;
BEGIN
    IF cur IS NULL THEN
        RETURN NEW;
    END IF;

    LOOP
        SELECT merged_into INTO nxt FROM entity_registry WHERE fuseki_uri = cur;
        IF NOT FOUND THEN
            IF hops > 0 THEN
                RAISE WARNING 'document_entity_links: merge chain from % is broken at % '
                    '(no entity_registry row); leaving entity_uri unchanged',
                    NEW.entity_uri, cur;
            END IF;
            RETURN NEW;
        END IF;
        EXIT WHEN nxt IS NULL;           -- cur is live
        hops := hops + 1;
        IF hops > 10 THEN
            RAISE WARNING 'document_entity_links: merge chain from % exceeds 10 hops '
                '(cycle or runaway chain); leaving entity_uri unchanged', NEW.entity_uri;
            RETURN NEW;
        END IF;
        cur := nxt;
    END LOOP;

    NEW.entity_uri := cur;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION document_entity_links_live_entity_uri() IS
    'BEFORE INSERT / UPDATE OF entity_uri on document_entity_links: follows '
    'entity_registry.merged_into (max 10 hops) so a written link does not name a '
    'merged-away entity. Unknown URIs and broken or cyclic chains are left unchanged. '
    'Does not fire when an entity is tombstoned after its links exist. Migration 132.';

DROP TRIGGER IF EXISTS trg_document_entity_links_live_entity_uri ON document_entity_links;
CREATE TRIGGER trg_document_entity_links_live_entity_uri
    BEFORE INSERT OR UPDATE OF entity_uri ON document_entity_links
    FOR EACH ROW
    EXECUTE FUNCTION document_entity_links_live_entity_uri();

-- -----------------------------------------------------------------------------
-- Migration bookkeeping
-- -----------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('132_document_entity_links_live_entity_uri', 'v1_live_entity_uri_trigger')
ON CONFLICT (migration_id) DO NOTHING;
