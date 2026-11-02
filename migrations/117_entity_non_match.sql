-- =============================================================================
-- DRAFT Migration: entity_non_match (hard veto on known false-merge pairs)
-- =============================================================================
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/117_entity_non_match.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/117_entity_non_match_down.sql
--
-- Promoted from DRAFT_ to the numbered sequence 2026-09-02, at operator
-- direction, to precede the 94-decision merge worklist. The veto table has to
-- exist BEFORE merges execute, not after: it is what stops an operator's split
-- being silently re-merged by the next ingest.
--
-- Date:     2026-08-31 (drafted)
-- Plan:     an internal planning document,
--           Phase 1, item #10 -- "entity_non_match(uri_a, uri_b, asserted_by,
--           reason) as a hard veto before any accept."
--
-- Purpose:  entity_registry has no negative-evidence surface. resolve_entity()
--           (api/personal_ingest_api.py) can re-derive an ALREADY-DISPROVEN
--           merge on the very next ingestion pass, because nothing tells it
--           two specific URIs were checked by a human and found to be
--           different real things. scripts/split_false_merges.py already
--           fixed 17 such collisions once (COLLISION_PAIRS) by giving the
--           loser a fresh URI -- but nothing stops the same JW-prefix / same
--           embedding-proximity confusion from re-merging the split halves on
--           the next document that mentions either of them. This table is
--           that missing negative-evidence surface: an explicit, auditable,
--           order-independent "these two URIs are NOT the same entity."
--
--           Companion design (unmerge, and the exact wiring into
--           resolve_entity()'s 6 accept points) is written up separately --
--           see the session that produced this draft. Do not wire either
--           without review; this file is schema only.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. entity_non_match
-- -----------------------------------------------------------------------------
-- Order-independent by construction: uri_lo/uri_hi are NOT "the first URI
-- asserted" and "the second" -- they are whichever of the pair sorts first
-- lexicographically. The BEFORE trigger below normalizes the order on every
-- INSERT/UPDATE so callers never have to sort the pair themselves (raw SQL,
-- a seeding script, and a future admin endpoint can all pass (a,b) or (b,a)
-- interchangeably); the CHECK constraint then just verifies the trigger did
-- its job, rather than being a burden every caller has to remember.
--
-- FK target is entity_registry(fuseki_uri), same column merged_into already
-- references (migration 101) -- fuseki_uri is UNIQUE and rows are NEVER hard-
-- deleted (only tombstoned via merged_into), so this FK stays valid forever,
-- including for a URI on either side that later itself gets merged into a
-- third entity. Deliberately NOT constrained to merged_into IS NULL on either
-- side: a vetoed URI that is later (legitimately, for an unrelated reason)
-- merged into a third entity should still be resolvable via
-- entity_non_match_partners() so that veto can be propagated onto the new
-- survivor -- see the "merge inheritance" note in the companion design.
CREATE TABLE IF NOT EXISTS entity_non_match (
    id           BIGSERIAL PRIMARY KEY,
    uri_lo       TEXT        NOT NULL REFERENCES entity_registry(fuseki_uri),
    uri_hi       TEXT        NOT NULL REFERENCES entity_registry(fuseki_uri),
    asserted_by  TEXT        NOT NULL,
    reason       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_entity_non_match_ordered CHECK (uri_lo < uri_hi),
    CONSTRAINT uq_entity_non_match_pair UNIQUE (uri_lo, uri_hi)
);

-- Reverse-direction lookups (uri_hi alone) would otherwise need a seq scan --
-- the UNIQUE index on (uri_lo, uri_hi) only serves uri_lo-led lookups.
CREATE INDEX IF NOT EXISTS idx_entity_non_match_uri_hi
    ON entity_non_match (uri_hi);

COMMENT ON TABLE entity_non_match IS
    'Hard veto table: pairs of entity_registry.fuseki_uri known, by human '
    'assertion, to be DIFFERENT real-world entities. Order-independent '
    '(uri_lo < uri_hi, enforced by trigger + CHECK). Consulted by '
    'resolve_entity() before any tier-1.1b/1.5/2a/2b accept, and should also '
    'be consulted by POST /entities/merge before an operator-driven merge. '
    'Seeded from scripts/split_false_merges.py COLLISION_PAIRS (17 pairs, '
    'confirmed real collisions) plus hand-labelled resolver_decisions rows '
    '(migration 115) where decision=''accepted'' was later confirmed wrong -- '
    'see the seeding script recommendation in the companion design doc. '
    'Plan: an internal planning document #10.';

COMMENT ON COLUMN entity_non_match.uri_lo IS
    'The lexicographically-smaller of the two URIs. Never insert directly out '
    'of order -- the BEFORE trigger normalizes it, but callers should not rely '
    'on that for readability; use entity_non_match_exists()/_partners() below '
    'rather than querying this column directly.';
COMMENT ON COLUMN entity_non_match.asserted_by IS
    'Who/what asserted the veto: an operator identity, or a script name for '
    'bulk-seeded rows (e.g. ''split_false_merges.py collision inventory'', '
    '''resolver_decisions weekly review 2026-08-31''). Never NULL -- an '
    'unattributed veto cannot be reviewed or revoked with confidence.';
COMMENT ON COLUMN entity_non_match.reason IS
    'Why these are different real things, in enough detail for a future '
    'reviewer to judge the assertion without re-deriving it (e.g. ''Songhees '
    'Nation is the First Nation govt; Songhees Catering is an unrelated food '
    'business -- see People/... note'').';

-- -----------------------------------------------------------------------------
-- 2. Order-normalizing trigger
-- -----------------------------------------------------------------------------
-- Swaps uri_lo/uri_hi into order BEFORE the row is validated, so the CHECK
-- constraint above is a pure invariant-verifier rather than something every
-- INSERT site has to remember to satisfy itself. Postgres runs BEFORE
-- triggers (which may rewrite NEW) prior to evaluating constraints on the
-- resulting row, so this ordering is safe and sufficient. LEAST/GREATEST need
-- no reference to OLD -- they work identically for INSERT and UPDATE.
CREATE OR REPLACE FUNCTION entity_non_match_normalize_order()
RETURNS trigger AS $$
DECLARE
    a TEXT := NEW.uri_lo;
    b TEXT := NEW.uri_hi;
BEGIN
    IF a = b THEN
        RAISE EXCEPTION 'entity_non_match: uri_lo and uri_hi are identical (%): '
            'an entity cannot be vetoed against itself', a;
    END IF;
    NEW.uri_lo := LEAST(a, b);
    NEW.uri_hi := GREATEST(a, b);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_entity_non_match_normalize_order ON entity_non_match;
CREATE TRIGGER trg_entity_non_match_normalize_order
    BEFORE INSERT OR UPDATE ON entity_non_match
    FOR EACH ROW
    EXECUTE FUNCTION entity_non_match_normalize_order();

-- -----------------------------------------------------------------------------
-- 3. Lookup helpers
-- -----------------------------------------------------------------------------
-- entity_non_match_exists(a, b): "is this SPECIFIC pair vetoed?" -- the shape
-- needed by POST /entities/merge (reject survivor/loser if vetoed) and by any
-- resolver check that already has two concrete candidate URIs in hand.
CREATE OR REPLACE FUNCTION entity_non_match_exists(p_uri_a TEXT, p_uri_b TEXT)
RETURNS boolean AS $$
    SELECT EXISTS (
        SELECT 1 FROM entity_non_match
        WHERE uri_lo = LEAST(p_uri_a, p_uri_b)
          AND uri_hi = GREATEST(p_uri_a, p_uri_b)
    );
$$ LANGUAGE sql STABLE;

-- entity_non_match_partners(uri): "what is this URI vetoed against?" -- the
-- shape resolve_entity() needs at tiers 1/1.1/1.1b/1.5/2a/2b: given the
-- winning candidate URI, fetch its (usually zero, occasionally 1-2) known
-- non-match partners in ONE indexed lookup, regardless of which side of the
-- stored pair the candidate happens to be on.
CREATE OR REPLACE FUNCTION entity_non_match_partners(p_uri TEXT)
RETURNS TABLE(partner_uri TEXT, reason TEXT, asserted_by TEXT, created_at TIMESTAMPTZ) AS $$
    SELECT CASE WHEN uri_lo = p_uri THEN uri_hi ELSE uri_lo END,
           entity_non_match.reason, entity_non_match.asserted_by, entity_non_match.created_at
    FROM entity_non_match
    WHERE uri_lo = p_uri OR uri_hi = p_uri;
$$ LANGUAGE sql STABLE;

COMMENT ON FUNCTION entity_non_match_exists(TEXT, TEXT) IS
    'True iff (uri_a, uri_b) -- in either order -- is a vetoed pair.';
COMMENT ON FUNCTION entity_non_match_partners(TEXT) IS
    'All URIs the given URI is vetoed against, with each row''s reason/asserted_by/created_at. '
    'Typically 0 rows -- this is the hot-path call resolve_entity() makes at every accept point.';

-- -----------------------------------------------------------------------------
-- Migration bookkeeping
-- -----------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('117_entity_non_match', 'v1_nonmatch_veto')
ON CONFLICT (migration_id) DO NOTHING;
