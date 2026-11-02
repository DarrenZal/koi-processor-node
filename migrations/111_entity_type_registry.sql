-- =============================================================================
-- Migration 111: Entity type registry scaffold + resolution instrumentation
-- =============================================================================
-- Date:     2026-08-22
-- Plan:     an internal planning document
-- Database: personal_koi (PostgreSQL 14.15)
--
-- Apply:    psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/111_entity_type_registry.sql
-- Rollback: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/111_entity_type_registry_down.sql
--
-- NOTE ON -1 / ON_ERROR_STOP: this file deliberately contains NO BEGIN/COMMIT and
-- NO "CREATE INDEX CONCURRENTLY". psql -1 wraps the whole file in one transaction
-- so a failure rolls everything back; by default psql would keep going after an
-- error and leave a half-applied schema that neither this file nor its down-
-- migration describes.
--
-- =============================================================================
-- WHAT THIS DOES NOT DO — read before extending it
-- =============================================================================
-- It adds NO constraint to entity_registry.entity_type. No FK, no CHECK.
-- 546 rows currently carry a type outside the seeded set, so a constraint here
-- would either fail or force the backfill into this transaction. Enforcement is
-- migration 113, and it must not ship until 112 has emptied that population.
--
-- allowed_entity_types is seeded DESCRIPTIVELY: it records the 28 types /health
-- serves from DEFAULT_SCHEMAS today. It is a reference table that nothing
-- enforces against. The final core vocabulary is 112's decision.
--
-- allowed_facets ships EMPTY on purpose. Every candidate facet presupposes a
-- core-type fold that 112 has not made yet.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Core type reference list  (FK target for allowed_facets; NOT for entity_registry)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS allowed_entity_types (
    entity_type   TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    extractable   BOOLEAN NOT NULL DEFAULT false,
    deprecated_at TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Shape CHECK in addition to any FK (idiom copied from 032_entity_relationships.sql):
    -- catches 'schema:X', 'bkc:X', 'paper', 'repo-doc' shapes independently of what is seeded.
    CONSTRAINT chk_allowed_entity_type_shape
        CHECK (entity_type ~ '^[A-Z][A-Za-z0-9]*$')
);

-- extractable = true for exactly the 7 the deep-extraction enum can emit
-- (scripts/schemas/deep_extraction_doc_v{1,2}.schema.json). Descriptive of the
-- current extractor, not a target state.
INSERT INTO allowed_entity_types (entity_type, description, extractable) VALUES
    ('Person',            'A human individual.',                                              true),
    ('Organization',      'A named collective agent: company, nonprofit, DAO, team.',          true),
    ('Project',           'A bounded named endeavour with scope and participants.',            true),
    ('Concept',           'An abstract idea, topic, method or framework.',                     true),
    ('Location',          'A geographic place: settlement, region, bioregion, venue.',         true),
    ('Protocol',          'A specified, executable procedure or standard.',                    true),
    ('CaseStudy',         'A narrative account of a practice in context.',                     true),
    ('Meeting',           'A convened conversation with an attendee roster.',                 false),
    ('Claim',             'An assertable proposition with a truth value.',                    false),
    ('Evidence',          'An artifact or observation bearing on a Claim.',                   false),
    ('Question',          'An open inquiry.',                                                 false),
    ('Task',              'A unit of actionable work with an assignee or due date.',          false),
    ('Commitment',        'A pledge by an agent, with a redeemer and validity interval.',     false),
    ('CommitmentPool',    'An aggregation container for commitments.',                        false),
    ('CommitmentAction',  'An action taken against a commitment.',                            false),
    ('Intent',            'A published want or offer seeking a match.',                       false),
    ('Practice',          'An enacted, embodied way of doing.',                               false),
    ('Pattern',           'A recurring recognised form.',                                     false),
    ('Playbook',          'A codified sequence of practices.',                                false),
    ('Bioregion',         'A life-place defined by ecological rather than political bounds.',  false),
    ('Initiative',        'A programme-scale endeavour spanning several projects.',           false),
    ('WorkItem',          'A tracker-managed unit of work.',                                  false),
    ('Milestone',         'A dated checkpoint.',                                              false),
    ('Decision',          'A settled choice among alternatives.',                             false),
    ('Outcome',           'A target or achieved end-state.',                                  false),
    ('Risk',              'A defeasible proposition about future harm.',                      false),
    ('Metric',            'A quantified measure.',                                            false),
    ('SpecDoc',           'A normative specification or ADR.',                                false)
ON CONFLICT (entity_type) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 2. Facet vocabulary, scoped by core type. Seeded EMPTY (see header).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS allowed_facets (
    facet       TEXT NOT NULL,
    applies_to  TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (facet, applies_to),

    CONSTRAINT chk_allowed_facet_shape CHECK (facet ~ '^[a-z][a-z0-9_]*$'),

    CONSTRAINT fk_facet_applies_to FOREIGN KEY (applies_to)
        REFERENCES allowed_entity_types(entity_type)
);


-- -----------------------------------------------------------------------------
-- 3. entity_registry.entity_facets
-- -----------------------------------------------------------------------------
-- PG >= 11 stores the default in the catalog, so ADD COLUMN ... NOT NULL DEFAULT
-- does NOT rewrite the 31,665-row heap. Verified server_version = 14.15.
ALTER TABLE entity_registry
    ADD COLUMN IF NOT EXISTS entity_facets TEXT[] NOT NULL DEFAULT '{}';

-- Array shape predicate.
-- array_to_string() is STABLE, not IMMUTABLE, so the obvious one-liner
--     CHECK (array_to_string(entity_facets, ',') ~ '...')
-- is rejected with "functions in check constraint must be marked IMMUTABLE".
-- array_ndims / array_position / cardinality / unnest are all IMMUTABLE.
CREATE OR REPLACE FUNCTION koi_facets_well_formed(f TEXT[])
RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT f IS NULL
        OR (    array_ndims(f) <= 1                                  -- no nested arrays
            AND array_position(f, NULL) IS NULL                      -- no NULL elements
            AND cardinality(f) <= 8                                  -- bounded
            AND NOT EXISTS (SELECT 1 FROM unnest(f) x
                             WHERE x !~ '^[a-z][a-z0-9_]*$')         -- shape (also rejects '')
            AND cardinality(f) = (SELECT count(DISTINCT x)
                                    FROM unnest(f) x));              -- no duplicates
$$;

ALTER TABLE entity_registry DROP CONSTRAINT IF EXISTS chk_entity_facets_shape;
ALTER TABLE entity_registry
    ADD CONSTRAINT chk_entity_facets_shape CHECK (koi_facets_well_formed(entity_facets));
-- Safe to add VALIDATED: every existing row is '{}'.

CREATE INDEX IF NOT EXISTS idx_entity_registry_facets
    ON entity_registry USING GIN (entity_facets);


-- -----------------------------------------------------------------------------
-- 4. Facet referential guard.
-- -----------------------------------------------------------------------------
-- A TEXT[] column cannot carry a real FOREIGN KEY -- PostgreSQL has no
-- array-element FK. This trigger IS the FK for (entity_facets[i], entity_type),
-- and is named so nobody later assumes a declared constraint exists.
--
-- Pair-wise, not facet-wise: a facet legal on Concept is NOT thereby legal on
-- Person. A deprecated type remains a valid applies_to -- deprecation means
-- "do not use for NEW entities" (112/113's concern), and rejecting it here
-- would break rows mid-backfill.
CREATE OR REPLACE FUNCTION entity_facets_registered_guard()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE bad TEXT;
BEGIN
    IF NEW.entity_facets IS NULL OR cardinality(NEW.entity_facets) = 0 THEN
        RETURN NEW;
    END IF;
    SELECT string_agg(x, ', ') INTO bad
      FROM unnest(NEW.entity_facets) x
     WHERE NOT EXISTS (SELECT 1 FROM allowed_facets af
                        WHERE af.facet = x AND af.applies_to = NEW.entity_type);
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION
          'unregistered facet(s) [%] for entity_type % on %; register in allowed_facets first',
          bad, NEW.entity_type, NEW.fuseki_uri
          USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END$$;

DROP TRIGGER IF EXISTS tr_entity_facets_registered ON entity_registry;
CREATE TRIGGER tr_entity_facets_registered
    BEFORE INSERT OR UPDATE OF entity_facets, entity_type ON entity_registry
    FOR EACH ROW EXECUTE FUNCTION entity_facets_registered_guard();
-- "UPDATE OF" is explicit and load-bearing: an unqualified UPDATE trigger would
-- fire on every alias / metadata / merged_into write in the system.


-- -----------------------------------------------------------------------------
-- 5. resolution_tier -- CREATE-PATH instrumentation
-- -----------------------------------------------------------------------------
-- SCOPE, stated so this column is not later read as something it is not:
-- store_new_entity() runs only when a row is CREATED. Tiers 1, 1.1, 1.1b, 1.5,
-- 1.x and 2 all resolve to EXISTING rows and create nothing, so there is no row
-- to stamp. This column records WHY THIS ROW WAS MINTED. It cannot supply
-- resolver hit rates or a resolutions:creates ratio -- those need one row per
-- resolution attempt (a log table), which is deliberately not built here.
--
-- What it does answer: of the rows created, how many were minted despite a name
-- that already existed -- i.e. the population that becomes a duplicate.
--
-- NO BACKFILL. Existing rows stay NULL so "pre-instrumentation" is
-- distinguishable from "measured"; inventing a tier for 31k historical rows
-- would manufacture a rate never observed.
ALTER TABLE entity_registry
    ADD COLUMN IF NOT EXISTS resolution_tier TEXT;

ALTER TABLE entity_registry DROP CONSTRAINT IF EXISTS chk_entity_resolution_tier;
ALTER TABLE entity_registry
    ADD CONSTRAINT chk_entity_resolution_tier CHECK (
        resolution_tier IS NULL OR resolution_tier IN (
            'tier3_created',            -- created; type-agnostic lookup found zero live rows
            'tier3_created_ambiguous',  -- created while >= 1 live row already shared the name
            'tier1_1b_bound',           -- reserved: a create AVERTED by a cross-type bind
            'federation',               -- domain_event_handlers._apply_entity insert
            'import',                   -- bulk importers (mediawiki_ingest) -- no resolver tier
            'manual',                   -- admin retype / operator action
            'backfill'                  -- reserved; not written by 111
        ));
-- VALIDATED: all existing rows are NULL.

CREATE INDEX IF NOT EXISTS idx_entity_registry_resolution_tier
    ON entity_registry (resolution_tier) WHERE resolution_tier IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 6. Reporting views. No constraints -- these only report.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW entity_type_drift AS
SELECT r.entity_type,
       count(*) FILTER (WHERE r.merged_into IS NULL)     AS live_rows,
       count(*) FILTER (WHERE r.merged_into IS NOT NULL) AS tombstoned_rows,
       (t.entity_type IS NOT NULL)                       AS registered
FROM entity_registry r
LEFT JOIN allowed_entity_types t ON t.entity_type = r.entity_type
GROUP BY r.entity_type, (t.entity_type IS NOT NULL)
ORDER BY registered, live_rows DESC;

-- allowed_predicates.subject_types/object_types are DECLARED but enforced
-- nowhere (zero code readers). Surfacing the violation count is useful;
-- acting on it is a separate, measured phase.
CREATE OR REPLACE VIEW edge_type_violations AS
SELECT e.predicate,
       s.entity_type AS subject_type,
       o.entity_type AS object_type,
       ap.subject_types AS declared_subject_types,
       ap.object_types  AS declared_object_types,
       count(*) AS edges
FROM entity_relationships e
JOIN allowed_predicates ap ON ap.predicate = e.predicate
LEFT JOIN entity_registry s ON s.fuseki_uri = e.subject_uri
LEFT JOIN entity_registry o ON o.fuseki_uri = e.object_uri
WHERE (ap.subject_types IS NOT NULL AND NOT (s.entity_type = ANY(ap.subject_types)))
   OR (ap.object_types  IS NOT NULL AND NOT (o.entity_type = ANY(ap.object_types)))
GROUP BY 1,2,3,4,5
ORDER BY edges DESC;


-- -----------------------------------------------------------------------------
-- 7. Comments
-- -----------------------------------------------------------------------------
COMMENT ON TABLE allowed_entity_types IS
  'Reference list of entity types. NOTHING enforces against it as of migration 111 -- entity_registry.entity_type has no FK and no CHECK. Seeded descriptively from DEFAULT_SCHEMAS (28 types, 2026-08-22). Enforcement is migration 113, gated on 112 emptying the non-canonical population.';
COMMENT ON TABLE allowed_facets IS
  'Additive facet vocabulary scoped by core type. Ships EMPTY: every candidate facet presupposes a core-type fold that migration 112 has not decided.';
COMMENT ON COLUMN entity_registry.entity_facets IS
  'Registered additive facets. Enforced by trigger tr_entity_facets_registered, NOT by a declared FK -- PostgreSQL cannot FK array elements.';
COMMENT ON COLUMN entity_registry.resolution_tier IS
  'Why THIS ROW was minted. CREATE-PATH ONLY: store_new_entity runs only on creation, so matching tiers (1/1.1/1.1b/1.5/1.x/2) resolve to existing rows and stamp nothing. Cannot supply resolver hit rates. NULL = created before instrumentation (2026-08-22).';


-- -----------------------------------------------------------------------------
-- Migration bookkeeping (pattern from 074_intent_registry.sql; 105-110 dropped this)
-- -----------------------------------------------------------------------------
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('111_entity_type_registry', 'v1_type_scaffold')
ON CONFLICT (migration_id) DO NOTHING;
