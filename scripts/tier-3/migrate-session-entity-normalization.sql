-- migrate-session-entity-normalization.sql
-- One-time repair of data written by older versions.
--
-- Background: an earlier session-entity write path (`ensure_entity_row` direct
-- INSERT, since replaced by the resolver's own persistence) used a different
-- normalization (`.lower().strip()`) than the canonical `normalize_entity_text`
-- (which strips `_` and `-` to spaces). Result: per session UUID there are
-- TWO entity_registry rows:
--   - Session-typed (canonical, hyphen-preserved normalized_text) — orphaned;
--     no facts reference it.
--   - Concept-typed (legacy hyphen-stripped normalized_text) — referenced by
--     every AUTHORED_WITHIN fact because `_resolve_or_create` defaults
--     to Concept and stripped hyphens during the original write.
--
-- This migration:
--   1. For each (session_label, Session_uri, Concept_uri) triple, UPDATE
--      AUTHORED_WITHIN facts.object_uri Concept_uri → Session_uri.
--   2. DELETE orphaned Concept-typed Session entities (only after step 1
--      removes all references).
--   3. Verify: the Session-typed entities now carry every AUTHORED_WITHIN
--      reference; Concept-typed Session entities = 0.
--
-- Atomic single transaction. Re-runnable: zero AUTHORED_WITHIN rows still
-- pointing at Concept-typed Session URIs after success → second run is no-op.
--
-- Run:
--   psql personal_koi -f /path/to/migrate-session-entity-normalization.sql

\echo '── B1 Pre-state ──'
SELECT
  'authored_within_concept_shape' AS label, COUNT(*) AS n
FROM knowledge_facts kf
JOIN entity_registry er ON er.fuseki_uri = kf.object_uri
WHERE er.entity_type = 'Concept'
  AND er.entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND kf.valid_to IS NULL
  AND kf.predicate = 'AUTHORED_WITHIN';
SELECT
  'authored_within_session_shape' AS label, COUNT(*) AS n
FROM knowledge_facts kf
JOIN entity_registry er ON er.fuseki_uri = kf.object_uri
WHERE er.entity_type = 'Session'
  AND er.entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND kf.valid_to IS NULL
  AND kf.predicate = 'AUTHORED_WITHIN';
SELECT
  'session_entity_registry_rows' AS label, entity_type, COUNT(*) AS n
FROM entity_registry
WHERE entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
GROUP BY entity_type;

BEGIN;

-- Step 1: build mapping table inside the transaction (Concept_uri → Session_uri
-- by matching entity_text). One row per session UUID.
CREATE TEMP TABLE _session_uri_map ON COMMIT DROP AS
SELECT
  er_concept.fuseki_uri AS concept_uri,
  er_session.fuseki_uri AS session_uri,
  er_concept.entity_text AS label
FROM entity_registry er_concept
JOIN entity_registry er_session
  ON er_session.entity_text = er_concept.entity_text
WHERE er_concept.entity_type = 'Concept'
  AND er_session.entity_type = 'Session'
  AND er_concept.entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$';

\echo '── Mapping (Concept_uri → Session_uri pairs) ──'
SELECT label, concept_uri, session_uri FROM _session_uri_map ORDER BY label;

-- Step 2: UPDATE AUTHORED_WITHIN facts to repoint object_uri.
-- Restrict to the exact predicate to avoid touching unrelated facts.
WITH updated AS (
  UPDATE knowledge_facts kf
  SET object_uri = m.session_uri
  FROM _session_uri_map m
  WHERE kf.object_uri = m.concept_uri
    AND kf.predicate = 'AUTHORED_WITHIN'
  RETURNING kf.id
)
SELECT 'facts_updated' AS label, COUNT(*) AS n FROM updated;

-- Step 3: DELETE orphaned Concept-typed Session entities. Safety: only
-- delete entities for which NO knowledge_facts reference remains
-- (across any predicate, any group, valid OR superseded).
WITH deleted AS (
  DELETE FROM entity_registry er
  USING _session_uri_map m
  WHERE er.fuseki_uri = m.concept_uri
    AND NOT EXISTS (
      SELECT 1 FROM knowledge_facts kf
      WHERE kf.subject_uri = m.concept_uri OR kf.object_uri = m.concept_uri
    )
  RETURNING er.fuseki_uri
)
SELECT 'concept_entities_deleted' AS label, COUNT(*) AS n FROM deleted;

\echo '── Post-state (still inside txn) ──'
SELECT
  'post_authored_within_concept_shape' AS label, COUNT(*) AS n
FROM knowledge_facts kf
JOIN entity_registry er ON er.fuseki_uri = kf.object_uri
WHERE er.entity_type = 'Concept'
  AND er.entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND kf.valid_to IS NULL
  AND kf.predicate = 'AUTHORED_WITHIN';
SELECT
  'post_authored_within_session_shape' AS label, COUNT(*) AS n
FROM knowledge_facts kf
JOIN entity_registry er ON er.fuseki_uri = kf.object_uri
WHERE er.entity_type = 'Session'
  AND er.entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  AND kf.valid_to IS NULL
  AND kf.predicate = 'AUTHORED_WITHIN';

COMMIT;

\echo '── Final verification (post-COMMIT) ──'
SELECT
  'final_session_entity_registry_rows' AS label, entity_type, COUNT(*) AS n
FROM entity_registry
WHERE entity_text ~ '^claude-code session [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
GROUP BY entity_type;
SELECT
  'final_total_authored_within' AS label, COUNT(*) AS n
FROM knowledge_facts
WHERE predicate = 'AUTHORED_WITHIN' AND valid_to IS NULL;
