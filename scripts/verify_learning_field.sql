-- Learning Field Phase 1 Verification Checklist
-- Run: psql personal_koi -f scripts/verify_learning_field.sql
--
-- Expected after a healthy projection run (scripts/project_bridge_notes.py):
--   source and review claims, concepts and questions all non-zero;
--   supports/opposes, about (Q→C) and related_to edges present;
--   0 orphaned review claims; cross-project concepts where projects share one.
--   Versioning: 0 supersedes until a projected note is edited.

\echo '=== AC4: Claims by layer ==='
SELECT metadata->>'claim_layer' AS layer, count(*) AS cnt
FROM claims WHERE metadata->>'source' = 'learning_field'
GROUP BY layer ORDER BY layer;

\echo '=== AC4: Claims-engine edges (via claim JOIN) ==='
SELECT er.predicate, count(*) AS cnt
FROM entity_relationships er
WHERE er.source = 'claims_engine'
  AND (er.subject_uri IN (SELECT entity_uri FROM claims WHERE metadata->>'source' = 'learning_field' AND entity_uri IS NOT NULL)
       OR er.object_uri IN (SELECT entity_uri FROM claims WHERE metadata->>'source' = 'learning_field' AND entity_uri IS NOT NULL))
GROUP BY er.predicate ORDER BY er.predicate;

\echo '=== AC4: Direct learning-field edges ==='
SELECT predicate, count(*) AS cnt
FROM entity_relationships WHERE source = 'learning_field'
GROUP BY predicate ORDER BY predicate;

\echo '=== AC4: Entity counts ==='
SELECT entity_type, count(*) AS cnt
FROM entity_registry WHERE metadata->>'source' = 'learning_field'
GROUP BY entity_type ORDER BY entity_type;

\echo '=== AC4: Orphaned review claims (expect 0) ==='
SELECT count(*) AS orphaned
FROM claims c
WHERE c.metadata->>'source' = 'learning_field'
  AND c.metadata->>'claim_layer' = 'review'
  AND c.entity_uri NOT IN (
    SELECT object_uri FROM entity_relationships
    WHERE predicate IN ('supports', 'opposes') AND source = 'learning_field'
  );

\echo '=== AC5: Cross-project concept dedup ==='
SELECT e.entity_text AS concept,
  count(DISTINCT c.source_document) AS distinct_sources,
  count(DISTINCT CASE WHEN c.source_document LIKE 'spore.%' THEN c.source_document END) AS spore,
  count(DISTINCT CASE WHEN c.source_document LIKE 'ic.%' THEN c.source_document END) AS ic
FROM entity_registry e
JOIN entity_relationships er ON er.object_uri = e.fuseki_uri AND er.predicate = 'about'
JOIN claims c ON c.entity_uri = er.subject_uri
  AND c.metadata->>'source' = 'learning_field' AND c.metadata->>'claim_layer' = 'source'
WHERE e.entity_type = 'Concept'
GROUP BY e.entity_text
HAVING count(DISTINCT CASE WHEN c.source_document LIKE 'spore.%' THEN 1 END) > 0
   AND count(DISTINCT CASE WHEN c.source_document LIKE 'ic.%' THEN 1 END) > 0
ORDER BY distinct_sources DESC;

\echo '=== Provenance: projection_batch + project_uri completeness ==='
SELECT metadata->>'claim_layer' AS layer,
  count(*) AS total,
  count(metadata->>'projection_batch') AS has_batch,
  count(metadata->>'project_uri') AS has_project_uri
FROM claims WHERE metadata->>'source' = 'learning_field'
GROUP BY layer ORDER BY layer;

\echo '=== Provenance: batch IDs ==='
SELECT DISTINCT metadata->>'projection_batch' AS batch
FROM claims WHERE metadata->>'source' = 'learning_field';

\echo '=== Versioning: supersedes chains (0 on fresh projection) ==='
SELECT count(*) AS claims_with_supersedes
FROM claims WHERE metadata->>'source' = 'learning_field' AND supersedes_rid IS NOT NULL;

\echo '=== Governance cluster keys (top 10 by support count) ==='
SELECT
  rc.metadata->>'governance_cluster_key' AS cluster_key,
  count(DISTINCT er.subject_uri) AS support_count,
  count(DISTINCT CASE WHEN er.predicate = 'opposes' THEN er.subject_uri END) AS oppose_count
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes') AND er.source = 'learning_field'
WHERE rc.metadata->>'claim_layer' = 'review'
GROUP BY rc.metadata->>'governance_cluster_key'
ORDER BY support_count DESC
LIMIT 10;
