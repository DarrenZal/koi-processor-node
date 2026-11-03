-- Learning Field Phase 2 Verification Checklist
-- Run: psql personal_koi -f scripts/verify_learning_field_phase2.sql
--
-- Validates convergence detection and synthesis pilot:
--   - Convergence query returns ranked clusters
--   - At least 3 field families exist
--   - At least 1 governance cluster has both supports and opposes
--   - Synthesis note SpecDoc entity exists
--   - Cross-project concepts verified

\echo '=== P2-AC3: Convergence query returns ranked clusters ==='
SELECT count(DISTINCT rc.metadata->>'governance_cluster_key') AS governance_clusters
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes')
  AND er.source = 'learning_field'
WHERE rc.metadata->>'source' = 'learning_field'
  AND rc.metadata->>'claim_layer' = 'review';

\echo '=== P2-AC4: Field families (expect >= 3 distinct concept slugs) ==='
SELECT count(DISTINCT split_part(rc.metadata->>'governance_cluster_key', ':', 2)) AS field_families
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes')
  AND er.source = 'learning_field'
WHERE rc.metadata->>'source' = 'learning_field'
  AND rc.metadata->>'claim_layer' = 'review';

\echo '=== P2-AC4: Intent-pressure family concepts appear separately ==='
SELECT split_part(rc.metadata->>'governance_cluster_key', ':', 2) AS concept_slug,
       count(DISTINCT rc.metadata->>'governance_cluster_key') AS clusters
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes')
  AND er.source = 'learning_field'
WHERE rc.metadata->>'source' = 'learning_field'
  AND rc.metadata->>'claim_layer' = 'review'
  AND split_part(rc.metadata->>'governance_cluster_key', ':', 2) IN (
    'intent-pressure', 'viable-continuation', 'persistence-ordering'
  )
GROUP BY concept_slug
ORDER BY concept_slug;

\echo '=== P2-AC5: Clusters with both supports AND opposes ==='
SELECT
  rc.metadata->>'governance_cluster_key' AS cluster_key,
  count(*) FILTER (WHERE er.predicate = 'supports') AS supports,
  count(*) FILTER (WHERE er.predicate = 'opposes') AS opposes
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes')
  AND er.source = 'learning_field'
WHERE rc.metadata->>'source' = 'learning_field'
  AND rc.metadata->>'claim_layer' = 'review'
GROUP BY rc.metadata->>'governance_cluster_key'
HAVING count(*) FILTER (WHERE er.predicate = 'supports') > 0
   AND count(*) FILTER (WHERE er.predicate = 'opposes') > 0
ORDER BY opposes DESC;

\echo '=== P2-AC8: Synthesis note SpecDoc entity exists ==='
SELECT fuseki_uri, metadata->>'research_subkind' AS subkind,
       metadata->>'doc_kind' AS kind, metadata->>'status' AS status
FROM entity_registry
WHERE entity_type = 'SpecDoc'
  AND metadata->>'research_subkind' = 'synthesis_note';

\echo '=== P2-AC8: Synthesis note depends_on edges ==='
SELECT er.subject_uri, er.predicate, er.object_uri
FROM entity_relationships er
WHERE er.subject_uri LIKE 'spec:spore.synthesis.%'
ORDER BY er.object_uri;

\echo '=== P2: Cross-project concepts (expect >= 2) ==='
SELECT e.entity_text AS concept,
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
ORDER BY concept;
