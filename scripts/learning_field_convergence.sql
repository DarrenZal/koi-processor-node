-- Learning Field Convergence Detection
-- Run: psql personal_koi -f scripts/learning_field_convergence.sql
--
-- Groups review claims by governance_cluster_key, resolves supersedes chains,
-- returns stance breakdown per cluster, and groups clusters into field families.
--
-- Key: governance_cluster_key = {target_spec_doc}:{concept_slug}
-- Field family = concept_slug (a concept spanning multiple governance clusters / target docs)

-- ============================================================
-- 1. Resolve supersedes chains transitively
-- ============================================================
-- If claim A supersedes B, and B supersedes C, then A is the "current" version.
-- Source claims linked to B or C should credit A's cluster.

\echo '=== Supersedes Chain Resolution ==='
WITH RECURSIVE chain AS (
  -- Base: claims with no superseder (they are current heads)
  SELECT claim_rid, claim_rid AS head_rid, 0 AS depth
  FROM claims
  WHERE metadata->>'source' = 'learning_field'
    AND claim_rid NOT IN (
      SELECT supersedes_rid FROM claims
      WHERE metadata->>'source' = 'learning_field' AND supersedes_rid IS NOT NULL
    )

  UNION ALL

  -- Walk backwards: find claims that the current head supersedes
  SELECT c.claim_rid, ch.head_rid, ch.depth + 1
  FROM claims c
  JOIN chain ch ON c.claim_rid = (
    SELECT supersedes_rid FROM claims
    WHERE claim_rid = ch.claim_rid
      AND metadata->>'source' = 'learning_field'
      AND supersedes_rid IS NOT NULL
  )
  WHERE c.metadata->>'source' = 'learning_field'
    AND ch.depth < 10 -- safety limit
)
SELECT count(*) AS total_chain_entries,
       count(DISTINCT head_rid) AS distinct_heads,
       max(depth) AS max_depth
FROM chain;

-- ============================================================
-- 2. Per-governance-cluster stance breakdown
-- ============================================================
\echo ''
\echo '=== Governance Cluster Stance Breakdown ==='

WITH supersedes_map AS (
  -- Map each claim_rid to its current head (for versioned claims)
  -- On fresh projection with 0 supersedes, every claim maps to itself
  WITH RECURSIVE chain AS (
    SELECT claim_rid, claim_rid AS head_rid
    FROM claims
    WHERE metadata->>'source' = 'learning_field'
      AND supersedes_rid IS NULL

    UNION ALL

    SELECT c.claim_rid, ch.head_rid
    FROM claims c
    JOIN chain ch ON ch.claim_rid = c.supersedes_rid
    WHERE c.metadata->>'source' = 'learning_field'
  )
  SELECT claim_rid, head_rid FROM chain
),
cluster_members AS (
  SELECT
    rc.metadata->>'governance_cluster_key' AS cluster_key,
    split_part(rc.metadata->>'governance_cluster_key', ':', 1) AS target_doc,
    split_part(rc.metadata->>'governance_cluster_key', ':', 2) AS concept_slug,
    rc.claim_rid AS review_rid,
    rc.statement AS review_statement,
    rc.metadata->>'target_section' AS target_section,
    rc.metadata->>'change_slug' AS change_slug,
    er.predicate AS stance,
    sc.claim_rid AS source_rid,
    sc.statement AS source_statement,
    sc.source_document,
    sc.metadata->>'project_uri' AS source_project_uri
  FROM claims rc
  JOIN entity_relationships er ON er.object_uri = rc.entity_uri
    AND er.predicate IN ('supports', 'opposes')
    AND er.source = 'learning_field'
  JOIN supersedes_map sm ON sm.claim_rid = rc.claim_rid
  JOIN claims sc ON sc.entity_uri = er.subject_uri
    AND sc.metadata->>'claim_layer' = 'source'
    AND sc.metadata->>'source' = 'learning_field'
  WHERE rc.metadata->>'source' = 'learning_field'
    AND rc.metadata->>'claim_layer' = 'review'
)
SELECT
  cluster_key,
  target_doc,
  concept_slug,
  review_rid,
  review_statement,
  target_section,
  change_slug,
  count(*) FILTER (WHERE stance = 'supports') AS support_count,
  count(*) FILTER (WHERE stance = 'opposes') AS oppose_count,
  count(DISTINCT source_document) AS distinct_sources,
  count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') AS spore_sources,
  count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') AS ic_sources,
  CASE WHEN count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') > 0
        AND count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') > 0
       THEN true ELSE false END AS cross_project,
  string_agg(DISTINCT source_document, ', ' ORDER BY source_document) AS source_notes
FROM cluster_members
GROUP BY cluster_key, target_doc, concept_slug, review_rid, review_statement, target_section, change_slug
ORDER BY
  -- 1. Cross-project clusters first
  CASE WHEN count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') > 0
        AND count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') > 0
       THEN 0 ELSE 1 END,
  -- 2. More distinct sources = more convergence
  count(DISTINCT source_document) DESC,
  -- 3. Fewer unresolved opposes = more mature
  count(*) FILTER (WHERE stance = 'opposes') ASC,
  cluster_key;

-- ============================================================
-- 3. Field Family Overview (concept-level aggregation)
-- ============================================================
\echo ''
\echo '=== Field Families (concept-level) ==='

WITH cluster_stats AS (
  SELECT
    split_part(rc.metadata->>'governance_cluster_key', ':', 2) AS concept_slug,
    rc.metadata->>'governance_cluster_key' AS cluster_key,
    split_part(rc.metadata->>'governance_cluster_key', ':', 1) AS target_doc,
    er.predicate AS stance,
    sc.source_document
  FROM claims rc
  JOIN entity_relationships er ON er.object_uri = rc.entity_uri
    AND er.predicate IN ('supports', 'opposes')
    AND er.source = 'learning_field'
  JOIN claims sc ON sc.entity_uri = er.subject_uri
    AND sc.metadata->>'claim_layer' = 'source'
    AND sc.metadata->>'source' = 'learning_field'
  WHERE rc.metadata->>'source' = 'learning_field'
    AND rc.metadata->>'claim_layer' = 'review'
)
SELECT
  concept_slug,
  count(DISTINCT cluster_key) AS governance_clusters,
  count(DISTINCT target_doc) AS target_docs,
  count(*) FILTER (WHERE stance = 'supports') AS total_supports,
  count(*) FILTER (WHERE stance = 'opposes') AS total_opposes,
  count(DISTINCT source_document) AS distinct_sources,
  count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') AS spore_sources,
  count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') AS ic_sources,
  CASE WHEN count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') > 0
        AND count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') > 0
       THEN 'CROSS-PROJECT' ELSE 'single-project' END AS project_span,
  string_agg(DISTINCT target_doc, ', ' ORDER BY target_doc) AS target_doc_list
FROM cluster_stats
GROUP BY concept_slug
ORDER BY
  -- Cross-project first
  CASE WHEN count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'spore.%') > 0
        AND count(DISTINCT source_document) FILTER (WHERE source_document LIKE 'ic.%') > 0
       THEN 0 ELSE 1 END,
  -- More governance clusters = wider spread
  count(DISTINCT cluster_key) DESC,
  -- More sources = more convergence
  count(DISTINCT source_document) DESC,
  concept_slug;

-- ============================================================
-- 4. Questions connected to field-family concepts
-- ============================================================
\echo ''
\echo '=== Questions by Field Family ==='

SELECT
  split_part(rc.metadata->>'governance_cluster_key', ':', 2) AS concept_slug,
  q.entity_text AS question,
  q.fuseki_uri AS question_uri
FROM claims rc
JOIN entity_relationships er ON er.object_uri = rc.entity_uri
  AND er.predicate IN ('supports', 'opposes')
  AND er.source = 'learning_field'
JOIN claims sc ON sc.entity_uri = er.subject_uri
  AND sc.metadata->>'claim_layer' = 'source'
JOIN entity_relationships ab ON ab.subject_uri = sc.entity_uri
  AND ab.predicate = 'about' AND ab.source = 'claims_engine'
JOIN entity_registry concept ON concept.fuseki_uri = ab.object_uri AND concept.entity_type = 'Concept'
JOIN entity_relationships qab ON qab.object_uri = concept.fuseki_uri
  AND qab.predicate = 'about' AND qab.source = 'learning_field'
JOIN entity_registry q ON q.fuseki_uri = qab.subject_uri AND q.entity_type = 'Question'
WHERE rc.metadata->>'source' = 'learning_field'
GROUP BY concept_slug, q.entity_text, q.fuseki_uri
ORDER BY concept_slug, q.entity_text;

-- ============================================================
-- 5. Synthesis-readiness summary
-- ============================================================
\echo ''
\echo '=== Synthesis Readiness ==='

WITH family_stats AS (
  SELECT
    split_part(rc.metadata->>'governance_cluster_key', ':', 2) AS concept_slug,
    count(DISTINCT rc.metadata->>'governance_cluster_key') AS governance_clusters,
    count(*) FILTER (WHERE er.predicate = 'supports') AS supports,
    count(*) FILTER (WHERE er.predicate = 'opposes') AS opposes,
    count(DISTINCT sc.source_document) AS distinct_sources,
    CASE WHEN count(DISTINCT sc.source_document) FILTER (WHERE sc.source_document LIKE 'spore.%') > 0
          AND count(DISTINCT sc.source_document) FILTER (WHERE sc.source_document LIKE 'ic.%') > 0
         THEN true ELSE false END AS cross_project
  FROM claims rc
  JOIN entity_relationships er ON er.object_uri = rc.entity_uri
    AND er.predicate IN ('supports', 'opposes')
    AND er.source = 'learning_field'
  JOIN claims sc ON sc.entity_uri = er.subject_uri
    AND sc.metadata->>'claim_layer' = 'source'
    AND sc.metadata->>'source' = 'learning_field'
  WHERE rc.metadata->>'source' = 'learning_field'
    AND rc.metadata->>'claim_layer' = 'review'
  GROUP BY concept_slug
)
SELECT
  concept_slug,
  governance_clusters,
  supports,
  opposes,
  distinct_sources,
  cross_project,
  CASE
    WHEN distinct_sources >= 2 AND opposes > 0 THEN 'READY (has tension)'
    WHEN distinct_sources >= 2 THEN 'READY (convergent)'
    WHEN distinct_sources = 1 THEN 'NEEDS more sources'
    ELSE 'INSUFFICIENT'
  END AS synthesis_readiness
FROM family_stats
ORDER BY
  cross_project DESC,
  distinct_sources DESC,
  opposes DESC;
