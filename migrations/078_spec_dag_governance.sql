-- Migration 078: Spec DAG Governance
-- Adds SpecDoc entity type support to the predicate system.
-- SpecDoc represents a document in a spec hierarchy (vision → architecture → spec → etc.)
-- that governs a project's direction and constrains downstream decisions.

-- Insert/extend depends_on to support SpecDoc subjects/objects.
-- Uses ON CONFLICT DO UPDATE to safely merge SpecDoc into existing type arrays
-- if depends_on already exists (e.g., from roadmap predicate migrations).
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types)
VALUES (
    'depends_on',
    'Spec or work item depends on another',
    ARRAY['SpecDoc', 'Outcome', 'Initiative', 'WorkItem', 'Milestone'],
    ARRAY['SpecDoc', 'Outcome', 'Initiative', 'WorkItem', 'Milestone']
)
ON CONFLICT (predicate) DO UPDATE
SET subject_types = (
    SELECT array_agg(DISTINCT t ORDER BY t) FROM unnest(
        array_cat(allowed_predicates.subject_types, ARRAY['SpecDoc'])
    ) AS t
),
object_types = (
    SELECT array_agg(DISTINCT t ORDER BY t) FROM unnest(
        array_cat(allowed_predicates.object_types, ARRAY['SpecDoc'])
    ) AS t
);

-- Insert governs predicate: links a spec hierarchy root (vision doc) to a Project entity.
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types)
VALUES (
    'governs',
    'Spec hierarchy root governs a project',
    ARRAY['SpecDoc'],
    ARRAY['Project']
)
ON CONFLICT (predicate) DO NOTHING;
