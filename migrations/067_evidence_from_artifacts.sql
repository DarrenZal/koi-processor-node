-- 067_evidence_from_artifacts.sql
-- Add derived_from predicate for evidence-from-artifacts endpoint (Steel Thread Phase B)

INSERT INTO allowed_predicates (predicate, description)
VALUES ('derived_from', 'Evidence entity derived from source artifacts')
ON CONFLICT DO NOTHING;
