-- Migration 081: Register 'about' predicate for Claim → {Person, Organization, Location, ...}
--
-- Backfill for databases (production `eliza`) where migrations 038_bkc_predicates and
-- 064_claims_engine were never applied but claims_engine is live. Without this row in
-- `allowed_predicates`, POST /claims/ with an `about_uri` fails the FK check on
-- `entity_relationships.predicate` and the server returns 500 (observed 2026-04-14
-- during dogfooding — claim creation with a Person subject silently failed from the UI).
--
-- Idempotent: ON CONFLICT DO NOTHING; re-running on a DB that already has 'about'
-- (via migration 038) is a no-op. Object types are the superset from 038 + 064 +
-- the Person/Location entries claims_engine needs, so this row is compatible with
-- both BKC and claims flows.

INSERT INTO allowed_predicates (predicate, description, subject_types, object_types)
VALUES (
  'about',
  'Claim or node is about a domain entity (dcterms:subject)',
  ARRAY['Evidence', 'Claim', 'Question', 'Protocol', 'Playbook'],
  ARRAY['Practice', 'Pattern', 'CaseStudy', 'Concept', 'Project', 'Bioregion',
        'Location', 'Organization', 'Person']
)
ON CONFLICT (predicate) DO NOTHING;
