-- Migration 066: Attestation layer for Claims Engine V2 Phase 1
-- Adds identity-bound attestations, operator tracking, and policy gates.

-- 1a. New predicates
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
  ('attests_claim', 'Reviewer attests to a claim', ARRAY['Person','Organization'], ARRAY['Claim']),
  ('operates_claim', 'Operator entered a claim', ARRAY['Person','Organization'], ARRAY['Claim'])
ON CONFLICT (predicate) DO NOTHING;

-- 1b. operator_uri on claims
ALTER TABLE claims ADD COLUMN IF NOT EXISTS operator_uri TEXT
  REFERENCES entity_registry(fuseki_uri);

-- 1c. claim_attestations table
CREATE TABLE IF NOT EXISTS claim_attestations (
  id SERIAL PRIMARY KEY,
  attestation_rid TEXT UNIQUE NOT NULL,
  claim_rid TEXT NOT NULL REFERENCES claims(claim_rid),
  reviewer_uri TEXT NOT NULL REFERENCES entity_registry(fuseki_uri),
  verdict TEXT NOT NULL DEFAULT 'pending'
    CHECK (verdict IN ('pending','approved','rejected','needs_info')),
  rationale TEXT,
  evidence_uris TEXT[],
  content_hash TEXT,
  graph_iri TEXT,
  attest_tx_hash TEXT,
  attest_timestamp TIMESTAMPTZ,
  attestor_address TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(claim_rid, reviewer_uri)
);

CREATE INDEX IF NOT EXISTS idx_attestations_claim ON claim_attestations(claim_rid);
CREATE INDEX IF NOT EXISTS idx_attestations_reviewer ON claim_attestations(reviewer_uri);
CREATE INDEX IF NOT EXISTS idx_attestations_verdict ON claim_attestations(verdict);

-- 1d. Self-register in koi_migrations (grandfathering cutoff)
-- applied_at defaults to NOW(), giving us the cutoff timestamp for policy gates
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('066_attestations', 'v2_attestation_layer')
ON CONFLICT (migration_id) DO NOTHING;
