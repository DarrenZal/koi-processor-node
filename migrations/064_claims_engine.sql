-- Claims Engine V1 — Schema migration
-- Lean core table + metadata JSONB, following Smith/Bennetts "self-stable assertion" model

-- Claim-specific predicates (extend allowed_predicates from migration 032)
INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
  ('makes_claim', 'Claimant makes an impact claim', ARRAY['Person', 'Organization'], ARRAY['Claim']),
  ('evidences_claim', 'Evidence entity supports a claim', ARRAY['Evidence'], ARRAY['Claim']),
  ('supersedes_claim', 'Newer claim version supersedes older', ARRAY['Claim'], ARRAY['Claim'])
ON CONFLICT (predicate) DO NOTHING;

-- Extend existing 'about' predicate (from 038_bkc_predicates) to include Location and Organization.
-- Safely rerunnable: adds each value only if not already present, then deduplicates.
UPDATE allowed_predicates
SET object_types = ARRAY(SELECT DISTINCT unnest(
    array_cat(object_types, ARRAY['Location', 'Organization'])
))
WHERE predicate = 'about';

-- Lean core table — matches Smith/Bennetts "self-stable assertion" model
-- statement + evidence refs + verification level + provenance
-- Everything else (quantity, unit, SDGs, dates, methodology) lives in metadata JSONB
CREATE TABLE claims (
  id SERIAL PRIMARY KEY,
  claim_rid TEXT UNIQUE NOT NULL,         -- orn:koi-net.claim:<content_hash>
  entity_uri TEXT,                        -- entity_registry.fuseki_uri (claim as graph entity)

  -- The assertion (Smith/Bennetts: "claim statement")
  claimant_uri TEXT NOT NULL,             -- entity_registry.fuseki_uri (must exist)
  statement TEXT NOT NULL,                -- plain-language impact assertion
  claim_type TEXT NOT NULL DEFAULT 'ecological',  -- ecological | social | financial | governance

  -- Verification level (V1: status enum; V2: protocol reference)
  verification TEXT NOT NULL DEFAULT 'self_reported',  -- self_reported | peer_reviewed | verified | ledger_anchored
  -- Evidence tracked via entity_relationships (evidences_claim predicate)

  -- Provenance
  source_document TEXT,                   -- document RID or path the claim was extracted from
  ai_confidence FLOAT,                    -- NULL if manually created

  -- Ledger anchoring
  content_hash TEXT,                      -- BLAKE2b-256 of canonical form
  ledger_iri TEXT,                        -- regen:113... from MsgAnchor
  ledger_timestamp TIMESTAMPTZ,

  -- Versioning
  supersedes_rid TEXT,                    -- previous version claim_rid (NULL for originals)

  -- Extensible fields — client-specific data lives here
  -- e.g. { "quantity": 22, "unit": "farms", "start_date": "2021-01-01",
  --        "end_date": "2023-12-31", "methodology": "...",
  --        "sdg_tags": ["SDG2", "SDG15"], "theme_tags": ["regenerative_agriculture"],
  --        "evidence_summary": "...", "subject_location": "Santa Barbara County" }
  metadata JSONB DEFAULT '{}',

  created_by TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Audit trail for verification transitions (insert-only)
CREATE TABLE claim_state_log (
  id SERIAL PRIMARY KEY,
  claim_rid TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  actor TEXT,
  reason TEXT,
  metadata JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_claims_claimant ON claims(claimant_uri);
CREATE INDEX idx_claims_entity ON claims(entity_uri);
CREATE INDEX idx_claims_verification ON claims(verification);
CREATE INDEX idx_claims_type ON claims(claim_type);
CREATE INDEX idx_claims_rid ON claims(claim_rid);
CREATE INDEX idx_claim_state_log_rid ON claim_state_log(claim_rid);
