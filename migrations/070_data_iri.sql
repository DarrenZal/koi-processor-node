-- =============================================================================
-- Migration 070: Add data_iri column to claims
-- =============================================================================
-- Date: 2026-03-12
-- Purpose: Bridge identifier between content hash and ledger-confirmed IRI.
--          data_iri is populated at prepare-anchor time (pre-broadcast);
--          ledger_iri is populated after broadcast confirms on-chain.
-- =============================================================================

ALTER TABLE claims ADD COLUMN IF NOT EXISTS data_iri TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_claims_data_iri
ON claims(data_iri) WHERE data_iri IS NOT NULL;

COMMENT ON COLUMN claims.data_iri IS 'Regen Data Module IRI derived from content_hash via derive_ledger_iri(). Populated at prepare-anchor time, before broadcast.';
