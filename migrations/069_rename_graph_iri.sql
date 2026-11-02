-- Migration 069: Rename graph_iri → ledger_iri in claim_attestations
-- Aligns with claims table convention (both store regen:... content IRIs)
ALTER TABLE claim_attestations RENAME COLUMN graph_iri TO ledger_iri;
