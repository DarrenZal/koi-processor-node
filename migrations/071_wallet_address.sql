-- =============================================================================
-- Migration 071: Add wallet_address column to entity_registry
-- =============================================================================
-- Date: 2026-03-12
-- Purpose: Personal signing address for reviewers (distinct from admin_address
--          which is the on-chain admin). Used for identity bridge in attestation
--          anchoring — reviewer's wallet becomes attestor_address.
-- =============================================================================

ALTER TABLE entity_registry ADD COLUMN IF NOT EXISTS wallet_address TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_wallet
ON entity_registry(wallet_address) WHERE wallet_address IS NOT NULL;

COMMENT ON COLUMN entity_registry.wallet_address IS 'Personal signing address for attestation anchoring (bech32 regen1... or EVM 0x...). Distinct from admin_address (on-chain entity admin).';
