-- Add tx_hash column to claims for reconciliation of timed-out broadcasts
ALTER TABLE claims ADD COLUMN IF NOT EXISTS tx_hash TEXT;
