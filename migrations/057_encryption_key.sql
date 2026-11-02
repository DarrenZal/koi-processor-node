-- E2EE: add encryption_key column for X25519 public keys
ALTER TABLE koi_net_nodes ADD COLUMN IF NOT EXISTS encryption_key TEXT;
