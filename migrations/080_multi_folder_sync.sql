-- Migration 080: Multi-folder vault sync
--
-- Changes vault_sync_peers PK from (peer_node_rid) to (peer_node_rid, shared_folder)
-- so the same peer can sync multiple folders (e.g., Shared + Meetings).
--
-- The scan loop in vault_sync.py already groups peers by shared_folder and scans
-- each folder independently — this migration just removes the DB constraint that
-- prevented multiple rows per peer.
--
-- NON-REVERSIBLE once multiple folder rows exist per peer.
-- Rollback: DELETE extra folder rows first, then re-add single-column PK.

-- 1. Drop old PK (peer_node_rid only)
ALTER TABLE vault_sync_peers DROP CONSTRAINT vault_sync_peers_pkey;

-- 2. Drop obsolete id column (relic from singleton era, always 1)
ALTER TABLE vault_sync_peers DROP COLUMN IF EXISTS id;

-- 3. Add composite PK (peer_node_rid, shared_folder)
ALTER TABLE vault_sync_peers ADD CONSTRAINT vault_sync_peers_pkey
    PRIMARY KEY (peer_node_rid, shared_folder);
