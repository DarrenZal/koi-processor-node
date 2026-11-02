-- Migration 060: Multi-peer vault sync
--
-- Removes singleton constraint from vault_sync_peers (allows multiple peers),
-- adds monotonic local_edit_seq to vault_sync_state,
-- widens event dedup index for forwarding fanout,
-- adds fast event_id lookup index for loop detection.
--
-- NON-REVERSIBLE once multiple peer rows exist.
-- Rollback: delete extra peers first, then re-add constraints.

-- 1. Remove singleton constraint from vault_sync_peers
ALTER TABLE vault_sync_peers DROP CONSTRAINT IF EXISTS vault_sync_peers_pkey;
ALTER TABLE vault_sync_peers DROP CONSTRAINT IF EXISTS vault_sync_peers_id_check;
ALTER TABLE vault_sync_peers ADD CONSTRAINT vault_sync_peers_pkey
    PRIMARY KEY (peer_node_rid);

-- 2. Monotonic local edit counter (fixes seq reset-to-1 bug)
ALTER TABLE vault_sync_state ADD COLUMN IF NOT EXISTS local_edit_seq INTEGER DEFAULT 0;
UPDATE vault_sync_state SET local_edit_seq = origin_seq WHERE local_edit_seq = 0;
ALTER TABLE vault_sync_state ALTER COLUMN local_edit_seq SET NOT NULL;

-- 3. Widen event dedup index to support forwarding fanout
--    Old: UNIQUE(source_node, event_id) — blocks same event_id to multiple targets
--    New: UNIQUE(source_node, event_id, target_node) — allows per-target unicast
DROP INDEX IF EXISTS idx_koi_net_events_source_event;
CREATE UNIQUE INDEX idx_koi_net_events_source_event_target
    ON koi_net_events (source_node, event_id, COALESCE(target_node, ''))
    WHERE event_id IS NOT NULL;

-- 4. Index for fast event_id-only lookup (loop detection in apply_event)
CREATE INDEX IF NOT EXISTS idx_vault_sync_applied_event_id
    ON vault_sync_applied_events (event_id);
