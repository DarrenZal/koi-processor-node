-- Migration 043: Add target_node column for recipient-scoped event delivery
--
-- Without this, poll() returns ALL unexpired events to ANY polling node.
-- With target_node, events can be unicast (non-NULL) or broadcast (NULL).

ALTER TABLE koi_net_events ADD COLUMN IF NOT EXISTS target_node TEXT;

-- Simple index for filtering by target
CREATE INDEX IF NOT EXISTS idx_koi_events_target
    ON koi_net_events(target_node);

-- Compound index optimized for the poll query pattern:
--   WHERE target_node = $1 AND expires_at > NOW() ORDER BY queued_at ASC
CREATE INDEX IF NOT EXISTS idx_koi_events_target_poll
    ON koi_net_events(target_node, expires_at, queued_at)
    WHERE target_node IS NOT NULL;
