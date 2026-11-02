-- Migration 061: Index on koi_net_edges.status
-- Supports admin edge queries and poller's APPROVED filter
CREATE INDEX IF NOT EXISTS idx_koi_net_edges_status ON koi_net_edges(status);
