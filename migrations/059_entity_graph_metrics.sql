-- 059_entity_graph_metrics.sql
-- Precomputed graph metrics for GraphRAG v1 (B2).
-- Stores community assignments (Leiden L1/L2) and betweenness centrality
-- per entity, cached by graph_version hash for invalidation.

CREATE TABLE IF NOT EXISTS entity_graph_metrics (
    entity_id       INTEGER REFERENCES entity_registry(id) ON DELETE CASCADE,
    community_l1    INTEGER,
    community_l2    INTEGER,
    betweenness     FLOAT DEFAULT 0.0,
    graph_version   VARCHAR(64) NOT NULL,
    computed_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (entity_id)
);

CREATE INDEX IF NOT EXISTS idx_egm_community ON entity_graph_metrics(community_l1);
CREATE INDEX IF NOT EXISTS idx_egm_version   ON entity_graph_metrics(graph_version);

-- Register migration
INSERT INTO koi_migrations (migration_id, checksum)
VALUES ('bkc:059_entity_graph_metrics', 'manual')
ON CONFLICT (migration_id) DO NOTHING;
