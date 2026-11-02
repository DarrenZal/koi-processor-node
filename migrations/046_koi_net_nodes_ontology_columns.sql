-- Migration 046: Add ontology fields to koi_net_nodes
--
-- Handshake/profile upserts now persist ontology metadata for peers.
-- Older databases created before this field expansion may be missing
-- ontology_uri / ontology_version and will throw 500 on handshake updates.

ALTER TABLE koi_net_nodes
    ADD COLUMN IF NOT EXISTS ontology_uri TEXT;

ALTER TABLE koi_net_nodes
    ADD COLUMN IF NOT EXISTS ontology_version TEXT;
