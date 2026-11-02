-- Migration: Create koi_write_log table (C14)
-- Purpose: One row per request that carries X-KOI-Client or arrives from the Mac
--          tunnel address (KOI_RECORDED_CLIENT_IP), whatever its method, so KOI writes can
--          be attributed and located. Records time, method, path, status, the
--          X-KOI-Client value (or null), the client address uvicorn reports
--          (client_ip), the request body's sha256, affected_ids and ids_reason.
--          It never stores the request body. (plan: remote-session-hosts-3090-first, step 8/13)
-- Date: 2026-10-20

CREATE TABLE IF NOT EXISTS koi_write_log (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,
    status      INTEGER,            -- filled in a finally; null counts as a possible write (RQ34)
    client      TEXT,               -- the X-KOI-Client value, or null
    client_ip   TEXT,               -- the client address uvicorn reports (C15)
    body_sha256 TEXT NOT NULL,      -- sha256 of the request body; never the body itself
    affected_ids TEXT[],            -- from the route's id source, or null (RQ35, RQ46)
    ids_reason  TEXT                -- why affected_ids is null: error|streamed|too_large|non_json|no_id_source (Y5)
);

CREATE INDEX IF NOT EXISTS idx_koi_write_log_ts ON koi_write_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_koi_write_log_client ON koi_write_log(client);
CREATE INDEX IF NOT EXISTS idx_koi_write_log_client_ip ON koi_write_log(client_ip);

COMMENT ON TABLE koi_write_log IS 'C14: attributes and locates KOI writes from tagged clients and the Mac tunnel address; never stores request bodies.';

INSERT INTO koi_migrations (migration_id, checksum) VALUES ('135_koi_write_log', 'v1_c14_write_log') ON CONFLICT (migration_id) DO NOTHING;
