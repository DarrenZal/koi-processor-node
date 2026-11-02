-- 110_deep_extract_lease.sql
--
-- Heartbeat + holder identity for the deep-extract advisory lock
-- (gaiaaiagent/koi-processor#35).
--
-- WHAT #35 REPORTED, AND WHAT IS STILL TRUE
-- -----------------------------------------
-- The issue's headline mechanism — a GLOBAL advisory lock `deep-extract-doc:global`
-- serializing all extraction fleet-wide — NO LONGER EXISTS. Verified 2026-08-05 on
-- origin/regen-prod, origin/stable and this branch: the only lock taken is the
-- PER-DOCUMENT `deep-extract-doc:<rid>`, and two distinct rids hash to distinct keys, so
-- concurrent extraction of DIFFERENT documents already works (confirmed empirically:
-- two pg_try_advisory_lock calls on different rids both returned true). The "deeper fix"
-- #35 recommended was therefore already implemented somewhere between the issue being
-- filed (2026-07-16) and now. Fleet starvation is gone.
--
-- What REMAINS true is the other half: there is still no heartbeat and no TTL. A holder
-- that HANGS rather than crashes keeps its session-level advisory lock — `finally` never
-- runs because the process never leaves the try — so the lock persists until that process
-- dies. Scope is now one document rather than the fleet, but the operational problem is
-- real (it was hit repeatedly during the 2026-07-31 Kurtz drain, each time requiring a
-- manual pkill), and the contended caller learns nothing: it just gets `skipped_locked`,
-- which naive callers read as "no work to do".
--
-- WHAT THIS TABLE ADDS
-- --------------------
-- Identity + liveness for whoever holds a document's lock, so a contender can tell
-- "someone is actively working on this" from "a zombie is holding it", and so a reclaim
-- can be identity-checked instead of racing on a stale pg_stat_activity snapshot.
--
-- `holder_backend_start` is the part that makes reclaim safe. #35 explicitly warns that
-- terminating by PID alone can kill a HEALTHY backend that has since reused the PID.
-- Postgres PIDs are recycled; the pair (pid, backend_start) is effectively unique, so a
-- reclaim that re-verifies both inside one transaction cannot hit the wrong process.
--
-- The advisory lock remains the actual mutex — it is correct and it auto-releases when
-- the process dies. This table is liveness metadata around it, not a replacement.

CREATE TABLE IF NOT EXISTS deep_extract_lease (
    document_rid          text PRIMARY KEY,
    holder_pid            integer     NOT NULL,
    -- Guards against PID reuse; see note above.
    holder_backend_start  timestamptz NOT NULL,
    run_id                text,
    acquired_at           timestamptz NOT NULL DEFAULT now(),
    last_heartbeat        timestamptz NOT NULL DEFAULT now()
);

-- Reaper/diagnostic lookups are "which leases are stale", so index the heartbeat.
CREATE INDEX IF NOT EXISTS idx_deep_extract_lease_heartbeat
    ON deep_extract_lease (last_heartbeat);

COMMENT ON TABLE deep_extract_lease IS
    'Liveness + identity for holders of the per-document deep-extract advisory lock (#35). '
    'The advisory lock is still the mutex; this makes a hung holder diagnosable and lets a '
    'reclaim verify (pid, backend_start) atomically instead of racing on a stale snapshot.';
COMMENT ON COLUMN deep_extract_lease.holder_backend_start IS
    'pg_stat_activity.backend_start of the holder. Paired with holder_pid this survives PID '
    'reuse, so terminating a stale holder cannot kill a healthy backend that inherited the PID.';
COMMENT ON COLUMN deep_extract_lease.last_heartbeat IS
    'Updated periodically by the holder. Staleness beyond DOC_EXTRACT_LEASE_TTL means the '
    'holder is wedged (alive but not progressing) rather than merely slow.';
