"""
TerminusDB outbox worker — drains terminusdb_outbox rows to TerminusDB.

Runs as a standalone process or as an asyncio background task.

Usage:
    python -m scripts.terminusdb.outbox_worker          # dry run: print the targets, start nothing
    python -m scripts.terminusdb.outbox_worker --run    # start the worker loop

Environment:
    POSTGRES_URL          PostgreSQL connection string (no default; config/personal.env is read)
    TERMINUSDB_URL        TerminusDB server URL (default: http://127.0.0.1:6363/)
    TERMINUSDB_DB         Database name (default: koi_knowledge_graph)
    TERMINUSDB_TEAM       Team (default: admin)
    TERMINUSDB_KEY        API key
    OUTBOX_POLL_INTERVAL  Seconds between poll cycles (default: 2)
    OUTBOX_BATCH_SIZE     Rows to claim per cycle (default: 10)
    OUTBOX_MAX_ATTEMPTS   Max retries before dead_letter (default: 5)
    OUTBOX_LEASE_TIMEOUT  Seconds before stale lease reclaim (default: 300)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import sys
from pathlib import Path

import asyncpg

# Add project root for imports
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

# terminusdb-client is not in requirements.txt. Without it --help and the dry run still
# work, and main() raises the ImportError.
try:
    from api.terminusdb_adapter import TerminusDBAdapter
    _IMPORT_ERROR = None
except ImportError as _exc:
    _IMPORT_ERROR = _exc

logger = logging.getLogger(__name__)

# Configuration
POLL_INTERVAL = float(os.getenv("OUTBOX_POLL_INTERVAL", "2"))
BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "10"))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "5"))
LEASE_TIMEOUT_S = int(os.getenv("OUTBOX_LEASE_TIMEOUT", "300"))

WORKER_ID = f"{platform.node()}:{os.getpid()}"


def _load_env():
    """Load config/personal.env if needed."""
    env_path = PROJECT_ROOT / "config" / "personal.env"
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    os.environ.setdefault(key.strip(), value.strip())


def _get_adapter() -> TerminusDBAdapter:
    return TerminusDBAdapter(
        url=os.getenv("TERMINUSDB_URL", "http://127.0.0.1:6363/"),
        db_name=os.getenv("TERMINUSDB_DB", "koi_knowledge_graph"),
        team=os.getenv("TERMINUSDB_TEAM", "admin"),
        key=os.getenv("TERMINUSDB_KEY", "root"),
    )


# --------------------------------------------------------------------------
# SQL
# --------------------------------------------------------------------------

CLAIM_SQL = """
UPDATE terminusdb_outbox
SET status = 'processing', claimed_at = NOW(), claimed_by = $1
WHERE id IN (
    SELECT id FROM terminusdb_outbox
    WHERE status = 'pending' AND next_attempt_at <= NOW()
    ORDER BY id LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, operation, payload, rid, source_rid, attempts;
"""

RECLAIM_STALE_SQL = """
UPDATE terminusdb_outbox
SET status = 'pending', claimed_at = NULL, claimed_by = NULL
WHERE status = 'processing'
  AND claimed_at < NOW() - ($1 || ' seconds')::INTERVAL;
"""

MARK_APPLIED_SQL = """
UPDATE terminusdb_outbox
SET status = 'applied', applied_at = NOW(), claimed_at = NULL, claimed_by = NULL
WHERE id = $1;
"""

MARK_RETRY_SQL = """
UPDATE terminusdb_outbox
SET status = 'pending',
    attempts = attempts + 1,
    error = $1,
    next_attempt_at = NOW() + (INTERVAL '2 seconds' * POWER(2, attempts))
                            * (0.7 + random() * 0.6),
    claimed_at = NULL,
    claimed_by = NULL
WHERE id = $2;
"""

MARK_DEAD_LETTER_SQL = """
UPDATE terminusdb_outbox
SET status = 'dead_letter', error = $1, claimed_at = NULL, claimed_by = NULL
WHERE id = $2;
"""


# --------------------------------------------------------------------------
# Processing
# --------------------------------------------------------------------------

def _apply_operation(adapter: TerminusDBAdapter, operation: str,
                     payload: dict, rid: str, source_rid: str) -> None:
    """Apply a single outbox operation to TerminusDB. Raises on failure."""
    if operation == "entity_upsert":
        adapter.upsert_entity(payload)
    elif operation == "assertion_upsert":
        adapter.upsert_assertion(payload)
    elif operation == "assertion_retract":
        adapter.retract_assertions_by_source(source_rid)
    else:
        raise ValueError(f"Unknown outbox operation: {operation}")


async def poll_cycle(pool: asyncpg.Pool, adapter: TerminusDBAdapter) -> int:
    """Run one poll cycle. Returns number of rows processed."""
    async with pool.acquire() as conn:
        # Reclaim stale leases
        await conn.execute(RECLAIM_STALE_SQL, str(LEASE_TIMEOUT_S))

        # Claim batch
        rows = await conn.fetch(CLAIM_SQL, WORKER_ID, BATCH_SIZE)

    if not rows:
        return 0

    processed = 0
    for row in rows:
        row_id = row["id"]
        operation = row["operation"]
        raw_payload = row["payload"]
        # asyncpg returns JSONB as dict, but handle string case defensively
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
        rid = row["rid"] or ""
        source_rid = row["source_rid"] or ""
        attempts = row["attempts"]

        try:
            _apply_operation(adapter, operation, payload, rid, source_rid)

            async with pool.acquire() as conn:
                await conn.execute(MARK_APPLIED_SQL, row_id)
            processed += 1
            logger.debug(f"Applied outbox row {row_id}: {operation} rid={rid[:40]}")

        except Exception as e:
            error_msg = str(e)[:500]
            async with pool.acquire() as conn:
                if attempts + 1 >= MAX_ATTEMPTS:
                    await conn.execute(MARK_DEAD_LETTER_SQL, error_msg, row_id)
                    logger.error(f"Dead-lettered outbox row {row_id} after {attempts + 1} attempts: {error_msg}")
                else:
                    await conn.execute(MARK_RETRY_SQL, error_msg, row_id)
                    logger.warning(f"Retry outbox row {row_id} (attempt {attempts + 1}): {error_msg}")

    return processed


async def run_worker_loop(pool: asyncpg.Pool, adapter: TerminusDBAdapter,
                          stop_event: asyncio.Event | None = None):
    """Main worker loop. Runs until stop_event is set or KeyboardInterrupt."""
    logger.info(f"Outbox worker started (id={WORKER_ID}, poll={POLL_INTERVAL}s, batch={BATCH_SIZE})")

    while True:
        if stop_event and stop_event.is_set():
            break
        try:
            count = await poll_cycle(pool, adapter)
            if count:
                logger.info(f"Processed {count} outbox rows")
        except Exception as e:
            logger.error(f"Poll cycle error: {e}")

        if stop_event:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=POLL_INTERVAL)
                break
            except asyncio.TimeoutError:
                pass
        else:
            await asyncio.sleep(POLL_INTERVAL)

    logger.info("Outbox worker stopped")


def _mask_dsn(dsn: str | None) -> str:
    """The DSN with any password replaced by ***."""
    import re
    return re.sub(r"(://[^:/@]*):[^@]*@", r"\1:***@", dsn or "")


def _targets() -> tuple[str | None, str, str]:
    return (os.getenv("POSTGRES_URL"), os.getenv("TERMINUSDB_URL", "http://127.0.0.1:6363/"),
            os.getenv("TERMINUSDB_DB", "koi_knowledge_graph"))


async def main():
    """Standalone entry point: the worker loop that --run starts."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    _load_env()
    if _IMPORT_ERROR is not None:
        raise _IMPORT_ERROR

    db_url, tdb_url, tdb_db = _targets()
    if not db_url:
        raise SystemExit("POSTGRES_URL is not set (environment or config/personal.env); "
                         "name the database whose terminusdb_outbox to drain")
    logger.info(f"Outbox worker: database {_mask_dsn(db_url)}, TerminusDB {tdb_url} database {tdb_db}")
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)

    adapter = _get_adapter()
    health = adapter.health()
    if not health.get("terminusdb_reachable"):
        logger.error(f"TerminusDB not reachable: {health.get('error', 'unknown')}")
        logger.info("Worker will start anyway — rows will accumulate and drain on recovery")

    try:
        await run_worker_loop(pool, adapter)
    finally:
        await pool.close()


def cli(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Drain terminusdb_outbox rows to TerminusDB. Without --run it prints its "
                    "targets and starts nothing.")
    parser.add_argument("--run", action="store_true",
                        help="Start the worker loop: claim, apply and mark rows of the "
                             "terminusdb_outbox in POSTGRES_URL until interrupted")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the targets and exit (the default; wins over --run)")
    args = parser.parse_args(argv)
    if args.dry_run or not args.run:
        _load_env()
        db_url, tdb_url, tdb_db = _targets()
        print("DRY RUN: nothing is started. Pass --run to start the worker loop.")
        print(f"  Database: {_mask_dsn(db_url) if db_url else '(POSTGRES_URL not set: --run would stop)'}")
        print(f"  TerminusDB: {tdb_url} database {tdb_db}")
        print(f"  Loop: every {POLL_INTERVAL}s claim up to {BATCH_SIZE} pending rows, reclaim "
              f"leases older than {LEASE_TIMEOUT_S}s, dead-letter after {MAX_ATTEMPTS} attempts")
        if _IMPORT_ERROR is not None:
            print(f"  Note: {_IMPORT_ERROR}; --run needs terminusdb-client installed")
        return 0
    asyncio.run(main())
    return 0


if __name__ == "__main__":
    sys.exit(cli())
