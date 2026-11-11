"""Database helpers for the peer-guard tests: reset and open only this suite's marked throwaway database."""

import asyncpg

TEST_DATABASE = "peer_guards_test"
MARKER = "peer_guards_disposable_test_database"
TABLES = [
    "koi_net_peer_aliases", "vault_sync_peers", "vault_sync_state", "vault_sync_applied_events", "vault_sync_metrics",
    "koi_net_edges", "koi_net_events", "koi_net_nodes",
]


class NotDisposable(RuntimeError):
    pass


async def reset(conn) -> None:
    """Empty the suite's tables, but only in this suite's own marked test database."""
    database = await conn.fetchval("SELECT current_database()")
    marked = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", "public." + MARKER)
    if database != TEST_DATABASE or not marked:
        raise NotDisposable(f"refusing to reset {database!r}: not the marked peer-guard test database")
    await conn.execute("TRUNCATE " + ", ".join(TABLES) + " RESTART IDENTITY CASCADE")


async def open_pool(pg_kwargs):
    conn = await asyncpg.connect(**pg_kwargs)
    try:
        await reset(conn)
    finally:
        await conn.close()
    return await asyncpg.create_pool(min_size=1, max_size=4, **pg_kwargs)
