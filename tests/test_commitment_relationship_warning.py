"""A4 (silent-success sweep rank 4): commitment/pool graph-edge inserts are a
guaranteed FK violation -- entity_relationships.object_uri references
entity_registry.fuseki_uri, and a commitment_rid/pool_rid is never registered
there. All 6 call sites in commitment_router.py swallowed the exception
(bare `except: pass` or a log-only warning) and still returned HTTP
200/201 with the primary row committed but the edge silently missing.

Fix: surface the failure via a `relationship_warning` field on the response
instead of raising -- the primary commitment/pool/claim row already commits
successfully outside any wrapping transaction (each conn.execute() outside an
explicit conn.transaction() auto-commits individually in this codebase), so
raising would incorrectly turn a partial success into a reported failure.

Uses the same real-DB, rollback-isolated pattern as test_claims_attestations.py
so the FK violation is genuine, not mocked.

Run:  pytest tests/test_commitment_relationship_warning.py -v
Requires: KOI_TEST_DATABASE_URL naming a test database with the KOI schema (uses rollback
transactions); the tests skip when it is unset.
"""

import sys
import time
from pathlib import Path

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))



class _SingleConnPool:
    """Wraps a real asyncpg.Connection to quack like asyncpg.Pool.acquire()."""

    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *a):
            pass

    def acquire(self):
        return self._CM(self._conn)


async def _setup_test_pledger(conn, uri):
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, 'Test Pledger', 'Person', 'test pledger')
        ON CONFLICT (fuseki_uri) DO NOTHING
    """, uri)
    return uri


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def test_app():
    """FastAPI app with BOTH commitment routers (/commitments, /pools) wired
    to one rollback transaction -- commitment_router.py exposes them as two
    separate factories (create_router, create_pool_router), each mounted
    separately in the real app too (personal_ingest_api.py:1835-1837)."""
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()
    try:
        pool = _SingleConnPool(conn)
        from api.routers.commitment_router import create_router, create_pool_router
        app = FastAPI()
        app.include_router(create_router(pool))
        app.include_router(create_pool_router(pool))
        yield app, conn
    finally:
        await tx.rollback()
        await conn.close()


@pytest.fixture
async def client(test_app):
    app, _ = test_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def conn(test_app):
    _, conn = test_app
    return conn


@pytest.mark.anyio
async def test_create_commitment_surfaces_relationship_warning_not_silent_200(client, conn):
    """The commitment row must still be created (201), but the response must
    make the guaranteed-failing pledges_commitment edge visible, not silently
    absorb it."""
    ts = int(time.time() * 1000)
    pledger_uri = f"urn:test:commitment-pledger-{ts}"
    await _setup_test_pledger(conn, pledger_uri)

    resp = await client.post("/commitments/create", json={
        "pledger_uri": pledger_uri,
        "title": f"Test pledge {ts}",
        "offer_type": "labor",
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["commitment_rid"], "commitment must still be created"
    assert body["relationship_warning"], (
        "relationship_warning must be set -- entity_relationships.object_uri "
        "has a FK to entity_registry, and a commitment_rid is never "
        "registered there, so this insert cannot succeed"
    )
    assert "pledges_commitment" in body["relationship_warning"]


@pytest.mark.anyio
async def test_create_pool_surfaces_relationship_warning_when_steward_set(client, conn):
    ts = int(time.time() * 1000)
    steward_uri = f"urn:test:pool-steward-{ts}"
    await _setup_test_pledger(conn, steward_uri)

    resp = await client.post("/pools/create", json={
        "name": f"Test Pool {ts}",
        "steward_uri": steward_uri,
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["pool_rid"], "pool must still be created"
    assert body["relationship_warning"]
    assert "governs_pool" in body["relationship_warning"]


@pytest.mark.anyio
async def test_create_pool_without_steward_has_no_relationship_warning(client, conn):
    """No steward_uri -> the governs_pool insert is never attempted, so there
    is nothing to warn about (proves the warning isn't spuriously set)."""
    ts = int(time.time() * 1000)
    resp = await client.post("/pools/create", json={"name": f"Test Pool No Steward {ts}"})
    assert resp.status_code == 201, resp.text
    assert resp.json()["relationship_warning"] is None
