"""POST /entities/merge through the real route: refusals, optional tables, concurrent merges.

Needs KOI_TEST_DATABASE_URL; skips otherwise. Rows use unique URIs and are removed afterwards.
"""
import asyncio
from types import SimpleNamespace
import uuid

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from api.routers import admin_router
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio
TOKEN = "entity-merge-route-test-token"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def route(monkeypatch):
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", TOKEN)
    pool = await asyncpg.create_pool(targets.database_url(), min_size=2, max_size=8, command_timeout=30)
    app = FastAPI()
    app.include_router(admin_router.create_router(pool), prefix="/entities")
    state = SimpleNamespace(pool=pool, uris=[])
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://node", timeout=60,
                                 headers={"Authorization": f"Bearer {TOKEN}"}) as client:
        state.client = client
        yield state
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM entity_merge_log WHERE loser_uri = ANY($1) OR survivor_uri = ANY($1)", state.uris)
        await conn.execute("UPDATE entity_registry SET merged_into = NULL WHERE fuseki_uri = ANY($1)", state.uris)
        await conn.execute("DELETE FROM entity_registry WHERE fuseki_uri = ANY($1)", state.uris)
    await pool.close()


async def _entity(route, text):
    uri = f"orn:personal-koi.entity:concept-merge-route-{uuid.uuid4().hex[:12]}"
    route.uris.append(uri)
    async with route.pool.acquire() as conn:
        size = await conn.fetchval("SELECT atttypmod FROM pg_attribute WHERE attrelid = 'entity_registry'::regclass AND attname = 'embedding'")
        await conn.execute(
            "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, embedding) "
            "VALUES ($1, $2, 'Concept', $3, array_fill(0.001::real, ARRAY[$4::int])::vector)",
            uri, text, text.lower(), size)
    return uri


def _merge(route, survivor, loser):
    return route.client.post("/entities/merge", json={"survivor_uri": survivor, "loser_uri": loser, "dry_run": False})


async def _merged_into(route, uri):
    async with route.pool.acquire() as conn:
        return await conn.fetchval("SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1", uri)


async def test_merge_works_whichever_optional_tables_exist(route):
    a = await _entity(route, f"Merge Route A {uuid.uuid4().hex[:8]}")
    b = await _entity(route, f"Merge Route B {uuid.uuid4().hex[:8]}")
    r = await _merge(route, a, b)
    assert r.status_code == 200, r.text
    assert await _merged_into(route, b) == a


async def test_persona_refusal_is_a_409_and_changes_nothing(route):
    survivor = await _entity(route, f"Merge Route Elsewhere {uuid.uuid4().hex[:8]}")
    persona = await _entity(route, f"Merge Route Person {uuid.uuid4().hex[:8]} (via Platform)")
    r = await _merge(route, survivor, persona)
    assert r.status_code == 409, r.text
    assert await _merged_into(route, persona) is None


async def test_serial_opposing_merges_are_refused(route):
    a = await _entity(route, f"Merge Route C {uuid.uuid4().hex[:8]}")
    b = await _entity(route, f"Merge Route D {uuid.uuid4().hex[:8]}")
    assert (await _merge(route, a, b)).status_code == 200
    assert (await _merge(route, b, a)).status_code == 409


@pytest.mark.parametrize("delay", [0.0, 0.002, 0.005, 0.01, 0.02, 0.04])
async def test_opposing_concurrent_merges_never_leave_a_cycle(route, delay):
    for _ in range(5):
        a = await _entity(route, f"Merge Route E {uuid.uuid4().hex[:8]}")
        b = await _entity(route, f"Merge Route F {uuid.uuid4().hex[:8]}")
        first = asyncio.create_task(_merge(route, a, b))
        await asyncio.sleep(delay)
        second = asyncio.create_task(_merge(route, b, a))
        statuses = sorted(r.status_code for r in await asyncio.gather(first, second))
        assert statuses == [200, 409], statuses
        assert not (await _merged_into(route, a) == b and await _merged_into(route, b) == a)
