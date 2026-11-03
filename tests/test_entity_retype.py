"""P3 tests: POST /entities/retype + register-entity force_type (2026-07-13).

Two concerns:

  1. The retype endpoint (api/routers/admin_router.py) — exercised end-to-end
     against a THROWAWAY Postgres schema whose search_path deliberately excludes
     ``public`` so a table the retype path forgot to create errors loudly rather
     than silently mutating real personal_koi data. Driven via httpx AsyncClient
     + ASGITransport (same event loop as the asyncpg pool — TestClient would bind
     the pool to a different loop). Service-token auth via KOI_CLAIMS_SERVICE_TOKEN.

  2. resolve_entity(skip_cross_type=...) — the force_type primitive. Direct calls
     against a minimal entity_registry; embeddings forced off.

DB-gated on KOI_TEST_DATABASE_URL.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")

from fastapi import FastAPI  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from tests import koi_test_targets as targets

_integration_skip = targets.requires_database

SVC_TOKEN = "test-token-h"
AUTH = {"Authorization": f"Bearer {SVC_TOKEN}"}


async def _create_retype_schema(pool, schema: str) -> None:
    """Create every table the retype path (_do_retype -> _do_merge) touches.

    Embeddings are TEXT (no pgvector needed). search_path is set to the schema
    only, so an unqualified reference to a table missing here raises rather than
    resolving to the real public table.
    """
    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}"')
        await conn.execute("""
            CREATE TABLE entity_registry (
                id              SERIAL PRIMARY KEY,
                fuseki_uri      TEXT UNIQUE NOT NULL,
                entity_text     TEXT NOT NULL,
                entity_type     TEXT,
                normalized_text TEXT NOT NULL,
                ledger_id       TEXT,
                metadata_iri    TEXT,
                admin_address   TEXT,
                aliases         TEXT[],
                jurisdiction    TEXT,
                class_id        TEXT,
                source          TEXT,
                first_seen_rid  TEXT,
                metadata        JSONB,
                embedding       TEXT,
                vault_rid       TEXT,
                phonetic_code   TEXT,
                node_private    BOOLEAN DEFAULT false,
                wallet_address  TEXT,
                koi_rid         TEXT,
                description     TEXT,
                embedding_3072  TEXT,
                resolution_tier TEXT,
                merged_into     TEXT,
                merged_at       TIMESTAMPTZ,
                merged_by       TEXT,
                created_at      TIMESTAMP DEFAULT now(),
                updated_at      TIMESTAMP DEFAULT now()
            )
        """)
        await conn.execute(
            "CREATE UNIQUE INDEX idx_entity_koi_rid ON entity_registry(koi_rid) "
            "WHERE koi_rid IS NOT NULL")
        await conn.execute(
            "CREATE UNIQUE INDEX idx_entity_wallet ON entity_registry(wallet_address) "
            "WHERE wallet_address IS NOT NULL")
        await conn.execute("""
            CREATE TABLE entity_relationships (
                id SERIAL PRIMARY KEY,
                subject_uri TEXT, predicate TEXT, object_uri TEXT,
                source TEXT, source_rid TEXT,
                UNIQUE (subject_uri, predicate, object_uri)
            )
        """)
        await conn.execute("""
            CREATE TABLE document_entity_links (
                id SERIAL PRIMARY KEY,
                document_rid TEXT, entity_uri TEXT, mention_count INT DEFAULT 1,
                context TEXT,
                UNIQUE (document_rid, entity_uri)
            )
        """)
        await conn.execute("""
            CREATE TABLE pending_relationships (
                id SERIAL PRIMARY KEY,
                subject_uri TEXT, object_uri TEXT, predicate TEXT,
                raw_unknown_label TEXT, unknown_side TEXT
            )
        """)
        await conn.execute(
            "CREATE TABLE knowledge_facts (id SERIAL PRIMARY KEY, "
            "subject_uri TEXT, object_uri TEXT)")
        await conn.execute(
            "CREATE TABLE claims (id SERIAL PRIMARY KEY, entity_uri TEXT, "
            "claimant_uri TEXT, operator_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE claim_attestations (id SERIAL PRIMARY KEY, "
            "reviewer_uri TEXT, evidence_uris TEXT[], metadata JSONB)")
        await conn.execute("""
            CREATE TABLE entity_rid_mappings (
                id SERIAL PRIMARY KEY,
                vault_rid TEXT UNIQUE, vault_path TEXT, canonical_uri TEXT,
                entity_type TEXT, name TEXT
            )
        """)
        await conn.execute(
            "CREATE TABLE intent_registry (id SERIAL PRIMARY KEY, entity_uri TEXT, "
            "publisher_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE task_registry (id SERIAL PRIMARY KEY, owner_uri TEXT, "
            "project_uri TEXT, collaborator_uris TEXT[], metadata JSONB)")
        await conn.execute(
            "CREATE TABLE commitments (id SERIAL PRIMARY KEY, pledger_uri TEXT, "
            "evidence_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE commitment_pools (id SERIAL PRIMARY KEY, steward_uri TEXT, "
            "bioregion_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE signals (id SERIAL PRIMARY KEY, subject_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE requirements (id SERIAL PRIMARY KEY, subject_uri TEXT, metadata JSONB)")
        await conn.execute(
            "CREATE TABLE assertion_history (id SERIAL PRIMARY KEY, subject TEXT, object_uri TEXT)")
        await conn.execute(
            "CREATE TABLE koi_extraction_records (id SERIAL PRIMARY KEY, "
            "subject_uri TEXT, object_uri TEXT)")
        await conn.execute("""
            CREATE TABLE entity_merge_log (
                id SERIAL PRIMARY KEY,
                survivor_uri TEXT NOT NULL, loser_uri TEXT NOT NULL,
                rewired JSONB NOT NULL DEFAULT '{}',
                merged_by TEXT, merged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                reverted_at TIMESTAMPTZ
            )
        """)


async def _seed_entity(conn, name: str, etype: str, *, with_mapping=True):
    """Insert an entity_registry row (+ optional rid mapping) and return its URI."""
    from api.personal_ingest_api import generate_entity_uri, normalize_entity_text
    uri = generate_entity_uri(name, etype)
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, "
        "normalized_text, source) VALUES ($1, $2, $3, $4, 'test') "
        "ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, name, etype, normalize_entity_text(name))
    if with_mapping:
        await conn.execute(
            "INSERT INTO entity_rid_mappings (vault_rid, vault_path, canonical_uri, "
            "entity_type, name) VALUES ($1, $2, $3, $4, $5)",
            f"orn:obsidian.entity:{uuid.uuid4().hex}", f"{etype}/{name}.md",
            uri, etype, name)
    return uri


@pytest_asyncio.fixture
async def retype_client(monkeypatch):
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", SVC_TOKEN)
    schema = f"retype_{uuid.uuid4().hex[:10]}"

    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        await _create_retype_schema(pool0, schema)
    finally:
        await pool0.close()

    # search_path = schema ONLY (no public) — missing tables error loudly.
    pool = await asyncpg.create_pool(
        targets.database_url(), min_size=1, max_size=4,
        server_settings={"search_path": f'"{schema}"'},
    )
    from api.routers.admin_router import create_router as create_admin_router
    app = FastAPI()
    app.include_router(create_admin_router(pool), prefix="/entities")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, pool

    try:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
    except Exception:
        pass
    await pool.close()


# ---------------------------------------------------------------------------
# Retype endpoint
# ---------------------------------------------------------------------------

@_integration_skip
@pytest.mark.asyncio
async def test_retype_project_to_person(retype_client):
    client, pool = retype_client
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, "Retype Probe Alpha", "Project")

    resp = await client.post(
        "/entities/retype",
        json={"uri": old_uri, "new_type": "Person"}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["old_type"] == "Project"
    assert body["new_type"] == "Person"
    assert body["applied"] is True
    assert body["already_typed"] is False
    assert body["merged_into_existing"] is False
    new_uri = body["new_uri"]
    assert new_uri.startswith("orn:personal-koi.entity:person-")
    assert new_uri != old_uri
    assert body["merge_log_id"] is not None

    # Old URI redirects to the new one.
    r2 = await client.get(f"/entities/{old_uri}/resolve")
    assert r2.status_code == 200, r2.text
    assert r2.json()["canonical_uri"] == new_uri
    assert r2.json()["redirected"] is True

    async with pool.acquire() as conn:
        # rid mapping rewired to new URI and re-typed
        m_type = await conn.fetchval(
            "SELECT entity_type FROM entity_rid_mappings WHERE canonical_uri = $1",
            new_uri)
        assert m_type == "Person"
        # old row tombstoned into the new one
        tomb = await conn.fetchval(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1", old_uri)
        assert tomb == new_uri
        # merge log carries the retype marker
        rewired = await conn.fetchval(
            "SELECT rewired FROM entity_merge_log WHERE survivor_uri = $1 "
            "AND loser_uri = $2", new_uri, old_uri)
        import json as _json
        rw = rewired if isinstance(rewired, dict) else _json.loads(rewired)
        assert rw["retype"] == {"from": "Project", "to": "Person"}


@_integration_skip
@pytest.mark.asyncio
async def test_retype_second_call_already_typed(retype_client):
    client, pool = retype_client
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, "Retype Probe Beta", "Project")

    r1 = await client.post(
        "/entities/retype",
        json={"uri": old_uri, "new_type": "Person"}, headers=AUTH)
    assert r1.status_code == 200, r1.text
    new_uri = r1.json()["new_uri"]

    # Second call on the now-correctly-typed survivor → no-op.
    r2 = await client.post(
        "/entities/retype",
        json={"uri": new_uri, "new_type": "Person"}, headers=AUTH)
    assert r2.status_code == 200, r2.text
    b2 = r2.json()
    assert b2["already_typed"] is True
    assert b2["applied"] is False
    assert b2["new_uri"] == new_uri


@_integration_skip
@pytest.mark.asyncio
async def test_retype_into_existing_twin(retype_client):
    client, pool = retype_client
    from api.personal_ingest_api import generate_entity_uri
    name = "Retype Twin Gamma"
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, name, "Project")
        twin_uri = await _seed_entity(conn, name, "Person")  # live Person twin

    resp = await client.post(
        "/entities/retype",
        json={"uri": old_uri, "new_type": "Person"}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["merged_into_existing"] is True
    assert body["new_uri"] == twin_uri == generate_entity_uri(name, "Person")

    async with pool.acquire() as conn:
        tomb = await conn.fetchval(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1", old_uri)
        assert tomb == twin_uri
        # The Project's rid mapping now points at the surviving Person twin.
        cu = await conn.fetchval(
            "SELECT canonical_uri FROM entity_rid_mappings WHERE name = $1 "
            "AND entity_type = 'Person'", name)
        assert cu == twin_uri


@_integration_skip
@pytest.mark.asyncio
async def test_retype_into_tombstoned_twin_resurrects(retype_client):
    """Regression: a TOMBSTONED row at the target URI used to block retype forever.

    Observed 2026-08-14 on both Signal and Notion. An earlier merge had run in the
    now-wrong direction (Concept merged INTO SoftwareApplication), so the canonical
    concept-* URI was occupied by that merge's tombstone. The retype path fell
    through to its INSERT and died on entity_registry_fuseki_uri_key, meaning the
    entity could never be moved back to its canonical type.

    When the tombstone's survivor chain leads back to the row being retyped, the
    tombstone is that row's own former self, so resurrecting it and merging back
    is precisely the reversal of the earlier merge.
    """
    client, pool = retype_client
    from api.personal_ingest_api import generate_entity_uri
    name = "Retype Tombstone Epsilon"
    async with pool.acquire() as conn:
        concept_uri = await _seed_entity(conn, name, "Concept")
        sa_uri = await _seed_entity(conn, name, "Project")
        # Simulate the historical wrong-direction merge: Concept -> Project.
        await conn.execute(
            "UPDATE entity_registry SET merged_into = $2, merged_at = NOW(), "
            "merged_by = 'historical' WHERE fuseki_uri = $1", concept_uri, sa_uri)

    resp = await client.post(
        "/entities/retype",
        json={"uri": sa_uri, "new_type": "Concept"}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["new_uri"] == concept_uri == generate_entity_uri(name, "Concept")
    assert body["merged_into_existing"] is True
    assert body["rewired"].get("resurrected_tombstone") == 1

    async with pool.acquire() as conn:
        # The resurrected row is live again and correctly typed.
        row = await conn.fetchrow(
            "SELECT merged_into, entity_type FROM entity_registry WHERE fuseki_uri = $1",
            concept_uri)
        assert row["merged_into"] is None
        assert row["entity_type"] == "Concept"
        # ...and the row we retyped is now the tombstone pointing at it.
        assert await conn.fetchval(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1",
            sa_uri) == concept_uri


@_integration_skip
@pytest.mark.asyncio
async def test_retype_into_foreign_tombstone_409(retype_client):
    """A tombstone belonging to a DIFFERENT entity must not be resurrected.

    Guessing here would silently hand one entity's canonical slot to another.
    """
    client, pool = retype_client
    name = "Retype Tombstone Zeta"
    async with pool.acquire() as conn:
        concept_uri = await _seed_entity(conn, name, "Concept")
        unrelated = await _seed_entity(conn, "Retype Unrelated Survivor", "Person")
        sa_uri = await _seed_entity(conn, name, "Project")
        # The Concept slot was merged into some OTHER entity entirely.
        await conn.execute(
            "UPDATE entity_registry SET merged_into = $2, merged_at = NOW(), "
            "merged_by = 'historical' WHERE fuseki_uri = $1", concept_uri, unrelated)

    resp = await client.post(
        "/entities/retype",
        json={"uri": sa_uri, "new_type": "Concept"}, headers=AUTH)
    assert resp.status_code == 409, resp.text
    assert "tombstoned" in resp.json()["detail"]

    async with pool.acquire() as conn:
        # Nothing moved.
        assert await conn.fetchval(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1",
            concept_uri) == unrelated
        assert await conn.fetchval(
            "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1",
            sa_uri) is None


@_integration_skip
@pytest.mark.asyncio
async def test_retype_unknown_type_422(retype_client):
    client, pool = retype_client
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, "Retype Probe Delta", "Project")
    resp = await client.post(
        "/entities/retype",
        json={"uri": old_uri, "new_type": "NotARealType"}, headers=AUTH)
    assert resp.status_code == 422, resp.text


@_integration_skip
@pytest.mark.asyncio
async def test_retype_dry_run_writes_nothing(retype_client):
    client, pool = retype_client
    from api.personal_ingest_api import generate_entity_uri
    name = "Retype Probe Epsilon"
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, name, "Project")

    resp = await client.post(
        "/entities/retype",
        json={"uri": old_uri, "new_type": "Person", "dry_run": True}, headers=AUTH)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["dry_run"] is True
    assert body["applied"] is False
    # rewired counts are reported (the dry run did the work then rolled back)
    assert isinstance(body["rewired"], dict)

    async with pool.acquire() as conn:
        # Source row untouched: still Project, not tombstoned.
        still = await conn.fetchrow(
            "SELECT entity_type, merged_into FROM entity_registry WHERE fuseki_uri = $1",
            old_uri)
        assert still["entity_type"] == "Project"
        assert still["merged_into"] is None
        # No new person row, no merge_log row.
        person_exists = await conn.fetchval(
            "SELECT 1 FROM entity_registry WHERE fuseki_uri = $1",
            generate_entity_uri(name, "Person"))
        assert person_exists is None
        log_count = await conn.fetchval("SELECT COUNT(*) FROM entity_merge_log")
        assert log_count == 0


@_integration_skip
@pytest.mark.asyncio
async def test_retype_requires_auth(retype_client):
    client, pool = retype_client
    async with pool.acquire() as conn:
        old_uri = await _seed_entity(conn, "Retype Probe Zeta", "Project")
    resp = await client.post(
        "/entities/retype", json={"uri": old_uri, "new_type": "Person"})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# register-entity force_type -> resolve_entity(skip_cross_type=...)
# ---------------------------------------------------------------------------

@_integration_skip
@pytest.mark.asyncio
async def test_force_type_suppresses_cross_type_collapse(monkeypatch):
    """Default resolve collapses a same-name different-type entity via Tier 1.1b;
    skip_cross_type (force_type) lets it create a distinct new-typed entity."""
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import ExtractedEntity, resolve_entity, normalize_entity_text

    monkeypatch.setattr(pia, "embedding_provider", None, raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)

    schema = f"forcetype_{uuid.uuid4().hex[:10]}"
    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        await _create_retype_schema(pool0, schema)
    finally:
        await pool0.close()
    pool = await asyncpg.create_pool(
        targets.database_url(), min_size=1, max_size=4,
        server_settings={"search_path": f'"{schema}"'})
    try:
        name = f"Polis {uuid.uuid4().hex[:6]}"
        async with pool.acquire() as conn:
            person_uri = await _seed_entity(conn, name, "Person", with_mapping=False)

        # Default: a Concept with the same name collapses onto the Person via 1.1b.
        async with pool.acquire() as conn:
            ent = ExtractedEntity(name=name, type="Concept", confidence=1.0)
            canon, is_new = await resolve_entity(conn, ent, context=None)
        assert is_new is False
        assert canon.uri == person_uri

        # force_type: skip_cross_type suppresses 1.1b → a distinct Concept is created.
        async with pool.acquire() as conn:
            ent = ExtractedEntity(name=name, type="Concept", confidence=1.0)
            canon2, is_new2 = await resolve_entity(
                conn, ent, context=None, skip_cross_type=True)
        assert is_new2 is True
        assert canon2.uri != person_uri
        assert canon2.uri.startswith("orn:personal-koi.entity:concept-")
    finally:
        try:
            async with pool.acquire() as conn:
                await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        except Exception:
            pass
        await pool.close()
