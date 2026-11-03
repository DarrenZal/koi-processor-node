import asyncio
import sys
from pathlib import Path
from uuid import uuid4

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# /knowledge/episodes is gated by make_service_token_auth(pool) (default
# env_var="KOI_CLAIMS_SERVICE_TOKEN" -- knowledge_router.py:710), added after
# this file's existing "succeeds" test was written, which is why it 401s
# without this. Same pattern as test_claims_attestations.py's auth fixture.
TEST_SERVICE_TOKEN = "test-token-a"
AUTH_HEADERS = {"Authorization": f"Bearer {TEST_SERVICE_TOKEN}"}


class _SingleConnPool:
    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *args):
            return None

    def acquire(self):
        return self._CM(self._conn)


class _ProbeConn:
    def __init__(self, regclasses):
        self.regclasses = regclasses
        self.fetchval_calls = []

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if "to_regclass" in query and args:
            return self.regclasses.get(args[0])
        return None


class _RouterConn:
    def __init__(self, *, fetch_rows=None, fetchval_values=None, forbidden=None):
        self.fetch_rows = fetch_rows or {}
        self.fetchval_values = fetchval_values or {}
        self.forbidden = forbidden or ()
        self.fetch_calls = []
        self.fetchval_calls = []
        self.execute_calls = []

    def _check_forbidden(self, query):
        normalized = " ".join(query.split())
        for needle in self.forbidden:
            assert needle not in normalized
        return normalized

    async def fetch(self, query, *args):
        normalized = self._check_forbidden(query)
        self.fetch_calls.append((normalized, args))
        for needle, rows in self.fetch_rows.items():
            if needle in normalized:
                return rows
        return []

    async def fetchval(self, query, *args):
        normalized = self._check_forbidden(query)
        self.fetchval_calls.append((normalized, args))
        for needle, value in self.fetchval_values.items():
            if needle in normalized:
                return value
        return None

    async def execute(self, query, *args):
        normalized = self._check_forbidden(query)
        self.execute_calls.append((normalized, args))
        return "OK"


class _SlowSessionProbeConn(_RouterConn):
    async def fetchval(self, query, *args):
        normalized = self._check_forbidden(query)
        self.fetchval_calls.append((normalized, args))
        if "table_name = 'session_chunks'" in normalized:
            await asyncio.sleep(0.05)
            return True
        for needle, value in self.fetchval_values.items():
            if needle in normalized:
                return value
        return None


def _build_app(
    conn,
    *,
    facts_surface_available,
    query_embed=None,
    document_embed=None,
):
    from api.routers.knowledge_router import create_router

    app = FastAPI()
    app.state.facts_surface_available = facts_surface_available
    app.include_router(
        create_router(
            _SingleConnPool(conn),
            generate_query_embedding=query_embed,
            generate_document_embedding=document_embed,
        ),
        prefix="/knowledge",
    )
    return app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_startup_sets_facts_surface_flag(monkeypatch):
    import api.personal_ingest_api as personal_ingest_api

    async def fake_create_pool(*args, **kwargs):
        return _SingleConnPool(fake_create_pool.conn)

    async def fake_noop(*args, **kwargs):
        return None

    monkeypatch.setattr(personal_ingest_api.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(personal_ingest_api, "create_embedding_provider", lambda: None)
    monkeypatch.setattr(personal_ingest_api, "ensure_schema", fake_noop)
    monkeypatch.setattr("api.graph_queries.verify_indexes", fake_noop)
    monkeypatch.setattr(personal_ingest_api, "_caps", None)

    for regclasses, expected in (
        (
            {
                "knowledge_facts": "public.knowledge_facts",
                "knowledge_episodes": "public.knowledge_episodes",
            },
            True,
        ),
        (
            {
                "knowledge_facts": "public.knowledge_facts",
                "knowledge_episodes": None,
            },
            False,
        ),
    ):
        conn = _ProbeConn(regclasses)
        fake_create_pool.conn = conn
        if hasattr(personal_ingest_api.app.state, "facts_surface_available"):
            delattr(personal_ingest_api.app.state, "facts_surface_available")

        await personal_ingest_api.startup()

        assert personal_ingest_api.app.state.facts_surface_available is expected
        assert any(
            args == ("knowledge_facts",) for _, args in conn.fetchval_calls
        )
        assert any(
            args == ("knowledge_episodes",) for _, args in conn.fetchval_calls
        )


@pytest.mark.anyio
async def test_unified_search_includes_facts_when_surface_available():
    async def query_embed(_query):
        return [0.1, 0.2, 0.3]

    conn = _RouterConn(
        fetch_rows={
            "FROM knowledge_facts f": [
                {
                    "id": "fact-1",
                    "subject_uri": "entity:test-subject",
                    "predicate": "SUPPORTS",
                    "object_uri": "entity:test-object",
                    "fact_text": "Test subject supports test object.",
                    "episode_name": "Episode One",
                    "subject_closure": None,
                    "score": 0.91,
                }
            ]
        }
    )
    app = _build_app(
        conn,
        facts_surface_available=True,
        query_embed=query_embed,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/knowledge/unified-search",
            params={"query": "supports", "include": "facts", "limit": 5},
        )

    assert response.status_code == 200
    assert response.headers["X-Facts-Surface"] == "available"
    body = response.json()
    assert body["facts"]
    assert body["facts"][0]["source"] == "fact"
    assert body["results"][0]["source"] == "fact"
    assert any("FROM knowledge_facts f" in query for query, _ in conn.fetch_calls)


@pytest.mark.anyio
async def test_unified_search_skips_facts_when_surface_unavailable():
    async def query_embed(_query):
        return [0.1, 0.2, 0.3]

    conn = _RouterConn(forbidden=("knowledge_facts", "knowledge_episodes"))
    app = _build_app(
        conn,
        facts_surface_available=False,
        query_embed=query_embed,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/knowledge/unified-search",
            params={"query": "supports", "include": "facts", "limit": 5},
        )

    assert response.status_code == 200
    assert response.headers["X-Facts-Surface"] == "unavailable"
    body = response.json()
    assert body["facts"] == []
    assert body["results"] == []


@pytest.mark.anyio
async def test_unified_search_times_out_slow_sessions_surface(monkeypatch):
    from api.routers import knowledge_router

    async def query_embed(_query):
        return [0.1, 0.2, 0.3]

    monkeypatch.setattr(
        knowledge_router,
        "UNIFIED_SEARCH_SESSIONS_TIMEOUT_SECONDS",
        0.01,
    )
    conn = _SlowSessionProbeConn(
        fetch_rows={
            "FROM knowledge_facts f": [
                {
                    "id": "fact-1",
                    "subject_uri": "entity:test-subject",
                    "predicate": "SUPPORTS",
                    "object_uri": "entity:test-object",
                    "fact_text": "Test subject supports test object.",
                    "episode_name": "Episode One",
                    "subject_closure": None,
                    "score": 0.91,
                }
            ]
        }
    )
    app = _build_app(
        conn,
        facts_surface_available=True,
        query_embed=query_embed,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/knowledge/unified-search",
            params={"query": "supports", "include": "facts,sessions", "limit": 5},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["facts"]
    assert body["results"][0]["source"] == "fact"
    assert body["surface_errors"] == {"sessions": "timeout"}
    assert all(result["source"] != "session" for result in body["results"])


@pytest.mark.anyio
async def test_create_episode_returns_503_when_surface_unavailable(monkeypatch):
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", TEST_SERVICE_TOKEN)
    conn = _RouterConn(forbidden=("knowledge_facts", "knowledge_episodes"))
    app = _build_app(conn, facts_surface_available=False)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", headers=AUTH_HEADERS
    ) as client:
        response = await client.post(
            "/knowledge/episodes",
            json={
                "name": "Unavailable episode",
                "facts": [
                    {
                        "subject": "Unavailable subject",
                        "predicate": "RELATES_TO",
                        "fact_text": "Unavailable subject relates to something.",
                    }
                ],
            },
        )

    assert response.status_code == 503
    assert response.json() == {
        "detail": {"error": "facts surface not configured on this node"}
    }


@pytest.mark.anyio
async def test_search_facts_returns_empty_when_surface_unavailable():
    conn = _RouterConn(forbidden=("knowledge_facts", "knowledge_episodes"))
    app = _build_app(conn, facts_surface_available=False)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/knowledge/facts/search",
            params={"query": "salmon"},
        )

    assert response.status_code == 200
    assert response.headers["X-Facts-Surface"] == "unavailable"
    assert response.json() == {"facts": [], "count": 0}


@pytest.mark.anyio
async def test_create_episode_succeeds_when_surface_available(monkeypatch):
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", TEST_SERVICE_TOKEN)
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    try:
        app = _build_app(conn, facts_surface_available=True)
        source_document = f"unit-test://facts-gate/{uuid4()}"

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", headers=AUTH_HEADERS
        ) as client:
            response = await client.post(
                "/knowledge/episodes",
                json={
                    "name": "Gate success episode",
                    "source_document": source_document,
                    "facts": [
                        {
                            "subject": "Gate success subject",
                            "predicate": "RELATES_TO",
                            "object_literal": "Gate success literal",
                            "fact_text": "Gate success subject relates to Gate success literal.",
                        }
                    ],
                    "create_entities": True,
                },
            )

        assert response.status_code == 201, response.text
        body = response.json()
        assert body["facts_created"] == 1
        assert body["facts_skipped"] == 0

        episode_id = await conn.fetchval(
            "SELECT id FROM knowledge_episodes WHERE source_document = $1",
            source_document,
        )
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1",
            episode_id,
        )

        assert episode_id is not None
        assert fact_count == 1
    finally:
        await tx.rollback()
        await conn.close()


@pytest.mark.anyio
async def test_tier3_conflict_reports_is_new_false_and_real_type(monkeypatch):
    """A4/A5 (silent-success sweep rank 5): Tier 3's `INSERT ... ON CONFLICT
    (fuseki_uri) DO NOTHING` never checked the command tag, so a 0-row insert
    (fuseki_uri already occupied) was still reported is_new=True with the
    caller's REQUESTED type echoed back -- never the existing row's actual
    type. That silently (a) miscounts entities_created, and (b) suppresses
    the type_mismatches diagnostic, since it only fires when the requested
    type differs from what _resolve_or_create returns, and pre-fix that
    return value was always forced to equal the request on a Tier-3 path.

    Live-log-proven (2026-08-24, personal-koi stderr.log): "Created new
    entity: Knowledge Organization Infrastructure -> orn:...concept-
    knowledge-organization-infrastructure-45521ed50fd5" logged twice for the
    identical URI -- the second call did not actually insert a row.

    Reproduced here deterministically (not relying on request-level timing):
    generate_entity_uri(name, entity_type) is a pure function of name+type,
    independent of what's stored in normalized_text. A pre-existing row at
    that exact URI, but with a normalized_text Tier 1's text search won't
    match, stands in for "the row exists but this request's lookups missed
    it" -- the same end state a genuine concurrent-request race produces,
    without relying on real concurrency timing to hit it.
    """
    from api.personal_ingest_api import generate_entity_uri, normalize_entity_text

    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", TEST_SERVICE_TOKEN)
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    try:
        app = _build_app(conn, facts_surface_available=True)
        source_document = f"unit-test://tier3-conflict/{uuid4()}"
        name = f"Tier3 Conflict Subject {uuid4().hex[:8]}"

        # Pre-existing row occupies the exact URI Tier 3 will compute for
        # (name, "Concept"), but under a normalized_text Tier 1 won't match.
        existing_uri = generate_entity_uri(name, "Concept")
        stale_normalized = normalize_entity_text(name) + "-stale-mismatch"
        await conn.execute("""
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, normalized_text, entity_type, source)
            VALUES ($1, $2, $3, 'Organization', 'test-setup')
        """, existing_uri, name, stale_normalized)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", headers=AUTH_HEADERS
        ) as client:
            response = await client.post(
                "/knowledge/episodes",
                json={
                    "name": "Tier3 conflict episode",
                    "source_document": source_document,
                    "facts": [
                        {
                            "subject": name,
                            "subject_type": "Concept",
                            "predicate": "RELATES_TO",
                            "object_literal": "conflict probe",
                            "fact_text": f"{name} relates to conflict probe.",
                        }
                    ],
                    "create_entities": True,
                },
            )

        assert response.status_code == 201, response.text
        body = response.json()

        # The fact must bind to the EXISTING row's URI (deterministic hash
        # match), not silently mint a phantom duplicate.
        assert body["facts_created"] == 1
        assert body["entities_created"] == 0, (
            "must not count this as a fresh creation -- the insert conflicted "
            "with an existing row"
        )
        mismatches = body["type_mismatches"]
        assert len(mismatches) == 1, f"expected exactly one type mismatch, got {mismatches}"
        m = mismatches[0]
        assert m["name"] == name
        assert m["role"] == "subject"
        assert m["requested_type"] == "Concept"
        assert m["resolved_type"] == "Organization"  # the EXISTING row's real type
        assert m["resolved_uri"] == existing_uri

        fact_row = await conn.fetchrow(
            "SELECT subject_uri FROM knowledge_facts WHERE episode_id = "
            "(SELECT id FROM knowledge_episodes WHERE source_document = $1)",
            source_document,
        )
        assert fact_row["subject_uri"] == existing_uri

        # No second row was minted at a different URI for the same name.
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM entity_registry WHERE entity_text = $1", name
        )
        assert count == 1
    finally:
        await tx.rollback()
        await conn.close()
