"""In-process ASGI tests for the project router (list_projects, briefing).

Follows the test_claims_reconcile.py fixture pattern: _SingleConnPool + rollback
transaction + httpx.ASGITransport.

Run:  pytest tests/test_project_router.py -v
Requires: KOI_TEST_DATABASE_URL naming a test database with the KOI schema (uses rollback
transactions); the tests skip when it is unset.
"""

import json
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



# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _SingleConnPool:
    """Wraps a single asyncpg.Connection to quack like asyncpg.Pool.acquire()."""

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


async def _seed_projects(conn):
    """Insert test project data. All within caller's transaction (rolled back)."""
    ts = int(time.time() * 1000)

    # 2 Project entities
    for uri, name, meta in [
        ("project:test-a", "Test Project A", {"project_id": "ta", "tier": 1, "docs_root": "docs/", "repos": ["test-a"]}),
        ("project:test-b", "Test Project B", {"project_id": "tb", "tier": 0, "docs_root": "docs/", "repos": ["test-b"]}),
    ]:
        await conn.execute(
            "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, source, first_seen_rid, metadata) "
            "VALUES ($1, $2, 'Project', $3, 'test', 'test', $4::jsonb)",
            uri, name, name.lower(), json.dumps(meta),
        )

    # 5 SpecDoc entities: 3 for project A, 2 for project B (including cross-project dep)
    specs = [
        ("spec:ta.project-vision", "ta.project-vision", {"doc_kind": "vision", "status": "active", "depends_on": [], "project_id": "ta"}),
        ("spec:ta.some-foundation", "ta.some-foundation", {"doc_kind": "foundation", "status": "active", "depends_on": ["ta.project-vision"], "project_id": "ta"}),
        ("spec:ta.some-spec", "ta.some-spec", {"doc_kind": "spec", "status": "active", "depends_on": ["ta.some-foundation"], "project_id": "ta"}),
        ("spec:tb.project-vision", "tb.project-vision", {"doc_kind": "vision", "status": "active", "depends_on": [], "project_id": "tb"}),
        ("spec:tb.alignment", "tb.alignment", {"doc_kind": "spec", "status": "active", "depends_on": ["tb.project-vision", "ta.some-spec"], "project_id": "tb"}),
    ]
    for uri, text, meta in specs:
        await conn.execute(
            "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, source, first_seen_rid, metadata) "
            "VALUES ($1, $2, 'SpecDoc', $3, 'test', 'test', $4::jsonb)",
            uri, text, text.lower(), json.dumps(meta),
        )

    # Relationships: governs + depends_on (local + cross-project)
    rels = [
        ("spec:ta.project-vision", "governs", "project:test-a"),
        ("spec:tb.project-vision", "governs", "project:test-b"),
        ("spec:ta.some-foundation", "depends_on", "spec:ta.project-vision"),
        ("spec:ta.some-spec", "depends_on", "spec:ta.some-foundation"),
        ("spec:tb.alignment", "depends_on", "spec:tb.project-vision"),
        ("spec:tb.alignment", "depends_on", "spec:ta.some-spec"),  # cross-project
    ]
    for s, p, o in rels:
        await conn.execute(
            "INSERT INTO entity_relationships (subject_uri, predicate, object_uri, source) "
            "VALUES ($1, $2, $3, 'test') ON CONFLICT DO NOTHING",
            s, p, o,
        )

    # 2 task rows (1 open, 1 done)
    await conn.execute(
        "INSERT INTO task_registry (task_key, title, status, project_uri) "
        "VALUES ($1, $2, $3, $4)",
        f"test-task-open-{ts}", "Open task", "open", "project:test-a",
    )
    await conn.execute(
        "INSERT INTO task_registry (task_key, title, status, project_uri) "
        "VALUES ($1, $2, $3, $4)",
        f"test-task-done-{ts}", "Done task", "done", "project:test-a",
    )

    # 1 ungoverned project (no governs edge)
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, source, first_seen_rid, metadata) "
        "VALUES ('project:test-ungoverned', 'Ungoverned', 'Project', 'ungoverned', 'test', 'test', '{}'::jsonb)",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def test_app():
    """Create a FastAPI app with project router wired to a rollback transaction."""
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    try:
        pool = _SingleConnPool(conn)
        await _seed_projects(conn)

        from api.routers.project_router import create_router
        router = create_router(pool)

        app = FastAPI()
        app.include_router(router, prefix="/project")

        yield app, conn
    finally:
        await tx.rollback()
        await conn.close()


@pytest.fixture
async def client(test_app):
    """Async httpx client using ASGI transport."""
    app, _ = test_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def conn(test_app):
    """Direct DB connection (same transaction as the app)."""
    _, conn = test_app
    return conn


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_list_projects_returns_governed(client):
    """Seeded governed projects appear with project_id, tier, docs_root, repos."""
    resp = await client.get("/project/projects")
    assert resp.status_code == 200
    projects = resp.json()

    uris = {p["uri"] for p in projects}
    assert "project:test-a" in uris
    assert "project:test-b" in uris

    proj_a = next(p for p in projects if p["uri"] == "project:test-a")
    assert proj_a["project_id"] == "ta"
    assert proj_a["tier"] == 1
    assert proj_a["docs_root"] == "docs/"
    assert proj_a["repos"] == ["test-a"]


@pytest.mark.anyio
async def test_list_projects_excludes_ungoverned(client):
    """Project without governs edge is absent from list."""
    resp = await client.get("/project/projects")
    assert resp.status_code == 200
    projects = resp.json()

    uris = {p["uri"] for p in projects}
    assert "project:test-ungoverned" not in uris


@pytest.mark.anyio
async def test_briefing_returns_hierarchy(client):
    """spec_hierarchy has root + correct node count + edges."""
    resp = await client.get("/project/briefing", params={"project": "project:test-a"})
    assert resp.status_code == 200
    data = resp.json()

    hierarchy = data["spec_hierarchy"]
    assert hierarchy is not None
    assert hierarchy["root"]["doc_id"] == "ta.project-vision"
    assert len(hierarchy["nodes"]) == 3  # vision + foundation + spec
    assert len(hierarchy["edges"]) >= 2  # foundation→vision, spec→foundation


@pytest.mark.anyio
async def test_briefing_with_external_deps(client):
    """include_external_deps=true → external_dependencies populated for project B."""
    resp = await client.get("/project/briefing", params={
        "project": "project:test-b",
        "include_external_deps": "true",
    })
    assert resp.status_code == 200
    data = resp.json()

    ext_deps = data["external_dependencies"]
    assert ext_deps is not None
    assert len(ext_deps) >= 1
    dep_ids = [d["doc_id"] for d in ext_deps]
    assert "ta.some-spec" in dep_ids


@pytest.mark.anyio
async def test_briefing_hierarchy_includes_cross_project_edges(client):
    """include_external_deps=true → cross-project edges and nodes in spec_hierarchy."""
    resp = await client.get("/project/briefing", params={
        "project": "project:test-b",
        "include_external_deps": "true",
    })
    assert resp.status_code == 200
    data = resp.json()

    hierarchy = data["spec_hierarchy"]
    assert hierarchy is not None

    # Cross-project edge should appear in edges
    edge_pairs = [(e["from"], e["to"]) for e in hierarchy["edges"]]
    assert ("tb.alignment", "ta.some-spec") in edge_pairs

    # Local edge still present
    assert ("tb.alignment", "tb.project-vision") in edge_pairs

    # External node present with external=True
    node_map = {n["doc_id"]: n for n in hierarchy["nodes"]}
    assert "ta.some-spec" in node_map
    assert node_map["ta.some-spec"]["external"] is True

    # Local nodes not marked external
    assert node_map["tb.project-vision"]["external"] is False
    assert node_map["tb.alignment"]["external"] is False


@pytest.mark.anyio
async def test_briefing_without_external_deps_flag(client):
    """Default → external_dependencies is null."""
    resp = await client.get("/project/briefing", params={"project": "project:test-b"})
    assert resp.status_code == 200
    data = resp.json()

    assert data["external_dependencies"] is None


@pytest.mark.anyio
async def test_briefing_project_not_found(client):
    """Unknown project → 404."""
    resp = await client.get("/project/briefing", params={"project": "project:nonexistent"})
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_briefing_active_tasks(client):
    """Only non-done tasks returned."""
    resp = await client.get("/project/briefing", params={"project": "project:test-a"})
    assert resp.status_code == 200
    data = resp.json()

    tasks = data["active_tasks"]
    assert len(tasks) == 1
    assert tasks[0]["title"] == "Open task"
    assert tasks[0]["status"] == "open"


@pytest.mark.anyio
async def test_briefing_no_hierarchy(client):
    """Project without governs → spec_hierarchy is null."""
    resp = await client.get("/project/briefing", params={"project": "project:test-ungoverned"})
    # ungoverned project exists but has no governs edge — should still be resolvable
    # but _build_spec_hierarchy returns None
    # Actually, the ungoverned project won't be found via _resolve_project because
    # it exists in entity_registry. Let's test it:
    assert resp.status_code == 200
    data = resp.json()
    assert data["spec_hierarchy"] is None
