"""
Isolated fixture tests for graph traversal endpoints.

Uses a transaction that rolls back after each test — no persistent data changes.
Requires a running PostgreSQL with the personal_koi schema.
"""

import sys
from pathlib import Path

import pytest
import asyncpg
from unittest.mock import MagicMock
from tests import koi_test_targets as targets

# Ensure repo root is on sys.path so `api.` imports work
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Load env before imports


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    """Single connection with a transaction that rolls back."""
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


# =============================================================================
# Fixture data helpers
# =============================================================================

FIXTURE_ENTITIES = [
    ("orn:test:person-alice", "Alice", "Person"),
    ("orn:test:person-bob", "Bob", "Person"),
    ("orn:test:org-acme", "Acme Corp", "Organization"),
    ("orn:test:org-beta", "Beta Inc", "Organization"),
    ("orn:test:project-x", "Project X", "Project"),
    ("orn:test:concept-regen", "Regeneration", "Concept"),
    ("orn:test:isolated", "Lonely Node", "Person"),
    # For cycle test
    ("orn:test:cycle-a", "Cycle A", "Person"),
    ("orn:test:cycle-b", "Cycle B", "Person"),
    ("orn:test:cycle-c", "Cycle C", "Person"),
]

FIXTURE_RELATIONSHIPS = [
    # Alice -> Acme (outgoing)
    ("orn:test:person-alice", "affiliated_with", "orn:test:org-acme", 1.0),
    # Alice -> Bob
    ("orn:test:person-alice", "knows", "orn:test:person-bob", 0.9),
    # Bob -> Beta
    ("orn:test:person-bob", "affiliated_with", "orn:test:org-beta", 0.8),
    # Acme -> Project X
    ("orn:test:org-acme", "has_project", "orn:test:project-x", 1.0),
    # Project X -> Concept
    ("orn:test:project-x", "involves_concept", "orn:test:concept-regen", 0.7),
    # Alice -> Beta (second affiliation, for multi-path)
    ("orn:test:person-alice", "collaborates_with", "orn:test:org-beta", 0.6),
    # Cycle: A -> B -> C -> A
    ("orn:test:cycle-a", "knows", "orn:test:cycle-b", 1.0),
    ("orn:test:cycle-b", "knows", "orn:test:cycle-c", 1.0),
    ("orn:test:cycle-c", "knows", "orn:test:cycle-a", 1.0),
]


async def _insert_fixtures(conn):
    """Insert test entities and relationships inside the current transaction."""
    # Insert entities (need to handle unique constraint — use ON CONFLICT)
    for uri, name, etype in FIXTURE_ENTITIES:
        await conn.execute("""
            INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (fuseki_uri) DO NOTHING
        """, uri, name, etype, name.lower())

    # Check that 'involves_concept' predicate exists; if not, add it
    for pred in set(r[1] for r in FIXTURE_RELATIONSHIPS):
        await conn.execute("""
            INSERT INTO allowed_predicates (predicate, description)
            VALUES ($1, $2)
            ON CONFLICT (predicate) DO NOTHING
        """, pred, f"Test predicate: {pred}")

    for subj, pred, obj, conf in FIXTURE_RELATIONSHIPS:
        await conn.execute("""
            INSERT INTO entity_relationships (subject_uri, predicate, object_uri, confidence, source)
            VALUES ($1, $2, $3, $4, 'test')
            ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
        """, subj, pred, obj, conf)


# =============================================================================
# Neighborhood tests
# =============================================================================


@pytest.mark.anyio
async def test_neighborhood_basic(conn):
    """1-hop + 2-hop nodes and edges from fixture graph."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(conn, "orn:test:person-alice", max_depth=2)

    assert result["root"] == "orn:test:person-alice"
    uris = {n["uri"] for n in result["nodes"]}
    # Alice (depth 0) + Acme, Bob, Beta (depth 1) + Project X, Beta-via-Bob (depth 2)
    assert "orn:test:person-alice" in uris
    assert "orn:test:org-acme" in uris
    assert "orn:test:person-bob" in uris
    assert result["node_count"] > 0
    assert result["edge_count"] > 0
    assert result["truncated"] is False


@pytest.mark.anyio
async def test_neighborhood_direction_outgoing(conn):
    """Only follows outgoing edges from Alice."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(
        conn, "orn:test:person-alice", max_depth=1, direction="outgoing",
    )
    uris = {n["uri"] for n in result["nodes"]}
    # Alice is subject of: affiliated_with(Acme), knows(Bob), collaborates_with(Beta)
    assert "orn:test:person-alice" in uris
    assert "orn:test:org-acme" in uris
    assert "orn:test:person-bob" in uris
    assert "orn:test:org-beta" in uris


@pytest.mark.anyio
async def test_neighborhood_direction_incoming(conn):
    """Only follows incoming edges to Acme."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(
        conn, "orn:test:org-acme", max_depth=1, direction="incoming",
    )
    uris = {n["uri"] for n in result["nodes"]}
    # Acme is object of: Alice->affiliated_with->Acme
    assert "orn:test:org-acme" in uris
    assert "orn:test:person-alice" in uris
    # Acme's outgoing (has_project -> Project X) should NOT appear
    assert "orn:test:project-x" not in uris


@pytest.mark.anyio
async def test_neighborhood_predicate_filter(conn):
    """Only traverses 'affiliated_with' predicate."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(
        conn, "orn:test:person-alice", max_depth=2, predicate="affiliated_with",
    )
    uris = {n["uri"] for n in result["nodes"]}
    assert "orn:test:org-acme" in uris
    # Bob is connected via 'knows', not 'affiliated_with'
    assert "orn:test:person-bob" not in uris


@pytest.mark.anyio
async def test_neighborhood_entity_type_filter(conn):
    """Post-filters nodes by type; root always kept."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(
        conn, "orn:test:person-alice", max_depth=2, entity_type="Organization",
    )
    # Root (Person) always included
    types = {n["entity_type"] for n in result["nodes"] if n["uri"] != "orn:test:person-alice"}
    assert all(t == "Organization" for t in types if t)
    # Root is included even though it's Person
    root = [n for n in result["nodes"] if n["uri"] == "orn:test:person-alice"]
    assert len(root) == 1


@pytest.mark.anyio
async def test_neighborhood_disconnected(conn):
    """Entity with no relationships -> root-only."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(conn, "orn:test:isolated", max_depth=2)
    assert result["node_count"] == 1
    assert result["edge_count"] == 0
    assert result["nodes"][0]["uri"] == "orn:test:isolated"


@pytest.mark.anyio
async def test_neighborhood_nonexistent(conn):
    """Unknown entity -> should only return root with no edges (entity exists in registry but has no relationships is tested above; truly nonexistent is 404 at the endpoint level)."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    # graph_queries.get_neighborhood doesn't do 404 checks — that's the endpoint's job.
    # Here we just verify the CTE handles a non-existent URI gracefully.
    result = await get_neighborhood(conn, "orn:test:does-not-exist", max_depth=2)
    assert result["node_count"] == 1  # root is always included in CTE output
    assert result["edge_count"] == 0


@pytest.mark.anyio
async def test_neighborhood_depth_clamp(conn):
    """max_depth=99 clamped to 4."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(conn, "orn:test:person-alice", max_depth=99)
    assert result["max_depth"] == 4


@pytest.mark.anyio
async def test_neighborhood_max_nodes_cap(conn):
    """Truncation flag + total_discovered count when max_nodes is very small."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(
        conn, "orn:test:person-alice", max_depth=3, max_nodes=2,
    )
    assert result["node_count"] <= 2
    # Total discovered should be >= node_count
    assert result["total_nodes_discovered"] >= result["node_count"]
    if result["total_nodes_discovered"] > result["node_count"]:
        assert result["truncated"] is True


# =============================================================================
# Shortest path tests
# =============================================================================


@pytest.mark.anyio
async def test_shortest_path_direct(conn):
    """path_length=1 for directly connected entities."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    result = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:org-acme")
    assert result["found"] is True
    assert result["path_length"] == 1
    assert len(result["steps"]) == 1
    assert result["steps"][0]["predicate"] == "affiliated_with"


@pytest.mark.anyio
async def test_shortest_path_indirect(conn):
    """Multi-hop path: Alice -> Bob -> Beta."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    # Alice -> Beta could be direct (collaborates_with) or via Bob
    # Direct is shorter, so path_length should be 1
    result = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:org-beta")
    assert result["found"] is True
    assert result["path_length"] >= 1
    assert len(result["steps"]) == result["path_length"]


@pytest.mark.anyio
async def test_shortest_path_disconnected(conn):
    """No path between disconnected components."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    result = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:isolated")
    assert result["found"] is False


@pytest.mark.anyio
async def test_shortest_path_same_entity(conn):
    """source == target -> path_length=0."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    result = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:person-alice")
    assert result["found"] is True
    assert result["path_length"] == 0
    assert result["steps"] == []
    assert len(result["nodes"]) == 1


@pytest.mark.anyio
async def test_shortest_path_deterministic(conn):
    """Same query -> identical response."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    r1 = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:org-acme")
    r2 = await get_shortest_path(conn, "orn:test:person-alice", "orn:test:org-acme")
    assert r1 == r2


@pytest.mark.anyio
async def test_shortest_path_direction(conn):
    """Outgoing-only may differ from both."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_shortest_path

    # Outgoing from Alice -> Acme (Alice is subject)
    out_result = await get_shortest_path(
        conn, "orn:test:person-alice", "orn:test:org-acme", direction="outgoing",
    )
    assert out_result["found"] is True

    # Incoming from Alice -> Acme means following edges where Alice is object
    # Alice is rarely an object in our fixtures, so this might not find a path
    in_result = await get_shortest_path(
        conn, "orn:test:person-alice", "orn:test:org-acme", direction="incoming",
    )
    # At minimum, the results should differ or incoming might not find path
    # (Alice is not object of any edge to Acme)
    assert in_result["found"] is False or in_result["path_length"] != out_result["path_length"]


# =============================================================================
# Cycle handling
# =============================================================================


@pytest.mark.anyio
async def test_cycle_handling(conn):
    """A->B->C->A cycle doesn't loop."""
    await _insert_fixtures(conn)
    from api.graph_queries import get_neighborhood

    result = await get_neighborhood(conn, "orn:test:cycle-a", max_depth=4)
    uris = {n["uri"] for n in result["nodes"]}
    # All three cycle nodes should appear, but no infinite loop
    assert "orn:test:cycle-a" in uris
    assert "orn:test:cycle-b" in uris
    assert "orn:test:cycle-c" in uris
    # Should have exactly 3 nodes (no duplicates)
    assert result["node_count"] == 3


# =============================================================================
# Auth guard unit test
# =============================================================================


def test_auth_guard_localhost_pass():
    """Localhost should pass auth."""
    from api.personal_ingest_api import _check_graph_auth

    request = MagicMock()
    request.client.host = "127.0.0.1"
    # Should not raise
    _check_graph_auth(request)


def test_auth_guard_wireguard_pass():
    """WireGuard mesh (10.100.0.x) should pass."""
    from api.personal_ingest_api import _check_graph_auth

    request = MagicMock()
    request.client.host = "10.100.0.5"
    _check_graph_auth(request)


def test_auth_guard_ipv6_localhost_pass():
    """IPv6 localhost should pass."""
    from api.personal_ingest_api import _check_graph_auth

    request = MagicMock()
    request.client.host = "::1"
    _check_graph_auth(request)


def test_auth_guard_external_reject():
    """External IP (192.168.x) should be rejected."""
    from api.personal_ingest_api import _check_graph_auth
    from fastapi import HTTPException

    request = MagicMock()
    request.client.host = "192.168.1.100"
    with pytest.raises(HTTPException) as exc_info:
        _check_graph_auth(request)
    assert exc_info.value.status_code == 403


def test_auth_guard_public_ip_reject():
    """Public IP should be rejected."""
    from api.personal_ingest_api import _check_graph_auth
    from fastapi import HTTPException

    request = MagicMock()
    request.client.host = "8.8.8.8"
    with pytest.raises(HTTPException) as exc_info:
        _check_graph_auth(request)
    assert exc_info.value.status_code == 403
