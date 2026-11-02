"""Unit tests for GET /knowledge/discourse-search (Piece A / G1, G3).

Everything here is deterministic + injectable: NO live DB. The endpoint executor
``_discourse_search`` takes a connection, so we drive it with fake connections
that return canned ``session_discourse_moves``-shaped rows (mirrors the mock-row
pattern in test_knowledge_router_facts_gate.py / test_chat_retrieval.py).

Covers:
  AC1  read-only (executor against a WriteGuardConn — any non-SELECT raises)
  AC2  filters: move_type, document_rid (=source_rid), status, limit, combined
  AC3  source-link enrichment (document → url+title; non-document → null)
  AC4  resolves-edge one-hop parent (set → {id,move_type,title}; root/orphan → null)
  AC5  graceful: empty query → created_at DESC; unknown move_type → empty;
       doc with no moves → empty; document_rid != source_rid → HTTP 400;
       null detail/title don't crash
  move_type serialization: comma form and repeated form both parse to a deduped list
"""
import asyncio

import pytest
from fastapi import HTTPException

from api.read_scope import personal_scope
from api.routers.knowledge_router import (
    _discourse_search,
    _normalize_move_types,
    create_router,
)


def _run(coro):
    return asyncio.run(coro)


# ── Fakes ────────────────────────────────────────────────────────────────────

class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class _DiscourseConn:
    """Routes the executor's batch queries by table name and records the SQL +
    args it was handed, so tests can assert SQL-shape, filter binding, batch
    (no-N+1) enrichment, and the resolves-hop."""

    def __init__(self, *, moves=None, titles=None, urls=None, parents=None):
        self._moves = moves or []
        self._titles = titles or []   # rows: {document_rid, title}
        self._urls = urls or []       # rows: {document_rid, source_url}
        self._parents = parents or [] # rows: {id, move_type, title}
        self.fetch_calls = []  # (sql, args)

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        if "to_regclass('session_exclusions')" in sql:
            # session searches check the exclusion list exists (migration 136)
            return [{"present": True}]
        if "FROM session_discourse_moves" in sql and "WHERE id = ANY" in sql:
            # the resolves-parent batch loader. Match "WHERE id = ANY" (not the
            # bare "id = ANY"): the main query now also contains
            # "source_rid = ANY(...)" from the source-visibility clause,
            # whose "rid = ANY" would otherwise collide with "id = ANY".
            wanted = set(args[0]) if args else set()
            return [p for p in self._parents if p["id"] in wanted]
        if "FROM session_discourse_moves" in sql:
            return list(self._moves)
        if "document_ingestion_log" in sql and "title" in sql:
            return list(self._titles)
        if "document_ingestion_log" in sql:
            return list(self._urls)
        return []


class _WriteGuardConn(_DiscourseConn):
    """Allows only SELECT (conn.fetch). Any write op raises — mechanically proves
    AC1 read-only (mirrors the governor's WriteGuardConn)."""

    async def fetch(self, sql, *args):
        assert sql.lstrip().upper().startswith(("WITH", "SELECT")), sql
        return await super().fetch(sql, *args)

    async def execute(self, *a, **k):
        raise AssertionError("discourse_search attempted a DB write (execute)")

    async def executemany(self, *a, **k):
        raise AssertionError("discourse_search attempted a DB write (executemany)")

    async def fetchval(self, *a, **k):
        raise AssertionError("discourse_search attempted fetchval")

    async def fetchrow(self, *a, **k):
        raise AssertionError("discourse_search attempted fetchrow")


def _move(**over):
    base = {
        "id": "m1",
        "move_type": "claim",
        "title": "A claim title",
        "detail": "claim detail text",
        "status": "asserted",
        "resolves_move_id": None,
        "source_rid": "document:abc",
    }
    base.update(over)
    return base


def _route(router, name):
    for r in router.routes:
        if getattr(r, "name", None) == name:
            return r.endpoint
    raise KeyError(name)


# ── move_type normalization (serialization contract) ─────────────────────────

def test_normalize_move_types_comma_and_repeated_both_parse():
    # repeated form: ['claim', 'evidence']
    assert _normalize_move_types(["claim", "evidence"]) == ["claim", "evidence"]
    # comma form: ['claim,evidence'] -> ['claim', 'evidence']
    assert _normalize_move_types(["claim,evidence"]) == ["claim", "evidence"]
    # mixed + dupes + whitespace -> deduped, order-preserving
    assert _normalize_move_types(["claim", " evidence ,claim", "thesis"]) == [
        "claim", "evidence", "thesis",
    ]


def test_normalize_move_types_empty():
    assert _normalize_move_types(None) == []
    assert _normalize_move_types([]) == []
    assert _normalize_move_types(["", "  ", ",", " , "]) == []


# ── AC1: read-only ───────────────────────────────────────────────────────────

def test_executor_is_read_only_via_guard_conn():
    conn = _WriteGuardConn(
        moves=[_move(resolves_move_id="p1")],
        titles=[{"document_rid": "document:abc", "title": "Paper A"}],
        urls=[{"document_rid": "document:abc", "source_url": "https://arxiv.org/abs/1"}],
        parents=[{"id": "p1", "move_type": "thesis", "title": "Parent thesis"}],
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="sheaf", limit=5))
    # If any non-SELECT had fired, the guard would have raised.
    assert out["count"] == 1
    assert out["query_mode"] == "lexical"


# ── AC2: filters bind into the WHERE clause + limit ──────────────────────────

def test_move_type_filter_binds_array():
    conn = _DiscourseConn(moves=[_move(move_type="counterpoint")])
    out = _run(_discourse_search(conn, scope=personal_scope(), move_types=["counterpoint"], limit=5))
    main_sql, main_args = conn.fetch_calls[0]
    assert "move_type = ANY" in main_sql
    assert ["counterpoint"] in [a for a in main_args]
    assert out["moves"][0]["move_type"] == "counterpoint"


def test_document_rid_filter_binds_source_rid():
    conn = _DiscourseConn(moves=[_move()])
    _run(_discourse_search(conn, scope=personal_scope(), source_rid="document:abc", limit=5))
    main_sql, main_args = conn.fetch_calls[0]
    assert "source_rid = $" in main_sql
    assert "document:abc" in main_args


def test_status_filter_and_limit_bind():
    conn = _DiscourseConn(moves=[_move(status="contested")])
    _run(_discourse_search(conn, scope=personal_scope(), status="contested", limit=7))
    main_sql, main_args = conn.fetch_calls[0]
    assert "status = $" in main_sql
    # limit is the last bound param.
    assert main_args[-1] == 7


def test_combined_filters_all_anded():
    conn = _DiscourseConn(moves=[_move(move_type="evidence", status="supported")])
    _run(_discourse_search(
        conn, scope=personal_scope(), query="oversmoothing", move_types=["evidence"],
        source_rid="document:abc", status="supported", limit=3))
    main_sql, _ = conn.fetch_calls[0]
    assert "plainto_tsquery" in main_sql           # query predicate
    assert "move_type = ANY" in main_sql           # move_type
    assert "source_rid = $" in main_sql            # document_rid alias
    assert "status = $" in main_sql                # status
    assert main_sql.count(" AND ") >= 4


# ── AC3: source-link enrichment ──────────────────────────────────────────────

def test_source_link_document_move_gets_url_and_title():
    conn = _DiscourseConn(
        moves=[_move(source_rid="document:abc")],
        titles=[{"document_rid": "document:abc", "title": "Sheaf Rep Learning"}],
        urls=[{"document_rid": "document:abc", "source_url": "https://arxiv.org/abs/2005.12798"}],
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="sheaf", limit=5))
    src = out["moves"][0]["source"]
    assert src["rid"] == "document:abc"
    assert src["title"] == "Sheaf Rep Learning"
    assert src["source_url"] == "https://arxiv.org/abs/2005.12798"


def test_source_link_non_document_move_is_null_never_fabricated():
    # A session-sourced move (source_rid not a document: rid) → source_url null,
    # title falls back to the rid (R3).
    conn = _DiscourseConn(moves=[_move(source_rid="claude-session:9")])
    out = _run(_discourse_search(conn, scope=personal_scope(), source_type="session", limit=5))
    src = out["moves"][0]["source"]
    assert src["source_url"] is None
    assert src["title"] == "claude-session:9"


def test_source_title_falls_back_to_rid_when_log_missing():
    # document rid with NO ingestion-log row → title fallback to rid, url null.
    conn = _DiscourseConn(moves=[_move(source_rid="document:missing")])
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    src = out["moves"][0]["source"]
    assert src["title"] == "document:missing"
    assert src["source_url"] is None


# ── AC4: resolves-edge one-hop ───────────────────────────────────────────────

def test_resolves_hop_returns_parent():
    conn = _DiscourseConn(
        moves=[_move(id="m1", resolves_move_id="p1")],
        parents=[{"id": "p1", "move_type": "thesis", "title": "Root thesis"}],
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    res = out["moves"][0]["resolves"]
    assert res == {"id": "p1", "move_type": "thesis", "title": "Root thesis"}


def test_root_move_resolves_null():
    conn = _DiscourseConn(moves=[_move(resolves_move_id=None)])
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    assert out["moves"][0]["resolves"] is None
    # No parent batch query issued when there are no resolves_move_ids.
    assert all("::uuid[]" not in sql for sql, _ in conn.fetch_calls)


def test_orphan_parent_resolves_null():
    # resolves_move_id set but parent not found (orphan / session-sourced) → null.
    conn = _DiscourseConn(
        moves=[_move(resolves_move_id="ghost")],
        parents=[],  # batch returns nothing
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    assert out["moves"][0]["resolves"] is None


def test_resolves_hop_is_single_batch_query_no_n_plus_1():
    conn = _DiscourseConn(
        moves=[_move(id="m1", resolves_move_id="p1"),
               _move(id="m2", resolves_move_id="p2")],
        parents=[{"id": "p1", "move_type": "thesis", "title": "T1"},
                 {"id": "p2", "move_type": "claim", "title": "C2"}],
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    parent_queries = [sql for sql, _ in conn.fetch_calls if "::uuid[]" in sql]
    assert len(parent_queries) == 1  # exactly one batch hop for both moves
    assert out["moves"][0]["resolves"]["title"] == "T1"
    assert out["moves"][1]["resolves"]["title"] == "C2"


# ── AC5: graceful paths ──────────────────────────────────────────────────────

def test_empty_query_orders_by_created_at_desc():
    conn = _DiscourseConn(moves=[_move()])
    _run(_discourse_search(conn, scope=personal_scope(), query="", limit=5))
    main_sql, _ = conn.fetch_calls[0]
    assert "plainto_tsquery" not in main_sql
    assert "ORDER BY created_at DESC" in main_sql


def test_present_query_orders_by_ts_rank():
    conn = _DiscourseConn(moves=[_move()])
    _run(_discourse_search(conn, scope=personal_scope(), query="oversmoothing", limit=5))
    main_sql, _ = conn.fetch_calls[0]
    assert "ts_rank" in main_sql
    assert "plainto_tsquery('english', $2)" in main_sql


def test_unknown_move_type_returns_empty():
    conn = _DiscourseConn(moves=[])  # DB matches nothing
    out = _run(_discourse_search(conn, scope=personal_scope(), move_types=["bogus"], limit=5))
    assert out == {"moves": [], "count": 0, "query_mode": "lexical"}


def test_doc_with_no_moves_returns_empty():
    conn = _DiscourseConn(moves=[])
    out = _run(_discourse_search(conn, scope=personal_scope(), source_rid="document:empty", limit=5))
    assert out["count"] == 0
    # No enrichment queries fire on an empty result set.
    assert len(conn.fetch_calls) == 1


def test_null_detail_and_title_do_not_crash():
    conn = _DiscourseConn(
        moves=[_move(title=None, detail=None, source_rid="document:abc")],
        titles=[{"document_rid": "document:abc", "title": "T"}],
    )
    out = _run(_discourse_search(conn, scope=personal_scope(), query="x", limit=5))
    assert out["moves"][0]["title"] is None
    assert out["moves"][0]["detail"] is None


# ── AC5: conflicting document_rid != source_rid → HTTP 400 (route handler) ────

def _build_router(conn):
    return create_router(_FakePool(conn))


def test_conflicting_document_rid_source_rid_raises_400():
    router = _build_router(_DiscourseConn(moves=[]))
    handler = _route(router, "discourse_search")
    with pytest.raises(HTTPException) as ei:
        _run(handler(
            scope=personal_scope(),
            query=None, move_type=None,
            document_rid="document:aaa", source_rid="document:bbb",
            status=None, source_type="document", limit=20))
    assert ei.value.status_code == 400
    assert "conflict" in str(ei.value.detail)


def test_equal_document_rid_source_rid_is_ok():
    conn = _DiscourseConn(moves=[_move(source_rid="document:abc")])
    router = _build_router(conn)
    handler = _route(router, "discourse_search")
    out = _run(handler(
        scope=personal_scope(),
        query=None, move_type=None,
        document_rid="document:abc", source_rid="document:abc",
        status=None, source_type="document", limit=20))
    assert out["count"] == 1


def test_route_normalizes_repeated_move_type_and_filters():
    conn = _DiscourseConn(moves=[_move(move_type="claim")])
    router = _build_router(conn)
    handler = _route(router, "discourse_search")
    out = _run(handler(
        scope=personal_scope(),
        query="sheaf", move_type=["claim", "evidence"],
        document_rid=None, source_rid=None,
        status=None, source_type="document", limit=10))
    main_sql, main_args = conn.fetch_calls[0]
    assert "move_type = ANY" in main_sql
    assert ["claim", "evidence"] in [a for a in main_args]
    assert out["query_mode"] == "lexical"
