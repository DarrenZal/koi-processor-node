"""Excluded sessions stay out of the user-facing session readers (migration 136).

Each test builds its own schema in the KOI_TEST_DATABASE_URL database: the session tables the
claude-sessions sensor creates, session_discourse_moves, plus session_exclusions from migration
136. It calls the real routes on personal_ingest_api.app, and /knowledge/discourse-search on the
real knowledge router, through httpx's ASGI transport, both the text and the semantic branch of
/search-sessions. An excluded session must be visible before it is marked, gone from every
per-session response after, and a control session must stay. That includes a discourse move in
a kept session whose resolves parent is in the excluded session. Aggregate counts and the
internal session_ingestion_log row are unchanged. The readers fail closed: a missing
session_exclusions table or KOI_SESSION_READERS_DISABLED gives 503, and the down migration
refuses while any session is marked. Document discourse search is unaffected.
"""
from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from tests import koi_test_targets as targets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

pytestmark = targets.requires_database

MIGRATION_136 = (ROOT / "migrations" / "136_session_exclusions.sql").read_text()
MIGRATION_136_DOWN = (ROOT / "migrations" / "136_session_exclusions_down.sql").read_text()
SESSION_DDL = """
-- Shadows public.koi_migrations, so the migration's own bookkeeping row lands in this schema.
CREATE TABLE koi_migrations (migration_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now(), checksum TEXT);
CREATE TABLE session_ingestion_log (
    session_id TEXT PRIMARY KEY, transcript_path TEXT, summary TEXT, first_prompt TEXT,
    message_count INT, chunk_count INT, files_accessed TEXT[], last_ingested_at TIMESTAMPTZ
);
CREATE TABLE session_chunks (
    id SERIAL PRIMARY KEY, session_rid TEXT, session_id TEXT, chunk_index INT, chunk_text TEXT,
    timestamp TIMESTAMPTZ, embedding_3072 vector(3072), created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE session_tool_usage (
    id SERIAL PRIMARY KEY, session_id TEXT, tool_name TEXT, call_count INT, is_mcp BOOLEAN,
    mcp_server TEXT, created_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE document_ingestion_log (document_rid TEXT PRIMARY KEY, title TEXT, source_url TEXT);
CREATE TABLE session_discourse_moves (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(), session_id TEXT, move_type TEXT NOT NULL,
    title TEXT NOT NULL, detail TEXT, status TEXT,
    resolves_move_id UUID REFERENCES session_discourse_moves(id),
    created_at TIMESTAMP DEFAULT now(), source_type TEXT NOT NULL DEFAULT 'session', source_rid TEXT,
    CONSTRAINT chk_discourse_source_identity CHECK (source_type = 'session' AND session_id IS NOT NULL
        OR source_type = 'document' AND source_rid IS NOT NULL)
);
"""
KEEP, GONE = "keep-0000-0000", "gone-1111-1111"


def _vec(axis: int) -> list[float]:
    v = [0.0] * 3072
    v[axis] = 1.0
    return v


@pytest_asyncio.fixture
async def env():
    from api import personal_ingest_api as pia

    schema = f"sess_excl_{uuid.uuid4().hex[:10]}"
    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=1)
    async with pool0.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}", public')
        await conn.execute(SESSION_DDL)
        await conn.execute(MIGRATION_136)
        for n, (sid, axis) in enumerate(((KEEP, 0), (GONE, 1))):
            await conn.execute(
                "INSERT INTO session_ingestion_log VALUES ($1, $2, $3, $4, 4, 2, $5, now() - make_interval(mins => $6))",
                sid, f"/h/.claude/projects/-p/{sid}.jsonl", f"summary of {sid}", f"first prompt of {sid}",
                [f"/repo/shared/file.py", f"/repo/{sid}/only.py"], n)
            for i in range(2):
                await conn.execute(
                    "INSERT INTO session_chunks (session_rid, session_id, chunk_index, chunk_text, timestamp, embedding_3072) "
                    "VALUES ($1, $2, $3, $4, now(), $5::vector)",
                    f"claude-session:{sid}", sid, i, f"User: marker text {sid} {i}", str(_vec(axis)))
            await conn.execute(
                "INSERT INTO session_tool_usage (session_id, tool_name, call_count, is_mcp, mcp_server) VALUES ($1, 'Bash', 3, false, NULL), ($1, 'mcp__koi__search', 2, true, 'koi')",
                sid)
        # Discourse moves: each session has a problem and a decision. KEEP's resolution resolves
        # GONE's problem, and a document claim resolves GONE's problem, so a parent title in the
        # excluded session would surface through a move that is itself kept.
        prob = {}
        for sid in (KEEP, GONE):
            prob[sid] = await conn.fetchval(
                "INSERT INTO session_discourse_moves (session_id, move_type, title, detail, status) "
                "VALUES ($1, 'problem', $2, 'marker detail', 'open') RETURNING id", sid, f"marker problem of {sid}")
            await conn.execute(
                "INSERT INTO session_discourse_moves (session_id, move_type, title, detail, status) "
                "VALUES ($1, 'decision', $2, 'marker detail', 'made')", sid, f"marker decision of {sid}")
        await conn.execute(
            "INSERT INTO session_discourse_moves (session_id, move_type, title, detail, status, resolves_move_id) "
            "VALUES ($1, 'resolution', 'marker resolution in keep', 'marker detail', 'resolved', $2)", KEEP, prob[GONE])
        await conn.execute("INSERT INTO document_ingestion_log VALUES ('document:abc', 'Paper A', 'https://example.org/a')")
        await conn.execute(
            "INSERT INTO session_discourse_moves (source_type, source_rid, move_type, title, detail, status, resolves_move_id) "
            "VALUES ('document', 'document:abc', 'claim', 'marker claim in paper', 'marker detail', 'asserted', $1)", prob[GONE])
    await pool0.close()

    pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=4, server_settings={"search_path": f'"{schema}", public'})
    saved = {k: getattr(pia, k) for k in ("db_pool", "embedding_provider", "ENABLE_SEMANTIC_MATCHING", "generate_query_embedding")}
    pia.db_pool = pool
    pia.embedding_provider = None
    pia.ENABLE_SEMANTIC_MATCHING = False
    from fastapi import FastAPI
    from api.routers.knowledge_router import create_router
    kapp = FastAPI()
    kapp.include_router(create_router(pool), prefix="/knowledge")  # mounted as personal_ingest_api does
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=pia.app), base_url="http://test")
    kclient = httpx.AsyncClient(transport=httpx.ASGITransport(app=kapp), base_url="http://test")
    try:
        yield pia, pool, client, kclient
    finally:
        await client.aclose()
        await kclient.aclose()
        for k, v in saved.items():
            setattr(pia, k, v)
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


def _semantic(pia, on: bool, axis: int = 1):
    async def fake_embed(_query):
        return _vec(axis)
    pia.embedding_provider = object() if on else None
    pia.ENABLE_SEMANTIC_MATCHING = on
    pia.generate_query_embedding = fake_embed


SESSION_READS = (("search text", "POST", "/search-sessions", {"query": "marker", "limit": 20}),
                 ("stats", "GET", "/session-stats", None), ("tools aggregate", "GET", "/session-tools", None),
                 ("tools by tool", "GET", "/session-tools?tool=Bash", None),
                 ("files by path", "GET", "/session-files?path_contains=shared/file.py", None),
                 ("files top", "GET", "/session-files", None))
DISCOURSE_SESSION = "/knowledge/discourse-search?source_type=session&limit=100"
DISCOURSE_DOCUMENT = "/knowledge/discourse-search?source_type=document&limit=100"


def _named(*texts) -> set[str]:
    return {sid for sid in (KEEP, GONE) for t in texts if t and sid in t}


async def _visible(pia, client, kclient) -> dict[str, set[str]]:
    """Which sessions each per-session response names."""
    out = {}
    for name, sem, body in (("search text", False, {"query": "marker", "limit": 20}),
                            ("search text scoped", False, {"query": "marker", "limit": 20, "session_id": GONE}),
                            ("search semantic", True, {"query": "anything", "limit": 20}),
                            ("search semantic scoped", True, {"query": "anything", "limit": 20, "session_id": GONE})):
        _semantic(pia, sem)
        r = await client.post("/search-sessions", json=body)
        assert r.status_code == 200, (name, r.text)
        out[name] = {x["session_id"] for x in r.json()["results"]}
    _semantic(pia, False)
    for name, path in (("stats recent", "/session-stats"), ("tools by tool", "/session-tools?tool=Bash"),
                       ("tools by mcp", "/session-tools?mcp_server=koi"),
                       ("files by path", "/session-files?path_contains=shared/file.py"), ("files top", "/session-files")):
        r = await client.get(path)
        assert r.status_code == 200, (name, r.text)
        rows = r.json().get("recent_sessions") if name == "stats recent" else r.json()["results"]
        out[name] = {x["session_id"] for x in rows}
    for name, path in (("discourse browse", DISCOURSE_SESSION), ("discourse query", DISCOURSE_SESSION + "&query=marker")):
        r = await kclient.get(path)
        assert r.status_code == 200, (name, r.text)
        moves = r.json()["moves"]
        out[name] = _named(*(m["title"] for m in moves))
        # the resolves parent of KEEP's resolution is GONE's problem
        out[name + " parent (scoped)"] = _named(*((m.get("resolves") or {}).get("title") for m in moves))
    return out


async def _statuses(client, kclient) -> dict[str, int]:
    out = {}
    for name, method, path, body in SESSION_READS:
        r = await (client.post(path, json=body) if method == "POST" else client.get(path))
        out[name] = r.status_code
    out["discourse session"] = (await kclient.get(DISCOURSE_SESSION)).status_code
    return out


async def _document_discourse(kclient) -> dict:
    r = await kclient.get(DISCOURSE_DOCUMENT)
    assert r.status_code == 200, r.text
    return r.json()


@pytest.mark.asyncio
async def test_marked_session_leaves_every_session_endpoint_and_control_stays(env):
    pia, pool, client, kclient = env
    before = await _visible(pia, client, kclient)
    for name, seen in before.items():
        assert GONE in seen, f"{name}: the fixture must show GONE before marking (positive control)"
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO session_exclusions (session_id, reason) VALUES ($1, 'test')", GONE)
    after = await _visible(pia, client, kclient)
    for name, seen in after.items():
        assert GONE not in seen, f"{name} still returns the excluded session"
        if "scoped" not in name:
            assert KEEP in seen, f"{name} lost the control session"
    r = (await kclient.get(DISCOURSE_SESSION)).json()
    kept = [m for m in r["moves"] if m["title"] == "marker resolution in keep"]
    assert len(kept) == 1 and kept[0]["resolves"] is None, "the kept move stays, its excluded parent does not"


@pytest.mark.asyncio
async def test_document_discourse_search_is_unaffected_and_never_shows_a_session_parent(env, monkeypatch):
    pia, pool, client, kclient = env
    for step in ("unmarked", "marked", "switch", "no table"):
        if step == "marked":
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO session_exclusions (session_id, reason) VALUES ($1, 'test')", GONE)
        if step == "switch":
            monkeypatch.setenv("KOI_SESSION_READERS_DISABLED", "true")
        if step == "no table":
            monkeypatch.delenv("KOI_SESSION_READERS_DISABLED")
            async with pool.acquire() as conn:
                await conn.execute("DROP TABLE session_exclusions")
        out = await _document_discourse(kclient)
        assert [m["title"] for m in out["moves"]] == ["marker claim in paper"], step
        assert out["moves"][0]["resolves"] is None, f"{step}: a document move showed a session parent"
        assert out["moves"][0]["source"]["title"] == "Paper A", step


@pytest.mark.asyncio
async def test_semantic_branch_really_ran(env):
    pia, pool, client, kclient = env
    _semantic(pia, True, axis=0)
    r = await client.post("/search-sessions", json={"query": "q", "limit": 1})
    top = r.json()["results"][0]
    assert top["session_id"] == KEEP and top["similarity"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_aggregates_and_internal_rows_unchanged(env):
    pia, pool, client, kclient = env
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO session_exclusions (session_id, reason) VALUES ($1, 'test')", GONE)
    stats = (await client.get("/session-stats")).json()
    assert stats["total_sessions"] == 2 and stats["total_chunks"] == 4
    tools = {t["tool_name"]: t for t in (await client.get("/session-tools")).json()["tool_stats"]}
    assert tools["Bash"]["session_count"] == 2
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT summary, first_prompt, files_accessed FROM session_ingestion_log WHERE session_id=$1", GONE)
    assert row["summary"] and row["first_prompt"] and len(row["files_accessed"]) == 2


@pytest.mark.asyncio
async def test_missing_table_fails_closed(env):
    pia, pool, client, kclient = env
    assert set((await _statuses(client, kclient)).values()) == {200}, "positive control: readers answer with the table"
    async with pool.acquire() as conn:
        await conn.execute(MIGRATION_136_DOWN)  # allowed: nothing is marked
        assert await conn.fetchval("SELECT to_regclass('session_exclusions')") is None
    statuses = await _statuses(client, kclient)
    assert statuses == {k: 503 for k in statuses}, statuses


@pytest.mark.asyncio
async def test_readers_disabled_switch_fails_closed_and_releases(env, monkeypatch):
    pia, pool, client, kclient = env
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO session_exclusions (session_id, reason) VALUES ($1, 'test')", GONE)
    monkeypatch.setenv("KOI_SESSION_READERS_DISABLED", "true")
    statuses = await _statuses(client, kclient)
    assert statuses == {k: 503 for k in statuses}, statuses
    monkeypatch.delenv("KOI_SESSION_READERS_DISABLED")
    assert set((await _statuses(client, kclient)).values()) == {200}
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM session_exclusions") == 1, "the switch keeps the marks"


@pytest.mark.asyncio
async def test_down_migration_refuses_while_a_session_is_marked(env):
    pia, pool, client, kclient = env
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO session_exclusions (session_id, reason) VALUES ($1, 'test')", GONE)
        with pytest.raises(asyncpg.RaiseError, match="refusing to drop"):
            async with conn.transaction():
                await conn.execute(MIGRATION_136_DOWN)
        assert await conn.fetchval("SELECT count(*) FROM session_exclusions") == 1


@pytest.mark.asyncio
async def test_migration_is_idempotent_and_matches_the_sensor_ddl(env):
    pia, pool, client, kclient = env
    async with pool.acquire() as conn:
        await conn.execute(MIGRATION_136)
        await conn.execute(MIGRATION_136)
        cols = [tuple(r) for r in await conn.fetch(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_name='session_exclusions' AND table_schema = current_schema() ORDER BY ordinal_position")]
    assert cols == [("session_id", "text", "NO"), ("excluded_at", "timestamp with time zone", "NO"), ("reason", "text", "YES")]


@pytest.mark.asyncio
async def test_filter_column_is_never_request_input():
    import inspect
    from api import personal_ingest_api as pia
    from api.routers import knowledge_router as kr
    calls = [l.strip() for l in inspect.getsource(pia).splitlines() if "await _session_exclusion_filter(conn," in l]
    calls += [l.strip() for l in inspect.getsource(kr).splitlines() if "await session_exclusion_filter(conn," in l]
    assert len(calls) == 5
    assert all(c.split('exclusion_filter(conn, ')[1].startswith('"') for c in calls)
