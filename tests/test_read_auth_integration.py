"""Combined in-process HTTP integration test for tranche-1 read auth.

Per-module tests show each piece works. This module shows they work TOGETHER:
requests go through the real app object (``api.personal_ingest_api.app``) and
its real middleware stack (AuthObservation -> KoiWriteLog -> CORS), the real
read-auth dependency, the real route handlers and their real SQL, against a
scratch database with seeded canary rows. Only the embedding provider is
stubbed (a deterministic 3072-dim vector), and the app's startup hook is not
run: the test sets ``db_pool`` and mounts the knowledge and /sql routers the
way startup does.

Database: a scratch database (koi_test_*) created through ``KOI_TEST_ADMIN_DSN``
(same convention as tests/test_read_scope.py) and dropped afterwards; pgvector
must be available. The koi_sql_ro role tests create a CLUSTER-WIDE role, so
they run only when ``KOI_TEST_THROWAWAY_CLUSTER=1`` (set by
scripts/read_auth_rehearsal.sh, which starts a throwaway cluster). Otherwise
they are skipped with that reason.

Run: scripts/read_auth_rehearsal.sh   (everything, throwaway cluster)
     KOI_TEST_ADMIN_DSN=... pytest tests/test_read_auth_integration.py -q
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import httpx
import pytest

from tests import koi_test_targets as targets

THROWAWAY = os.getenv("KOI_TEST_THROWAWAY_CLUSTER") == "1"
WITHHELD = "document:2f6c912bb900dcd324473b8a4228118526a3a4104342c14553b9d3bc4de5be3f"
DIM = 3072
_LOOP = asyncio.new_event_loop()


def _run(c):
    return _LOOP.run_until_complete(c)


# --- credentials (synthetic) ----------------------------------------------------
OP = "it-operator-mcp-token-7f3a"            # KOI_MCP_TOKEN -> personal
FIELD_BIO = "it-field-bio-token-91c2"
FIELD_BIO_UNASSIGNED = "it-field-bio-unassigned-token-55d0"
PUBLIC_GRANT = "it-public-grant-token-0b7e"  # grants pub-spore and (withheld) WITHHELD
SESS_OP = "it-session-operator-3e21"
SESS_GUEST = "it-session-guest-8a44"
SESS_EXPIRED = "it-session-expired-c9f1"
SESS_REVOKED = "it-session-revoked-2d6b"
ALL_SECRETS = (OP, FIELD_BIO, FIELD_BIO_UNASSIGNED, PUBLIC_GRANT,
               SESS_OP, SESS_GUEST, SESS_EXPIRED, SESS_REVOKED)
OPERATOR_EMAIL = "operator@example.org"

# --- canary markers: each appears only in rows of one visibility class ----------
PRIVATE = "PRIVATE-MARKER"
EMAIL_SUBJECT = "EMAIL-SUBJECT-MARKER"
WITHHELD_M = "WITHHELD-MARKER"
SPORE = "SPORE-MARKER"
UNASSIGNED = "UNASSIGNED-MARKER"
NOMETA = "NOMETA-MARKER"
UNLABELED = "UNLABELED-MARKER"
BIO = "BIO-PUBLIC-MARKER"
WIKI = "WIKI-PUBLIC-MARKER"
DOCS = "DOCS-PUBLIC-MARKER"
RESTRICTED = (PRIVATE, EMAIL_SUBJECT, WITHHELD_M, SPORE, UNASSIGNED, NOMETA, UNLABELED)


def _vec(theta: float) -> str:
    import math
    v = [0.0] * DIM
    v[0], v[1] = math.cos(theta), math.sin(theta)
    return "[" + ",".join(repr(x) for x in v) + "]"


QUERY_VEC = [1.0] + [0.0] * (DIM - 1)


async def _stub_embed(text, prompt_type="query", **_):
    return list(QUERY_VEC)


DDL = """
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE koi_memories (rid text PRIMARY KEY, is_private boolean DEFAULT false,
  access_source text, content jsonb, metadata jsonb, source_sensor text,
  created_at timestamptz DEFAULT now());
CREATE TABLE koi_memory_chunks (chunk_rid text PRIMARY KEY, document_rid text, content jsonb,
  metadata jsonb, embedding_3072 vector(3072), created_at timestamptz DEFAULT now());
CREATE TABLE document_field_membership (document_rid text, field_id text);
CREATE TABLE email_metadata (rid text, subject text, from_name text, from_address text,
  date_sent timestamptz);
CREATE TABLE entity_registry (fuseki_uri text PRIMARY KEY, entity_text text, entity_type text,
  node_private boolean NOT NULL DEFAULT false, merged_into text, embedding_3072 vector(3072),
  metadata jsonb, normalized_text text);
CREATE TABLE entity_rid_mappings (canonical_uri text, vault_path text, last_synced timestamptz);
CREATE TABLE document_entity_links (document_rid text, entity_uri text);
CREATE TABLE knowledge_episodes (id uuid PRIMARY KEY, name text, source_document text, metadata jsonb);
CREATE TABLE knowledge_facts (id uuid PRIMARY KEY, subject_uri text, predicate text, object_uri text,
  fact_text text, source_node_rid text, episode_id uuid, valid_to timestamptz,
  fact_embedding_3072 vector(3072), created_at timestamptz DEFAULT now());
CREATE TABLE document_ingestion_log (document_rid text, source_url text, title text);
CREATE TABLE session_discourse_moves (id uuid PRIMARY KEY, episode_id uuid, session_id text,
  move_type text, title text, detail text, status text, resolves_move_id uuid,
  created_at timestamptz DEFAULT now(), source_type text, source_rid text);
CREATE TABLE session_exclusions (session_id text);
CREATE TABLE session_tokens (token_hash text PRIMARY KEY, user_email text,
  expires_at timestamptz, revoked_at timestamptz);
CREATE TABLE koi_query_log (query_text text, agent_id text, confidence_score real,
  tables_accessed text[], row_count int, execution_ms real, created_at timestamptz DEFAULT now());
"""

# rid -> (is_private | None for "no koi_memories row", field | None, sensor, chunk specs)
# chunk spec: (theta, text, chunk metadata)
SOURCES = {
    "document:pub-bio": (False, "bio", "doc", [(0.30, f"alpha {BIO}", {})]),
    "document:priv-bio": (True, "bio", "doc",
                          [(0.01 + i * 0.0009, f"alpha {PRIVATE} {i}", {}) for i in range(210)]),
    "document:pub-spore": (False, "spore", "doc", [(0.31, f"alpha {SPORE}", {})]),
    "document:unassigned": (False, None, "doc", [(0.32, f"alpha {UNASSIGNED}", {})]),
    WITHHELD: (False, "bio", "doc", [(0.33, f"alpha {WITHHELD_M}", {})]),
    "document:nometa": (None, "bio", "doc", [(0.36, f"alpha {NOMETA}", {})]),
    "document:unlabeled": ("unset", "bio", "doc", [(0.37, f"alpha {UNLABELED}", {})]),
    "orn:gmail.message:canary": (True, "bio", "email-sensor", [(0.05, f"alpha {PRIVATE} mail", {})]),
    "mediawiki:p2p:Canary": (False, "bio", "mediawiki-sensor",
                             [(0.34, f"alpha {WIKI}", {})]),
    "doc-scanner:notes-repo/pub.md": (False, "bio", "doc-scanner",
                                           [(0.35, f"alpha {DOCS}", {"repo": "notes-repo"})]),
    "doc-scanner:notes-repo/priv.md": (True, "bio", "doc-scanner",
                                            [(0.02, f"alpha {PRIVATE} doc", {"repo": "notes-repo"})]),
}

ENTITIES = [  # (uri, text, theta, node_private, mention source)
    ("e:bio-pub", f"alpha Entity {BIO}", 0.30, False, "document:pub-bio"),
    ("e:priv-only", f"alpha Entity {PRIVATE}", 0.01, False, "document:priv-bio"),
    ("e:withheld-only", f"alpha Entity {WITHHELD_M}", 0.02, False, WITHHELD),
    ("e:node-private", f"alpha Entity node {PRIVATE}", 0.03, True, "document:pub-bio"),
]

FACTS = [  # (text, theta, source rid, subject)
    (f"alpha fact {BIO}", 0.30, "document:pub-bio", "e:bio-pub"),
    (f"alpha fact about a hidden subject {BIO}", 0.29, "document:pub-bio", "e:priv-only"),
    (f"alpha fact {PRIVATE}", 0.01, "document:priv-bio", "e:bio-pub"),
    (f"alpha fact orphan {PRIVATE}", 0.02, None, "e:bio-pub"),
    (f"alpha fact {WITHHELD_M}", 0.03, WITHHELD, "e:bio-pub"),
]

MOVES = {k: uuid.uuid4() for k in ("child", "parent", "session", "withheld")}


async def _seed(conn):
    await conn.execute(DDL)
    for rid, (priv, fld, sensor, chunks) in SOURCES.items():
        if priv is not None:
            labeled = priv != "unset"
            await conn.execute(
                "INSERT INTO koi_memories(rid,is_private,access_source,content,metadata,source_sensor)"
                " VALUES($1,$2,$3,$4::jsonb,'{}'::jsonb,$5)",
                rid, priv is True, ("test-private" if priv else "test-public") if labeled else None,
                json.dumps({"title": f"title {rid}", "text": chunks[0][1]}), sensor)
        if fld is not None:
            await conn.execute("INSERT INTO document_field_membership VALUES($1,$2)", rid, fld)
        for i, (theta, text, meta) in enumerate(chunks):
            await conn.execute(
                "INSERT INTO koi_memory_chunks(chunk_rid,document_rid,content,metadata,embedding_3072)"
                " VALUES($1,$2,$3::jsonb,$4::jsonb,$5::vector)",
                f"{rid}#c{i}", rid, json.dumps({"text": text}), json.dumps(meta), _vec(theta))
    await conn.execute(
        "INSERT INTO email_metadata VALUES('orn:gmail.message:canary',$1,'n','a@example.org',now())",
        EMAIL_SUBJECT)
    for uri, text, theta, npriv, src in ENTITIES:
        meta = ({"closure": {"status": "REFUTED", "note": f"closure {PRIVATE}"}}
                if uri == "e:priv-only" else {})
        await conn.execute(
            "INSERT INTO entity_registry(fuseki_uri,entity_text,entity_type,node_private,"
            "embedding_3072,metadata,normalized_text) VALUES($1,$2,'Concept',$3,$4::vector,$5::jsonb,lower($2))",
            uri, text, npriv, _vec(theta), json.dumps(meta))
        await conn.execute("INSERT INTO document_entity_links VALUES($1,$2)", src, uri)
    for text, theta, src, subject in FACTS:
        await conn.execute(
            "INSERT INTO knowledge_facts(id,subject_uri,predicate,fact_text,source_node_rid,"
            "fact_embedding_3072) VALUES($1,$2,'mentions',$3,$4,$5::vector)",
            uuid.uuid4(), subject, text, src, _vec(theta))
    await conn.executemany(
        "INSERT INTO session_discourse_moves(id,move_type,title,detail,status,resolves_move_id,"
        "source_type,source_rid,session_id) VALUES($1,$2,$3,$4,'asserted',$5,$6,$7,$8)",
        [
            (MOVES["child"], "claim", f"alpha child {BIO}", "x", MOVES["parent"],
             "document", "document:pub-bio", None),
            (MOVES["parent"], "thesis", f"alpha parent {PRIVATE}", "x", None,
             "document", "document:priv-bio", None),
            (MOVES["session"], "claim", f"alpha session {PRIVATE}", "x", None,
             "session", None, "s1"),
            (MOVES["withheld"], "claim", f"alpha move {WITHHELD_M}", "x", None,
             "document", WITHHELD, None),
        ])
    now = datetime.now(timezone.utc)
    for tok, email, exp, rev in (
        (SESS_OP, OPERATOR_EMAIL, now + timedelta(days=1), None),
        (SESS_GUEST, "guest@example.org", now + timedelta(days=1), None),
        (SESS_EXPIRED, OPERATOR_EMAIL, now - timedelta(days=1), None),
        (SESS_REVOKED, OPERATOR_EMAIL, now + timedelta(days=1), now),
    ):
        await conn.execute("INSERT INTO session_tokens VALUES($1,$2,$3,$4)",
                           hashlib.sha256(tok.encode()).hexdigest(), email, exp, rev)


async def _pool(dsn, size):
    # created inside the loop: asyncpg.Pool binds the running loop at construction
    return await asyncpg.create_pool(dsn, min_size=1, max_size=size)


def _sha(t):
    return hashlib.sha256(t.encode()).hexdigest()


def _scopes_file(tmp_path, mapping, name="scopes.json"):
    p = tmp_path / name
    p.write_text(json.dumps(mapping))
    return str(p)


VALID_SCOPES = {
    _sha(FIELD_BIO): {"view": "field", "fields": ["bio"], "include_unassigned": False,
                      "principal": "svc:bio"},
    _sha(FIELD_BIO_UNASSIGNED): {"view": "field", "fields": ["bio"], "include_unassigned": True},
    _sha(PUBLIC_GRANT): {"view": "public", "sources": ["document:pub-spore", WITHHELD]},
}


# --- fixtures ---------------------------------------------------------------------

@pytest.fixture(scope="module")
def stack(tmp_path_factory):
    admin_dsn = targets.admin_dsn()
    name = targets.scratch_name("ra")
    admin = _run(asyncpg.connect(admin_dsn))
    _run(admin.execute(f'CREATE DATABASE "{name}"'))
    _run(admin.close())
    dsn = targets.with_database(admin_dsn, name)
    c = _run(asyncpg.connect(dsn))
    _run(_seed(c))
    _run(c.close())
    pool = _run(_pool(dsn, 4))

    import api.personal_ingest_api as pia
    from api.routers.knowledge_router import create_router as create_knowledge_router

    saved = (pia.db_pool, pia.generate_query_embedding)
    pia.db_pool = pool
    pia.generate_query_embedding = _stub_embed
    app = pia.app
    app.router.routes[:] = [r for r in app.router.routes
                            if not getattr(r, "path", "").startswith(("/knowledge", "/sql"))]
    app.include_router(create_knowledge_router(pool, generate_query_embedding=_stub_embed),
                       prefix="/knowledge")
    _mount_sql(app, None, pool)
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 50123)),
        base_url="http://koi.test")
    yield {"app": app, "client": client, "pool": pool, "dsn": dsn, "db": name}

    _run(client.aclose())
    pia.db_pool, pia.generate_query_embedding = saved
    _run(pool.close())
    admin = _run(asyncpg.connect(admin_dsn))
    _run(admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1", name))
    _run(admin.execute(f'DROP DATABASE IF EXISTS "{targets.droppable(name)}"'))
    _run(admin.close())


def _mount_sql(app, ro_pool, log_pool):
    from api.routers.query_router import create_router as create_query_router
    app.router.routes[:] = [r for r in app.router.routes if getattr(r, "path", "") != "/sql"]
    app.include_router(create_query_router(ro_pool, None, log_pool=log_pool), prefix="/sql")


@pytest.fixture(autouse=True)
def env(monkeypatch, tmp_path):
    for e in ("KOI_ADMIN_TOKEN", "KOI_CLAIMS_SERVICE_TOKEN", "KOI_READ_ROUTES_DISABLED",
              "KOI_WITHHELD_SOURCE_RIDS", "KOI_SESSION_READERS_DISABLED"):
        monkeypatch.delenv(e, raising=False)
    monkeypatch.setenv("KOI_STATE_DIR", str(tmp_path / "state"))   # no admin_token file
    monkeypatch.setenv("KOI_MCP_TOKEN", OP)
    monkeypatch.setenv("KOI_READ_SCOPES_FILE", _scopes_file(tmp_path, VALID_SCOPES))
    monkeypatch.setenv("KOI_PERSONAL_PRINCIPALS", OPERATOR_EMAIL)
    monkeypatch.setenv("KOI_AUTH_MODE", "enforce")
    # unified-search's vault surface shells out to a BM25 index under $HOME;
    # point HOME at an empty directory so no test can read the real vault.
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    yield


def _req(stack, method, path, token=None, *, headers=None, **kw):
    h = dict(headers or {})
    if token is not None:
        h["Authorization"] = f"Bearer {token}"
    return _run(stack["client"].request(method, path, headers=h, **kw))


def _search(stack, token=None, limit=10, **kw):
    return _req(stack, "POST", "/search", token, json={"query": "alpha", "limit": limit}, **kw)


def _unified(stack, token=None, include="entities,facts,wiki,memories,docs", **kw):
    return _req(stack, "GET", "/knowledge/unified-search", token,
                params={"query": "alpha", "limit": 50, "include": include}, **kw)


def _markers(resp):
    text = resp.text
    return {m for m in RESTRICTED + (BIO, WIKI, DOCS) if m in text}


T1 = [
    ("POST", "/search", {"json": {"query": "alpha"}}),
    ("GET", "/search", {"params": {"q": "alpha"}}),
    ("POST", "/query", {"json": {"query": "alpha"}}),
    ("GET", "/knowledge/discourse-search", {"params": {"query": "alpha"}}),
    ("GET", "/knowledge/unified-search", {"params": {"query": "alpha", "include": "memories"}}),
    ("POST", "/sql", {"json": {"sql": "SELECT entity_text FROM entity_registry"}}),
]


# --- wiring -----------------------------------------------------------------------

def test_middleware_order_is_observation_outermost(stack):
    assert [m.cls.__name__ for m in stack["app"].user_middleware] == [
        "AuthObservationMiddleware", "KoiWriteLogMiddleware", "CORSMiddleware"]


def test_exactly_the_tranche1_routes_carry_the_read_auth_dependency(stack):
    def deps(dependant):
        for d in dependant.dependencies:
            yield d.call
            yield from deps(d)
    guarded = set()
    for r in stack["app"].routes:
        if hasattr(r, "dependant") and any(
                getattr(c, "__name__", "") == "read_scope_dependency" for c in deps(r.dependant)):
            guarded |= {(m, r.path) for m in r.methods}
    assert guarded == {(m, p) for m, p, _ in T1}


# --- AT1: enforce mode, no / bad credential -------------------------------------

@pytest.mark.parametrize("method,path,kw", T1)
def test_enforce_rejects_missing_credential(stack, method, path, kw):
    r = _req(stack, method, path, **kw)
    assert r.status_code == 401, r.text
    assert r.headers.get("www-authenticate") == "Bearer"
    assert not _markers(r)


@pytest.mark.parametrize("method,path,kw", T1)
@pytest.mark.parametrize("headers", [
    {"Authorization": "Bearer not-a-known-token"},
    {"Authorization": "Basic Zm9vOmJhcg=="},
    {"Authorization": "Bearer"},
    {"Cookie": f"koi_session={SESS_OP}"},          # cookies are not a read credential
    {"Authorization": f"Bearer {SESS_EXPIRED}"},
    {"Authorization": f"Bearer {SESS_REVOKED}"},
])
def test_enforce_rejects_unrecognized_credentials(stack, method, path, kw, headers):
    r = _req(stack, method, path, headers=headers, **kw)
    assert r.status_code == 401, (headers, r.text)


@pytest.mark.parametrize("bad_mode", ["enforced", "on", "true", "LOG ", "strict"])
def test_unrecognized_mode_value_enforces(stack, monkeypatch, bad_mode):
    monkeypatch.setenv("KOI_AUTH_MODE", bad_mode)
    if bad_mode.strip().lower() == "log":
        pytest.skip("'LOG ' normalizes to log")
    assert _search(stack).status_code == 401


def test_health_stays_reachable_without_credential(stack):
    r = _req(stack, "GET", "/health")
    assert r.status_code not in (401, 403)


# --- AT2-in-process: the operator sees what the route returns today -------------

def test_operator_token_sees_private_rows_and_email_subject(stack):
    r = _search(stack, OP)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "personal"
    assert PRIVATE in r.text and EMAIL_SUBJECT in r.text


def test_operator_session_token_is_personal(stack):
    r = _search(stack, SESS_OP)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "personal"
    assert PRIVATE in r.text


def test_session_of_unlisted_principal_is_forbidden(stack):
    assert _search(stack, SESS_GUEST).status_code == 403


# The pre-change SQL, copied verbatim from regen-prod 325d7a5 (api/personal_ingest_api.py
# POST /search, and knowledge_router unified-search 'memories'). The operator's
# results must equal what these return: personal adds nothing to today's query.
ORIG_SEARCH_SQL = """
    SELECT * FROM (
        SELECT DISTINCT ON (document_rid)
            document_rid as rid, title, content_preview,
            similarity, source_sensor, metadata, created_at
        FROM (
            SELECT c.document_rid,
                   m.content->>'title' as title,
                   LEFT(c.content->>'text', 500) as content_preview,
                   1 - (c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) as similarity,
                   m.source_sensor, m.metadata, m.created_at
            FROM koi_memory_chunks c
            JOIN koi_memories m ON m.rid = c.document_rid
            WHERE c.embedding_3072 IS NOT NULL
            ORDER BY c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT 200
        ) cand
        ORDER BY document_rid, similarity DESC
    ) ded
    ORDER BY similarity DESC
    LIMIT $2
"""
ORIG_MEMORIES_SQL = """
    WITH hits AS (
        SELECT mc.chunk_rid, mc.document_rid,
               1 - (mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score
        FROM koi_memory_chunks mc
        WHERE mc.embedding_3072 IS NOT NULL
          AND mc.document_rid NOT LIKE 'mediawiki:%'
          AND mc.document_rid NOT LIKE 'doc-scanner:%'
        ORDER BY mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
        LIMIT 20
    )
    SELECT h.chunk_rid FROM hits h ORDER BY h.score DESC
"""


def test_operator_search_equals_pre_change_query(stack):
    want = _run(stack["pool"].fetch(ORIG_SEARCH_SQL, str(QUERY_VEC), 10))
    r = _search(stack, OP, limit=10)
    assert [x["rid"] for x in r.json()["results"]] == [w["rid"] for w in want]


def test_operator_unified_search_equals_pre_change_memories(stack):
    want = [w["chunk_rid"] for w in _run(stack["pool"].fetch(ORIG_MEMORIES_SQL, str(QUERY_VEC)))]
    r = _unified(stack, OP)
    assert r.status_code == 200 and "surfaces_withheld" not in r.json()
    got = [x["chunk_rid"] for x in sorted(r.json()["results"], key=lambda x: -x["score"])
           if x["source"] in ("memory", "email", "substack", "calendar")]
    assert got == want
    assert {PRIVATE, WITHHELD_M, BIO, WIKI, DOCS} <= _markers(r)


def test_operator_sees_hidden_subject_closure_on_fact(stack):
    r = _unified(stack, OP, include="facts")
    facts = [x for x in r.json()["results"] if x["source"] == "fact"]
    assert any(f["metadata"].get("subject_closure", {}).get("status") == "REFUTED" for f in facts)


def test_operator_discourse_search_sees_parent(stack):
    r = _req(stack, "GET", "/knowledge/discourse-search", OP, params={"query": "alpha"})
    assert r.status_code == 200
    child = [m for m in r.json()["moves"] if BIO in (m["title"] or "")][0]
    assert child["resolves"] and PRIVATE in child["resolves"]["title"]


# --- AT3/AT10/AT11/AT15/AT16: field and public scopes -----------------------------

def test_field_search_sees_only_public_in_field_and_fills_k(stack):
    # 210 private chunks rank above every field chunk. Visibility inside the
    # 200-candidate ANN scan is what lets the field token still fill k (AT10).
    r = _search(stack, FIELD_BIO, limit=3)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "field"
    rids = [x["rid"] for x in r.json()["results"]]
    assert sorted(rids) == sorted(["document:pub-bio", "mediawiki:p2p:Canary",
                                   "doc-scanner:notes-repo/pub.md"])
    assert not (_markers(r) & set(RESTRICTED))


def test_field_get_search_and_query_match_post(stack):
    g = _req(stack, "GET", "/search", FIELD_BIO, params={"q": "alpha", "limit": 10})
    q = _req(stack, "POST", "/query", FIELD_BIO, json={"query": "alpha", "limit": 10})
    for r in (g, q):
        assert r.status_code == 200
        assert not (_markers(r) & set(RESTRICTED)) and BIO in r.text
    assert "email" not in json.dumps(q.json()["results"])


def test_include_unassigned_flag_controls_unassigned_sources(stack):
    assert UNASSIGNED not in _search(stack, FIELD_BIO).text
    r = _search(stack, FIELD_BIO_UNASSIGNED)
    assert r.status_code == 200 and UNASSIGNED in r.text
    assert not (_markers(r) & (set(RESTRICTED) - {UNASSIGNED}))


def test_public_grant_cannot_reach_withheld_source(stack):
    r = _search(stack, PUBLIC_GRANT)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "public"
    assert [x["rid"] for x in r.json()["results"]] == ["document:pub-spore"]
    assert WITHHELD_M not in r.text


def test_field_unified_search_every_surface_filtered(stack):
    r = _unified(stack, FIELD_BIO, include="entities,facts,sessions,wiki,vault,memories,docs")
    assert r.status_code == 200, r.text
    body = r.json()
    assert not (_markers(r) & set(RESTRICTED)), _markers(r)
    assert sorted(body["surfaces_withheld"]) == ["sessions", "vault"]
    by_source = {}
    for x in body["results"]:
        by_source.setdefault(x["source"], []).append(x)
    assert [e["uri"] for e in by_source["entity"]] == ["e:bio-pub"]          # AT11 pivot
    assert len(by_source["fact"]) == 2 and all(BIO in f["text"] for f in by_source["fact"])
    # F2: the hidden subject's closure does not ride along on a visible fact
    assert all("subject_closure" not in f["metadata"] for f in by_source["fact"])
    assert [w["document_rid"] for w in by_source["wiki"]] == ["mediawiki:p2p:Canary"]
    assert [m["document_rid"] for m in by_source["memory"]] == ["document:pub-bio"]
    assert len(by_source["doc"]) == 1 and DOCS in by_source["doc"][0]["text"]


def test_field_unified_search_text_fallback_is_filtered(stack, monkeypatch):
    import api.routers.knowledge_router as kr  # noqa: F401
    async def _none(*a, **k):
        return None
    app = stack["app"]
    from api.routers.knowledge_router import create_router as ckr
    app.router.routes[:] = [r for r in app.router.routes
                            if not getattr(r, "path", "").startswith("/knowledge")]
    app.include_router(ckr(stack["pool"], generate_query_embedding=_none), prefix="/knowledge")
    try:
        r = _unified(stack, FIELD_BIO, include="entities,facts,docs")
        assert r.status_code == 200 and r.json().get("degraded") is True
        assert not (_markers(r) & set(RESTRICTED)), _markers(r)
        p = _unified(stack, OP, include="entities,facts,docs")
        assert PRIVATE in p.text                      # the fallback still serves personal
    finally:
        app.router.routes[:] = [r for r in app.router.routes
                                if not getattr(r, "path", "").startswith("/knowledge")]
        app.include_router(ckr(stack["pool"], generate_query_embedding=_stub_embed),
                           prefix="/knowledge")


def test_field_discourse_search_masks_restricted_parent_and_sessions(stack):
    r = _req(stack, "GET", "/knowledge/discourse-search", FIELD_BIO, params={"query": "alpha"})
    assert r.status_code == 200
    moves = r.json()["moves"]
    assert [BIO in (m["title"] or "") for m in moves] == [True]
    assert moves[0]["resolves"] is None                       # AT16
    assert not (_markers(r) & set(RESTRICTED))
    s = _req(stack, "GET", "/knowledge/discourse-search", FIELD_BIO,
             params={"source_type": "session"})
    assert s.status_code == 200 and s.json()["count"] == 0   # AT13


@pytest.mark.parametrize("call", [
    lambda st: _req(st, "GET", "/search", FIELD_BIO,
                    params={"q": "alpha", "view": "personal", "include_private": "true"}),
    lambda st: _req(st, "POST", "/search", FIELD_BIO,
                    json={"query": "alpha", "view": "personal", "include_private": True}),
    lambda st: _req(st, "GET", "/knowledge/unified-search", FIELD_BIO,
                    params={"query": "alpha", "view": "personal", "include_private": "true",
                            "include": "memories,entities"}),
])
def test_request_parameters_cannot_widen_scope(stack, call):     # AT4
    r = call(stack)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "field"
    assert not (_markers(r) & set(RESTRICTED))


# --- /sql: personal only, fail closed ---------------------------------------------

def test_sql_field_and_public_tokens_get_403(stack):
    for tok in (FIELD_BIO, PUBLIC_GRANT):
        r = _req(stack, "POST", "/sql", tok, json={"sql": "SELECT entity_text FROM entity_registry"})
        assert r.status_code == 403, r.text


def test_sql_without_read_pool_is_503_even_for_operator(stack):
    r = _req(stack, "POST", "/sql", OP, json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 503 and "fall back" in r.json()["detail"]


# --- the two reported config defects, through HTTP -------------------------------

def test_unknown_view_in_scopes_file_is_503_not_personal(stack, monkeypatch, tmp_path):
    for view in ("admin", "personal", "Field"):
        bad = dict(VALID_SCOPES)
        bad[_sha(FIELD_BIO)] = {"view": view, "fields": ["bio"]}
        monkeypatch.setenv("KOI_READ_SCOPES_FILE", _scopes_file(tmp_path, bad, f"s-{view}.json"))
        r = _search(stack, FIELD_BIO)
        assert r.status_code == 503, (view, r.text)
        assert PRIVATE not in r.text
        # the operator's env token does not depend on the file
        assert _search(stack, OP).status_code == 200


def test_string_false_include_unassigned_is_503_not_widened(stack, monkeypatch, tmp_path):
    bad = dict(VALID_SCOPES)
    bad[_sha(FIELD_BIO)] = {"view": "field", "fields": ["bio"], "include_unassigned": "false"}
    monkeypatch.setenv("KOI_READ_SCOPES_FILE", _scopes_file(tmp_path, bad, "s-false.json"))
    r = _search(stack, FIELD_BIO)
    assert r.status_code == 503 and UNASSIGNED not in r.text


def test_missing_scopes_file_is_503_for_scoped_tokens(stack, monkeypatch, tmp_path):
    monkeypatch.setenv("KOI_READ_SCOPES_FILE", str(tmp_path / "absent.json"))
    assert _search(stack, FIELD_BIO).status_code == 503
    assert _search(stack, OP).status_code == 200


# --- log mode (S1-S5): today's behavior for no credential, never broader -------

def test_log_mode_no_credential_is_todays_personal_view(stack, monkeypatch):
    monkeypatch.setenv("KOI_AUTH_MODE", "log")
    r = _search(stack)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "personal"


@pytest.mark.parametrize("headers", [
    {"Authorization": "Bearer not-a-known-token"},
    {"Authorization": "Basic Zm9vOmJhcg=="},
    {"Authorization": f"Bearer {SESS_EXPIRED}"},
    {"Authorization": f"Bearer {SESS_GUEST}"},
])
def test_log_mode_unrecognized_credential_is_least_privilege(stack, monkeypatch, headers):
    monkeypatch.setenv("KOI_AUTH_MODE", "log")
    r = _search(stack, headers=headers)
    assert r.status_code == 200 and r.headers["x-koi-read-scope"] == "public"
    assert not (_markers(r) & set(RESTRICTED))


def test_log_mode_still_403s_scoped_sql(stack, monkeypatch):
    monkeypatch.setenv("KOI_AUTH_MODE", "log")
    r = _req(stack, "POST", "/sql", FIELD_BIO, json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 403


@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer not-a-known-token"},
                                     {"Cookie": f"koi_session={SESS_OP}"}])
def test_sql_is_token_gated_even_in_log_mode(stack, monkeypatch, headers):
    # D7's early gate: /sql needs the personal credential before S6 enforces the rest.
    monkeypatch.setenv("KOI_AUTH_MODE", "log")
    r = _req(stack, "POST", "/sql", headers=headers,
             json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 401
    assert _search(stack, headers=headers).status_code == 200      # the others stay in log mode


def test_sql_ro_log_mode_operator_token_works(stack, ro_sql, monkeypatch):
    monkeypatch.setenv("KOI_AUTH_MODE", "log")
    r = _req(stack, "POST", "/sql", OP, json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 200


# --- recovery lever: per-route kill switch keeps auth, returns 503 --------------

def test_kill_switch_disables_one_route_only(stack, monkeypatch):
    monkeypatch.setenv("KOI_READ_ROUTES_DISABLED", "search")
    assert _search(stack, OP).status_code == 503
    assert _req(stack, "GET", "/search", OP, params={"q": "alpha"}).status_code == 503
    assert _req(stack, "POST", "/query", OP, json={"query": "alpha"}).status_code == 200
    assert _req(stack, "POST", "/query", json={"query": "alpha"}).status_code == 401


@pytest.mark.parametrize("value", ["unified_search", "search,bogus", "SQL"])
def test_kill_switch_typo_disables_every_t1_route(stack, monkeypatch, value):
    monkeypatch.setenv("KOI_READ_ROUTES_DISABLED", value)
    for method, path, kw in T1:
        assert _req(stack, method, path, OP, **kw).status_code == 503, (value, path)


def test_kill_switch_all(stack, monkeypatch):
    monkeypatch.setenv("KOI_READ_ROUTES_DISABLED", "all")
    for method, path, kw in T1:
        assert _req(stack, method, path, OP, **kw).status_code == 503


# --- observation log (AT6 in process) ---------------------------------------------

def test_observation_log_records_origin_and_never_a_secret(stack):
    from api import auth_observability as ao
    _search(stack, FIELD_BIO, headers={"Origin": "http://page.example"})
    for h in ao._AUTH_LOG.handlers:
        h.flush()
    path = ao.log_sink_description()
    assert os.path.exists(path), path
    with open(path) as fh:
        text = fh.read()
    recs = [json.loads(line) for line in text.splitlines() if line.strip()]
    mine = [r for r in recs if r.get("path") == "/search" and r.get("origin") == "http://page.example"]
    assert mine and mine[-1]["status"] == 200
    for secret in ALL_SECRETS:
        assert secret not in text


# --- AT12: /sql as koi_sql_ro (throwaway cluster only; roles are cluster-wide) ----

@pytest.fixture
def ro_sql(stack):
    if not THROWAWAY:
        pytest.skip("koi_sql_ro role DDL needs KOI_TEST_THROWAWAY_CLUSTER=1 "
                    "(scripts/read_auth_rehearsal.sh); roles are cluster-wide")
    from urllib.parse import urlparse
    if urlparse(targets.admin_dsn()).port in (None, 5432):
        pytest.skip("refusing role DDL: KOI_TEST_ADMIN_DSN is on the default port, where a "
                    "real cluster usually listens")
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts"))
    import sql_ro_role
    admin = _run(asyncpg.connect(stack["dsn"]))
    # the scratch schema lacks some allowlisted tables; create stand-ins so the
    # setup's GRANT names every allowlisted relation, as on the live database.
    from api.routers.query_router import ALLOWED_TABLES
    for t in sorted(ALLOWED_TABLES):
        if not _run(admin.fetchval("SELECT to_regclass($1) IS NOT NULL", f"public.{t}")):
            _run(admin.execute(f'CREATE TABLE "{t}" (id int)'))
    _run(admin.execute(
        'DO $$BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname=\'koi_sql_ro\') THEN '
        'DROP OWNED BY koi_sql_ro; DROP ROLE koi_sql_ro; END IF; END$$'))
    for s in sql_ro_role.setup_sql(stack["db"]):
        _run(admin.execute(s))
    checks = _run(sql_ro_role.verify(admin, stack["db"]))
    assert all(ok for _, ok, _ in checks), [c for c in checks if not c[1]]
    ro_dsn = targets.with_user(stack["dsn"], "koi_sql_ro")
    ro_pool = _run(_pool(ro_dsn, 2))
    _mount_sql(stack["app"], ro_pool, stack["pool"])
    yield ro_pool
    _mount_sql(stack["app"], None, stack["pool"])
    _run(ro_pool.close())
    for s in sql_ro_role.backout_sql():
        _run(admin.execute(s))
    assert _run(admin.fetchval("SELECT count(*) FROM pg_roles WHERE rolname='koi_sql_ro'")) == 0
    _run(admin.close())


def test_sql_ro_allowlisted_read_works(stack, ro_sql):
    r = _req(stack, "POST", "/sql", OP, json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 200 and r.json()["row_count"] == len(ENTITIES)


@pytest.mark.parametrize("sql,params", [
    ("SELECT query_to_xml($1,$2,$3,$4)", ["SELECT token_hash FROM session_tokens", True, False, ""]),
    ("SELECT query_to_xml($1, $2, $3, $4)", ["SELECT 1", True, False, ""]),
])
def test_sql_ro_query_to_xml_denied_by_privilege(stack, ro_sql, sql, params):
    from api.routers import query_router as qr
    r = _req(stack, "POST", "/sql", OP, json={"sql": sql, "params": params})
    assert r.status_code == 400
    if qr._HAVE_SQLGLOT:
        assert "AST" in r.json()["detail"]
    else:   # regex guards pass it; the role is what stops it
        assert "permission denied" in r.json()["detail"]


def test_sql_ro_role_cannot_read_unlisted_table_or_files(stack, ro_sql):
    async def probe():
        c = await ro_sql.acquire()
        out = {}
        try:
            for name, q in (("session_tokens", "SELECT count(*) FROM session_tokens"),
                            ("query_to_xml", "SELECT query_to_xml('SELECT 1', true, false, '')"),
                            ("pg_read_file", "SELECT pg_read_file('postgresql.conf')"),
                            ("copy_program", "COPY (SELECT 1) TO PROGRAM 'true'"),
                            ("insert", "INSERT INTO entity_registry(fuseki_uri) VALUES ('x')"),
                            ("create", "CREATE TABLE ro_probe (x int)")):
                try:
                    await c.fetch(q)
                    out[name] = "allowed"
                except asyncpg.exceptions.InsufficientPrivilegeError:
                    out[name] = "denied"
        finally:
            await ro_sql.release(c)
        return out
    assert _run(probe()) == {k: "denied" for k in (
        "session_tokens",
        "query_to_xml",
        "pg_read_file",
        "copy_program",
        "insert",
        "create")}


def test_sql_ro_backout_lever_is_503_not_superuser(stack, ro_sql, monkeypatch):
    monkeypatch.setenv("KOI_READ_ROUTES_DISABLED", "sql")
    r = _req(stack, "POST", "/sql", OP, json={"sql": "SELECT entity_text FROM entity_registry"})
    assert r.status_code == 503


# --- review follow-ups ----------------------------------------------------------

@pytest.mark.parametrize("mode,expect", [("enforce", 401), ("log", 200)])
def test_non_ascii_credential_is_unrecognized_not_500(stack, monkeypatch, mode, expect):   # F3
    monkeypatch.setenv("KOI_AUTH_MODE", mode)
    r = _search(stack, headers={"Authorization": "Bearer \xe9t\xe9".encode("latin-1")})
    assert r.status_code == expect, r.text
    if expect == 200:
        assert r.headers["x-koi-read-scope"] == "public"


def test_unreadable_admin_token_file_keeps_other_tokens_working(stack, monkeypatch, tmp_path):  # F4
    state = tmp_path / "state-dir-admin"
    (state / "admin_token").mkdir(parents=True)      # a directory: open() raises
    monkeypatch.setenv("KOI_STATE_DIR", str(state))
    assert _search(stack, OP).status_code == 200
    assert _search(stack, "anything").status_code == 401


@pytest.mark.parametrize("dup", [
    [("Authorization", f"Bearer {OP}"), ("Authorization", "Bearer other")],
    [("Authorization", "Bearer other"), ("Authorization", f"Bearer {OP}")],
    [("X-API-Key", OP), ("X-API-Key", OP)],
])
def test_repeated_credential_headers_are_malformed(stack, dup):          # F8
    r = _run(stack["client"].post("/search", headers=dup, json={"query": "alpha"}))
    assert r.status_code == 401


def test_privileged_sql_pool_is_refused(stack):                          # F5
    from api.routers.query_router import read_pool_is_least_privilege
    assert _run(read_pool_is_least_privilege(stack["pool"])) is False      # the admin role: superuser
    _mount_sql(stack["app"], stack["pool"], stack["pool"])
    try:
        r = _req(stack, "POST", "/sql", OP, json={"sql": "SELECT entity_text FROM entity_registry"})
        assert r.status_code == 503 and "privileged" in r.json()["detail"]
    finally:
        _mount_sql(stack["app"], None, stack["pool"])


def test_ro_sql_pool_passes_the_privilege_check(stack, ro_sql):          # F5 positive control
    from api.routers.query_router import read_pool_is_least_privilege
    assert _run(read_pool_is_least_privilege(ro_sql)) is True
