"""End-to-end scope test for _discourse_search (tranche-1 read auth), scratch DB.

Exercises the wired helper's real SQL: move visibility on the main query and the
restricted-parent masking on the resolves lookup. Personal sees all; a field
scope sees only public in-field document moves and never a restricted parent.

Needs KOI_TEST_ADMIN_DSN (see tests/koi_test_targets.py); skips without it.
"""
import uuid
import asyncio
import pytest
import asyncpg

from api.routers.knowledge_router import _discourse_search
from api.read_scope import personal_scope, field_scope
from tests import koi_test_targets as targets

WITHHELD = "document:2f6c912bb900dcd324473b8a4228118526a3a4104342c14553b9d3bc4de5be3f"
_LOOP = asyncio.new_event_loop()


def _run(c):
    return _LOOP.run_until_complete(c)

DDL = """
CREATE TABLE koi_memories (rid text PRIMARY KEY, is_private boolean, access_source text);
CREATE TABLE document_field_membership (document_rid text, field_id text);
CREATE TABLE session_exclusions (session_id text);
CREATE TABLE document_ingestion_log (document_rid text, source_url text, title text);
CREATE TABLE session_discourse_moves (
  id uuid PRIMARY KEY, episode_id uuid, session_id text, move_type text, title text,
  detail text, status text, resolves_move_id uuid, created_at timestamptz DEFAULT now(),
  source_type text, source_rid text);
"""

IDS = {k: uuid.uuid4() for k in ("child", "parent_priv", "sess")}


async def _seed(conn):
    await conn.execute(DDL)
    await conn.executemany("INSERT INTO koi_memories(rid,is_private,access_source) VALUES($1,$2,$3)",
                           [("document:pubA", False, "test-public"),
                            ("document:privB", True, "test-private"),
                            (WITHHELD, False, "test-public")])
    await conn.executemany("INSERT INTO document_field_membership VALUES($1,$2)",
                           [("document:pubA", "bio"), ("document:privB", "bio"), (WITHHELD, "bio")])
    await conn.executemany(
        "INSERT INTO session_discourse_moves(id,move_type,title,detail,status,resolves_move_id,source_type,source_rid,session_id)"
        " VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
        [
            # child (public, in bio) resolves a PRIVATE parent
            (IDS["child"], "claim", "trust claim", "about membership", "asserted",
             IDS["parent_priv"], "document", "document:pubA", None),
            # the private parent
            (IDS["parent_priv"], "thesis", "private parent", "secret",
             "asserted",
             None, "document", "document:privB", None),
            # a session move
            (IDS["sess"], "claim", "session trust note", "x", "asserted",
             None, "session", None, "s1"),
        ],
    )


@pytest.fixture(scope="module")
def conn():
    admin_dsn = targets.admin_dsn()
    name = targets.scratch_name("ds")
    a = _run(asyncpg.connect(admin_dsn)); _run(a.execute(f'CREATE DATABASE "{name}"')); _run(a.close())
    c = _run(asyncpg.connect(targets.with_database(admin_dsn, name)))
    _run(_seed(c))
    yield c
    _run(c.close())
    a = _run(asyncpg.connect(admin_dsn))
    _run(a.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1", name))
    _run(a.execute(f'DROP DATABASE IF EXISTS "{targets.droppable(name)}"')); _run(a.close())


def _titles(res):
    return {m["title"] for m in res["moves"]}


def test_personal_sees_both_document_moves_and_the_parent(conn):
    res = _run(_discourse_search(conn, source_type="document", scope=personal_scope()))
    assert {"trust claim", "private parent"} <= _titles(res)
    child = next(m for m in res["moves"] if m["title"] == "trust claim")
    assert child["resolves"] is not None and child["resolves"]["title"] == "private parent"


def test_field_sees_only_public_child_and_parent_is_masked(conn):
    res = _run(_discourse_search(conn, source_type="document", scope=field_scope({"bio"})))
    assert _titles(res) == {"trust claim"}            # private parent not returned as a row
    child = res["moves"][0]
    assert child["resolves"] is None                   # restricted parent masked (AT16)


def test_personal_sees_session_move_but_field_does_not(conn):
    p = _run(_discourse_search(conn, source_type="session", scope=personal_scope()))
    assert "session trust note" in _titles(p)
    f = _run(_discourse_search(conn, source_type="session", scope=field_scope({"bio"})))
    assert _titles(f) == set()                         # no session grant for non-personal
