"""Scratch-database tests for api.read_scope visibility predicates (tranche-1 read auth).

Creates a throwaway database, installs a minimal fixture, runs each predicate as
a WHERE clause and asserts the visible set under personal / field / public
scopes. Covers the three agreed fixes: session policy, fail-closed privacy, and
withheld-beats-grant. The database is dropped at the end.

Needs KOI_TEST_ADMIN_DSN (see tests/koi_test_targets.py); skips without it.
"""
import uuid
import asyncio
import pytest
import asyncpg

from api.read_scope import (
    personal_scope, field_scope, public_scope,
    source_visible_sql, move_visible_sql, entity_visible_sql,
)
from tests import koi_test_targets as targets

WITHHELD = "document:2f6c912bb900dcd324473b8a4228118526a3a4104342c14553b9d3bc4de5be3f"


async def _make_db():
    name = targets.scratch_name("rs")
    admin = await asyncpg.connect(targets.admin_dsn())
    await admin.execute(f'CREATE DATABASE "{name}"')
    await admin.close()
    return name


async def _drop_db(name):
    admin = await asyncpg.connect(targets.admin_dsn())
    await admin.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1", name)
    await admin.execute(f'DROP DATABASE IF EXISTS "{targets.droppable(name)}"')
    await admin.close()


DDL = """
CREATE TABLE koi_memories (rid text PRIMARY KEY, is_private boolean DEFAULT false,
  access_source text);
CREATE TABLE document_field_membership (document_rid text, field_id text);
CREATE TABLE session_discourse_moves (
  id uuid PRIMARY KEY, source_type text, source_rid text, session_id text);
CREATE TABLE session_exclusions (session_id text);
CREATE TABLE entity_registry (fuseki_uri text, node_private boolean);
CREATE TABLE document_entity_links (document_rid text, entity_uri text);
"""

# source rid -> (is_private or None for "no metadata row", field or None)
SOURCES = {
    "document:pubA":       (False, "bio"),
    "document:privB":      (True,  "bio"),
    "document:pubC":       (False, "spore"),
    "document:unassigned": (False, None),
    "document:nometa":     (None,  "bio"),   # no koi_memories row at all
    "document:unlabeled":  ("unset", "bio"), # row exists, privacy never set (false by default)
    WITHHELD:              (False, "bio"),
    "mediawiki:wikiX":     (False, "bio"),
}


async def _seed(conn):
    await conn.execute(DDL)
    for rid, (priv, fld) in SOURCES.items():
        if priv == "unset":
            await conn.execute("INSERT INTO koi_memories(rid) VALUES($1)", rid)
        elif priv is not None:
            await conn.execute(
                "INSERT INTO koi_memories(rid,is_private,access_source) VALUES($1,$2,$3)",
                rid, priv, "test-private" if priv else "test-public")
        if fld is not None:
            await conn.execute(
                "INSERT INTO document_field_membership(document_rid,field_id) VALUES($1,$2)", rid, fld)
    # moves
    global M
    M = {k: uuid.uuid4() for k in ("m_pubA", "m_privB", "m_withheld", "m_sessOK", "m_sessEx")}
    await conn.executemany(
        "INSERT INTO session_discourse_moves(id,source_type,source_rid,session_id) VALUES($1,$2,$3,$4)",
        [
            (M["m_pubA"], "document", "document:pubA", None),
            (M["m_privB"], "document", "document:privB", None),
            (M["m_withheld"], "document", WITHHELD, None),
            (M["m_sessOK"], "session", None, "sess-ok"),
            (M["m_sessEx"], "session", None, "sess-excluded"),
        ],
    )
    await conn.execute("INSERT INTO session_exclusions(session_id) VALUES('sess-excluded')")


async def _visible_sources(conn, scope):
    sql, params = source_visible_sql(scope, "m.rid", 1)
    rows = await conn.fetch(f"SELECT rid FROM koi_memories m WHERE {sql}", *params)
    # koi_memories lacks the 'nometa' row by design; test it separately via moves/fields
    return {r["rid"] for r in rows}


async def _visible_by_field_table(conn, scope):
    """Visibility keyed off every source that appears in field membership OR koi_memories,
    so we can see 'nometa' (which has a field row but no memory row)."""
    sql, params = source_visible_sql(scope, "s.rid", 1)
    rows = await conn.fetch(
        f"WITH s AS (SELECT rid FROM koi_memories UNION SELECT document_rid FROM document_field_membership) "
        f"SELECT rid FROM s WHERE {sql}", *params)
    return {r["rid"] for r in rows}


async def _visible_moves(conn, scope):
    sql, params = move_visible_sql(scope, "t", 1)
    rows = await conn.fetch(f"SELECT id FROM session_discourse_moves t WHERE {sql}", *params)
    inv = {v: k for k, v in M.items()}
    return {inv[r["id"]] for r in rows}


_LOOP = asyncio.new_event_loop()


def _run(coro):
    return _LOOP.run_until_complete(coro)


@pytest.fixture(scope="module")
def db():
    name = _run(_make_db())
    dsn = targets.with_database(targets.admin_dsn(), name)
    conn = _run(asyncpg.connect(dsn))
    _run(_seed(conn))
    yield conn
    _run(conn.close())
    _run(_drop_db(name))


def test_personal_sees_all_non_mediawiki_sources(db):
    v = _run(_visible_by_field_table(db, personal_scope()))
    assert "document:pubA" in v and "document:privB" in v and "document:nometa" in v
    assert WITHHELD in v                      # personal keeps the withheld source
    assert "mediawiki:wikiX" not in v         # held corpus excluded in every view


def test_field_sees_only_public_in_its_field(db):
    v = _run(_visible_by_field_table(db, field_scope({"bio"})))
    assert v == {"document:pubA"}             # privB private, pubC other field, unassigned/nometa/withheld excluded


def test_field_missing_metadata_fails_closed(db):
    # 'nometa' is in field 'bio' but has no koi_memories row → must be withheld
    v = _run(_visible_by_field_table(db, field_scope({"bio"})))
    assert "document:nometa" not in v


def test_unlabeled_default_false_is_not_public(db):
    # is_private defaults to false; with no access_source label that is "never
    # set", not "public" (review F1). Personal still sees it.
    assert "document:unlabeled" not in _run(_visible_by_field_table(db, field_scope({"bio"})))
    assert "document:unlabeled" not in _run(_visible_by_field_table(
        db, field_scope({"bio"}, sources={"document:unlabeled"})))
    assert "document:unlabeled" in _run(_visible_by_field_table(db, personal_scope()))


def test_field_include_unassigned_adds_only_public_unassigned(db):
    v = _run(_visible_by_field_table(db, field_scope({"bio"}, include_unassigned=True)))
    assert "document:unassigned" in v
    assert "document:nometa" not in v         # still fails closed


def test_withheld_beats_explicit_source_grant(db):
    v = _run(_visible_by_field_table(db, field_scope({"bio"}, sources=[WITHHELD])))
    assert WITHHELD not in v                   # grant cannot bypass the hard deny
    assert "document:pubA" in v


def test_public_sees_nothing_without_grant(db):
    v = _run(_visible_by_field_table(db, public_scope()))
    assert v == set()


def test_public_with_source_grant_sees_only_that_public_source(db):
    v = _run(_visible_by_field_table(db, public_scope(sources=["document:pubA"])))
    assert v == {"document:pubA"}
    v2 = _run(_visible_by_field_table(db, public_scope(sources=["document:privB"])))
    assert v2 == set()                         # grant does not override privacy


def test_personal_sees_sessions_minus_exclusions(db):
    v = _run(_visible_moves(db, personal_scope()))
    assert {"m_pubA", "m_privB", "m_withheld", "m_sessOK"} <= v
    assert "m_sessEx" not in v                 # excluded session hidden even from personal


def test_field_sees_no_sessions_and_only_public_doc_moves(db):
    v = _run(_visible_moves(db, field_scope({"bio"})))
    assert v == {"m_pubA"}                     # no session moves, no private, no withheld


def test_entity_predicate_excludes_node_private_in_every_scope(db):
    _run(db.execute("DELETE FROM entity_registry"))
    _run(db.execute("DELETE FROM document_entity_links"))
    _run(db.execute("INSERT INTO entity_registry VALUES ('e:pub', false), ('e:priv', true),"
                    " ('e:privonly', false)"))
    # e:pub is mentioned in a public in-field doc, e:privonly only in a private one,
    # e:priv in the public doc but node_private.
    _run(db.execute("INSERT INTO document_entity_links VALUES ('document:pubA','e:pub'),"
                    " ('document:privB','e:privonly'), ('document:pubA','e:priv')"))
    expect = {
        "personal": {"e:pub", "e:privonly"},   # today: node_private only
        "field": {"e:pub"},                    # needs a mention in a visible source
        "public": set(),                       # no grant, no visible mention
    }
    for scope in (personal_scope(), field_scope({"bio"}), public_scope()):
        sql, params = entity_visible_sql(scope, "er")
        rows = _run(db.fetch(f"SELECT fuseki_uri FROM entity_registry er WHERE {sql}", *params))
        assert {r["fuseki_uri"] for r in rows} == expect[scope.view], scope.view
