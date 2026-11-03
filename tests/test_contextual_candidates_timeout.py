"""Contextual candidates (POST /get-contextual-candidates): the two fixes of a5e1990.

1. The vault-people fallback leaves out people who already have a document link. It does
   so with NOT EXISTS, which Postgres always plans as an anti join (on the live database,
   over its idx_del_entity_uri index, which no migration here creates; elsewhere a hash anti
   join). The NOT IN form it replaced was planned as a SubPlan over the whole of
   document_entity_links and took about 76 s per call on the live database (2026-10-13),
   past the client's 30 s limit.
   document_entity_links.entity_uri is NOT NULL, so both forms return the same rows. The
   difference is the plan, which the first test checks on the exact SQL the function sends.
2. An attendee name that matches more than ATTENDEE_DOC_CAP documents carries no
   disambiguation signal (a node operator linked to thousands of documents pulled about a
   thousand candidates into every meeting). It is skipped and reported in skipped_attendees.

Every test runs against KOI_TEST_DATABASE_URL inside a transaction that is
rolled back, so nothing persists.
"""

from __future__ import annotations

import uuid

import asyncpg
import pytest
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()   # nothing this file does ever persists
    await c.close()


@pytest.fixture
def pia():
    from api import personal_ingest_api
    return personal_ingest_api


class Recording:
    """The connection, recording the SQL of each fetch the function sends."""

    def __init__(self, conn):
        self._conn, self.sql = conn, []

    async def fetch(self, sql, *args):
        self.sql.append(sql)
        return await self._conn.fetch(sql, *args)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class Pool:
    """db_pool for the endpoint: acquire() hands out the test connection."""

    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class Acquired:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False
        return Acquired()


async def _person(conn, tag, name, *, vault=False):
    uri = f"orn:personal-koi.entity:person-ctx-{tag}-{name.lower().split()[0]}"
    await conn.execute("INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, "
                       "node_private) VALUES ($1, $2, 'Person', $3, false)", uri, name, name.lower())
    if vault:
        await conn.execute("INSERT INTO entity_rid_mappings (vault_rid, vault_path, canonical_uri) "
                           "VALUES ($1, $2, $3)", f"vault:ctx-{tag}-{name}", f"People/{name}.md", uri)
    return uri


async def _link(conn, document_rid, uri):
    await conn.execute("INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1, $2)",
                       document_rid, uri)


async def test_the_vault_people_fallback_is_an_anti_join_and_keeps_its_rows(conn, pia):
    tag = uuid.uuid4().hex[:8]
    unlinked = await _person(conn, tag, f"Vaultonly {tag}", vault=True)
    linked = await _person(conn, tag, f"Vaultlinked {tag}", vault=True)
    await _link(conn, f"document:ctx-{tag}-1", linked)
    rec = Recording(conn)
    out = await pia.get_contextual_candidates_internal(rec, entity_types=["Person"])
    assert linked not in {c["uri"] for c in out["candidates"]}     # a linked person is not a fallback
    (sql,) = [s for s in rec.sql if "entity_rid_mappings" in s]
    # The rows: the function's own query without its LIMIT 50 (which picks 50 of however many
    # the test database holds), restricted to the two fixture people. Exactly the unlinked one.
    assert sql.count("LIMIT 50") == 1
    rows = await conn.fetch(f"SELECT fuseki_uri FROM ({sql.replace('LIMIT 50', '')}) q "
                            "WHERE fuseki_uri = ANY($1::text[])", [unlinked, linked])
    assert [r["fuseki_uri"] for r in rows] == [unlinked]
    plan = "\n".join(r[0] for r in await conn.fetch("EXPLAIN (COSTS OFF) " + sql))
    assert "Anti Join" in plan and "SubPlan" not in plan, plan


async def test_an_attendee_in_more_documents_than_the_cap_is_skipped_and_reported(conn, pia, monkeypatch):
    monkeypatch.setattr(pia, "ATTENDEE_DOC_CAP", 3)
    tag = uuid.uuid4().hex[:8]
    busy = await _person(conn, tag, f"Busyattendee {tag}")         # 4 documents: over the cap
    rare = await _person(conn, tag, f"Rareattendee {tag}")         # 3 documents: at the cap, kept
    with_busy = await _person(conn, tag, f"Withbusy {tag}")
    with_rare = await _person(conn, tag, f"Withrare {tag}")
    busy_docs = [f"document:ctx-{tag}-busy-{i}" for i in range(4)]
    rare_docs = [f"document:ctx-{tag}-rare-{i}" for i in range(3)]
    for doc in busy_docs:
        await _link(conn, doc, busy)
        await _link(conn, doc, with_busy)
    for doc in rare_docs:
        await _link(conn, doc, rare)
        await _link(conn, doc, with_rare)
    attendees = [f"Busyattendee {tag}", f"Rareattendee {tag}"]
    out = await pia.get_contextual_candidates_internal(conn, attendees=attendees, entity_types=["Person"])
    assert out["skipped_attendees"] == [f"Busyattendee {tag}"]
    assert sorted(out["related_documents"]) == sorted(rare_docs)
    assert out["context_types"] == ["attendees"]
    uris = {c["uri"] for c in out["candidates"]}
    assert {rare, with_rare} <= uris and not ({busy, with_busy} & uris)
    # The endpoint passes the report through.
    monkeypatch.setattr(pia, "db_pool", Pool(conn))
    resp = await pia.get_contextual_candidates(pia.ContextualCandidatesRequest(attendees=attendees,
                                                                              entity_types=["Person"]))
    assert resp["skipped_attendees"] == [f"Busyattendee {tag}"]
    assert resp["related_document_count"] == 3 and resp["candidate_count"] == len(resp["candidates"])


async def test_no_attendee_over_the_cap_skips_nothing(conn, pia, monkeypatch):
    monkeypatch.setattr(pia, "ATTENDEE_DOC_CAP", 3)
    tag = uuid.uuid4().hex[:8]
    rare = await _person(conn, tag, f"Rareattendee {tag}")
    docs = [f"document:ctx-{tag}-{i}" for i in range(2)]
    for doc in docs:
        await _link(conn, doc, rare)
    out = await pia.get_contextual_candidates_internal(conn, attendees=[f"Rareattendee {tag}"], entity_types=["Person"])
    assert out["skipped_attendees"] == [] and sorted(out["related_documents"]) == sorted(docs)
