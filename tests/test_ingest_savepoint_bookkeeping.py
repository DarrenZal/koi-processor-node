"""/ingest reports, counts and federates only entities whose savepoint committed.

Needs KOI_TEST_DATABASE_URL; skips otherwise. Everything rolls back with the fixture.
"""
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
    await tx.rollback()
    await c.close()


class _OneConnPool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *_exc):
                return False

        return _Ctx()


@pytest.fixture
def env(monkeypatch, conn):
    from api import federation_events
    from api import personal_ingest_api as pia

    events = []

    async def capture_event(domain, event_type, rid, payload, **_kw):
        events.append((domain, event_type, rid))

    async def no_outbox(*_a, **_kw):
        return None

    async def no_receipt(*_a, **_kw):
        raise RuntimeError("receipts are not under test")

    monkeypatch.setattr(pia, "db_pool", _OneConnPool(conn))
    monkeypatch.setattr(pia, "embedding_provider", None, raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    monkeypatch.setattr(pia, "_log_resolver_decision", lambda *a, **kw: None)
    monkeypatch.setattr(pia, "create_receipt", no_receipt)
    monkeypatch.setattr(pia, "enqueue_outbox", no_outbox)
    monkeypatch.setattr(federation_events, "emit_domain_event", capture_event)
    return events


async def _existing(conn, text=None):
    """A live entity, with an embedding where the schema requires one."""
    text = text or f"Bookkeeping Existing {uuid.uuid4().hex[:10]}"
    uri = f"orn:personal-koi.entity:concept-{text.lower().replace(' ', '-')}"
    size = await conn.fetchval(
        "SELECT atttypmod FROM pg_attribute WHERE attrelid = 'entity_registry'::regclass AND attname = 'embedding'")
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, embedding) "
        "VALUES ($1, $2, 'Concept', $3, array_fill(0.001::real, ARRAY[$4::int])::vector)",
        uri, text, text.lower(), size)
    return uri, text


async def _ingest(entities):
    from api import personal_ingest_api as pia

    return await pia.ingest_extraction(pia.IngestRequest(
        document_rid=f"orn:test.doc:bookkeeping-{uuid.uuid4().hex[:10]}",
        entities=[pia.ExtractedEntity(type="Concept", **e) for e in entities], source="test"))


async def test_a_committed_entity_is_reported_and_federated(conn, env):
    uri, name = await _existing(conn)
    out = await _ingest([{"name": name, "context": "fine"}])
    assert out.success is True
    assert out.stats.resolved_entities == 1
    assert [e.uri for e in out.canonical_entities] == [uri]
    assert [r for _d, _t, r in env if _d == "entity"] == [uri]


async def test_a_failed_link_is_not_reported_as_resolved(conn, env):
    _uri, name = await _existing(conn)
    out = await _ingest([{"name": name, "context": "a\x00b"}])  # the link insert rejects the NUL byte
    assert out.success is False
    assert (out.stats.resolved_entities, out.stats.failed_entities) == (0, 1)
    assert out.canonical_entities == []
    assert env == []


async def test_a_new_entity_that_fails_is_not_listed_or_federated(conn, env):
    name = f"Bookkeeping Rolled Back {uuid.uuid4().hex[:10]}"
    out = await _ingest([{"name": name, "context": "a\x00b"}])  # storing it rejects the NUL byte
    assert out.success is False
    assert (out.stats.new_entities, out.stats.failed_entities) == (0, 1)
    assert out.canonical_entities == []
    assert env == []
    assert await conn.fetchval("SELECT count(*) FROM entity_registry WHERE entity_text = $1", name) == 0


async def test_one_failure_does_not_disturb_the_others(conn, env):
    good_uri, good = await _existing(conn)
    _bad_uri, bad = await _existing(conn)
    out = await _ingest([{"name": bad, "context": "a\x00b"}, {"name": good, "context": "fine"}])
    assert (out.stats.resolved_entities, out.stats.failed_entities) == (1, 1)
    assert [e.uri for e in out.canonical_entities] == [good_uri]
    assert [r for _d, _t, r in env if _d == "entity"] == [good_uri]
