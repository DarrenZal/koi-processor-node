"""Unit tests for the document_entity_link federation PUBLISHER side.

Federation Phase 1 step 9 — emit_doclink_event + the Group A / Group B wiring
contract.

The publisher half:
  - emit_doclink_event() builds a document_entity_link NEW event with a
    publisher-supplied mention_delta and a fresh _federation_event_id.
  - Group A sites (always-+1 upserts) emit delta=1 unconditionally.
  - Group B sites (ON CONFLICT DO NOTHING) emit delta=1 ONLY when a row was
    actually inserted — doclink_row_created() reads the execute() status.

The roundtrip test feeds a publisher payload through 2d's _apply_doclink to
pin the publisher↔subscriber contract; it needs a DB and uses the same
rollback-per-test fixture as test_domain_event_handlers.py.
"""

import uuid

import asyncpg
import pytest

import api.federation_events as fe
from api.federation_events import (
    doclink_rid,
    doclink_row_created,
    emit_doclink_event,
)
from tests import koi_test_targets as targets

TEST_SOURCE_NODE = "orn:koi-net.node:test-peer+aaaa"


@pytest.fixture
def anyio_backend():
    return "asyncio"


class FakeQueue:
    """Captures .add() kwargs so tests can inspect what was queued."""

    def __init__(self):
        self.calls = []

    async def add(self, **kwargs):
        self.calls.append(kwargs)


@pytest.fixture
def fake_queue(monkeypatch):
    """Wire a capturing queue + enable knowledge federation. Restores the
    real module-global queue on teardown so other tests are unaffected."""
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
    original = fe._event_queue
    q = FakeQueue()
    fe.set_event_queue(q)
    yield q
    fe.set_event_queue(original)


# ---------------------------------------------------------------------------
# doclink_row_created — the Group B rows-affected guard
# ---------------------------------------------------------------------------


def test_doclink_row_created_parses_status_strings():
    assert doclink_row_created("INSERT 0 1") is True
    assert doclink_row_created("INSERT 0 0") is False
    assert doclink_row_created("") is False
    assert doclink_row_created(None) is False


# ---------------------------------------------------------------------------
# emit_doclink_event — payload shape
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_emit_doclink_event_payload_shape(fake_queue):
    doc = f"orn:doc:{uuid.uuid4()}"
    ent = f"orn:entity:{uuid.uuid4()}"

    await emit_doclink_event(doc, ent, 1, context="test ctx")

    assert len(fake_queue.calls) == 1
    call = fake_queue.calls[0]
    assert call["event_type"] == "NEW"
    assert call["rid"] == doclink_rid(doc, ent)
    assert call["rid"].startswith("orn:personal-koi.doclink:")
    # event_id is the caller-supplied federation event id, a valid UUID.
    uuid.UUID(call["event_id"])

    contents = call["contents"]
    assert contents["_koi_domain"] == "document_entity_link"
    payload = contents["payload"]
    assert payload["document_rid"] == doc
    assert payload["entity_uri"] == ent
    assert payload["mention_delta"] == 1
    assert payload["context"] == "test ctx"
    # emit_domain_event injects the federation event id into the payload so the
    # subscriber can dedup without depending on queue-level fields.
    assert payload["_federation_event_id"] == call["event_id"]


# ---------------------------------------------------------------------------
# Group A — always-+1 sites emit delta=1 unconditionally
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_group_a_site_emits_delta_1(fake_queue):
    """Group A sites (e.g. github_sensor, personal_ingest_api) run an upsert
    that always moves mention_count by exactly 1, so they pass a literal 1."""
    doc = f"orn:doc:{uuid.uuid4()}"
    ent = f"orn:entity:{uuid.uuid4()}"

    # Mirrors the Group A wiring: after a successful upsert, emit delta=1.
    await emit_doclink_event(doc, ent, 1)

    assert len(fake_queue.calls) == 1
    assert fake_queue.calls[0]["contents"]["payload"]["mention_delta"] == 1


# ---------------------------------------------------------------------------
# Group B — ON CONFLICT DO NOTHING sites gate the emit on rows-affected
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_group_b_site_emits_on_insert(fake_queue):
    """Group B site, row actually created ("INSERT 0 1") → emit delta=1."""
    doc = f"orn:doc:{uuid.uuid4()}"
    ent = f"orn:entity:{uuid.uuid4()}"

    status = "INSERT 0 1"  # simulated conn.execute() return for a fresh insert
    if doclink_row_created(status):
        await emit_doclink_event(doc, ent, 1)

    assert len(fake_queue.calls) == 1
    assert fake_queue.calls[0]["contents"]["payload"]["mention_delta"] == 1


@pytest.mark.anyio
async def test_group_b_site_silent_on_conflict(fake_queue):
    """Group B site, conflict hit ("INSERT 0 0") → emit NOTHING.

    This is the critical publisher-side double-count guard: the publisher
    state did not move, so there is nothing to federate. Emitting delta=1
    here would tell the additive subscriber to increment a count the
    publisher never changed.
    """
    doc = f"orn:doc:{uuid.uuid4()}"
    ent = f"orn:entity:{uuid.uuid4()}"

    status = "INSERT 0 0"  # ON CONFLICT DO NOTHING hit — no row created
    if doclink_row_created(status):
        await emit_doclink_event(doc, ent, 1)

    assert fake_queue.calls == []


# ---------------------------------------------------------------------------
# Feature flag — KOI_FEDERATE_KNOWLEDGE off → zero emits
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_flag_off_no_emit(monkeypatch):
    monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
    original = fe._event_queue
    q = FakeQueue()
    fe.set_event_queue(q)
    try:
        await emit_doclink_event(
            f"orn:doc:{uuid.uuid4()}", f"orn:entity:{uuid.uuid4()}", 1
        )
        assert q.calls == []
    finally:
        fe.set_event_queue(original)


# ---------------------------------------------------------------------------
# Publisher ↔ subscriber contract — roundtrip through _apply_doclink (needs DB)
# ---------------------------------------------------------------------------


@pytest.fixture
async def conn():
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


@pytest.mark.anyio
async def test_emit_doclink_roundtrips_through_apply_doclink(fake_queue, conn):
    """Build a payload via emit_doclink_event, feed it through 2d's
    _apply_doclink, and assert the doclink lands with the right count."""
    from api.domain_event_handlers import _apply_doclink

    doc = f"orn:doc:{uuid.uuid4()}"
    ent = f"orn:entity:{uuid.uuid4()}"

    await emit_doclink_event(doc, ent, 1, context="roundtrip ctx")
    call = fake_queue.calls[0]
    payload = call["contents"]["payload"]
    rid = call["rid"]

    await _apply_doclink(conn, rid, "NEW", payload, TEST_SOURCE_NODE)

    row = await conn.fetchrow(
        "SELECT mention_count, context FROM document_entity_links "
        "WHERE document_rid = $1 AND entity_uri = $2",
        doc, ent,
    )
    assert row is not None
    assert row["mention_count"] == 1
    assert row["context"] == "roundtrip ctx"

    # The injected _federation_event_id was recorded for subscriber dedup.
    applied = await conn.fetchval(
        "SELECT 1 FROM federation_applied_events "
        "WHERE domain = 'document_entity_link' AND event_id = $1::uuid",
        payload["_federation_event_id"],
    )
    assert applied == 1
