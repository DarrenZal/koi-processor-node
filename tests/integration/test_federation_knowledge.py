"""Federation Phase 4 — end-to-end integration test for knowledge-domain federation.

Federation plan, Phase 4 step 20.

The 45 Phase-1 unit tests prove each handler in isolation. This file proves the
*wiring*: an event emitted on node A actually lands on node B through the real
event transport + poller dispatch path.

Harness:
  - One real Postgres (`personal_koi`), one connection, one transaction rolled
    back at teardown. Same pattern as tests/test_federation_bridge.py.
  - "Node A" and "node B" are just distinct koi-net RIDs. No second database is
    needed: the emit path writes ONLY koi_net_events; the apply path writes ONLY
    knowledge_episodes / knowledge_facts / document_entity_links /
    federation_applied_events. The two footprints are disjoint, so a shared DB
    does not let A's state masquerade as B's.
  - Transport is exercised at the function level: `emit_domain_event` writes the
    queue row, the REAL `EventQueue.poll` moves it, the REAL
    `KOIPoller._process_event` dispatches it to `apply_domain_event`. The only
    thing skipped vs. production is the literal httpx call + envelope signing —
    `_poll_peer`'s HTTP shell. Handlers and the poller dispatch are NOT mocked.

Note on scenario 2 ("FORGET soft-deletes a fact"): the plan's wording assumes
`_apply_knowledge_fact` has FORGET semantics. It does not. As of this branch:
  - No publisher emits a FORGET event for any knowledge domain.
  - `apply_domain_event` routes FORGET to `_handle_forget`, whose table map has
    no knowledge-domain entry — so a FORGET knowledge_* event is a silent no-op.
  - The REAL fact soft-delete path is a `knowledge_episode` UPDATE whose bundled
    fact now carries a non-null `valid_to`; `_insert_fact`'s
    `ON CONFLICT (id) DO UPDATE SET valid_to = EXCLUDED.valid_to` applies it.
Scenario 2 below therefore tests that real path. A literal FORGET no-op is
asserted alongside so the current behavior is pinned, not hidden.
"""

import uuid

import asyncpg
import pytest

from api.domain_event_handlers import apply_domain_event  # noqa: F401  (import-smoke)
from api.event_queue import EventQueue
from api.federation_events import (
    emit_doclink_event,
    emit_domain_event,
    set_event_queue,
)
from api.koi_poller import KOIPoller
from tests import koi_test_targets as targets


NODE_A = "orn:koi-net.node:test-fed-a+aaaa"
NODE_B = "orn:koi-net.node:test-fed-b+bbbb"


# ---------------------------------------------------------------------------
# Fixtures — single conn, rolled-back transaction (tests/test_federation_bridge
# pattern). koi_net_events is pre-scoped so the real EventQueue.poll returns only
# events this test emits (a working database can hold thousands of other events,
# and poll()'s LIMIT 50 would never reach the test's rows).
# ---------------------------------------------------------------------------

class _SingleConnPool:
    """Wraps a single asyncpg.Connection to quack like asyncpg.Pool."""

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


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    # Mark every currently-live event delivered to NODE_B so the real poll()
    # below only ever surfaces events this test emits. Rolled back at teardown —
    # production koi_net_events is untouched.
    await _conn.execute(
        """
        UPDATE koi_net_events
        SET delivered_to = array_append(delivered_to, $1)
        WHERE NOT ($1 = ANY(delivered_to)) AND expires_at > NOW()
        """,
        NODE_B,
    )
    yield _conn
    await tx.rollback()
    await _conn.close()


@pytest.fixture
def pool(conn):
    return _SingleConnPool(conn)


@pytest.fixture
def node_a(pool):
    """Wire the federation_events module singleton to node A's queue.

    emit_domain_event / emit_doclink_event publish through this singleton; the
    EventQueue is constructed with NODE_A as its node_rid so emitted rows carry
    source_node = NODE_A.
    """
    eq_a = EventQueue(pool, NODE_A)
    set_event_queue(eq_a)
    yield eq_a
    set_event_queue(None)


# ---------------------------------------------------------------------------
# Transport + apply helpers — the real code path, HTTP shell removed.
# ---------------------------------------------------------------------------

def _make_poller(pool):
    """A real KOIPoller with just enough wired to run _process_event."""
    poller = KOIPoller.__new__(KOIPoller)
    poller.pool = pool
    poller.vault_sync = None
    return poller


async def _dispatch(poller, ev):
    """Feed one polled event dict through the real poller dispatch path."""
    await poller._process_event(
        rid=ev["rid"],
        event_type=ev["event_type"],
        contents=ev["contents"],
        manifest=ev["manifest"],
        source_node=ev["source_node"],
        event_id=ev["event_id"],
    )


async def _federate(pool, *, source=NODE_A, dest=NODE_B):
    """Poll dest's view of the queue and dispatch every event from `source`.

    Returns the list of event dicts that were dispatched (so a caller can
    re-dispatch them — see scenario 4's replay).
    """
    eq_dest = EventQueue(pool, dest)
    events = await eq_dest.poll(dest)
    mine = [e for e in events if e["source_node"] == source]
    poller = _make_poller(pool)
    for ev in mine:
        await _dispatch(poller, ev)
    return mine


# ---------------------------------------------------------------------------
# Payload builders — shapes mirror api/routers/knowledge_router.py's emit site
# and tests/unit/test_domain_event_handlers.py::_episode_payload.
# ---------------------------------------------------------------------------

def _fact(index, *, valid_to=None):
    return {
        "id": str(uuid.uuid4()),
        "subject_uri": f"orn:entity:subj-{index}",
        "predicate": "TEST_PRED",
        "object_uri": f"orn:entity:obj-{index}",
        "object_literal": None,
        "fact_text": f"integration fact {index}",
        "valid_from": "2026-05-14T00:00:00Z",
        "valid_to": valid_to,
        "created_at": "2026-05-14T00:00:00Z",
        "group_id": "personal",
        "source_node_rid": NODE_A,
        "turn_range_start": index,
        "turn_range_end": index + 1,
        "embedding_column": None,
        "embedding_value": None,
    }


def _episode_payload(episode_id, facts):
    return {
        "id": episode_id,
        "name": "integration test episode",
        "content": "integration test content",
        "source_description": "phase 4 integration test",
        "source_document": "test.md",
        "group_id": "personal",
        "valid_at": "2026-05-14T00:00:00Z",
        "created_at": "2026-05-14T00:00:00Z",
        "metadata": {"phase4": True},
        "facts": facts,
    }


def _episode_rid(episode_id):
    return f"orn:personal-koi.knowledge-episode:{episode_id}"


# ---------------------------------------------------------------------------
# Scenario 1 — episode + facts federate
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.integration
@pytest.mark.federation
async def test_episode_and_facts_federate(conn, pool, node_a, monkeypatch):
    """Episode + 3 facts written on A land on B with matching UUIDs."""
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")

    ep_id = str(uuid.uuid4())
    ev_id = str(uuid.uuid4())
    facts = [_fact(i) for i in range(3)]
    payload = _episode_payload(ep_id, facts)

    await emit_domain_event(
        "knowledge_episode", "NEW", _episode_rid(ep_id), payload,
        payload_event_id=ev_id,
    )

    dispatched = await _federate(pool)
    assert len(dispatched) == 1, "exactly one knowledge_episode event should transit A->B"

    ep = await conn.fetchrow(
        "SELECT name, content, group_id FROM knowledge_episodes WHERE id = $1::uuid",
        ep_id,
    )
    assert ep is not None, "episode should exist on node B"
    assert ep["name"] == "integration test episode"
    assert ep["content"] == "integration test content"
    assert ep["group_id"] == "personal"

    rows = await conn.fetch(
        "SELECT id::text AS id FROM knowledge_facts WHERE episode_id = $1::uuid",
        ep_id,
    )
    assert len(rows) == 3, "all 3 bundled facts should land on node B"
    assert {r["id"] for r in rows} == {f["id"] for f in facts}, "fact UUIDs must match originator"

    applied = await conn.fetchval(
        "SELECT 1 FROM federation_applied_events "
        "WHERE domain = 'knowledge_episode' AND event_id = $1::uuid",
        ev_id,
    )
    assert applied == 1, "idempotency row should be recorded after apply"


# ---------------------------------------------------------------------------
# Scenario 2 — fact soft-delete federates (real path: episode UPDATE w/ valid_to)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.integration
@pytest.mark.federation
async def test_fact_soft_delete_federates(conn, pool, node_a, monkeypatch):
    """A fact's valid_to set on A propagates to B; the row is preserved, not deleted.

    See module docstring: the codebase has no FORGET emit for knowledge domains,
    so the genuine soft-delete path is a knowledge_episode UPDATE re-emitting the
    fact with a non-null valid_to.
    """
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")

    ep_id = str(uuid.uuid4())
    fact = _fact(0)
    fact_id = fact["id"]

    # Initial NEW federation — fact is live (valid_to NULL).
    await emit_domain_event(
        "knowledge_episode", "NEW", _episode_rid(ep_id),
        _episode_payload(ep_id, [fact]),
        payload_event_id=str(uuid.uuid4()),
    )
    await _federate(pool)

    pre = await conn.fetchrow(
        "SELECT valid_to FROM knowledge_facts WHERE id = $1::uuid", fact_id,
    )
    assert pre is not None and pre["valid_to"] is None, "fact should land live"

    # Soft-delete: re-emit the episode as UPDATE with the same fact carrying valid_to.
    retired_fact = {**fact, "valid_to": "2026-05-14T12:00:00Z"}
    await emit_domain_event(
        "knowledge_episode", "UPDATE", _episode_rid(ep_id),
        _episode_payload(ep_id, [retired_fact]),
        payload_event_id=str(uuid.uuid4()),
    )
    dispatched = await _federate(pool)
    assert len(dispatched) == 1, "the UPDATE event should transit A->B"

    post = await conn.fetchrow(
        "SELECT valid_to FROM knowledge_facts WHERE id = $1::uuid", fact_id,
    )
    assert post is not None, "soft-delete must PRESERVE the row, not delete it"
    assert post["valid_to"] is not None, "valid_to should be set after the UPDATE federates"

    # Pin current behavior: a literal FORGET knowledge_fact event is inert
    # (no _handle_forget mapping for knowledge domains). Not a bug to fix here —
    # documented for the plan. The row must be untouched by the no-op.
    poller = _make_poller(pool)
    await poller._process_event(
        rid=f"orn:personal-koi.knowledge-fact:{fact_id}",
        event_type="FORGET",
        contents={"_koi_domain": "knowledge_fact", "payload": {"id": fact_id}},
        manifest=None,
        source_node=NODE_A,
        event_id=str(uuid.uuid4()),
    )
    still = await conn.fetchval(
        "SELECT 1 FROM knowledge_facts WHERE id = $1::uuid", fact_id,
    )
    assert still == 1, "literal FORGET is currently a no-op — row remains"


# ---------------------------------------------------------------------------
# Scenario 3 — doclink federates with correct mention_count
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.integration
@pytest.mark.federation
async def test_doclink_federates(conn, pool, node_a, monkeypatch):
    """A document_entity_link emitted on A lands on B with matching mention_count."""
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")

    uid = uuid.uuid4().hex[:12]
    document_rid = f"orn:personal-koi.document:fed-test-{uid}"
    entity_uri = f"orn:personal-koi.entity:concept-{uid}"

    await emit_doclink_event(document_rid, entity_uri, 1, context="phase4 ctx")

    dispatched = await _federate(pool)
    assert len(dispatched) == 1, "exactly one doclink event should transit A->B"

    row = await conn.fetchrow(
        "SELECT mention_count, context FROM document_entity_links "
        "WHERE document_rid = $1 AND entity_uri = $2",
        document_rid, entity_uri,
    )
    assert row is not None, "doclink should exist on node B"
    assert row["mention_count"] == 1, "mention_count should match the emitted delta"
    assert row["context"] == "phase4 ctx"


# ---------------------------------------------------------------------------
# Scenario 4 — duplicate delivery does NOT double-increment (load-bearing)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.integration
@pytest.mark.federation
async def test_duplicate_doclink_delivery_does_not_double_increment(
    conn, pool, node_a, monkeypatch,
):
    """Delivering the SAME doclink event to B twice increments mention_count once.

    This is the end-to-end proof of the federation_applied_events idempotency
    design — _apply_doclink's check-first-apply ordering. The replay reuses the
    identical event dict (same _federation_event_id), exactly the redelivery a
    poll-without-confirm cycle would produce.
    """
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")

    uid = uuid.uuid4().hex[:12]
    document_rid = f"orn:personal-koi.document:dup-test-{uid}"
    entity_uri = f"orn:personal-koi.entity:concept-{uid}"

    # One emit -> one queue row -> one event_id.
    await emit_doclink_event(document_rid, entity_uri, 1)

    eq_b = EventQueue(pool, NODE_B)
    events = [e for e in await eq_b.poll(NODE_B) if e["source_node"] == NODE_A]
    assert len(events) == 1, "exactly one doclink event should be queued"
    ev = events[0]

    poller = _make_poller(pool)
    await _dispatch(poller, ev)   # first delivery
    await _dispatch(poller, ev)   # redelivery of the identical event

    row = await conn.fetchrow(
        "SELECT mention_count FROM document_entity_links "
        "WHERE document_rid = $1 AND entity_uri = $2",
        document_rid, entity_uri,
    )
    assert row is not None, "doclink should exist on node B"
    assert row["mention_count"] == 1, (
        "duplicate delivery must NOT double-increment — federation_applied_events "
        "idempotency guard failed end-to-end"
    )


# ---------------------------------------------------------------------------
# Scenario 5 — flag-off is inert
# ---------------------------------------------------------------------------

@pytest.mark.anyio
@pytest.mark.integration
@pytest.mark.federation
async def test_flag_off_is_inert(conn, pool, node_a, monkeypatch):
    """With KOI_FEDERATE_KNOWLEDGE unset, an episode write on A produces nothing.

    The publish-side gate in emit_domain_event drops the event before it reaches
    the queue — so node B sees zero events and zero rows.
    """
    monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)

    ep_id = str(uuid.uuid4())
    ev_id = str(uuid.uuid4())
    payload = _episode_payload(ep_id, [_fact(0)])

    await emit_domain_event(
        "knowledge_episode", "NEW", _episode_rid(ep_id), payload,
        payload_event_id=ev_id,
    )

    # Nothing should have been queued from node A.
    eq_b = EventQueue(pool, NODE_B)
    events = [e for e in await eq_b.poll(NODE_B) if e["source_node"] == NODE_A]
    assert events == [], "flag-off emit must not queue any event"

    # And nothing should exist on node B.
    assert await conn.fetchval(
        "SELECT COUNT(*) FROM knowledge_episodes WHERE id = $1::uuid", ep_id,
    ) == 0, "no episode row should appear with the flag off"
    assert await conn.fetchval(
        "SELECT COUNT(*) FROM federation_applied_events WHERE event_id = $1::uuid", ev_id,
    ) == 0, "no idempotency row should appear with the flag off"
