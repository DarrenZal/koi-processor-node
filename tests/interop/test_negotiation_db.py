"""Edge negotiation and first contact against a real schema (api/koi_net_negotiation.py).

Run by tests/interop/run_gate.py against a throwaway clone of personal_koi
(`--db-tests`, and by default in every gate run). Collected only when KOI_TEST_DATABASE_URL
is set (tests/interop/conftest.py), so it never skips: when it runs, it has a database.

The gate proves the paths a stock node exercises (approval, operator approval, our own
subscription). These are the paths it does not: refusals, an approval we never proposed,
FORGET, and the queue rules a partial node and a narrow WEBHOOK edge depend on. Peers here
are partial nodes, so everything we send them lands in koi_net_events and can be read back.
"""
from __future__ import annotations

import json
import uuid

import asyncpg
import pytest
import pytest_asyncio

from api import koi_net_negotiation as neg
from api.event_queue import EventQueue, own_node_bundle
from api.koi_protocol import NodeProfile, NodeProvides
from api.node_identity import DEFAULT_EVENT_TYPES, DEFAULT_STATE_TYPES
from tests import koi_test_targets as targets


# pytest.ini sets asyncio_mode = strict: every async test and fixture is marked explicitly.
pytestmark = pytest.mark.asyncio


def _node(name: str) -> str:
    return f"orn:koi-net.node:conformance-{name}+{uuid.uuid4().hex}{uuid.uuid4().hex}"


@pytest_asyncio.fixture
async def pool():
    url = targets.configured("KOI_TEST_DATABASE_URL")
    if not url:
        pytest.fail("KOI_TEST_DATABASE_URL is not set: run through tests/interop/run_gate.py")
    p = await asyncpg.create_pool(url, min_size=1, max_size=4)
    async with p.acquire() as conn:
        name = await conn.fetchval("SELECT current_database()")
    if "test" not in name.split("_"):
        pytest.fail(f"refusing database {name!r}")
    yield p
    await p.close()


@pytest_asyncio.fixture
async def world(pool, monkeypatch):
    """Our node, one partial peer, and a negotiator; rows removed afterwards."""
    monkeypatch.delenv("KOI_NET_AUTO_APPROVE_EDGES", raising=False)
    monkeypatch.delenv("KOI_NET_RID_TYPES_OF_INTEREST", raising=False)
    ours, peer = _node("ours"), _node("peer")
    profile = NodeProfile(node_rid=ours, node_name="conformance-ours", node_type="FULL",
                          base_url="http://127.0.0.1:9/koi-net", public_key="MFkw-ours",
                          provides=NodeProvides(event=list(DEFAULT_EVENT_TYPES), state=list(DEFAULT_STATE_TYPES)))
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status)
               VALUES ($1, 'conformance-peer', 'PARTIAL', NULL, 'MFkw-peer', 'active')""", peer)
    eq = EventQueue(pool, ours)
    n = neg.Negotiator(pool=pool, own_rid=ours, own_profile=profile, private_key=None, event_queue=eq,
                       own_bundle=lambda: own_node_bundle(profile))
    w = {"pool": pool, "ours": ours, "peer": peer, "eq": eq, "neg": n, "profile": profile}
    yield w
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM koi_net_events WHERE target_node = $1 OR source_node = ANY($2::text[])", peer, [ours, peer])
        await conn.execute("DELETE FROM koi_net_edges WHERE source_node = ANY($1::text[]) OR target_node = ANY($1::text[])", [ours, peer])
        await conn.execute("DELETE FROM koi_net_nodes WHERE node_rid = $1", peer)


async def _queued_for(w):
    async with w["pool"].acquire() as conn:
        rows = await conn.fetch("SELECT event_type, rid, contents FROM koi_net_events WHERE target_node = $1 ORDER BY id", w["peer"])
    return [(r["event_type"], r["rid"], json.loads(r["contents"]) if r["contents"] else None) for r in rows]


async def _edges(w):
    async with w["pool"].acquire() as conn:
        rows = await conn.fetch(
            "SELECT edge_rid, source_node, target_node, edge_type, status, rid_types FROM koi_net_edges "
            "WHERE source_node = ANY($1::text[]) AND target_node = ANY($1::text[])", [w["ours"], w["peer"]])
    return [dict(r) for r in rows]


def _proposal(w, edge_type="POLL", rid_types=("orn:koi-net.vault-file",)):
    return neg.bundle_event("NEW", neg.edge_bundle(w["ours"], w["peer"], edge_type, "PROPOSED", list(rid_types)))


async def test_a_webhook_subscription_from_a_partial_node_is_refused_with_forget(world):
    w = world
    assert await w["neg"].handle([_proposal(w, "WEBHOOK")], w["peer"]) == 1
    assert await _edges(w) == []
    assert await _queued_for(w) == [("FORGET", neg.edge_rid_for(w["ours"], w["peer"]), None)]


async def test_a_subscription_to_types_we_do_not_provide_is_refused(world):
    w = world
    await w["neg"].handle([_proposal(w, rid_types=["orn:koi-net.person-we-never-emit"])], w["peer"])
    assert await _edges(w) == []
    assert [e[0] for e in await _queued_for(w)] == ["FORGET"]


async def test_a_valid_subscription_waits_for_an_operator_by_default(world):
    w = world
    await w["neg"].handle([_proposal(w)], w["peer"])
    (edge,) = await _edges(w)
    assert (edge["edge_rid"], edge["status"], edge["edge_type"], edge["rid_types"]) == \
        (neg.edge_rid_for(w["ours"], w["peer"]), "PROPOSED", "POLL", ["orn:koi-net.vault-file"])
    assert await _queued_for(w) == []  # nothing is sent until it is approved

    # The operator approves; the subscriber is sent the approved edge.
    async with w["pool"].acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE koi_net_edges SET status = 'APPROVED' WHERE edge_rid = $1 "
            "RETURNING source_node, target_node, edge_type, status, rid_types, metadata", edge["edge_rid"])
    await w["neg"].send_edge_state(dict(row))
    ((etype, rid, contents),) = await _queued_for(w)
    assert (etype, rid, contents["status"], contents["source"], contents["target"]) == \
        ("UPDATE", edge["edge_rid"], "APPROVED", w["ours"], w["peer"])


async def test_auto_approve_sends_the_approved_edge(world, monkeypatch):
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", "true")
    await w["neg"].handle([_proposal(w)], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["APPROVED"]
    assert [(e[0], e[2]["status"]) for e in await _queued_for(w)] == [("UPDATE", "APPROVED")]


async def test_an_approval_of_an_edge_we_never_proposed_is_ignored(world):
    w = world
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    assert await w["neg"].handle([approval], w["peer"]) == 0
    assert await _edges(w) == []


async def test_by_default_a_peer_cannot_approve_our_proposal_alone(world):
    """Register 26: deferral (the default) holds our proposal for an operator, membrane off too."""
    w = world
    assert w["neg"].membrane_on() is False
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "PROPOSED", ["orn:koi-net.node"])
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    assert await w["neg"].handle([approval], w["peer"]) == 1
    async with w["pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT status, metadata FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                                  w["peer"], w["ours"])
    assert row["status"] == "PROPOSED"
    assert json.loads(row["metadata"])["peer_approved"] is True


async def test_with_deferral_off_an_approval_of_our_proposal_marks_it_approved(world):
    w = world
    w["neg"].defer_new_edges = lambda: False
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "PROPOSED", ["orn:koi-net.node"])
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    assert await w["neg"].handle([approval], w["peer"]) == 1
    assert [(e["source_node"], e["status"]) for e in await _edges(w)] == [(w["peer"], "APPROVED")]


async def test_forget_of_an_edge_revokes_it_locally(world, monkeypatch):
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", "true")
    await w["neg"].handle([_proposal(w)], w["peer"])
    await w["neg"].handle([neg.forget_event(neg.edge_rid_for(w["ours"], w["peer"]))], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["REVOKED"]


async def test_another_nodes_bundle_is_neither_handled_nor_queued(world):
    w = world
    other = _node("other")
    ev = {"rid": other, "event_type": "NEW", "manifest": None, "contents": {"node_type": "FULL"}}
    assert await w["neg"].handle([ev], w["peer"]) == 0
    async with w["pool"].acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM koi_net_events WHERE rid = $1", other) == 0


async def test_first_contact_from_a_partial_node_queues_our_bundle_for_its_poll(world):
    w = world
    hello = {"rid": w["peer"], "event_type": "NEW", "manifest": None,
             "contents": {"node_type": "PARTIAL", "provides": {"event": [], "state": []}, "public_key": "MFkw-peer"}}
    assert await w["neg"].handle([hello], w["peer"]) == 1
    assert [(e[0], e[1]) for e in await _queued_for(w)] == [("NEW", w["ours"])]

    served = await w["eq"].poll_own_protocol_objects(w["peer"])
    assert [e["rid"] for e in served] == [w["ours"]]
    assert await w["eq"].poll_own_protocol_objects(w["peer"]) == []  # delivered once


async def test_a_profile_update_never_changes_the_pinned_key(world):
    """The envelope was verified with the pinned key, so the sender holds it; a different
    public_key in its contents is not adopted (koi_net_nodes.public_key is never written here)."""
    w = world
    hello = {"rid": w["peer"], "event_type": "NEW", "contents": {"node_type": "PARTIAL", "public_key": "MFkw-other"}}
    assert await w["neg"].handle([hello], w["peer"]) == 1
    async with w["pool"].acquire() as conn:
        assert await conn.fetchval("SELECT public_key FROM koi_net_nodes WHERE node_rid = $1", w["peer"]) == "MFkw-peer"


async def test_our_own_unicast_edge_object_passes_a_narrow_edge(world, monkeypatch):
    """A partial node subscribed to vault files still receives the edge negotiation we queue for it."""
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", "true")
    await w["neg"].handle([_proposal(w)], w["peer"])
    served = await w["eq"].poll(w["peer"], 50, ["orn:koi-net.vault-file"])
    assert [(e["event_type"], e["rid"]) for e in served] == [("UPDATE", neg.edge_rid_for(w["ours"], w["peer"]))]


async def test_peek_marks_excluded_rows_delivered_so_a_webhook_edge_cannot_starve(world):
    w = world
    await w["eq"].add(event_type="NEW", rid="orn:koi-net.person:" + uuid.uuid4().hex, manifest=None,
                      contents={"x": 1}, target_node=w["peer"])
    async def delivered():
        async with w["pool"].acquire() as conn:
            return await conn.fetchval("SELECT delivered_to FROM koi_net_events WHERE target_node = $1", w["peer"])

    # peek is read-only by default (tests/test_federation_bridge.py relies on it)...
    assert await w["eq"].peek_undelivered(w["peer"], 50, []) == []
    assert w["peer"] not in (await delivered() or [])
    # ...and the WEBHOOK pusher asks for excluded rows to be marked.
    assert await w["eq"].peek_undelivered(w["peer"], 50, [], mark_excluded=True) == []
    assert w["peer"] in await delivered()


# --- fixes of 2026-11-06 -------------------------------------------------------------

async def test_concurrent_first_contacts_do_not_exhaust_a_small_pool(world):
    """A handler held a connection while a queued delivery waited for another.
    With a 2-connection pool, a few concurrent first contacts hung for good."""
    import asyncio
    w = world
    small = await asyncpg.create_pool(targets.configured("KOI_TEST_DATABASE_URL"), min_size=1, max_size=2)
    peers = [_node(f"burst{i}") for i in range(6)]
    try:
        async with small.acquire() as conn:
            for peer in peers:
                await conn.execute("INSERT INTO koi_net_nodes (node_rid, node_name, node_type, public_key, status) "
                                   "VALUES ($1, 'conformance-burst', 'PARTIAL', 'MFkw', 'active')", peer)
        n = neg.Negotiator(pool=small, own_rid=w["ours"], own_profile=w["profile"], private_key=None,
                           event_queue=EventQueue(small, w["ours"]), own_bundle=lambda: own_node_bundle(w["profile"]))
        hellos = [n.handle([{"rid": p, "event_type": "NEW", "contents": {"node_type": "PARTIAL"}}], p) for p in peers]
        assert await asyncio.wait_for(asyncio.gather(*hellos), timeout=20) == [1] * len(peers)
    finally:
        async with small.acquire() as conn:
            await conn.execute("DELETE FROM koi_net_events WHERE target_node = ANY($1::text[])", peers)
            await conn.execute("DELETE FROM koi_net_nodes WHERE node_rid = ANY($1::text[])", peers)
        await small.close()


async def test_under_the_membrane_a_peer_cannot_approve_our_proposal_alone(world):
    """Our auto-proposal approved by the peer must still wait for an operator,
    even with KOI_NET_DEFER_UNKNOWN_HANDSHAKE=false (deferral off): the membrane alone holds it."""
    w = world
    w["neg"].membrane_on = lambda: True
    w["neg"].defer_new_edges = lambda: False
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "PROPOSED", ["orn:koi-net.node"])
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    assert await w["neg"].handle([approval], w["peer"]) == 1
    async with w["pool"].acquire() as conn:
        row = await conn.fetchrow("SELECT status, metadata FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                                  w["peer"], w["ours"])
    assert row["status"] == "PROPOSED"
    assert json.loads(row["metadata"])["peer_approved"] is True


async def test_an_operator_rejection_sticks(world):
    """A re-proposal does not turn REJECTED back into PROPOSED."""
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "REJECTED", ["orn:koi-net.vault-file"])
    await w["neg"].handle([_proposal(w)], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["REJECTED"]
    assert [e[0] for e in await _queued_for(w)] == ["FORGET"]


async def test_an_approved_edge_re_proposed_narrower_stays_approved(world, monkeypatch):
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "APPROVED",
                                    ["orn:koi-net.vault-file", "orn:koi-net.node"])
    await w["neg"].handle([_proposal(w, rid_types=["orn:koi-net.vault-file"])], w["peer"])
    assert [(e["status"], e["rid_types"]) for e in await _edges(w)] == [("APPROVED", ["orn:koi-net.vault-file"])]


async def test_forget_revokes_every_row_for_the_pair(world):
    """/handshake may have added a row under its own key next to the bundle row."""
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "APPROVED", ["orn:koi-net.vault-file"])
        await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                           "VALUES ($1, $2, $3, 'POLL', 'APPROVED', ARRAY['Vault-file'])",
                           f"orn:koi-net.edge:{w['ours']}>{w['peer']}:poll", w["ours"], w["peer"])
    await w["neg"].handle([neg.forget_event(neg.edge_rid_for(w["ours"], w["peer"]))], w["peer"])
    assert sorted(e["status"] for e in await _edges(w)) == ["REVOKED", "REVOKED"]


async def test_an_unsafe_base_url_is_not_stored_or_called(world):
    """A fragment would cut off koi_net_url's path and aim our signed POST anywhere."""
    w = world
    hello = {"rid": w["peer"], "event_type": "NEW",
             "contents": {"node_type": "FULL", "base_url": "http://127.0.0.1:9/anything#", "provides": {}}}
    await w["neg"].handle([hello], w["peer"])
    await w["neg"].drain()
    async with w["pool"].acquire() as conn:
        assert await conn.fetchval("SELECT base_url FROM koi_net_nodes WHERE node_rid = $1", w["peer"]) is None
    assert not w["neg"]._tasks  # nothing was pushed


async def test_one_reply_per_request_and_no_duplicate_queueing(world):
    """Twenty NEW events in one request get one reply, and a second first
    contact does not queue our bundle again while the first still waits."""
    w = world
    hello = {"rid": w["peer"], "event_type": "NEW", "contents": {"node_type": "PARTIAL"}}
    await w["neg"].handle([hello] * 20, w["peer"])
    w["neg"]._last_reply.clear()  # past the cooldown
    await w["neg"].handle([hello], w["peer"])
    assert [(e[0], e[1]) for e in await _queued_for(w)] == [("NEW", w["ours"])]


async def test_a_rejection_survives_the_peers_forget(world):
    """FORGET used to turn REJECTED into REVOKED, and a re-proposal then passed."""
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "REJECTED", ["orn:koi-net.vault-file"])
    await w["neg"].handle([neg.forget_event(neg.edge_rid_for(w["ours"], w["peer"]))], w["peer"])
    await w["neg"].handle([_proposal(w)], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["REJECTED"]


async def test_a_rejected_row_behind_a_newer_handshake_row_still_refuses(world):
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "REJECTED", ["orn:koi-net.vault-file"])
        await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                           "VALUES ($1, $2, $3, 'POLL', 'PROPOSED', ARRAY['Vault-file'])",
                           f"orn:koi-net.edge:{w['ours']}>{w['peer']}:poll", w["ours"], w["peer"])
    await w["neg"].handle([_proposal(w)], w["peer"])
    assert sorted(e["status"] for e in await _edges(w)) == ["PROPOSED", "REJECTED"]
    assert [e[0] for e in await _queued_for(w)] == ["FORGET"]


async def test_the_poller_never_calls_an_unsafe_stored_url(world):
    """/handshake stored a profile's base_url unchecked, and the poller used it."""
    from api.koi_poller import KOIPoller
    w = world
    async with w["pool"].acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET node_type = 'FULL', base_url = 'http://127.0.0.1:9/x#' "
                           "WHERE node_rid = $1", w["peer"])
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"])
    poller = KOIPoller(w["pool"], w["ours"], node_profile=w["profile"], event_queue=w["eq"])
    called = []

    async def record(**kwargs):
        called.append(kwargs)

    poller._poll_peer = record
    await poller._poll_all_peers()
    assert called == []


async def test_first_contact_does_not_re_propose_a_rejected_subscription(world, monkeypatch):
    """An operator's rejection of OUR subscription to a peer sticks too."""
    w = world
    monkeypatch.setenv("KOI_NET_RID_TYPES_OF_INTEREST", "orn:koi-net.node")
    async with w["pool"].acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET node_type = 'FULL' WHERE node_rid = $1", w["peer"])
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "REJECTED", ["orn:koi-net.node"])
    hello = {"rid": w["peer"], "event_type": "NEW", "contents": {"node_type": "FULL", "provides": {}}}
    await w["neg"].handle([hello], w["peer"])
    await w["neg"].drain()
    async with w["pool"].acquire() as conn:
        status = await conn.fetchval("SELECT status FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                                     w["peer"], w["ours"])
    assert status == "REJECTED"


# --- a peer's negotiation only narrows an edge ---------------------------------------------------

@pytest.mark.parametrize("auto", ["false", "true"])
@pytest.mark.parametrize("held", ["REVOKED", "REJECTED"])
async def test_t7_a_held_serving_edge_stays_held_against_a_peer_proposal(world, monkeypatch, held, auto):
    """T7, T9: our edge to the peer, revoked or rejected by an operator, plus the peer's
    subscription proposal, stays held with auto-approve off or on; the peer is told FORGET."""
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", auto)
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", held, ["orn:koi-net.vault-file"])
    assert await w["neg"].handle([_proposal(w)], w["peer"]) == 1
    assert [e["status"] for e in await _edges(w)] == [held]
    assert [e[0] for e in await _queued_for(w)] == ["FORGET"]


async def test_t9_a_revoked_row_holds_the_pair_behind_a_newer_proposed_row(world, monkeypatch):
    """T9: remove-peer.sh revokes every row and /handshake may add one; with auto-approve on,
    nothing for the pair becomes APPROVED while no row is APPROVED and one is held."""
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", "true")
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "REVOKED", ["orn:koi-net.vault-file"])
        await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                           "VALUES ($1, $2, $3, 'POLL', 'PROPOSED', ARRAY['Vault-file'])",
                           f"orn:koi-net.edge:{w['ours']}>{w['peer']}:poll", w["ours"], w["peer"])
    await w["neg"].handle([_proposal(w)], w["peer"])
    assert sorted(e["status"] for e in await _edges(w)) == ["PROPOSED", "REVOKED"]


@pytest.mark.parametrize("held", ["REVOKED", "REJECTED"])
async def test_t9_a_peer_cannot_approve_our_held_subscription(world, monkeypatch, held):
    """T9, our direction: the peer's APPROVED for our subscription it holds does nothing, with
    deferral off and auto-approve on."""
    w = world
    monkeypatch.setenv("KOI_NET_AUTO_APPROVE_EDGES", "true")
    w["neg"].defer_new_edges = lambda: False
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", held, ["orn:koi-net.node"])
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    await w["neg"].handle([approval], w["peer"])
    assert [e["status"] for e in await _edges(w)] == [held]


async def test_t8_first_contact_does_not_re_propose_a_revoked_subscription(world, monkeypatch):
    """T8: our subscription to the peer, revoked by an operator, plus the peer's first contact,
    stays REVOKED and no proposal is sent. The control (nothing held) does send one, so the
    proposal path is reached."""
    w = world
    monkeypatch.setenv("KOI_NET_RID_TYPES_OF_INTEREST", "orn:koi-net.node")
    sent = []

    async def record(peer, events, operator=False):
        sent.extend(events)
    w["neg"].deliver = record
    async with w["pool"].acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET node_type = 'FULL' WHERE node_rid = $1", w["peer"])
    hello = {"rid": w["peer"], "event_type": "NEW",
             "contents": {"node_type": "FULL", "base_url": "http://peer.invalid/koi-net",
                          "provides": {"event": ["orn:koi-net.node"], "state": []}}}

    await w["neg"].handle([hello], w["peer"])  # control
    assert [e["rid"] for e in sent if e["rid"].startswith("orn:koi-net.edge:")] == [neg.edge_rid_for(w["peer"], w["ours"])]
    async with w["pool"].acquire() as conn:
        await conn.execute("UPDATE koi_net_edges SET status = 'REVOKED' WHERE source_node = $1 AND target_node = $2",
                           w["peer"], w["ours"])
    sent.clear()
    w["neg"]._last_reply.clear()  # past the cooldown
    await w["neg"].handle([hello], w["peer"])
    assert [e["rid"] for e in sent] == [w["ours"]]  # our bundle only, no proposal
    assert [e["status"] for e in await _edges(w)] == ["REVOKED"]


async def test_t10_a_type_change_demotes_and_only_the_operator_restores(world):
    """T10: the peer widening its subscription demotes APPROVED to PROPOSED (allowed);
    proposing the old types again does not restore APPROVED; an operator's approval does."""
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "APPROVED", ["orn:koi-net.vault-file"])
    await w["neg"].handle([_proposal(w, rid_types=["orn:koi-net.vault-file", "orn:koi-net.node"])], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["PROPOSED"]
    await w["neg"].handle([_proposal(w, rid_types=["orn:koi-net.vault-file"])], w["peer"])
    assert [e["status"] for e in await _edges(w)] == ["PROPOSED"]


@pytest.mark.parametrize("held", ["REVOKED", "REJECTED"])
async def test_t12_an_operator_revoke_between_read_and_write_still_holds(world, held):
    """T12: _upsert_edge reads the pair, then writes, with no
    lock. An operator revokes the APPROVED row in between (on a second connection, as
    remove-peer.sh does), and the peer re-proposes the same scope, so approved_already is true.
    The row must stay held and the peer be sent FORGET.

    The interleaving is fixed by wrapping _edge_row: its first call is the subscription branch's
    approved_already read, its second is _upsert_edge's, after its status check. The revoke
    lands after that second read returns."""
    w = world
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["ours"], w["peer"], "POLL", "APPROVED", ["orn:koi-net.vault-file"])
    original, calls = w["neg"]._edge_row, []

    async def edge_row_then_operator_revokes(conn, source, target):
        row = await original(conn, source, target)
        calls.append(row["status"] if row else None)
        if len(calls) == 2:
            async with w["pool"].acquire() as operator:
                await operator.execute(
                    "UPDATE koi_net_edges SET status = $3, updated_at = NOW() WHERE source_node = $1 OR target_node = $2",
                    w["peer"], w["peer"], held)
        return row

    w["neg"]._edge_row = edge_row_then_operator_revokes
    assert await w["neg"].handle([_proposal(w)], w["peer"]) == 1
    assert calls == ["APPROVED", "APPROVED"]  # both reads saw APPROVED: the race window was hit
    assert [e["status"] for e in await _edges(w)] == [held]
    assert [e[0] for e in await _queued_for(w)] == ["FORGET"]


# --- the peer-approved branch re-checks the stored state ---------------------------------------

def _revoke_after_first_read(w, held):
    """Wrap _edge_row so an operator sets the pair to `held` on a second connection right after
    the branch's read returns, before its write (as remove-peer.sh / /edges/reject write it)."""
    original, calls = w["neg"]._edge_row, []

    async def wrapped(conn, source, target):
        row = await original(conn, source, target)
        calls.append(row["status"] if row else None)
        if len(calls) == 1:
            async with w["pool"].acquire() as operator:
                await operator.execute(
                    "UPDATE koi_net_edges SET status = $3, updated_at = NOW() WHERE source_node = $1 OR target_node = $2",
                    w["peer"], w["peer"], held)
        return row

    w["neg"]._edge_row = wrapped
    return calls


async def _our_proposal_row(w):
    async with w["pool"].acquire() as conn:
        await w["neg"]._upsert_edge(conn, w["peer"], w["ours"], "POLL", "PROPOSED", ["orn:koi-net.node"])
        return await conn.fetchrow("SELECT status, rid_types, metadata FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                                   w["peer"], w["ours"])


@pytest.mark.parametrize("held", ["REVOKED", "REJECTED"])
async def test_t13_under_the_opt_out_a_revoke_between_read_and_write_is_not_revived(world, caplog, held):
    """T13: membrane off and deferral off, the peer's APPROVED would approve our proposal. An
    operator revokes or rejects it after the branch reads it PROPOSED: it stays held, and no
    approval is logged."""
    import logging
    w = world
    w["neg"].defer_new_edges = lambda: False
    await _our_proposal_row(w)
    calls = _revoke_after_first_read(w, held)
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", ["orn:koi-net.node"]))
    with caplog.at_level(logging.INFO, logger="api.koi_net_negotiation"):
        await w["neg"].handle([approval], w["peer"])
    assert calls == ["PROPOSED"]  # the read saw PROPOSED: the race window was hit
    assert [e["status"] for e in await _edges(w)] == [held]
    assert not any("approved our subscription" in r.getMessage() for r in caplog.records)
    assert any("no longer PROPOSED" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("held", ["REVOKED", "REJECTED"])
async def test_t14_on_the_waiting_branch_a_held_row_keeps_its_rid_types_and_metadata(world, held):
    """T14: deferral on (the default), the branch would record peer_approved and the granted
    types. After the operator's revoke it changes nothing on the held row."""
    w = world
    before = await _our_proposal_row(w)
    _revoke_after_first_read(w, held)
    approval = neg.bundle_event("UPDATE", neg.edge_bundle(w["peer"], w["ours"], "POLL", "APPROVED", []))
    await w["neg"].handle([approval], w["peer"])
    async with w["pool"].acquire() as conn:
        after = await conn.fetchrow("SELECT status, rid_types, metadata FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                                    w["peer"], w["ours"])
    assert after["status"] == held
    assert after["rid_types"] == before["rid_types"] == ["orn:koi-net.node"]
    assert "peer_approved" not in json.loads(after["metadata"])


# --- a node bundle applies only when its manifest is newer ---------------------------------------

def _node_bundle_event(w, timestamp, base_url):
    return {"rid": w["peer"], "event_type": "UPDATE",
            "manifest": {"rid": w["peer"], "timestamp": timestamp, "sha256_hash": "x" + timestamp},
            "contents": {"node_type": "FULL", "base_url": base_url, "provides": {"event": ["orn:koi-net.node"], "state": []}}}


async def _peer_row(w):
    async with w["pool"].acquire() as conn:
        return await conn.fetchrow("SELECT node_type, base_url, provides_event FROM koi_net_nodes WHERE node_rid = $1", w["peer"])


async def test_c_a_replayed_older_node_bundle_does_not_roll_the_profile_back(world):
    w = world
    w["neg"].deliver = _no_delivery
    newer = _node_bundle_event(w, "2026-11-06T12:00:01Z", "http://peer.invalid/newer")
    older = _node_bundle_event(w, "2026-11-06T12:00:00Z", "http://peer.invalid/older")
    older["contents"]["node_type"] = "PARTIAL"
    await w["neg"].handle([newer], w["peer"])
    assert (await _peer_row(w))["base_url"] == "http://peer.invalid/newer"
    w["neg"]._last_reply.clear()
    await w["neg"].handle([older], w["peer"])  # the replay: older manifest
    row = await _peer_row(w)
    assert (row["node_type"], row["base_url"], row["provides_event"]) == ("FULL", "http://peer.invalid/newer", ["orn:koi-net.node"])
    w["neg"]._last_reply.clear()
    await w["neg"].handle([_node_bundle_event(w, "2026-11-06T12:00:01Z", "http://peer.invalid/same")], w["peer"])
    assert (await _peer_row(w))["base_url"] == "http://peer.invalid/newer"  # same timestamp: not newer


async def test_c_a_newer_node_bundle_is_applied(world):
    w = world
    w["neg"].deliver = _no_delivery
    await w["neg"].handle([_node_bundle_event(w, "2026-11-06T12:00:00Z", "http://peer.invalid/one")], w["peer"])
    w["neg"]._last_reply.clear()
    await w["neg"].handle([_node_bundle_event(w, "2026-11-06T12:00:05+00:00", "http://peer.invalid/two")], w["peer"])
    assert (await _peer_row(w))["base_url"] == "http://peer.invalid/two"


async def _no_delivery(peer, events, operator=False):
    return None
