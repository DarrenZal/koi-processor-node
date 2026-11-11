"""A stranger's first inbound edge is PROPOSED, however it reached the handshake.

_unwrap_request persists a koi_net_nodes row for any signed request whose payload carries a
NEW event of the sender's own node bundle, before any route logic runs. That row, or an approved
edge in the other direction, must not get a new inbound edge APPROVED: only the operator does,
through POST /koi-net/edges/approve. These tests go through the real bootstrap path, not a
replaced _unwrap_request.
"""
import pytest

from api.koi_envelope import sign_envelope
from tests.test_koi_net_peer_identity import (  # noqa: F401  (fixtures)
    _edge,
    _handshake,
    _payload,
    anyio_backend,
    node,
)

pytestmark = pytest.mark.anyio


def _own_bundle_event(who):
    return {"event_type": "NEW", "rid": who.rid,
            "contents": {"public_key": who.pub, "node_name": who.name, "base_url": "http://peer.invalid/x"}}


async def _row_exists(node, who):
    async with node.pool.acquire() as conn:
        return await conn.fetchval("SELECT 1 FROM koi_net_nodes WHERE node_rid = $1", who.rid) is not None


async def test_a_signed_broadcast_bootstrap_does_not_make_a_stranger_known(node, monkeypatch):
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "true")
    who = node.peer()
    bcast = sign_envelope({"type": "events_payload", "events": [_own_bundle_event(who)]}, who.rid, node.me.rid, who.key)
    await node.client.post("/koi-net/events/broadcast", json=bcast)
    assert await _row_exists(node, who)  # the bootstrap path really wrote the row

    hs = sign_envelope(_handshake(who.rid, who.name, public_key=who.pub), who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/handshake", json=hs)
    assert r.status_code == 200, r.text
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "PROPOSED"


async def test_a_bundle_inside_a_signed_handshake_does_not_make_a_stranger_known(node, monkeypatch):
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "true")
    who = node.peer()
    body = _handshake(who.rid, who.name, public_key=who.pub)
    body["events"] = [_own_bundle_event(who)]
    first = await node.client.post("/koi-net/handshake", json=sign_envelope(body, who.rid, node.me.rid, who.key))
    # HandshakeRequest rejects the extra field, but the bootstrap in _unwrap_request has
    # already run and left a node row: that row is what this test proves is not "known".
    assert first.status_code == 400, first.text
    assert first.json()["error_code"] == "INVALID_HANDSHAKE"
    assert await _row_exists(node, who)
    hs = sign_envelope(_handshake(who.rid, who.name, public_key=who.pub), who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/handshake", json=hs)
    assert r.status_code == 200, r.text
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "PROPOSED"


async def test_an_approved_outbound_edge_does_not_approve_a_new_inbound_edge(node, monkeypatch):
    # Only the us->peer edge exists and is APPROVED. The handshake creates peer->us fresh, and
    # that new edge waits for the operator like any other.
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "true")
    who = node.peer()
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', 'http://peer.invalid/original', $3, 'active')",
            who.rid, who.name, who.pub)
        await conn.execute(
            "INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
            "VALUES ($1, $2, $3, 'POLL', 'APPROVED', ARRAY['person'])",
            f"orn:koi-net.edge:{node.me.rid}>{who.rid}:poll", node.me.rid, who.rid)
    assert await _edge(node, who.rid, node.me.rid) is None
    hs = sign_envelope(_handshake(who.rid, who.name, public_key=who.pub, base_url="http://peer.invalid/original"),
                       who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/handshake", json=hs)
    assert r.status_code == 200, r.text
    assert _payload(r)["edge_status"] == "PROPOSED"
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "PROPOSED"
    assert (await _edge(node, node.me.rid, who.rid))["status"] == "APPROVED"


async def test_the_operator_approves_the_inbound_edge_and_a_re_handshake_keeps_it(node, monkeypatch):
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "true")
    who = node.peer()
    # First contact is unsigned: this node has no key for the peer yet.
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=who.pub))
    assert r.status_code == 200, r.text
    assert _payload(r)["edge_status"] == "PROPOSED"
    edge_rid = f"orn:koi-net.edge:{who.rid}>{node.me.rid}:poll"
    # A remote caller cannot approve it.
    assert (await node.client.post("/koi-net/edges/approve", json={"edge_rid": edge_rid})).status_code in (401, 403)
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "PROPOSED"
    a = await node.operator.post("/koi-net/edges/approve", json={"edge_rid": edge_rid})
    assert a.status_code == 200, a.text
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "APPROVED"
    hs = sign_envelope(_handshake(who.rid, who.name, public_key=who.pub), who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/handshake", json=hs)
    assert r.status_code == 200, r.text
    assert _payload(r)["edge_status"] == "APPROVED"
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "APPROVED"
