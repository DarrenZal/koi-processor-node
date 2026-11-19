"""A rejected node stays rejected, under any RID its key could take, and /handshake reports
the stored edge status (register entry 26).

Through the real request path: the router mounted on a FastAPI app, requests signed with
the node's own key. Run by tests/interop/run_gate.py against a throwaway clone (default DB
tests); collected only when KOI_TEST_DATABASE_URL is set, so it never skips.
"""
from types import SimpleNamespace
import uuid

import asyncpg
import httpx
import pytest
from fastapi import FastAPI

from api import koi_net_router as knr
from api.event_queue import EventQueue
from api.koi_envelope import sign_envelope
from api.koi_protocol import NodeProfile, NodeProvides
from api.node_identity import derive_node_rid, generate_keypair, get_public_key_der_b64
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _identity(prefix, key=None):
    key = key or generate_keypair()
    name = f"{prefix}-{uuid.uuid4().hex[:10]}"
    return SimpleNamespace(key=key, name=name, rid=derive_node_rid(name, key.public_key()), pub=get_public_key_der_b64(key))


@pytest.fixture
async def node(monkeypatch):
    url = targets.configured("KOI_TEST_DATABASE_URL")
    if not url:
        pytest.fail("KOI_TEST_DATABASE_URL is not set: run through tests/interop/run_gate.py")
    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    me = _identity("rejection-test-node")
    profile = NodeProfile(node_rid=me.rid, node_name=me.name, node_type="FULL",
                          provides=NodeProvides(event=[], state=[]), public_key=me.pub)
    for attr, value in (("_db_pool", pool), ("_private_key", me.key), ("_node_profile", profile),
                        ("_event_queue", EventQueue(pool, me.rid)), ("_vault_sync", None)):
        monkeypatch.setattr(knr, attr, value)
    for var in ("KOI_STRICT_MODE", "KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "KOI_REQUIRE_SIGNED_ENVELOPES", "KOI_ADMIN_TOKEN",
                "KOI_STATE_DIR"):  # the token file too: an inherited state dir could hold one
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL", "true")
    app = FastAPI()
    app.include_router(knr.koi_net_router, prefix="/koi-net")
    state = SimpleNamespace(pool=pool, me=me, created=[me.rid])
    remote = httpx.ASGITransport(app=app, client=("198.51.100.9", 40002))
    async with httpx.AsyncClient(transport=remote, base_url="http://node") as client:
        state.client = client
        yield state
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM koi_net_edges WHERE source_node = ANY($1) OR target_node = ANY($1)", state.created)
        await conn.execute("DELETE FROM koi_net_peer_aliases WHERE node_rid = ANY($1)", state.created)
        await conn.execute("DELETE FROM koi_net_nodes WHERE node_rid = ANY($1)", state.created)
        await conn.execute("DELETE FROM koi_net_events WHERE source_node = ANY($1)", state.created)
    try:
        await asyncio.wait_for(pool.close(), 30)
    except TimeoutError:
        pool.terminate()
        pytest.fail("a pool connection was still checked out 30 s after the test: pool.close() did not finish")


def _handshake(who, **extra):
    return {"type": "handshake", "profile": {"node_rid": who.rid, "node_name": who.name, "node_type": "FULL",
                                            "base_url": "http://peer.invalid/koi-net", "public_key": who.pub,
                                            "provides": {"event": [], "state": []}, **extra}}


def _signed(node, who, payload):
    return sign_envelope(payload, who.rid, node.me.rid, who.key)


async def _reject(node, who):
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', 'http://peer.invalid/koi-net', $3, 'rejected')", who.rid, who.name, who.pub)


async def _status(node, rid):
    async with node.pool.acquire() as conn:
        return await conn.fetchval("SELECT status FROM koi_net_nodes WHERE node_rid = $1", rid)


async def test_a_rejected_node_signing_its_own_handshake_stays_rejected(node):
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _reject(node, who)
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who)))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED"
    assert await _status(node, who.rid) == "rejected"


async def test_a_rejected_key_under_another_name_is_refused(node):
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)  # same key, new name, new RID
    node.created += [who.rid, again.rid]
    await _reject(node, who)
    r = await node.client.post("/koi-net/handshake", json=_handshake(again))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text
    # Signed, the handshake verifies against its own bound profile key, and the key is barred.
    r = await node.client.post("/koi-net/handshake", json=_signed(node, again, _handshake(again)))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text
    assert await _status(node, again.rid) is None


async def test_a_rejected_key_is_not_registered_again_by_broadcast_bootstrap(node):
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)
    node.created += [who.rid, again.rid]
    await _reject(node, who)
    from rid_lib.core import RID
    from rid_lib.ext import Bundle
    bundle = Bundle.generate(RID.from_string(again.rid), {"node_type": "FULL", "base_url": "http://peer.invalid/koi-net",
                                                          "provides": {"event": [], "state": []}, "public_key": again.pub})
    payload = {"type": "events_payload", "events": [{"rid": again.rid, "event_type": "NEW",
                                                     "manifest": bundle.manifest.model_dump(mode="json"),
                                                     "contents": bundle.contents}]}
    await node.client.post("/koi-net/events/broadcast", json=_signed(node, again, payload))
    assert await _status(node, again.rid) is None


async def test_the_reported_edge_status_is_the_stored_one(node):
    """Under the membrane (see the next-but-one test for the case that tells the two apart)."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', 'http://peer.invalid/koi-net', $3, 'active')", who.rid, who.name, who.pub)
        await conn.execute(
            "INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
            "VALUES ($1, $2, $3, 'POLL', 'PROPOSED', ARRAY[]::text[])",
            f"orn:koi-net.edge:{who.rid}>{node.me.rid}:poll", who.rid, node.me.rid)
    r = await node.client.post("/koi-net/handshake", json=_handshake(who))
    assert r.status_code == 200, r.text
    body = r.json()
    body = body.get("payload", body)
    assert body["edge_status"] == "PROPOSED"


# --- second round (2026-11-06): bypasses, revoked nodes, re-admit, gates ------------------

ADMIN = "rejection-test-admin-token"


async def test_a_first_handshake_without_a_key_is_refused(node):
    """A keyless row would later take its key from wherever base_url points."""
    who = _identity("rejection-test-keyless")
    node.created.append(who.rid)
    body = _handshake(who)
    body["profile"].pop("public_key")
    r = await node.client.post("/koi-net/handshake", json=body)
    assert r.status_code == 400 and r.json()["error_code"] == "MISSING_PUBLIC_KEY", r.text
    assert await _status(node, who.rid) is None


async def test_a_re_encoded_rejected_key_is_still_refused(node):
    """Whitespace in the base64 loads as the same key but fails a string comparison."""
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)
    node.created += [who.rid, again.rid]
    await _reject(node, who)
    body = _handshake(again)
    body["profile"]["public_key"] = again.pub[:20] + "\n" + again.pub[20:]
    r = await node.client.post("/koi-net/handshake", json=body)
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text


async def test_a_rejected_key_bootstrap_refuses_the_request(node):
    """It used to write no row but still serve the request."""
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)
    node.created += [who.rid, again.rid]
    await _reject(node, who)
    from rid_lib.core import RID
    from rid_lib.ext import Bundle
    bundle = Bundle.generate(RID.from_string(again.rid), {"node_type": "FULL", "base_url": "http://peer.invalid/koi-net",
                                                          "provides": {"event": [], "state": []}, "public_key": again.pub})
    payload = {"type": "events_payload", "events": [{"rid": again.rid, "event_type": "NEW",
                                                     "manifest": bundle.manifest.model_dump(mode="json"), "contents": bundle.contents}]}
    r = await node.client.post("/koi-net/events/broadcast", json=_signed(node, again, payload))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text


async def test_revoked_is_barred_like_rejected(node):
    who = _identity("rejection-test-revoked")
    node.created.append(who.rid)
    await _reject(node, who)
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET status = 'revoked' WHERE node_rid = $1", who.rid)
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who)))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED"
    assert await _status(node, who.rid) == "revoked"


async def test_approving_or_gating_a_rejected_peer_fails_until_it_is_readmitted(node, monkeypatch):
    """approve_edge and the gates never read node status, and there was no way back in."""
    monkeypatch.setenv("KOI_ADMIN_TOKEN", ADMIN)
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _reject(node, who)
    edge_rid = f"orn:koi-net.edge:{node.me.rid}>{who.rid}:poll"
    async with node.pool.acquire() as conn:
        await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                           "VALUES ($1, $2, $3, 'POLL', 'PROPOSED', ARRAY[]::text[])", edge_rid, node.me.rid, who.rid)
    operator = httpx.AsyncClient(transport=httpx.ASGITransport(app=node.client._transport.app, client=("127.0.0.1", 40003)),
                                 base_url="http://node", headers={"Authorization": f"Bearer {ADMIN}"})
    async with operator:
        r = await operator.post("/koi-net/edges/approve", json={"edge_rid": edge_rid})
        assert r.status_code == 409 and r.json()["error_code"] == "NODE_REJECTED", r.text
        async with node.pool.acquire() as conn:
            await conn.execute("UPDATE koi_net_edges SET status = 'APPROVED' WHERE edge_rid = $1", edge_rid)
        assert not await knr._check_approved_edge(who.rid, direction="poll")  # barred node: no edge counts
        r = await operator.post("/koi-net/nodes/readmit", json={"node_rid": who.rid})
        assert r.status_code == 200 and await _status(node, who.rid) == "active", r.text
        assert await knr._check_approved_edge(who.rid, direction="poll")
        r = await operator.post("/koi-net/nodes/readmit", json={"node_rid": who.rid})
        assert r.status_code == 404  # only a barred node can be re-admitted


async def test_the_reported_edge_status_is_the_stored_one_even_when_the_computed_one_differs(node, monkeypatch):
    """With the opt-out the computed status is APPROVED, the stored one PROPOSED."""
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "false")
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', 'http://peer.invalid/koi-net', $3, 'active')", who.rid, who.name, who.pub)
        await conn.execute(
            "INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
            "VALUES ($1, $2, $3, 'POLL', 'PROPOSED', ARRAY[]::text[])",
            f"orn:koi-net.edge:{who.rid}>{node.me.rid}:poll", who.rid, node.me.rid)
    r = await node.client.post("/koi-net/handshake", json=_handshake(who))
    body = r.json()
    body = body.get("payload", body)
    assert r.status_code == 200 and body["edge_status"] == "PROPOSED", r.text


async def test_the_negotiator_ignores_a_rejected_node(node):
    """First contact from a rejected node rewrote its profile and was answered."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _reject(node, who)
    from api.koi_net_negotiation import Negotiator
    from api.event_queue import own_node_bundle
    neg = Negotiator(pool=node.pool, own_rid=node.me.rid, own_profile=knr._node_profile, private_key=None,
                     event_queue=knr._event_queue, own_bundle=lambda: own_node_bundle(knr._node_profile))
    hello = {"rid": who.rid, "event_type": "NEW", "contents": {"node_type": "PARTIAL", "base_url": None}}
    assert await neg.handle([hello], who.rid) == 0
    async with node.pool.acquire() as conn:
        assert await conn.fetchval("SELECT node_type FROM koi_net_nodes WHERE node_rid = $1", who.rid) == "FULL"
        assert await conn.fetchval("SELECT count(*) FROM koi_net_events WHERE target_node = $1", who.rid) == 0


# --- third round (2026-11-06): keyless rows, barred keys and the poller (T1-T6) ---

async def _active(node, who, key=True):
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', 'http://peer.invalid/koi-net', $3, 'active')", who.rid, who.name,
            who.pub if key else None)


async def _edge(node, source, target, status, edge_type="POLL"):
    edge_rid = f"orn:koi-net.edge:{source}>{target}:{edge_type.lower()}"
    async with node.pool.acquire() as conn:
        await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                           "VALUES ($1, $2, $3, $4, $5, ARRAY[]::text[])", edge_rid, source, target, edge_type, status)
    return edge_rid


async def _edge_status(node, edge_rid):
    async with node.pool.acquire() as conn:
        return await conn.fetchval("SELECT status FROM koi_net_edges WHERE edge_rid = $1", edge_rid)


_REAL_ASYNC_CLIENT = httpx.AsyncClient  # captured once: a second patch must not wrap the first


def _health_served(monkeypatch, module, who_rid, pub):
    """Make `module`'s httpx.AsyncClient answer GET /koi-net/health with this node and key."""
    def answer(request):
        return httpx.Response(200, json={"node": {"node_rid": who_rid, "node_name": "served", "node_type": "FULL",
                                                  "public_key": pub}})
    monkeypatch.setattr(module.httpx, "AsyncClient", lambda *a, **k: _REAL_ASYNC_CLIENT(
        transport=httpx.MockTransport(answer), timeout=k.get("timeout", 10.0)))


@pytest.mark.parametrize("stored", ["APPROVED", "REVOKED", "REJECTED"])
async def test_a_signed_re_handshake_keeps_the_stored_inbound_edge_status(node, stored):
    """T3, T4: the handshake creates missing edges and never changes an existing one's status."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    edge_rid = await _edge(node, who.rid, node.me.rid, stored)
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who)))
    assert r.status_code == 200, r.text
    body = r.json()
    body = body.get("payload", body)
    assert body["edge_status"] == stored and await _edge_status(node, edge_rid) == stored


async def test_a_rejected_node_broadcasting_is_refused_off_the_membrane_and_stays_rejected(node, monkeypatch):
    """T5 (bootstrap path): the bar holds on every signed request, membrane off too."""
    monkeypatch.setenv("KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL", "false")
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _reject(node, who)
    for path, payload in (("/koi-net/events/broadcast", {"type": "events_payload", "events": []}),
                          ("/koi-net/events/poll", {"type": "poll_events", "rid": who.rid, "limit": 5})):
        r = await node.client.post(path, json=_signed(node, who, payload))
        assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", (path, r.text)
    assert await _status(node, who.rid) == "rejected"


async def test_a_node_registered_before_its_key_was_barred_is_refused(node, monkeypatch):
    """The bar follows the key to RIDs that held it before the rejection."""
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)
    node.created += [who.rid, again.rid]
    await _active(node, who)
    await _active(node, again)
    edge_rid = await _edge(node, node.me.rid, again.rid, "APPROVED")
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET status = 'rejected' WHERE node_rid = $1", who.rid)
    r = await node.client.post("/koi-net/handshake", json=_signed(node, again, _handshake(again)))
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text
    unsigned = _handshake(again)
    unsigned["profile"].pop("public_key")  # so the profile's own key check cannot be what refuses
    r = await node.client.post("/koi-net/handshake", json=unsigned)
    assert r.status_code == 403 and r.json()["error_code"] == "NODE_REJECTED", r.text
    assert not await knr._check_approved_edge(again.rid, direction="poll")
    assert await _status(node, again.rid) == "active" and await _edge_status(node, edge_rid) == "APPROVED"


async def test_a_keyless_row_cannot_be_claimed_with_another_key(node, monkeypatch):
    """A keyless row takes only the key its RID is bound to, by handshake or key refresh."""
    victim = _identity("rejection-test-keyless")
    stranger = _identity("rejection-test-stranger")
    node.created += [victim.rid, stranger.rid]
    await _active(node, victim, key=False)
    claim = _handshake(victim)
    claim["profile"]["public_key"] = stranger.pub
    r = await node.client.post("/koi-net/handshake", json=claim)
    assert r.status_code == 403 and r.json()["error_code"] == "SOURCE_NODE_KEY_BINDING_FAILED", r.text
    _health_served(monkeypatch, knr, victim.rid, stranger.pub)  # base_url serves the stranger's key
    assert await knr._refresh_peer_public_key(victim.rid) is None
    async with node.pool.acquire() as conn:
        assert await conn.fetchval("SELECT public_key FROM koi_net_nodes WHERE node_rid = $1", victim.rid) is None
    # The control: the bound key is accepted and pinned.
    r = await node.client.post("/koi-net/handshake", json=_handshake(victim))
    assert r.status_code == 200, r.text
    async with node.pool.acquire() as conn:
        assert await conn.fetchval("SELECT public_key FROM koi_net_nodes WHERE node_rid = $1", victim.rid) == victim.pub


async def test_key_refresh_never_reactivates_a_barred_node(node, monkeypatch):
    """T5 (key refresh): the router's refresh and the poller's key learning both refuse."""
    from api import koi_poller
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _reject(node, who)
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET public_key = NULL WHERE node_rid = $1", who.rid)
    _health_served(monkeypatch, knr, who.rid, who.pub)
    assert await knr._refresh_peer_public_key(who.rid) is None
    _health_served(monkeypatch, koi_poller, who.rid, who.pub)
    poller = koi_poller.KOIPoller(pool=node.pool, node_rid=node.me.rid)
    assert await poller._learn_peer_public_key(who.rid, "http://peer.invalid/koi-net") is None
    async with node.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT status, public_key FROM koi_net_nodes WHERE node_rid = $1", who.rid)
    assert row["status"] == "rejected" and row["public_key"] is None


async def test_the_poller_learns_only_a_key_bound_to_the_rid(node, monkeypatch):
    """The URL could serve any key; the control learns the bound one."""
    from api import koi_poller
    who, stranger = _identity("rejection-test-peer"), _identity("rejection-test-stranger")
    node.created.append(who.rid)
    await _active(node, who, key=False)
    poller = koi_poller.KOIPoller(pool=node.pool, node_rid=node.me.rid)
    _health_served(monkeypatch, koi_poller, who.rid, stranger.pub)
    assert await poller._learn_peer_public_key(who.rid, "http://peer.invalid/koi-net") is None
    _health_served(monkeypatch, koi_poller, who.rid, who.pub)
    assert await poller._learn_peer_public_key(who.rid, "http://peer.invalid/koi-net") == who.pub
    assert await _status(node, who.rid) == "active"


async def test_the_poller_neither_polls_nor_pushes_to_a_barred_node(node, monkeypatch):
    """The poll and webhook loops pick APPROVED edges only to nodes that are not barred,
    under their own RID or a key barred under another. The active peer is the control."""
    from api import koi_poller
    ok, rejected = _identity("rejection-test-ok"), _identity("rejection-test-peer")
    holder = _identity("rejection-test-renamed", key=rejected.key)  # same key, still 'active'
    node.created += [ok.rid, rejected.rid, holder.rid]
    for who in (ok, holder):
        await _active(node, who)
    await _reject(node, rejected)
    for who in (ok, rejected, holder):
        await _edge(node, who.rid, node.me.rid, "APPROVED", "POLL")
        await _edge(node, node.me.rid, who.rid, "APPROVED", "WEBHOOK")
    polled, pushed = [], []

    async def poll_peer(source_node, **_):
        polled.append(source_node)

    async def peek(target_node, **_):
        pushed.append(target_node)
        return []

    poller = koi_poller.KOIPoller(pool=node.pool, node_rid=node.me.rid, event_queue=SimpleNamespace(peek_undelivered=peek))
    monkeypatch.setattr(poller, "_poll_peer", poll_peer)
    await poller._poll_all_peers()
    await poller._push_webhook_peers()
    assert polled == [ok.rid] and pushed == [ok.rid]


async def test_no_admin_token_refuses_the_operator_with_503_and_writes_nothing(node, monkeypatch, tmp_path):
    """T6: fail closed, loopback included."""
    monkeypatch.setenv("KOI_STATE_DIR", str(tmp_path))  # no token file there; KOI_ADMIN_TOKEN is unset
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    edge_rid = await _edge(node, node.me.rid, who.rid, "PROPOSED")
    loopback = httpx.AsyncClient(transport=httpx.ASGITransport(app=node.client._transport.app, client=("127.0.0.1", 40004)),
                                 base_url="http://node", headers={"Authorization": "Bearer anything"})
    async with loopback:
        r = await loopback.post("/koi-net/edges/approve", json={"edge_rid": edge_rid})
        assert r.status_code == 503 and r.json()["error_code"] == "ADMIN_TOKEN_UNCONFIGURED", r.text
        r = await loopback.post("/koi-net/edges/reject", json={"edge_rid": edge_rid, "deactivate_node": True})
        assert r.status_code == 503, r.text
    assert await _edge_status(node, edge_rid) == "PROPOSED" and await _status(node, who.rid) == "active"


async def test_a_rid_holding_a_rejected_key_learns_nothing_and_is_not_negotiated_with(node, monkeypatch):
    """The key bar, not only the status bar, on key refresh,
    poller key learning and the negotiator. `again` is active and shares a rejected node's key."""
    from api import koi_poller
    from api.koi_net_negotiation import Negotiator
    from api.event_queue import own_node_bundle
    who = _identity("rejection-test-peer")
    again = _identity("rejection-test-renamed", key=who.key)
    node.created += [who.rid, again.rid]
    await _reject(node, who)
    await _active(node, again, key=False)
    _health_served(monkeypatch, knr, again.rid, again.pub)  # a key bound to again's RID
    assert await knr._refresh_peer_public_key(again.rid) is None
    poller = koi_poller.KOIPoller(pool=node.pool, node_rid=node.me.rid)
    assert await poller._learn_peer_public_key(again.rid, "http://peer.invalid/koi-net") is None
    async with node.pool.acquire() as conn:
        assert await conn.fetchval("SELECT public_key FROM koi_net_nodes WHERE node_rid = $1", again.rid) is None
        await conn.execute("UPDATE koi_net_nodes SET public_key = $2 WHERE node_rid = $1", again.rid, again.pub)
    neg = Negotiator(pool=node.pool, own_rid=node.me.rid, own_profile=knr._node_profile, private_key=None,
                     event_queue=knr._event_queue, own_bundle=lambda: own_node_bundle(knr._node_profile))
    hello = {"rid": again.rid, "event_type": "NEW", "contents": {"node_type": "PARTIAL", "base_url": None}}
    assert await neg.handle([hello], again.rid) == 0
    async with node.pool.acquire() as conn:
        assert await conn.fetchval("SELECT node_type FROM koi_net_nodes WHERE node_rid = $1", again.rid) == "FULL"


async def test_an_unsigned_handshake_cannot_rewrite_a_keyless_row(node, monkeypatch):
    """The bound key is public, so an unsigned caller holding
    only it may not change a keyless row's address or encryption key. The node itself, signing,
    can (the control)."""
    victim = _identity("rejection-test-keyless")
    node.created.append(victim.rid)
    await _active(node, victim, key=False)
    edge_rid = await _edge(node, victim.rid, node.me.rid, "APPROVED")
    claim = _handshake(victim, base_url="http://attacker.invalid/koi-net", encryption_key="attacker-x25519")
    r = await node.client.post("/koi-net/handshake", json=claim)
    assert r.status_code == 401 and r.json()["error_code"] == "SIGNED_HANDSHAKE_REQUIRED", r.text
    async with node.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT base_url, encryption_key, public_key FROM koi_net_nodes WHERE node_rid = $1",
                                  victim.rid)
    assert (row["base_url"], row["encryption_key"], row["public_key"]) == ("http://peer.invalid/koi-net", None, None)

    async def no_refresh(_rid):
        return None  # no network: the signed handshake verifies against its own profile key
    monkeypatch.setattr(knr, "_refresh_peer_public_key", no_refresh)
    moved = _handshake(victim, base_url="http://peer.invalid/moved")
    r = await node.client.post("/koi-net/handshake", json=_signed(node, victim, moved))
    assert r.status_code == 200, r.text
    async with node.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT base_url, public_key FROM koi_net_nodes WHERE node_rid = $1", victim.rid)
    assert (row["base_url"], row["public_key"]) == ("http://peer.invalid/moved", victim.pub)
    assert await _edge_status(node, edge_rid) == "APPROVED"


async def test_a_new_node_can_sign_its_first_handshake_with_the_key_its_profile_carries(node):
    """With no stored key, a signed handshake verifies against
    its profile's key when the RID is bound to it, and the new edge still waits for an operator.
    A profile key the RID is not bound to verifies nothing."""
    who, stranger = _identity("rejection-test-new"), _identity("rejection-test-stranger")
    node.created += [who.rid, stranger.rid]
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who)))
    assert r.status_code == 200, r.text
    assert await _status(node, who.rid) == "active"
    assert await _edge_status(node, f"orn:koi-net.edge:{who.rid}>{node.me.rid}:poll") == "PROPOSED"
    forged = _handshake(who)
    forged["profile"]["public_key"] = stranger.pub
    victim = _identity("rejection-test-victim", key=who.key)  # a RID no row holds yet
    node.created.append(victim.rid)
    forged["profile"]["node_rid"] = victim.rid
    r = await node.client.post("/koi-net/handshake", json=sign_envelope(forged, victim.rid, node.me.rid, stranger.key))
    assert r.status_code in (400, 401, 403) and r.json()["error_code"] == "UNKNOWN_SOURCE_NODE", r.text
    assert await _status(node, victim.rid) is None


async def test_the_poller_binds_keys_under_the_node_rid_policy(node, monkeypatch):
    """A legacy16-bound key is learned only while
    KOI_ALLOW_LEGACY16_NODE_RID allows it, as in the router."""
    from api import koi_poller
    key = generate_keypair()
    legacy = SimpleNamespace(rid=derive_node_rid(f"rejection-test-legacy-{uuid.uuid4().hex[:10]}", key.public_key(),
                                                 "legacy16"),
                             name="legacy", pub=get_public_key_der_b64(key))
    node.created.append(legacy.rid)
    await _active(node, legacy, key=False)
    _health_served(monkeypatch, koi_poller, legacy.rid, legacy.pub)
    poller = koi_poller.KOIPoller(pool=node.pool, node_rid=node.me.rid)
    monkeypatch.setenv("KOI_ALLOW_LEGACY16_NODE_RID", "false")
    assert await poller._learn_peer_public_key(legacy.rid, "http://peer.invalid/koi-net") is None
    monkeypatch.setenv("KOI_ALLOW_LEGACY16_NODE_RID", "true")
    assert await poller._learn_peer_public_key(legacy.rid, "http://peer.invalid/koi-net") == legacy.pub


async def test_a_handshake_carrying_a_non_ec_key_is_refused_not_a_500(node):
    """A RID bound to an Ed25519 key, signed or unsigned."""
    from cryptography.hazmat.primitives.asymmetric import ed25519
    from api.koi_envelope import public_key_to_der_b64
    ed_pub = ed25519.Ed25519PrivateKey.generate().public_key()
    who = SimpleNamespace(rid=derive_node_rid(f"rejection-test-ed25519-{uuid.uuid4().hex[:10]}", ed_pub),
                          name="ed25519", pub=public_key_to_der_b64(ed_pub), key=generate_keypair())
    node.created.append(who.rid)
    r = await node.client.post("/koi-net/handshake", json=_handshake(who))
    assert r.status_code == 400 and r.json()["error_code"] == "INVALID_PUBLIC_KEY", r.text
    # Signed (with some EC key; the profile's Ed25519 key is the one it would verify against).
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who)))
    assert r.status_code == 400 and r.json()["error_code"] == "UNKNOWN_SOURCE_NODE", r.text
    assert await _status(node, who.rid) is None


async def test_the_approval_response_shows_what_was_approved(node, monkeypatch):
    """/edges/approve returns the peer's stored address and key
    fingerprints, for the operator to compare with the peer's own values out of band."""
    monkeypatch.setenv("KOI_ADMIN_TOKEN", ADMIN)
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_nodes SET encryption_key = $2 WHERE node_rid = $1", who.rid,
                           "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg=")
    edge_rid = await _edge(node, who.rid, node.me.rid, "PROPOSED")
    operator = httpx.AsyncClient(transport=httpx.ASGITransport(app=node.client._transport.app, client=("127.0.0.1", 40005)),
                                 base_url="http://node", headers={"Authorization": f"Bearer {ADMIN}"})
    async with operator:
        r = await operator.post("/koi-net/edges/approve", json={"edge_rid": edge_rid})
    assert r.status_code == 200, r.text
    peer = r.json()["peer"]
    assert peer["node_rid"] == who.rid and peer["base_url"] == "http://peer.invalid/koi-net"
    assert peer["public_key_fingerprint"] == knr._key_fingerprint(who.pub)
    assert peer["encryption_key_fingerprint"] == knr._key_fingerprint("eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg=")
    assert peer["encryption_key_fingerprint"].startswith("SHA256:")


async def test_t11_an_operator_reopens_a_held_edge_then_approves_it(node, monkeypatch, tmp_path):
    """T11: /edges/reopen moves REVOKED or REJECTED to PROPOSED and /edges/approve then
    approves it; /edges/approve alone refuses a held edge. Admin-only, and refused for a barred
    node; nothing changes on a refusal."""
    monkeypatch.setenv("KOI_STATE_DIR", str(tmp_path))
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    edge_rid = await _edge(node, node.me.rid, who.rid, "REVOKED")
    app = node.client._transport.app
    loopback = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 40006)), base_url="http://node")
    async with loopback:
        r = await loopback.post("/koi-net/edges/reopen", json={"edge_rid": edge_rid})
        assert r.status_code == 503 and r.json()["error_code"] == "ADMIN_TOKEN_UNCONFIGURED", r.text
        monkeypatch.setenv("KOI_ADMIN_TOKEN", ADMIN)
        r = await loopback.post("/koi-net/edges/reopen", json={"edge_rid": edge_rid})
        assert r.status_code == 401, r.text
        assert await _edge_status(node, edge_rid) == "REVOKED"
        op = {"Authorization": f"Bearer {ADMIN}"}
        r = await loopback.post("/koi-net/edges/approve", json={"edge_rid": edge_rid}, headers=op)
        assert r.status_code == 404 and await _edge_status(node, edge_rid) == "REVOKED", r.text
        r = await loopback.post("/koi-net/edges/reopen", json={"edge_rid": edge_rid}, headers=op)
        assert r.status_code == 200 and r.json()["peer"]["base_url"] == "http://peer.invalid/koi-net", r.text
        assert await _edge_status(node, edge_rid) == "PROPOSED"
        r = await loopback.post("/koi-net/edges/reopen", json={"edge_rid": edge_rid}, headers=op)
        assert r.status_code == 404 and r.json()["error_code"] == "EDGE_NOT_HELD", r.text
        r = await loopback.post("/koi-net/edges/approve", json={"edge_rid": edge_rid}, headers=op)
        assert r.status_code == 200 and await _edge_status(node, edge_rid) == "APPROVED", r.text
        # A barred node's held edge is not reopened.
        barred = _identity("rejection-test-barred")
        node.created.append(barred.rid)
        await _reject(node, barred)
        held = await _edge(node, node.me.rid, barred.rid, "REJECTED")
        r = await loopback.post("/koi-net/edges/reopen", json={"edge_rid": held}, headers=op)
        assert r.status_code == 409 and r.json()["error_code"] == "NODE_REJECTED", r.text
        assert await _edge_status(node, held) == "REJECTED"


# --- replay of /handshake: a signed handshake carries issued_at -----------------------------

def _iso(delta_seconds=0):
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) + timedelta(seconds=delta_seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


async def _base_url(node, rid):
    async with node.pool.acquire() as conn:
        return await conn.fetchval("SELECT base_url FROM koi_net_nodes WHERE node_rid = $1", rid)


async def test_r1_a_replayed_older_signed_handshake_is_refused_and_a_newer_one_accepted(node):
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    first = _signed(node, who, _handshake(who, base_url="http://peer.invalid/first", issued_at=_iso(-2)))
    second = _signed(node, who, _handshake(who, base_url="http://peer.invalid/second", issued_at=_iso(-1)))
    assert (await node.client.post("/koi-net/handshake", json=first)).status_code == 200
    assert (await node.client.post("/koi-net/handshake", json=second)).status_code == 200  # newer: accepted
    assert await _base_url(node, who.rid) == "http://peer.invalid/second"
    r = await node.client.post("/koi-net/handshake", json=first)  # the replay would roll base_url back
    assert r.status_code == 409 and r.json()["error_code"] == "STALE_HANDSHAKE", r.text
    assert await _base_url(node, who.rid) == "http://peer.invalid/second"
    # A replay that changes nothing is refused too (harmless): every self-signed
    # handshake with a stamp must be newer than the stored one, so a replay cannot write the
    # older profile back after a newer change lands between its read and its write (T19).
    r = await node.client.post("/koi-net/handshake", json=second)
    assert r.status_code == 409 and await _base_url(node, who.rid) == "http://peer.invalid/second", r.text


async def test_r1_outside_the_window_or_missing_after_a_stored_value_is_refused(node):
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/old", issued_at=_iso(-1200))))
    assert r.status_code == 409 and r.json()["error_code"] == "STALE_HANDSHAKE", r.text
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/future", issued_at=_iso(1200))))
    assert r.status_code == 409, r.text
    # No stored value yet: a signed change without issued_at is accepted as before (older peers).
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who, base_url="http://peer.invalid/legacy")))
    assert r.status_code == 200 and await _base_url(node, who.rid) == "http://peer.invalid/legacy", r.text
    assert (await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/current", issued_at=_iso())))).status_code == 200
    # Once a value is stored, a signed change without one could be a pre-upgrade replay: refused.
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who, base_url="http://peer.invalid/legacy")))
    assert r.status_code == 409 and await _base_url(node, who.rid) == "http://peer.invalid/current", r.text
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/x", issued_at="not a time")))
    assert r.status_code == 409, r.text


# --- a re-handshake changes rid_types only on a PROPOSED edge ----------------------------------

@pytest.mark.parametrize("status", ["APPROVED", "REVOKED", "PROPOSED"])
async def test_t15_a_signed_re_handshake_keeps_a_granted_edges_rid_types(node, status):
    """T15: an operator narrowed our edge to the peer to {SpecDoc}, and the peer's inbound edge
    to {orn:koi-net.node}. A signed re-handshake keeps both unless the edge is still PROPOSED,
    which takes the advertised types as before (the control)."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    outbound = await _edge(node, node.me.rid, who.rid, status)
    inbound = await _edge(node, who.rid, node.me.rid, status)
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_edges SET rid_types = ARRAY['SpecDoc'] WHERE edge_rid = $1", outbound)
        await conn.execute("UPDATE koi_net_edges SET rid_types = ARRAY['orn:koi-net.node'] WHERE edge_rid = $1", inbound)
    hs = _handshake(who, issued_at=_iso())
    hs["profile"]["provides"] = {"event": ["orn:koi-net.person", "orn:koi-net.node"], "state": []}
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, hs))
    assert r.status_code == 200, r.text
    async with node.pool.acquire() as conn:
        out_types = await conn.fetchval("SELECT rid_types FROM koi_net_edges WHERE edge_rid = $1", outbound)
        in_types = await conn.fetchval("SELECT rid_types FROM koi_net_edges WHERE edge_rid = $1", inbound)
    if status == "PROPOSED":
        assert in_types == ["orn:koi-net.person", "orn:koi-net.node"]  # the peer's provides, as before
        assert out_types != ["SpecDoc"]  # our provides, as before
    else:
        assert (out_types, in_types) == (["SpecDoc"], ["orn:koi-net.node"])


# --- the freshness decision is atomic ----------------------------------------------------------

import asyncio  # noqa: E402
import contextvars  # noqa: E402
from contextlib import asynccontextmanager  # noqa: E402

_HELD = contextvars.ContextVar("held_handshake", default=None)


class _HoldingConn:
    """Delegates to a pool connection, but a request marked `held` waits at its key-pinning
    read: the first await after /handshake's freshness check, before its profile upsert."""

    def __init__(self, conn):
        self._conn = conn

    def __getattr__(self, name):
        return getattr(self._conn, name)

    async def fetchval(self, sql, *args, **kwargs):
        gate = _HELD.get()
        if gate is not None and sql.startswith("SELECT public_key FROM koi_net_nodes"):
            gate["at_check"].set()
            await gate["release"].wait()
        return await self._conn.fetchval(sql, *args, **kwargs)


async def _release_after_failure(gate, task):
    """A step failed while a request was held: release it and wait for it, so it gives its pool
    connection back. Left held, it keeps the connection, and the fixture's pool.close() waits for
    it forever (seen once for 80 minutes, under a deliberately broken build). The step's failure is the report, so the
    held request's own outcome is dropped."""
    gate["release"].set()
    try:
        await asyncio.wait_for(task, 10)
    except (Exception, asyncio.CancelledError):
        pass


class _HoldingPool:
    def __init__(self, pool):
        self._pool = pool

    def __getattr__(self, name):
        return getattr(self._pool, name)

    def acquire(self):
        pool = self._pool

        @asynccontextmanager
        async def held():
            async with pool.acquire() as conn:
                yield _HoldingConn(conn)
        return held()


async def test_t16_two_racing_signed_changes_cannot_leave_the_older_profile(node, monkeypatch):
    """T16: A (t1) passes its freshness check, then waits; B (t2 > t1) completes; A resumes.
    A must be refused (409) and write nothing: the row keeps B's base_url and encryption_key,
    the stored issued_at is t2, and A adds no alias and touches no edge."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    first = _signed(node, who, _handshake(who, base_url="http://peer.invalid/t0", issued_at=_iso(-3)))
    assert (await node.client.post("/koi-net/handshake", json=first)).status_code == 200
    monkeypatch.setattr(knr, "_db_pool", _HoldingPool(node.pool))
    t1, t2 = _iso(-2), _iso(-1)
    a = _signed(node, who, _handshake(who, base_url="http://peer.invalid/a", encryption_key="enc-a",
                                      node_name=who.name + "-a", issued_at=t1))
    b = _signed(node, who, _handshake(who, base_url="http://peer.invalid/b", encryption_key="enc-b", issued_at=t2))
    gate = {"at_check": asyncio.Event(), "release": asyncio.Event()}

    async def send_a():
        _HELD.set(gate)
        return await node.client.post("/koi-net/handshake", json=a)

    task_a = asyncio.create_task(send_a())
    try:
        await asyncio.wait_for(gate["at_check"].wait(), 10)  # A passed its check and is waiting
        rb = await node.client.post("/koi-net/handshake", json=b)
        assert rb.status_code == 200, rb.text
        async with node.pool.acquire() as conn:
            edges_after_b = await conn.fetch("SELECT edge_rid, updated_at FROM koi_net_edges WHERE source_node = $1 OR target_node = $1 ORDER BY edge_rid", who.rid)
    except BaseException:
        await _release_after_failure(gate, task_a)
        raise
    gate["release"].set()
    ra = await asyncio.wait_for(task_a, 10)
    assert ra.status_code == 409 and ra.json()["error_code"] == "STALE_HANDSHAKE", ra.text
    async with node.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT base_url, encryption_key, node_name, metadata->>'handshake_issued_at' AS iss "
                                  "FROM koi_net_nodes WHERE node_rid = $1", who.rid)
        edges_after_a = await conn.fetch("SELECT edge_rid, updated_at FROM koi_net_edges WHERE source_node = $1 OR target_node = $1 ORDER BY edge_rid", who.rid)
        alias_a = await conn.fetchval("SELECT 1 FROM koi_net_peer_aliases WHERE alias = $1", (who.name + "-a").lower())
    assert (row["base_url"], row["encryption_key"], row["node_name"]) == ("http://peer.invalid/b", "enc-b", who.name)
    assert row["iss"] == t2
    assert edges_after_a == edges_after_b and alias_a is None


async def test_t16_control_a_lone_newer_signed_change_applies(node):
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    for when, url in ((_iso(-2), "http://peer.invalid/one"), (_iso(-1), "http://peer.invalid/two")):
        r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who, base_url=url, issued_at=when)))
        assert r.status_code == 200, r.text
    async with node.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT base_url, metadata->>'handshake_issued_at' AS iss FROM koi_net_nodes WHERE node_rid = $1", who.rid)
    assert row["base_url"] == "http://peer.invalid/two" and row["iss"] is not None


async def _stored_issued_at(node, rid):
    async with node.pool.acquire() as conn:
        return await conn.fetchval("SELECT metadata->>'handshake_issued_at' FROM koi_net_nodes WHERE node_rid = $1", rid)


async def test_t17_an_out_of_window_stamp_on_an_unchanged_handshake_is_not_stored(node):
    """T17: only an issued_at inside the window is stored, so a far-future stamp on a handshake
    that changes nothing cannot lock the node's later changes out."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    unchanged = _signed(node, who, _handshake(who, issued_at=_iso(86400)))
    assert (await node.client.post("/koi-net/handshake", json=unchanged)).status_code == 200
    assert await _stored_issued_at(node, who.rid) is None
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/later", issued_at=_iso())))
    assert r.status_code == 200 and await _base_url(node, who.rid) == "http://peer.invalid/later", r.text


async def test_t17_an_out_of_window_stamp_at_first_contact_is_not_stored(node):
    who = _identity("rejection-test-new")
    node.created.append(who.rid)
    first = _signed(node, who, _handshake(who, issued_at=_iso(86400)))  # signed with its profile key
    assert (await node.client.post("/koi-net/handshake", json=first)).status_code == 200
    assert await _status(node, who.rid) == "active" and await _stored_issued_at(node, who.rid) is None
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/later", issued_at=_iso())))
    assert r.status_code == 200 and await _base_url(node, who.rid) == "http://peer.invalid/later", r.text



# --- no caller writes back a profile it read stale -----------------------------------------------

async def _race(node, monkeypatch, who, held_request, t2):
    """Send `held_request`, hold it after its read (at its key-pinning fetchval), let a signed
    change B (P2, t2) complete, then release it. Returns its response."""
    monkeypatch.setattr(knr, "_db_pool", _HoldingPool(node.pool))
    b = _signed(node, who, _handshake(who, base_url="http://peer.invalid/b", encryption_key="enc-b", issued_at=t2))
    gate = {"at_check": asyncio.Event(), "release": asyncio.Event()}

    async def send_held():
        _HELD.set(gate)
        return await node.client.post("/koi-net/handshake", json=held_request)

    task = asyncio.create_task(send_held())
    try:
        await asyncio.wait_for(gate["at_check"].wait(), 10)
        rb = await node.client.post("/koi-net/handshake", json=b)
        assert rb.status_code == 200, rb.text
    except BaseException:
        await _release_after_failure(gate, task)
        raise
    gate["release"].set()
    return await asyncio.wait_for(task, 10)


async def _profile(node, rid):
    async with node.pool.acquire() as conn:
        return await conn.fetchrow("SELECT base_url, encryption_key, metadata->>'handshake_issued_at' AS iss "
                                   "FROM koi_net_nodes WHERE node_rid = $1", rid)


async def test_t18_an_unsigned_unchanged_handshake_cannot_write_an_older_profile_back(node, monkeypatch):
    """T18: an unsigned handshake carrying P0's public values reads 'unchanged'. If a signed
    change to P2 lands before its write, it must not write P0 back."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    t2 = _iso(-1)
    r = await _race(node, monkeypatch, who, _handshake(who), t2)
    assert r.status_code == 200, r.text  # unchanged as it read it; it writes nothing now
    row = await _profile(node, who.rid)
    assert (row["base_url"], row["encryption_key"], row["iss"]) == ("http://peer.invalid/b", "enc-b", t2)


async def test_t19_a_replay_of_an_older_signed_handshake_cannot_write_its_profile_back(node, monkeypatch):
    """T19: a replay of the peer's own accepted (P0, t1) reads 'unchanged'; after B (t2)
    lands it is refused, and the row keeps P2 under t2."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    t1, t2 = _iso(-2), _iso(-1)
    h1 = _signed(node, who, _handshake(who, issued_at=t1))
    assert (await node.client.post("/koi-net/handshake", json=h1)).status_code == 200
    r = await _race(node, monkeypatch, who, h1, t2)
    assert r.status_code == 409 and r.json()["error_code"] == "STALE_HANDSHAKE", r.text
    row = await _profile(node, who.rid)
    assert (row["base_url"], row["encryption_key"], row["iss"]) == ("http://peer.invalid/b", "enc-b", t2)


async def test_t20_a_pre_upgrade_replay_without_issued_at_cannot_write_its_profile_back(node, monkeypatch):
    """T20: a replay of a legacy signed handshake (no issued_at) read while nothing was
    stored; B (t2) lands first, so it writes no profile field."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    h0 = _signed(node, who, _handshake(who))  # what an older peer signs: no issued_at
    assert (await node.client.post("/koi-net/handshake", json=h0)).status_code == 200
    t2 = _iso(-1)
    r = await _race(node, monkeypatch, who, h0, t2)
    assert r.status_code in (200, 409), r.text
    row = await _profile(node, who.rid)
    assert (row["base_url"], row["encryption_key"], row["iss"]) == ("http://peer.invalid/b", "enc-b", t2)


async def test_f_controls_the_operator_and_a_lone_signed_change_still_write(node, monkeypatch):
    monkeypatch.setenv("KOI_ADMIN_TOKEN", ADMIN)
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/self", issued_at=_iso(-1))))
    assert r.status_code == 200 and await _base_url(node, who.rid) == "http://peer.invalid/self", r.text
    operator = httpx.AsyncClient(transport=httpx.ASGITransport(app=node.client._transport.app, client=("127.0.0.1", 40007)),
                                 base_url="http://node", headers={"Authorization": f"Bearer {ADMIN}"})
    async with operator:
        r = await operator.post("/koi-net/handshake", json=_handshake(who, base_url="http://peer.invalid/operator"))
    assert r.status_code == 200 and await _base_url(node, who.rid) == "http://peer.invalid/operator", r.text


async def test_t20_a_legacy_change_read_before_a_stamp_was_stored_is_refused_at_the_write(node, monkeypatch):
    """T20, NULL to present: a legacy self-signed change (no issued_at) is
    allowed by its read because nothing was stored; B stores t2 before its write, so the write
    itself refuses it (409) and the row keeps P2 under t2."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    legacy_change = _signed(node, who, _handshake(who, base_url="http://peer.invalid/legacy"))
    t2 = _iso(-1)
    r = await _race(node, monkeypatch, who, legacy_change, t2)
    assert r.status_code == 409 and r.json()["error_code"] == "STALE_HANDSHAKE", r.text
    row = await _profile(node, who.rid)
    assert (row["base_url"], row["encryption_key"], row["iss"]) == ("http://peer.invalid/b", "enc-b", t2)


async def test_t20_with_a_stamp_already_stored_a_legacy_handshake_writes_no_profile_field(node):
    """T20, already stored: an unchanged legacy handshake is accepted and writes nothing; a
    changing one is refused (409). The row keeps the stamped profile."""
    who = _identity("rejection-test-peer")
    node.created.append(who.rid)
    await _active(node, who)
    t1 = _iso(-1)
    r = await node.client.post("/koi-net/handshake", json=_signed(
        node, who, _handshake(who, base_url="http://peer.invalid/stamped", encryption_key="enc-s", issued_at=t1)))
    assert r.status_code == 200, r.text
    unchanged = _handshake(who, base_url="http://peer.invalid/stamped", encryption_key="enc-s")
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, unchanged))
    assert r.status_code == 200, r.text
    r = await node.client.post("/koi-net/handshake", json=_signed(node, who, _handshake(who, base_url="http://peer.invalid/legacy")))
    assert r.status_code == 409, r.text
    row = await _profile(node, who.rid)
    assert (row["base_url"], row["encryption_key"], row["iss"]) == ("http://peer.invalid/stamped", "enc-s", t1)
