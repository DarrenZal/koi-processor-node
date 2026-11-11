"""KOI-net router guards for public issues 2, 3A and 3B, on a throwaway Postgres (see conftest.py).

- Issue 2, admission: a node's first handshake leaves its inbound edge PROPOSED until an operator approves it.
  An existing edge keeps its status on a re-handshake, and the handshake reports the stored status.
- Issue 3A, poll: while the approved-edge gate is on, an unsigned poll is unidentified, whatever node_id it names.
- Issue 3B, fetch: /manifests/fetch, /bundles/fetch and /rids/fetch serve a caller only through the edge on which
  this node serves that caller (source = this node, target = caller), the edge /events/poll checks.

Requests go through the real FastAPI routing with a chosen client address. Signed requests use real P-256 keys.
Run: python -B -m pytest -q -p no:cacheprovider tests/peer_guards
"""
import asyncio
import json

import httpx
import pytest
from fastapi import FastAPI

import api.koi_net_router as R
from api.event_queue import EventQueue
from api.koi_envelope import sign_envelope
from api.koi_protocol import NodeProfile, NodeProvides
from api.node_identity import derive_node_rid, generate_keypair, get_public_key_der_b64

from peer_guards_db import open_pool

TOKEN = "test-admin-token-not-a-real-secret"
OWN = "orn:koi-net.node:test-self+" + "0" * 64
RID = "orn:koi-net.specdoc:peer-guard-test+00"
OUTSIDE = "192.0.2.99"  # a documentation address: not loopback, so no operator path


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Router globals and flags. Flags as on a node with the approved-edge gate on, strict mode off, admission
    setting unset; signed envelopes are not required so that an unsigned request reaches the handler."""
    state = tmp_path / "koi-state"
    state.mkdir()
    (state / "admin_token").write_text(TOKEN + "\n")
    monkeypatch.setenv("KOI_STATE_DIR", str(state))
    for name in ("KOI_ADMIN_TOKEN", "KOI_STRICT_MODE", "KOI_REQUIRE_SIGNED_ENVELOPES", "KOI_ENFORCE_TARGET_MATCH",
                 "KOI_ENFORCE_SOURCE_KEY_RID_BINDING", "KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "KOI_FEDERATION_RID_TYPES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL", "true")
    for name in ("_db_pool", "_event_queue", "_node_profile", "_private_key", "_vault_sync"):
        monkeypatch.setattr(R, name, None)
    monkeypatch.setattr(R, "_node_profile", NodeProfile(
        node_rid=OWN, node_name="test-self", node_type="FULL", base_url="http://self.invalid:8351",
        provides=NodeProvides(event=["orn:koi-net.specdoc"], state=[])))
    return monkeypatch


class Peer:
    def __init__(self, name):
        self.key = generate_keypair()
        self.der = get_public_key_der_b64(self.key)
        self.rid = derive_node_rid(name, self.key.public_key())
        self.name = name

    def profile(self):
        return {"node_rid": self.rid, "node_name": self.name, "node_type": "FULL",
                "base_url": "http://peer.invalid:8351", "provides": {"event": ["orn:koi-net.specdoc"], "state": []},
                "public_key": self.der}

    def sign(self, payload):
        return sign_envelope(payload, self.rid, OWN, self.key)


def run(pg, scenario):
    """Run one scenario in one event loop, with a fresh pool on the reset test database."""
    async def go():
        pool = await open_pool(pg)
        R._db_pool = pool
        R._event_queue = EventQueue(pool, OWN)
        app = FastAPI()
        app.include_router(R.koi_net_router, prefix="/koi-net")

        async def call(path, host, body, bearer=None):
            headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
            transport = httpx.ASGITransport(app=app, client=(host, 50000), raise_app_exceptions=False)
            async with httpx.AsyncClient(transport=transport, base_url="http://koi.test") as c:
                return await c.post(path, json=body, headers=headers)
        try:
            return await scenario(pool, call)
        finally:
            await pool.close()
    return asyncio.run(go())


async def add_node(conn, peer):
    await conn.execute("INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
                       "VALUES ($1, $2, 'FULL', 'http://peer.invalid:8351', $3, 'active')", peer.rid, peer.name, peer.der)


async def add_edge(conn, source, target, status="APPROVED"):
    await conn.execute("INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) "
                       "VALUES ($1, $2, $3, 'POLL', $4, NULL)", f"orn:koi-net.edge:{source}>{target}:poll",
                       source, target, status)


async def add_event(conn):
    manifest = {"rid": RID, "timestamp": "2026-11-07T00:00:00Z", "sha256_hash": "0" * 64}
    contents = {"title": "T", "content": "# T\n"}
    await conn.execute("INSERT INTO koi_net_events (event_type, rid, manifest, contents, source_node, target_node) "
                       "VALUES ('NEW', $1, $2::jsonb, $3::jsonb, $4, NULL)", RID, json.dumps(manifest),
                       json.dumps(contents), OWN)


async def edge_status(conn, source, target):
    return await conn.fetchval("SELECT status FROM koi_net_edges WHERE source_node = $1 AND target_node = $2",
                               source, target)


def body(resp):
    data = resp.json()
    return data.get("payload", data) if isinstance(data, dict) else data


# ------------------------------------------------------------------ issue 2: admission

def test_issue2_first_handshake_leaves_inbound_edge_proposed(env, pg):
    peer = Peer("first-contact")

    async def scenario(pool, call):
        r = await call("/koi-net/handshake", OUTSIDE, {"type": "handshake", "profile": peer.profile()})
        async with pool.acquire() as conn:
            stored = await edge_status(conn, peer.rid, OWN)
            node = await conn.fetchval("SELECT 1 FROM koi_net_nodes WHERE node_rid = $1", peer.rid)
        return r, stored, node
    r, stored, node = run(pg, scenario)
    assert r.status_code == 200, (r.status_code, r.text)
    assert node == 1  # premise: the handshake was processed and the node row written
    assert stored == "PROPOSED", stored
    assert body(r).get("edge_status") == "PROPOSED", r.text


def test_issue2_operator_approved_edge_stays_approved_on_rehandshake(env, pg):
    """Control: after the operator approves, a signed re-handshake keeps APPROVED and reports it."""
    peer = Peer("approved-peer")

    async def scenario(pool, call):
        await call("/koi-net/handshake", OUTSIDE, {"type": "handshake", "profile": peer.profile()})
        edge = f"orn:koi-net.edge:{peer.rid}>{OWN}:poll"
        async with pool.acquire() as conn:
            await conn.execute("UPDATE koi_net_edges SET status = 'PROPOSED' WHERE edge_rid = $1", edge)
        a = await call("/koi-net/edges/approve", "127.0.0.1", {"edge_rid": edge}, bearer=TOKEN)
        r = await call("/koi-net/handshake", OUTSIDE, peer.sign({"type": "handshake", "profile": peer.profile()}))
        async with pool.acquire() as conn:
            stored = await edge_status(conn, peer.rid, OWN)
        return a, r, stored
    a, r, stored = run(pg, scenario)
    assert a.status_code == 200 and a.json().get("status") == "approved", (a.status_code, a.text)
    assert r.status_code == 200, (r.status_code, r.text)
    assert stored == "APPROVED", stored
    assert body(r).get("edge_status") == "APPROVED", r.text


def test_issue2_revoked_edge_stays_revoked_and_is_reported_as_stored(env, pg):
    peer = Peer("revoked-peer")

    async def scenario(pool, call):
        await call("/koi-net/handshake", OUTSIDE, {"type": "handshake", "profile": peer.profile()})
        async with pool.acquire() as conn:
            await conn.execute("UPDATE koi_net_edges SET status = 'REVOKED' WHERE source_node = $1", peer.rid)
        r = await call("/koi-net/handshake", OUTSIDE, peer.sign({"type": "handshake", "profile": peer.profile()}))
        async with pool.acquire() as conn:
            stored = await edge_status(conn, peer.rid, OWN)
        return r, stored
    r, stored = run(pg, scenario)
    assert r.status_code == 200, (r.status_code, r.text)
    assert stored == "REVOKED", stored
    assert body(r).get("edge_status") == "REVOKED", r.text


def test_issue2_operator_can_turn_admission_off(env, pg):
    """Control: KOI_NET_DEFER_UNKNOWN_HANDSHAKE=false restores the old auto-approval of a new inbound edge."""
    env.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "false")
    peer = Peer("opt-out-peer")

    async def scenario(pool, call):
        r = await call("/koi-net/handshake", OUTSIDE, {"type": "handshake", "profile": peer.profile()})
        async with pool.acquire() as conn:
            return r, await edge_status(conn, peer.rid, OWN)
    r, stored = run(pg, scenario)
    assert r.status_code == 200 and stored == "APPROVED", (r.status_code, stored)


# ------------------------------------------------------------------ issue 3A: poll identity

def _poll_world(peer):
    async def setup(conn):
        await add_node(conn, peer)
        await add_edge(conn, OWN, peer.rid)  # we serve the peer: the edge /events/poll checks
        await add_event(conn)
    return setup


async def delivered(conn, peer):
    return await conn.fetchval("SELECT bool_or($1 = ANY(delivered_to)) FROM koi_net_events", peer.rid)


def test_issue3a_unsigned_poll_naming_an_approved_peer_gets_nothing(env, pg):
    peer = Peer("polling-peer")

    async def scenario(pool, call):
        async with pool.acquire() as conn:
            await _poll_world(peer)(conn)
        r = await call("/koi-net/events/poll", OUTSIDE, {"type": "poll_events", "limit": 50, "node_id": peer.rid})
        async with pool.acquire() as conn:
            return r, await delivered(conn, peer)
    r, was_delivered = run(pg, scenario)
    assert r.status_code == 200, (r.status_code, r.text)
    assert body(r).get("events") == [], r.text
    assert not was_delivered


def test_issue3a_signed_poll_still_receives(env, pg):
    """Control: the same peer, signed, receives the event and it is marked delivered."""
    peer = Peer("polling-peer")

    async def scenario(pool, call):
        async with pool.acquire() as conn:
            await _poll_world(peer)(conn)
        r = await call("/koi-net/events/poll", OUTSIDE, peer.sign({"type": "poll_events", "limit": 50}))
        async with pool.acquire() as conn:
            return r, await delivered(conn, peer)
    r, was_delivered = run(pg, scenario)
    assert r.status_code == 200, (r.status_code, r.text)
    assert [e["rid"] for e in body(r).get("events", [])] == [RID], r.text
    assert was_delivered


# ------------------------------------------------------------------ issue 3B: fetch edge direction

def _fetch_calls(peer):
    return [
        ("/koi-net/manifests/fetch", {"type": "fetch_manifests", "rids": [RID]}, "manifests"),
        ("/koi-net/bundles/fetch", {"type": "fetch_bundles", "rids": [RID]}, "bundles"),
        ("/koi-net/rids/fetch", {"type": "fetch_rids"}, "rids"),
    ]


def _fetch(env, pg, edges):
    """Signed fetches by one peer on all three routes; `edges` lists (source, target) APPROVED edges to create.
    /rids/fetch lists from tables outside this suite, so its query is replaced by one that returns RID: the gate
    under test runs before it."""
    env.setattr(R, "_build_rids_fetch_query", lambda rid_types, groups: ("SELECT $1::text AS koi_rid", [RID]))
    peer = Peer("fetching-peer")

    async def scenario(pool, call):
        async with pool.acquire() as conn:
            await add_node(conn, peer)
            await add_event(conn)
            for source, target in edges:
                await add_edge(conn, peer.rid if source == "peer" else OWN, peer.rid if target == "peer" else OWN)
        out = {}
        for path, payload, key in _fetch_calls(peer):
            r = await call(path, OUTSIDE, peer.sign(payload))
            assert r.status_code == 200, (path, r.status_code, r.text)
            out[key] = body(r).get(key)
        return out
    return run(pg, scenario)


def test_issue3b_fetch_through_the_callers_own_edge_gets_nothing(env, pg):
    """Only the peer-to-us edge, the one a first handshake used to approve, is APPROVED."""
    out = _fetch(env, pg, [("peer", "own")])
    assert out == {"manifests": [], "bundles": [], "rids": []}, out


def test_issue3b_fetch_through_the_edge_we_serve_gets_rows(env, pg):
    out = _fetch(env, pg, [("own", "peer")])
    assert len(out["manifests"]) == 1 and len(out["bundles"]) == 1 and out["rids"] == [RID], out


def test_issue3b_fetch_with_both_edges_gets_rows(env, pg):
    """Premise control: the routes serve the row at all, on any version, when both edges are APPROVED."""
    out = _fetch(env, pg, [("own", "peer"), ("peer", "own")])
    assert len(out["manifests"]) == 1 and len(out["bundles"]) == 1 and out["rids"] == [RID], out
