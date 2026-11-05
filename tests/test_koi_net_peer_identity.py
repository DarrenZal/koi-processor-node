"""A known peer's record changes only under its own signature or from the local operator, the
deferral setting takes effect, and the approved-edge gate uses one identity rule and one edge on
every route.

Needs KOI_TEST_DATABASE_URL; skips otherwise. Rows use unique RIDs and are removed afterwards.
"""
import json
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
ORIGINAL_URL = "http://peer.invalid/original"


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _new_identity(prefix):
    key = generate_keypair()
    name = f"{prefix}-{uuid.uuid4().hex[:10]}"
    return SimpleNamespace(key=key, name=name, rid=derive_node_rid(name, key.public_key()),
                           pub=get_public_key_der_b64(key))


def _handshake(rid, name, **profile):
    body = {"node_rid": rid, "node_name": name, "node_type": "FULL",
            "provides": {"event": profile.pop("events", []), "state": []}, **profile}
    return {"type": "handshake", "profile": body}


@pytest.fixture
async def node(monkeypatch):
    url = targets.database_url()
    pool = await asyncpg.create_pool(url, min_size=1, max_size=4)
    me = _new_identity("identity-test-node")
    profile = NodeProfile(node_rid=me.rid, node_name=me.name, node_type="FULL",
                          provides=NodeProvides(event=[], state=[]), public_key=me.pub)
    monkeypatch.setattr(knr, "_db_pool", pool)
    monkeypatch.setattr(knr, "_private_key", me.key)
    monkeypatch.setattr(knr, "_node_profile", profile)
    monkeypatch.setattr(knr, "_event_queue", EventQueue(pool, me.rid))
    monkeypatch.setattr(knr, "_vault_sync", None)
    for var in ("KOI_STRICT_MODE", "KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "KOI_REQUIRE_SIGNED_ENVELOPES", "KOI_ADMIN_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL", "true")
    app = FastAPI()
    app.include_router(knr.koi_net_router, prefix="/koi-net")
    state = SimpleNamespace(pool=pool, me=me, created=[me.rid], event_rids=[], entity_uris=[], aliases=[])

    def peer(prefix="identity-test-peer"):
        who = _new_identity(prefix)
        state.created.append(who.rid)
        state.aliases.append(who.name.lower())
        return who

    state.peer = peer
    remote = httpx.ASGITransport(app=app, client=("198.51.100.7", 40000))
    local = httpx.ASGITransport(app=app, client=("127.0.0.1", 40001))
    async with httpx.AsyncClient(transport=remote, base_url="http://node") as client, \
            httpx.AsyncClient(transport=local, base_url="http://node") as operator:
        state.client, state.operator = client, operator
        yield state
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM koi_net_edges WHERE source_node = ANY($1) OR target_node = ANY($1)", state.created)
        await conn.execute("DELETE FROM koi_net_peer_aliases WHERE node_rid = ANY($1) OR alias = ANY($2)", state.created, state.aliases)
        await conn.execute("DELETE FROM koi_net_nodes WHERE node_rid = ANY($1)", state.created)
        await conn.execute("DELETE FROM koi_net_events WHERE rid = ANY($1)", state.event_rids)
        await conn.execute("DELETE FROM entity_registry WHERE fuseki_uri = ANY($1)", state.entity_uris)
    await pool.close()


async def _seed_known_peer(node, who, *, status="APPROVED"):
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, encryption_key, status) "
            "VALUES ($1, $2, 'FULL', $3, $4, 'original-key', 'active')",
            who.rid, who.name, ORIGINAL_URL, who.pub)
        await conn.execute(
            "INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types) VALUES "
            "($1, $2, $3, 'POLL', $5, ARRAY['person']), ($4, $3, $2, 'POLL', $5, ARRAY['person'])",
            f"orn:koi-net.edge:{who.rid}>{node.me.rid}:poll", who.rid, node.me.rid,
            f"orn:koi-net.edge:{node.me.rid}>{who.rid}:poll", status)


async def _row(node, who):
    async with node.pool.acquire() as conn:
        return await conn.fetchrow("SELECT base_url, public_key, encryption_key, last_seen FROM koi_net_nodes WHERE node_rid = $1", who.rid)


async def _edge(node, source, target):
    async with node.pool.acquire() as conn:
        return await conn.fetchrow("SELECT status, rid_types FROM koi_net_edges WHERE source_node = $1 AND target_node = $2", source, target)


def _payload(response):
    body = response.json()
    return body.get("payload", body)


async def test_first_contact_without_a_signature_still_registers(node):
    who = node.peer()
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=who.pub))
    assert r.status_code == 200, r.text
    assert (await _row(node, who))["public_key"] == who.pub


async def test_deferral_setting_applies_to_a_never_seen_node(node, monkeypatch):
    monkeypatch.setenv("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", "true")
    who = node.peer()
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=who.pub))
    assert r.json()["edge_status"] == "PROPOSED"
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "PROPOSED"


async def test_without_the_setting_the_inbound_edge_is_approved(node):
    who = node.peer()
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=who.pub))
    assert r.json()["edge_status"] == "APPROVED"


@pytest.mark.parametrize("with_matching_key", [False, True])
@pytest.mark.parametrize("strict", [False, True])
async def test_unsigned_handshake_cannot_change_a_known_peer(node, monkeypatch, strict, with_matching_key):
    if strict:
        monkeypatch.setenv("KOI_STRICT_MODE", "true")
    who = node.peer()
    await _seed_known_peer(node, who)
    extra = {"public_key": who.pub} if with_matching_key else {}
    r = await node.client.post("/koi-net/handshake", json=_handshake(
        who.rid, who.name, base_url="http://peer.invalid/changed", encryption_key="changed-key", events=["changed-type"], **extra))
    assert r.status_code == 401, r.text
    assert r.json()["error_code"] == "SIGNED_HANDSHAKE_REQUIRED"
    row = await _row(node, who)
    assert (row["base_url"], row["encryption_key"]) == (ORIGINAL_URL, "original-key")
    assert (await _edge(node, who.rid, node.me.rid))["rid_types"] == ["person"]


async def test_unsigned_handshake_that_changes_nothing_is_accepted_and_keeps_edge_scope(node):
    who = node.peer()
    await _seed_known_peer(node, who)  # its edges are scoped narrower than the empty provides list
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, base_url=ORIGINAL_URL, public_key=who.pub))
    assert r.status_code == 200, r.text
    assert (await _edge(node, who.rid, node.me.rid))["rid_types"] == ["person"]
    assert (await _edge(node, node.me.rid, who.rid))["rid_types"] == ["person"]
    assert (await _row(node, who))["last_seen"] is None, "an unsigned request is not proof of life"


async def test_unsigned_handshake_creates_missing_edges_for_a_known_peer(node):
    who = node.peer()
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, public_key, status) "
            "VALUES ($1, $2, 'FULL', $3, $4, 'active')", who.rid, who.name, ORIGINAL_URL, who.pub)
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, base_url=ORIGINAL_URL, public_key=who.pub))
    assert r.status_code == 200, r.text
    assert (await _edge(node, who.rid, node.me.rid)) is not None
    assert (await _edge(node, node.me.rid, who.rid))["status"] == "PROPOSED"


async def test_handshake_signed_by_the_pinned_key_updates_the_peer(node):
    who = node.peer()
    await _seed_known_peer(node, who)
    envelope = sign_envelope(_handshake(who.rid, who.name, base_url="http://peer.invalid/moved"), who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/handshake", json=envelope)
    assert r.status_code == 200, r.text
    assert (await _row(node, who))["base_url"] == "http://peer.invalid/moved"


async def test_local_operator_can_update_a_known_peer(node):
    who = node.peer()
    await _seed_known_peer(node, who)
    r = await node.operator.post("/koi-net/handshake", json=_handshake(
        who.rid, who.name, base_url="http://peer.invalid/operator", events=["person", "vault-file"]))
    assert r.status_code == 200, r.text
    assert (await _row(node, who))["base_url"] == "http://peer.invalid/operator"
    assert (await _edge(node, who.rid, node.me.rid))["rid_types"] == ["person", "vault-file"]


async def test_a_different_public_key_is_still_refused(node):
    who, other = node.peer(), node.peer("identity-test-other")
    await _seed_known_peer(node, who)
    r = await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=other.pub))
    assert r.status_code == 403
    assert r.json()["error_code"] == "KEY_MISMATCH"


async def test_a_new_node_does_not_take_over_an_existing_alias_but_the_operator_can_move_it(node):
    first, second, third = node.peer(), node.peer("identity-test-second"), node.peer("identity-test-third")
    shared_name = f"identity-test-shared-{uuid.uuid4().hex[:10]}"
    node.aliases.append(shared_name)

    async def alias():
        async with node.pool.acquire() as conn:
            return await conn.fetchval("SELECT node_rid FROM koi_net_peer_aliases WHERE alias = $1", shared_name)

    await node.client.post("/koi-net/handshake", json=_handshake(first.rid, shared_name, public_key=first.pub))
    await node.client.post("/koi-net/handshake", json=_handshake(second.rid, shared_name, public_key=second.pub))
    assert await alias() == first.rid
    await node.operator.post("/koi-net/handshake", json=_handshake(third.rid, shared_name, public_key=third.pub))
    assert await alias() == third.rid


async def _queue_event(node):
    rid = f"orn:koi-net.person:identity-test-{uuid.uuid4().hex[:10]}+0001"
    node.event_rids.append(rid)
    async with node.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO koi_net_events (event_type, rid, manifest, contents, source_node) "
            "VALUES ('NEW', $1, '{}'::jsonb, $2::jsonb, $3)", rid, json.dumps({"name": "marker"}), node.me.rid)
    return rid


async def test_unsigned_poll_naming_an_approved_peer_gets_nothing(node):
    who = node.peer()
    await _seed_known_peer(node, who)
    rid = await _queue_event(node)
    r = await node.client.post("/koi-net/events/poll", json={"type": "poll_events", "node_id": who.rid, "limit": 10})
    assert r.status_code == 200
    assert r.json()["events"] == []
    async with node.pool.acquire() as conn:
        assert who.rid not in await conn.fetchval("SELECT delivered_to FROM koi_net_events WHERE rid = $1", rid)


async def test_the_peers_signed_poll_receives_its_event(node):
    who = node.peer()
    await _seed_known_peer(node, who)
    rid = await _queue_event(node)
    envelope = sign_envelope({"type": "poll_events", "limit": 500}, who.rid, node.me.rid, who.key)
    r = await node.client.post("/koi-net/events/poll", json=envelope)
    assert r.status_code == 200, r.text
    assert rid in [e["rid"] for e in _payload(r)["events"]]


async def _seed_directory_entity(node):
    tag = uuid.uuid4().hex[:10]
    uri, koi_rid = f"orn:personal-koi.entity:person-identity-test-{tag}", f"orn:koi-net.person:identity-test-{tag}+0001"
    node.entity_uris.append(uri)
    async with node.pool.acquire() as conn:
        size = await conn.fetchval("SELECT atttypmod FROM pg_attribute WHERE attrelid = 'entity_registry'::regclass AND attname = 'embedding'")
        await conn.execute(
            "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, embedding, koi_rid) "
            "VALUES ($1, $2, 'Person', $3, array_fill(0.001::real, ARRAY[$4::int])::vector, $5)",
            uri, f"Identity Test {tag}", f"identity test {tag}", size, koi_rid)
    return koi_rid


async def test_fetch_routes_require_the_edge_that_serves_the_caller(node):
    koi_rid = await _seed_directory_entity(node)
    who = node.peer()
    await node.client.post("/koi-net/handshake", json=_handshake(who.rid, who.name, public_key=who.pub))
    assert (await _edge(node, who.rid, node.me.rid))["status"] == "APPROVED"
    assert (await _edge(node, node.me.rid, who.rid))["status"] == "PROPOSED"
    envelope = sign_envelope({"type": "fetch_rids"}, who.rid, node.me.rid, who.key)
    assert koi_rid not in _payload(await node.client.post("/koi-net/rids/fetch", json=envelope))["rids"]
    async with node.pool.acquire() as conn:
        await conn.execute("UPDATE koi_net_edges SET status = 'APPROVED' WHERE source_node = $1 AND target_node = $2", node.me.rid, who.rid)
    envelope = sign_envelope({"type": "fetch_rids"}, who.rid, node.me.rid, who.key)
    assert koi_rid in _payload(await node.client.post("/koi-net/rids/fetch", json=envelope))["rids"]
