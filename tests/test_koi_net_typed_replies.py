"""The protocol routes parse requests and build replies with koi-net's own models.

- /manifests/fetch, /bundles/fetch and /rids/fetch parse requests with FetchManifests,
  FetchBundles and FetchRids. A request koi-net's model rejects (here a RID string rid-lib
  cannot parse) gets 400 INVALID_PAYLOAD; koi-net's own server answers 422.
- Replies are ManifestsPayload, BundlesPayload and RidsPayload. A stored record rid-lib
  cannot represent reads as not_found, and a koi_rid it cannot parse is not listed.
- A koi-net peer's poll reply is an EventsPayload. A peer running our code keeps the raw form
  with event_id (divergences 16, 17).

No database: the pool and queue are stubs, so these run offline.
"""
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

import api.koi_net_floor  # noqa: F401
import api.koi_net_router as knr
from api.koi_protocol import NodeProfile, NodeProvides
from koi_net.protocol.api.models import BundlesPayload, EventsPayload, ManifestsPayload, RidsPayload

OWN = "orn:koi-net.node:typed-own+" + "a" * 64
PEER = "orn:koi-net.node:typed-peer+" + "b" * 64
GOOD = "orn:koi-net.vault-file:Shared/good.md"
BAD_MANIFEST = "orn:koi-net.vault-file:Shared/bad-manifest.md"


class _Conn:
    def __init__(self, rows=None, fetch_rows=None):
        self.rows, self.fetch_rows = rows or {}, fetch_rows or []

    async def fetchrow(self, _sql, rid, *_args):
        return self.rows.get(rid)

    async def fetch(self, _sql, *_args):
        return self.fetch_rows

    async def execute(self, *_args):
        return "UPDATE 0"


def _pool(conn):
    @asynccontextmanager
    async def acquire():
        yield conn
    return SimpleNamespace(acquire=acquire)


@pytest.fixture
def node(monkeypatch):
    monkeypatch.setattr(knr, "_security_policy", lambda: {"require_approved_edge_for_poll": False})
    monkeypatch.setattr(knr, "_node_profile", NodeProfile(
        node_rid=OWN, node_name="typed-own", node_type="FULL", base_url="http://127.0.0.1:9/koi-net",
        provides=NodeProvides(event=["orn:koi-net.node"], state=["orn:koi-net.node"]), public_key="MFkw"))
    monkeypatch.setattr(knr, "_wrap_response", lambda payload, _target, _signed: payload)

    async def no_blocks(_conn):
        return frozenset()
    monkeypatch.setattr(knr, "blocked_hashes", no_blocks)

    def call(endpoint, payload, conn=None, source=PEER):
        async def unwrap(_request, allow_unsigned=False):
            return payload, source, True
        monkeypatch.setattr(knr, "_unwrap_request", unwrap)
        monkeypatch.setattr(knr, "_db_pool", _pool(conn or _Conn()))

        async def run():
            response = await endpoint(None)
            return response.status_code, json.loads(response.body)
        return run()
    return call


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", [knr.manifests_fetch, knr.bundles_fetch], ids=["manifests", "bundles"])
async def test_a_rid_rid_lib_cannot_parse_gets_400(node, endpoint):
    status, body = await node(endpoint, {"rids": [GOOD, "no-colon-key"]})
    assert status == 400 and body["error_code"] == "INVALID_PAYLOAD", body


@pytest.mark.asyncio
async def test_a_bundles_request_without_rids_gets_400_as_koi_net_requires_them(node):
    status, body = await node(knr.bundles_fetch, {"type": "fetch_bundles"})
    assert status == 400, body


@pytest.mark.asyncio
async def test_rid_types_that_are_not_strings_get_400(node):
    status, body = await node(knr.rids_fetch, {"rid_types": [42]})
    assert status == 400 and body["error_code"] == "INVALID_PAYLOAD", body


def _rows():
    good = {"manifest": {"rid": GOOD, "timestamp": "2026-11-06T10:00:00Z", "sha256_hash": "x"},
            "contents": {"text": "hi"}}
    bad = {"manifest": {"rid": "no-colon", "timestamp": "2026-11-06T10:00:00Z"}, "contents": {"text": "x"}}
    return {GOOD: good, BAD_MANIFEST: bad}


@pytest.mark.asyncio
async def test_manifests_reply_is_koi_nets_and_an_unrepresentable_record_is_not_found(node):
    status, body = await node(knr.manifests_fetch, {"rids": [OWN, GOOD, BAD_MANIFEST]}, _Conn(rows=_rows()))
    assert status == 200, body
    parsed = ManifestsPayload.model_validate(body)
    assert [str(m.rid) for m in parsed.manifests] == [OWN, GOOD]
    assert [str(r) for r in parsed.not_found] == [BAD_MANIFEST]
    assert body == parsed.model_dump(mode="json", exclude_none=True)  # koi-net re-serialises it unchanged


@pytest.mark.asyncio
async def test_bundles_reply_is_koi_nets_and_an_unrepresentable_record_is_not_found(node):
    status, body = await node(knr.bundles_fetch, {"rids": [OWN, GOOD, BAD_MANIFEST]}, _Conn(rows=_rows()))
    assert status == 200, body
    parsed = BundlesPayload.model_validate(body)
    assert [str(b.manifest.rid) for b in parsed.bundles] == [OWN, GOOD]
    assert [str(r) for r in parsed.not_found] == [BAD_MANIFEST]
    assert body == parsed.model_dump(mode="json", exclude_none=True)


@pytest.mark.asyncio
async def test_rids_listing_leaves_out_a_koi_rid_rid_lib_cannot_parse(node):
    conn = _Conn(fetch_rows=[{"koi_rid": "orn:koi-net.person:p-1"}, {"koi_rid": "no-colon-key"}])
    status, body = await node(knr.rids_fetch, {"rid_types": None}, conn)
    assert status == 200, body
    parsed = RidsPayload.model_validate(body)
    assert [str(r) for r in parsed.rids] == [OWN, "orn:koi-net.person:p-1"]
    assert body == parsed.model_dump(mode="json", exclude_none=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("extensions", [False, True])
async def test_poll_reply_is_an_events_payload_for_koi_net_and_raw_for_our_peers(node, monkeypatch, extensions):
    stored = [
        {"rid": GOOD, "event_type": "NEW", "event_id": "ev-1",
         "manifest": {"rid": GOOD, "timestamp": "2026-11-06T10:00:00Z"}, "contents": {"text": "hi"}},
        {"rid": "task-key-without-colon", "event_type": "NEW", "event_id": "ev-2", "contents": {"t": 1}},
    ]

    async def poll(*_args, **_kwargs):
        return stored
    monkeypatch.setattr(knr, "_event_queue", SimpleNamespace(poll=poll))

    async def reads(_node):
        return extensions
    monkeypatch.setattr(knr, "_peer_reads_extensions", reads)
    conn = _Conn()
    conn.fetchrow = lambda *_a, **_k: _async({"rid_types": None})
    status, body = await node(knr.events_poll, {"type": "poll_events", "limit": 5}, conn)
    assert status == 200, body
    if extensions:
        assert [e["event_id"] for e in body["events"]] == ["ev-1", "ev-2"]
    else:
        parsed = EventsPayload.model_validate(body)
        assert [str(e.rid) for e in parsed.events] == [GOOD]  # the colon-less key is withheld
        assert "event_id" not in json.dumps(body)
        assert body == parsed.model_dump(mode="json", exclude_none=True)


async def _async(value):
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", [knr.manifests_fetch, knr.bundles_fetch], ids=["manifests", "bundles"])
@pytest.mark.parametrize("stored", [
    {"manifest": {"rid": GOOD, "timestamp": 1730000000}, "contents": {"t": 1}},  # numeric timestamp
    {"manifest": ["not", "a", "dict"], "contents": {"t": 1}},
    {"manifest": "not json", "contents": {"t": 1}},
], ids=["numeric-timestamp", "list-manifest", "unparseable-json"])
async def test_whatever_a_peer_stored_reads_as_not_found_not_a_500(node, endpoint, stored):
    """The stored manifest is built defensively, as complete_manifest is."""
    status, body = await node(endpoint, {"rids": [GOOD]}, _Conn(rows={GOOD: stored}))
    assert status == 200, body
    assert body["not_found"] == [GOOD]
