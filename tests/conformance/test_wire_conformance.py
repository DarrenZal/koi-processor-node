"""KOI-net wire conformance: our node's payloads, parsed by the upstream `koi-net` models.

Runs in a separate venv from tests/conformance/requirements.txt (koi-net 2.1.2 needs
Python >= 3.12). Our node has also imported `koi_net.protocol` at runtime since 2026-11-05;
these tests are an independent koi-net client and import nothing from api/.

The tests act as a fresh koi-net client against a THROWAWAY instance of our node:
every request is built and signed with koi-net's own models (`UnsignedEnvelope.sign_with`),
and every response is parsed with the response-envelope model koi-net itself uses for that
path (`koi_net.protocol.model_map.API_MODEL_MAP`) and verified with our node's public key.

Run (see tests/conformance/README.md):

    KOI_TEST_API_URL=http://127.0.0.1:<port> <koinet-venv>/bin/python -m pytest tests/conformance

The KOI-net endpoints are KOI_TEST_API_URL + /koi-net. The node must be named
conformance-* and listen on 127.0.0.1/localhost; tests/koi_test_targets.py refuses the
live API's port. Skips when KOI_TEST_API_URL is unset.
"""
import json
import os
from urllib.parse import urlparse

import pytest
from cryptography.exceptions import InvalidSignature
from pydantic import ValidationError

koi_net = pytest.importorskip("koi_net", reason="koi-net is not installed in this interpreter; see tests/conformance/requirements.txt")
httpx = pytest.importorskip("httpx")

from rid_lib import RID  # noqa: E402
from rid_lib.ext import Bundle  # noqa: E402
from rid_lib.ext.utils import sha256_hash  # noqa: E402
from rid_lib.types import KoiNetEdge, KoiNetNode  # noqa: E402

from koi_net.protocol.api.models import (  # noqa: E402
    ErrorResponse,
    EventsPayload,
    FetchBundles,
    FetchManifests,
    FetchRids,
    PollEvents,
)
from koi_net.protocol.api.paths import (  # noqa: E402
    BROADCAST_EVENTS_PATH,
    FETCH_BUNDLES_PATH,
    FETCH_MANIFESTS_PATH,
    FETCH_RIDS_PATH,
    POLL_EVENTS_PATH,
)
from koi_net.protocol.edge import EdgeProfile  # noqa: E402
from koi_net.protocol.envelope import UnsignedEnvelope  # noqa: E402
from koi_net.protocol.event import Event, EventType  # noqa: E402
from koi_net.protocol.model_map import API_MODEL_MAP  # noqa: E402
from koi_net.protocol.node import NodeProfile, NodeProvides, NodeType  # noqa: E402
from koi_net.protocol.secure import PrivateKey, PublicKey  # noqa: E402

from tests import koi_test_targets as targets  # noqa: E402


def _node_url() -> str:
    """KOI_TEST_API_URL + /koi-net, on loopback only (the guard has refused the live port)."""
    url = targets.api_url() + "/koi-net"
    u = urlparse(url)
    if u.hostname not in ("127.0.0.1", "localhost") or u.port is None:
        pytest.fail(f"refusing to run against {url}: a throwaway node on 127.0.0.1 with an explicit port only")
    return url


@pytest.fixture(scope="module")
def node():
    """Our throwaway node's identity, discovered from its /health extension endpoint."""
    base = _node_url()
    health = httpx.get(f"{base}/health", timeout=10).json()["node"]
    if not health["node_name"].startswith("conformance-"):
        pytest.fail(f"refusing: node {health['node_name']!r} is not a conformance-* throwaway node")
    return {
        "base": base,
        "rid": KoiNetNode.from_string(health["node_rid"]),
        "public_key": PublicKey.from_der(health["public_key"]),
        "health": health,
    }


@pytest.fixture(scope="module")
def client(node):
    """A fresh koi-net identity that introduces itself the way a stock node does."""
    return _introduce(node, "conformance-client")


@pytest.fixture(scope="module")
def other_client(node):
    """A second fresh identity, for behaviour that involves two peers."""
    return _introduce(node, "conformance-other")


def _introduce(node, name):
    priv = PrivateKey.generate()
    rid = KoiNetNode(name, sha256_hash(priv.public_key().to_der()))
    profile = NodeProfile(
        base_url="http://127.0.0.1:9/koi-net",  # never contacted
        node_type=NodeType.FULL,
        provides=NodeProvides(event=[], state=[]),
        public_key=priv.public_key().to_der(),
    )
    bundle = Bundle.generate(rid, profile.model_dump())
    c = {"priv": priv, "rid": rid, "bundle": bundle}
    # koi_net.components.handshaker.Handshaker.handshake_with: FORGET then NEW of own bundle
    resp = _post(node, c, BROADCAST_EVENTS_PATH, EventsPayload(events=[
        Event.from_rid(event_type=EventType.FORGET, rid=rid),
        Event.from_bundle(event_type=EventType.NEW, bundle=bundle),
    ]))
    c["handshake_status"] = resp.status_code
    c["handshake_body"] = resp.text
    return c


def _post(node, client, path, request, signature=None):
    env = UnsignedEnvelope[type(request)](
        payload=request, source_node=client["rid"], target_node=node["rid"]
    ).sign_with(client["priv"])
    body = env.model_dump_json(exclude_none=True)
    if signature is not None:
        data = json.loads(body)
        data["signature"] = signature
        body = json.dumps(data)
    return httpx.post(node["base"] + path, content=body,
                      headers={"Content-Type": "application/json"}, timeout=20)


def _parse_signed(node, path, resp, verify=True):
    """Parse a response exactly as koi_net's RequestHandler does, then verify the signature
    (koi-net re-serializes the parsed payload with model_dump_json(exclude_none=True))."""
    assert resp.status_code == 200, f"{path}: HTTP {resp.status_code} {resp.text[:300]}"
    model = API_MODEL_MAP[path].response_envelope
    envelope = model.model_validate_json(resp.text)
    if verify:
        envelope.verify_with(node["public_key"])
    return envelope.payload


# --- requests our node accepts ---------------------------------------------------

def test_upstream_handshake_broadcast_is_accepted(client):
    """A stock node joins by broadcasting FORGET + NEW of its own node bundle."""
    assert client["handshake_status"] == 200, client["handshake_body"][:300]


# --- responses our node emits ----------------------------------------------------

def test_own_node_profile_parses(node):
    """NodeProfile on the wire: koi_net.protocol.node.NodeProfile."""
    h = node["health"]
    NodeProfile.model_validate({k: h[k] for k in ("base_url", "node_type", "provides", "public_key")})


def test_fetch_rids_response_parses(node, client):
    payload = _parse_signed(node, FETCH_RIDS_PATH, _post(node, client, FETCH_RIDS_PATH, FetchRids()))
    assert isinstance(payload.rids, list)


def test_fetch_manifests_response_parses(node, client):
    """Non-empty request: the client's own RID (its handshake NEW event carries a manifest)
    and a RID that does not exist. Both lists are exercised, and the signature must verify."""
    missing = KoiNetNode("conformance-absent", "0" * 64)
    payload = _parse_signed(node, FETCH_MANIFESTS_PATH, _post(
        node, client, FETCH_MANIFESTS_PATH, FetchManifests(rids=[client["rid"], missing])))
    assert missing in payload.not_found, f"not_found={[str(r) for r in payload.not_found]}"
    assert len(payload.manifests) + len(payload.not_found) == 2


def test_fetch_bundles_returns_our_node_bundle(node, client):
    """A stock node learns a peer's profile by fetching the peer's own KoiNetNode bundle."""
    resp = _post(node, client, FETCH_BUNDLES_PATH, FetchBundles(rids=[node["rid"]]))
    # Setup problems fail outright (pytest.fail is not an AssertionError), so only the
    # divergence itself counts as the expected failure.
    try:
        payload = _parse_signed(node, FETCH_BUNDLES_PATH, resp)
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"setup: /bundles/fetch did not return a verifiable payload: {exc!r}")
    assert [b.rid for b in payload.bundles] == [node["rid"]], (
        f"own node bundle not served: bundles={[str(b.rid) for b in payload.bundles]} "
        f"not_found={[str(r) for r in payload.not_found]}")
    profile = payload.bundles[0].validate_contents(NodeProfile)
    # Divergence 21: a stock node subscribes to what we provide, compared as RID types.
    assert KoiNetNode in profile.provides.event and KoiNetEdge in profile.provides.event, profile.provides.event


def test_fetch_rids_lists_our_node_for_node_requests(node, client):
    """A stock node catches up with fetch_rids(rid_types=[KoiNetNode]) after proposing an edge."""
    payload = _parse_signed(node, FETCH_RIDS_PATH, _post(node, client, FETCH_RIDS_PATH, FetchRids(rid_types=[KoiNetNode])))
    assert node["rid"] in payload.rids, [str(r) for r in payload.rids]


def test_fetch_manifests_serves_our_node_manifest(node, client):
    payload = _parse_signed(node, FETCH_MANIFESTS_PATH, _post(node, client, FETCH_MANIFESTS_PATH, FetchManifests(rids=[node["rid"]])))
    assert [m.rid for m in payload.manifests] == [node["rid"]]


def test_inbound_broadcast_events_are_served_to_every_poller(node, client, other_client):
    """Divergence 23, PINNED (recorded; the decision is open, so this asserts today's behaviour).

    /events/broadcast stores a peer's events in the outbound broadcast table with no target,
    and poll does not exclude the sender, so a second peer is served them and so is the
    sender itself. If this test fails, the behaviour changed: update the register's entry 23."""
    probe = Bundle.generate(RID.from_string(f"orn:conformance.probe:{client['rid'].hash[:12]}-relay"),
                            {"probe": "divergence 23"})
    resp = _post(node, client, BROADCAST_EVENTS_PATH,
                 EventsPayload(events=[Event.from_bundle(event_type=EventType.NEW, bundle=probe)]))
    if resp.status_code != 200:
        pytest.fail(f"setup: broadcast HTTP {resp.status_code} {resp.text[:300]}")

    def served_to(peer):
        try:
            payload = _parse_signed(node, POLL_EVENTS_PATH,
                                    _post(node, peer, POLL_EVENTS_PATH, PollEvents(limit=500)), verify=False)
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"setup: poll did not return a parseable payload: {exc!r}")
        return [e for e in payload.events if e.rid == probe.rid]

    assert served_to(other_client), "a second peer was not served the first peer's broadcast event"
    assert served_to(client), "the sender was not served its own broadcast event back"


def test_poll_events_response_parses(node, client):
    payload = _parse_signed(node, POLL_EVENTS_PATH, _post(node, client, POLL_EVENTS_PATH, PollEvents(limit=50)),
                            verify=False)
    assert isinstance(payload.events, list)


def test_poll_events_signature_verifies(node, client):
    # Poll delivers each event once per requester, so create one and verify the same
    # response that carries it; an empty event list would verify trivially.
    root = node["base"].rsplit("/koi-net", 1)[0]
    httpx.post(f"{root}/tasks/ingest", json={"taskKey": f"conformance::{client['rid'].hash[:12]}-sig",
                                             "title": "conformance sig", "status": "open"}, timeout=20)
    resp = _post(node, client, POLL_EVENTS_PATH, PollEvents(limit=50))
    if resp.status_code != 200:
        pytest.fail(f"setup: poll HTTP {resp.status_code} {resp.text[:300]}")
    envelope = API_MODEL_MAP[POLL_EVENTS_PATH].response_envelope.model_validate_json(resp.text)
    if not envelope.payload.events:
        pytest.fail("setup: poll returned no events, so there is nothing to verify")
    envelope.verify_with(node["public_key"])


def test_events_emitted_by_our_node_parse(node, client):
    """Events our node emits itself (a task domain event), as delivered by poll."""
    task_key = f"conformance::{client['rid'].hash[:12]}"
    root = node["base"].rsplit("/koi-net", 1)[0]
    r = httpx.post(f"{root}/tasks/ingest", json={"taskKey": task_key, "title": "conformance event",
                                                 "status": "open"}, timeout=20)
    assert r.status_code == 200, r.text[:200]
    payload = _parse_signed(node, POLL_EVENTS_PATH, _post(node, client, POLL_EVENTS_PATH, PollEvents(limit=200)),
                            verify=False)
    ours = [e for e in payload.events if task_key in str(e.rid) or task_key in json.dumps(e.contents or {})]
    assert ours, f"no event for {task_key} in poll ({len(payload.events)} events)"


@pytest.mark.xfail(strict=True, raises=AssertionError,
                   reason="divergence 17: domain events use application keys (a task key) as "
                          "their RID instead of a KOI RID, and NEW events carry no manifest")
def test_emitted_domain_events_use_koi_rids_and_manifests(node, client):
    task_key = f"conformance::{client['rid'].hash[:12]}-rid"
    root = node["base"].rsplit("/koi-net", 1)[0]
    httpx.post(f"{root}/tasks/ingest", json={"taskKey": task_key, "title": "conformance rid", "status": "open"},
               timeout=20)
    try:
        payload = _parse_signed(node, POLL_EVENTS_PATH,
                                _post(node, client, POLL_EVENTS_PATH, PollEvents(limit=200)), verify=False)
    except Exception as exc:  # noqa: BLE001
        pytest.fail(f"setup: poll did not return a parseable payload: {exc!r}")
    ours = [e for e in payload.events if task_key in str(e.rid) or task_key in json.dumps(e.contents or {})]
    if not ours:
        pytest.fail(f"setup: no event for {task_key} in poll ({len(payload.events)} events)")
    ev = ours[0]
    assert str(ev.rid).startswith("orn:"), f"event RID is not a KOI RID: {ev.rid!s}"
    assert ev.event_type == EventType.FORGET or ev.manifest is not None, "NEW/UPDATE event without manifest"


def test_handshake_extension_profile_parses(node):
    """/koi-net/handshake is our extension (divergence 7); the profile it returns must still be a
    koi-net NodeProfile. Only the protocol fields are checked; the rest are our extensions."""
    priv = PrivateKey.generate()
    rid = KoiNetNode("conformance-handshake", sha256_hash(priv.public_key().to_der()))
    req = {"type": "handshake", "defer_approval": True, "profile": {
        "node_rid": str(rid), "node_name": "conformance-handshake", "node_type": "PARTIAL",
        "provides": {"event": [], "state": []}, "public_key": priv.public_key().to_der()}}
    r = httpx.post(f"{node['base']}/handshake", json=req, timeout=20)
    assert r.status_code == 200, r.text[:300]
    body = r.json()
    body = body.get("payload", body)
    prof = body["profile"]
    NodeProfile.model_validate({k: prof[k] for k in ("base_url", "node_type", "provides", "public_key")})


def test_error_response_parses(node, client):
    """A rejected request gets a body koi_net's RequestHandler can read (ErrorResponse)."""
    r = _post(node, client, FETCH_RIDS_PATH, FetchRids(), signature="AAAA")
    assert 400 <= r.status_code < 500, r.status_code
    ErrorResponse.model_validate_json(r.text)


# --- known divergences (docs/architecture/koi-protocol-conformance.md) -------------

def _make_edge(node):
    """Create an APPROVED edge on the throwaway node through our /handshake extension
    (divergence 7); unauthenticated /koi-net/edges lists APPROVED edges only."""
    priv = PrivateKey.generate()
    rid = KoiNetNode("conformance-edge", sha256_hash(priv.public_key().to_der()))
    req = {"type": "handshake", "defer_approval": False, "profile": {
        "node_rid": str(rid), "node_name": "conformance-edge", "node_type": "PARTIAL",
        "provides": {"event": [], "state": []}, "public_key": priv.public_key().to_der()}}
    r = httpx.post(f"{node['base']}/handshake", json=req, timeout=20)
    assert r.status_code == 200, r.text[:300]
    # Every new inbound edge starts PROPOSED (register entry 26); approve it as an operator
    # would. tests/interop/run_gate.py passes the throwaway node's admin token.
    token = os.environ.get("KOI_GATE_ADMIN_TOKEN")
    if not token:
        pytest.fail("KOI_GATE_ADMIN_TOKEN is not set: run this suite through tests/interop/run_gate.py")
    body = r.json()
    edge_rid = body.get("payload", body)["edge_rid"]
    a = httpx.post(f"{node['base']}/edges/approve", json={"edge_rid": edge_rid},
                   headers={"Authorization": f"Bearer {token}"}, timeout=20)
    assert a.status_code == 200, a.text[:300]


@pytest.mark.xfail(strict=True, raises=ValidationError,
                   reason="divergence 12, admin endpoint only: GET /koi-net/edges (ours, not in "
                          "koi-net) keeps edge_rid/source_node/target_node; edges on the wire are "
                          "EdgeProfile since 2026-11-05 (proven by the interop gate)")
def test_edges_endpoint_matches_edge_profile(node):
    _make_edge(node)
    edges = httpx.get(f"{node['base']}/edges", timeout=10).json()
    rows = edges.get("edges", edges) if isinstance(edges, dict) else edges
    assert rows, "no edges to check (the setup above should have created one)"
    for row in rows:
        EdgeProfile.model_validate(row)


def test_edge_rid_type_known_to_rid_lib():
    """Sanity: the edge RID type upstream negotiates with exists in our pinned rid-lib."""
    assert KoiNetEdge.__name__ == "KoiNetEdge"
