"""api/koi_net_negotiation.py: wire objects and decisions, no database (divergences 5, 7, 12, 18, 20).

koi-net verifies a signature by re-serialising the payload it parsed, so the objects built
here must have koi-net's fields in koi-net's order. The end-to-end proof is the interop gate
(tests/interop/); the database paths are in tests/interop/test_negotiation_db.py.
"""
from __future__ import annotations

import json

import pytest
from rid_lib.ext import Bundle
from rid_lib.ext.utils import sha256_hash
from rid_lib.types import KoiNetEdge

from api import koi_net_negotiation as neg

OURS = "orn:koi-net.node:ours+" + "a" * 64
PEER = "orn:koi-net.node:peer+" + "b" * 64
THIRD = "orn:koi-net.node:third+" + "c" * 64


def test_edge_rid_is_the_section_3_2_1_hash():
    assert neg.edge_rid_for(OURS, PEER) == str(KoiNetEdge(sha256_hash(OURS + PEER)))
    assert neg.edge_rid_for(OURS, PEER) != neg.edge_rid_for(PEER, OURS)  # direction matters


def test_edge_contents_follow_edge_profile_and_have_no_local_statuses():
    c = neg.edge_contents(OURS, PEER, "POLL", "PROPOSED", ["orn:koi-net.node"])
    assert list(c) == ["@context", "source", "target", "edge_type", "status", "rid_types"]
    assert c["@context"] == "orn:koi-net.context:koi-net.edge"
    for status in ("REJECTED", "REVOKED"):
        with pytest.raises(ValueError):
            neg.edge_contents(OURS, PEER, "POLL", status, [])
    with pytest.raises(ValueError):
        neg.edge_contents(OURS, PEER, "PUSH", "PROPOSED", [])


def test_edge_bundle_is_a_rid_lib_bundle_under_the_hash_rid():
    raw = neg.edge_bundle(OURS, PEER, "WEBHOOK", "APPROVED", ["orn:koi-net.vault-file"])
    b = Bundle.model_validate(raw)
    assert str(b.rid) == neg.edge_rid_for(OURS, PEER)
    assert b.manifest.sha256_hash == Bundle.generate(b.rid, b.contents).manifest.sha256_hash
    json.dumps(raw)


def test_events_follow_koi_net_event_field_order():
    raw = neg.edge_bundle(OURS, PEER, "POLL", "PROPOSED", [])
    ev = neg.bundle_event("NEW", raw)
    assert list(ev) == ["rid", "event_type", "manifest", "contents"]
    assert ev["rid"] == raw["manifest"]["rid"]
    assert neg.forget_event("orn:koi-net.edge:" + "d" * 64) == {"rid": "orn:koi-net.edge:" + "d" * 64, "event_type": "FORGET"}
    assert list(neg.events_payload([ev])) == ["type", "events"]


@pytest.mark.parametrize("contents, ok", [
    (neg.edge_contents(OURS, PEER, "POLL", "PROPOSED", ["orn:koi-net.node"]), True),
    (json.dumps(neg.edge_contents(OURS, PEER, "POLL", "PROPOSED", [])), True),
    ({"source": OURS, "target": PEER, "edge_type": "PUSH", "status": "PROPOSED", "rid_types": []}, False),
    ({"source": OURS, "target": PEER, "edge_type": "POLL", "status": "REVOKED", "rid_types": []}, False),
    ({"source": "orn:koi-net.edge:x", "target": PEER, "edge_type": "POLL", "status": "PROPOSED", "rid_types": []}, False),
    ({"source": OURS, "target": PEER, "edge_type": "POLL", "status": "PROPOSED", "rid_types": "orn:koi-net.node"}, False),
    ("not json", False), (None, False),
])
def test_parse_edge_contents(contents, ok):
    assert (neg.parse_edge_contents(contents) is not None) is ok


def _edge_event(source, target, status="PROPOSED"):
    return neg.bundle_event("NEW", neg.edge_bundle(source, target, "POLL", status, []))


@pytest.mark.parametrize("event, allowed", [
    ({"rid": PEER, "event_type": "NEW", "contents": {}}, True),        # its own node object
    ({"rid": PEER, "event_type": "FORGET"}, True),
    ({"rid": THIRD, "event_type": "NEW", "contents": {}}, False),      # another node's object
    (_edge_event(OURS, PEER), True),                                  # it subscribes to us
    (_edge_event(PEER, OURS, "APPROVED"), True),                      # it approves our subscription
    (_edge_event(THIRD, PEER), False),                                # an edge that is not ours
    ({"rid": "orn:koi-net.edge:" + "e" * 64, "event_type": "FORGET"}, True),
    ({"rid": "orn:koi-net.vault-file:Shared/x.md", "event_type": "NEW"}, False),  # data, not protocol
    ({"rid": None, "event_type": "NEW"}, False),
])
def test_first_contact_allowed(event, allowed):
    assert neg.first_contact_allowed(event, PEER, OURS) is allowed


FULL = {"node_type": "FULL", "base_url": "http://127.0.0.1:9/koi-net"}
PARTIAL = {"node_type": "PARTIAL", "base_url": None}
PROVIDES = ["orn:koi-net.node", "orn:koi-net.edge", "orn:koi-net.vault-file"]


def _edge(edge_type, rid_types):
    return {"source": OURS, "target": PEER, "edge_type": edge_type, "status": "PROPOSED", "rid_types": rid_types}


@pytest.mark.parametrize("edge, peer, refused", [
    (_edge("WEBHOOK", ["orn:koi-net.vault-file"]), FULL, None),
    (_edge("POLL", ["orn:koi-net.vault-file"]), PARTIAL, None),
    (_edge("WEBHOOK", ["orn:koi-net.node"]), PARTIAL, "partial nodes cannot use webhooks"),
    (_edge("WEBHOOK", ["orn:koi-net.node"]), {"node_type": "FULL", "base_url": None}, "partial nodes cannot use webhooks"),
    (_edge("POLL", ["orn:koi-net.person"]), FULL, "requested RID types not provided"),
    (_edge("POLL", ["ORN:KOI-NET.VAULT-FILE"]), FULL, None),  # compared case-insensitively
])
def test_subscription_refusal(edge, peer, refused):
    reason = neg.subscription_refusal(edge, PROVIDES, peer)
    assert (reason is None) if refused is None else (reason or "").startswith(refused)


def test_node_and_edge_objects_are_always_provided():
    assert neg.subscription_refusal(_edge("POLL", ["orn:koi-net.node", "orn:koi-net.edge"]), [], FULL) is None


def test_interest_overlap():
    assert neg.interest_overlap(["orn:koi-net.node"], []) == ["orn:koi-net.node"]  # implicit
    assert neg.interest_overlap(["orn:x.y", "orn:koi-net.node", "orn:koi-net.node"], ["orn:a.b"]) == ["orn:koi-net.node"]
    assert neg.interest_overlap(["orn:a.b"], ["orn:a.b"]) == ["orn:a.b"]
    assert neg.interest_overlap([], ["orn:a.b"]) == []


def test_rate_limiter():
    lim = neg.ProtocolRateLimiter(limit=3, window=10)
    assert lim.allow(PEER, 2, now=0) and lim.allow(PEER, 1, now=1)
    assert not lim.allow(PEER, 1, now=2)
    assert lim.allow(THIRD, 3, now=2)  # per node
    assert lim.allow(PEER, 1, now=11.5)  # the window has moved past the first two


def test_configuration_defaults(monkeypatch):
    monkeypatch.delenv("KOI_NET_AUTO_APPROVE_EDGES", raising=False)
    monkeypatch.delenv("KOI_NET_RID_TYPES_OF_INTEREST", raising=False)
    assert neg.auto_approve_edges() is False  # D1
    assert neg.rid_types_of_interest() == []  # D4
    monkeypatch.setenv("KOI_NET_RID_TYPES_OF_INTEREST", " orn:koi-net.node , ,orn:koi-net.edge")
    assert neg.rid_types_of_interest() == ["orn:koi-net.node", "orn:koi-net.edge"]


# --- fixes of 2026-11-06 -------------------------------------------------------------

@pytest.mark.parametrize("url, ok", [
    ("http://127.0.0.1:8000/koi-net", True), ("https://example.org/koi-net/", True), ("http://10.100.0.2:8400", True),
    ("http://h:1/x#", False), ("http://h:1/x?y=", False), ("http://h/x;p", False),
    ("ftp://h/koi-net", False), ("http://user:pw@h/koi-net", False), ("http://h:99999/koi-net", False),
    ("http://h/a b", False), ("http:///koi-net", False), ("", False), (None, False), (42, False),
])
def test_valid_peer_base_url(url, ok):
    """The URL comes from a bundle anyone can sign, and we push to it on first contact."""
    assert (neg.valid_peer_base_url(url) is not None) is ok


def test_our_own_url_is_not_a_peer_url():
    assert neg.valid_peer_base_url("http://127.0.0.1:9/koi-net/", own_base_url="http://127.0.0.1:9/koi-net") is None


def test_python_floor_raises_import_error_on_old_interpreters(monkeypatch):
    """Under 3.11 koi-net raised SyntaxError, which `except ImportError` missed."""
    import importlib
    import sys
    import api.koi_net_floor as floor

    monkeypatch.setattr(sys, "version_info", (3, 11, 9, "final", 0))
    with pytest.raises(ImportError, match="Python >= 3.12"):
        importlib.reload(floor)
    monkeypatch.undo()
    importlib.reload(floor)  # the real interpreter passes


@pytest.mark.parametrize("url", ["http://h/a\x0cb", "http://h/a\x00b", "http://h/a\x7fb", "http://h/a b",
                                 "http://h/a​b", "http://h/%23/koi-net"])
def test_valid_peer_base_url_refuses_invisible_characters(url):
    """A percent-encoded # stays in the path and is harmless."""
    if "%23" in url:
        assert neg.valid_peer_base_url(url) == url
    else:
        assert neg.valid_peer_base_url(url) is None


@pytest.mark.parametrize("spelling", ["HTTP://127.0.0.1:9", "http://localhost:9", "http://127.0.0.1:9/koi-net",
                                      "http://[::1]:9/koi-net/"])
def test_our_own_url_is_refused_in_any_spelling(spelling):
    """The self check used to compare strings."""
    assert neg.valid_peer_base_url(spelling, own_base_url="http://127.0.0.1:9/koi-net") is None


@pytest.mark.parametrize("spelling", ["http://127.0.0.1:9/./", "http://127.0.0.1:9/x/..", "http://127.1:9", "http://2130706433:9",
                                      "http://0.0.0.0:9", "http://localhost.:9", "http://[::ffff:127.0.0.1]:9",
                                      "http://[0:0:0:0:0:0:0:1]:9", "http://127.0.0.2:9/koi-net"])
def test_our_own_url_is_refused_in_resolver_spellings(spelling):
    """Spellings an HTTP client turns back into our own address."""
    assert neg.valid_peer_base_url(spelling, own_base_url="http://127.0.0.1:9/koi-net") is None


def test_a_non_ascii_host_is_refused_and_other_peers_still_pass():
    assert neg.valid_peer_base_url("http://127。0。0。1:9") is None
    assert neg.valid_peer_base_url("http://127.0.0.1:10/koi-net", own_base_url="http://127.0.0.1:9/koi-net")
    assert neg.valid_peer_base_url("http://10.100.0.2:8400", own_base_url="http://127.0.0.1:9/koi-net")
