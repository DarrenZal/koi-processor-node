"""Divergences 21 and 15: what our node advertises, how an edge's rid_types scope delivery,
and our own node bundle. Pure functions, no database.

21: provides.event held bare type names ("Vault-file", "Person"), not RID types, and listed
neither orn:koi-net.node nor orn:koi-net.edge. A stock koi-net node subscribes to the
intersection of its rid_types_of_interest with provides.event, so it proposed no edge.

Changing provides.event changes the rid_types /handshake writes into an existing peer's
edge, so the edge filter must give that peer exactly what it got before. The golden test
below replays the filter as it was before this change (copied from api/event_queue.py at
regen-prod ad2e2ab) and compares.
"""
from __future__ import annotations

import json

import pytest
from rid_lib import RIDType
from rid_lib.ext import Bundle

from api import event_queue as eq
from api import node_identity as ni

OURS = "orn:koi-net.node:conformance-ours+" + "a" * 64
OTHER = "orn:koi-net.node:conformance-other+" + "b" * 64

# Every event shape our broadcast table holds: our emitters' and inbound relays (23).
EVENTS = {
    "vault-legacy-ns": ("orn:koi-net.vault-file:Shared/Projects/a.md", None, OURS),
    "vault-owned-ns": ("orn:personal-koi.vault-file:Shared/b.md", None, OURS),
    "koi-rid-person": ("orn:koi-net.person:" + "c" * 32, None, OURS),
    "koi-rid-meeting": ("orn:koi-net.meeting:" + "d" * 32, None, OURS),
    "obsidian-note": ("orn:obsidian.note:Shared/x.md", None, OURS),
    "domain-entity": ("orn:personal-koi.entity:e1", {"_koi_domain": "entity", "payload": {}}, OURS),
    "domain-task": ("conformance::task-1", {"_koi_domain": "task", "payload": {}}, OURS),
    "domain-doclink": ("orn:personal-koi.doclink:f1", {"_koi_domain": "doclink", "payload": {}}, OURS),
    # contents as asyncpg returns a jsonb column read without a codec: a JSON string
    "domain-entity-json": ("orn:personal-koi.entity:e2", json.dumps({"_koi_domain": "entity", "payload": {}}), OURS),
    "own-node": (OURS, None, OURS),
    "own-edge": ("orn:koi-net.edge:" + "e" * 64, None, OURS),
    "relayed-node": (OTHER, None, OTHER),
    "relayed-edge": ("orn:koi-net.edge:" + "f" * 64, None, OTHER),
    "other-ns-person": ("orn:other.person:x", None, OURS),
    "garbage": ("not a rid", None, OURS),
}

LEGACY_PROVIDES = ["Organization", "Person", "Project", "Concept", "Location", "Vault-file"]


def _old_admits(rid_types, rid, contents):
    """The filter in api/event_queue.py poll() before this change, verbatim in logic."""
    if rid_types is None:
        return True
    c = json.loads(contents) if isinstance(contents, str) and contents else contents
    c = c if isinstance(c, dict) else {}
    lowered = [rt.lower() for rt in rid_types]
    if "_koi_domain" in c:
        domain = c.get("_koi_domain")
        return not (not domain or str(domain).lower() not in lowered)
    rid_type = eq.extract_rid_type(rid)
    return not (rid_type is None or rid_type.lower() not in lowered)


def _admitted(rid_types):
    return {name for name, (rid, contents, source) in EVENTS.items()
            if eq.edge_admits(rid_types, rid, contents, source, OURS)}


def test_legacy_edge_delivery_is_unchanged():
    """An edge declared with the old bare names gets exactly what the old filter gave it."""
    old = {name for name, (rid, contents, _src) in EVENTS.items() if _old_admits(LEGACY_PROVIDES, rid, contents)}
    # Positive control on the replay. A bare name matches the type segment of any namespace,
    # so the old filter also admitted orn:other.person.
    assert old == {"vault-legacy-ns", "vault-owned-ns", "koi-rid-person", "other-ns-person"}
    assert _admitted(LEGACY_PROVIDES) == old


def test_edge_rewritten_from_new_provides_adds_only_our_node_and_edge_objects():
    """At its next /handshake a peer's edge gets our new provides.event. It must receive what it
    did before, plus our own node and edge objects, and never another node's relayed ones."""
    old = _admitted(LEGACY_PROVIDES)
    new = _admitted(list(ni.DEFAULT_EVENT_TYPES))
    # The one narrowing: a RID type matches its namespace exactly, as koi-net does, so a
    # foreign namespace that shares a type segment (orn:other.person) no longer passes.
    # Our emitters never produce one: /register-entity mints every koi_rid as
    # orn:koi-net.<type>, and both vault-file namespaces are one type.
    assert new == (old - {"other-ns-person"}) | {"own-node", "own-edge"}


def test_upstream_rid_types_match_exactly():
    assert _admitted(["orn:koi-net.person"]) == {"koi-rid-person"}
    assert _admitted(["orn:koi-net.node"]) == {"own-node"}
    assert _admitted(["orn:koi-net.edge"]) == {"own-edge"}
    # The two vault-file namespaces are one type while divergence 1's migration is open.
    assert _admitted(["orn:koi-net.vault-file"]) == {"vault-legacy-ns", "vault-owned-ns"}
    assert _admitted(["orn:personal-koi.vault-file"]) == {"vault-legacy-ns", "vault-owned-ns"}


def test_domain_events_still_match_by_domain_name():
    assert _admitted(["entity", "task"]) == {"domain-entity", "domain-entity-json", "domain-task"}
    # A RID type never admits a domain event (divergence 9 keeps the axes apart).
    assert "domain-entity" not in _admitted(["orn:personal-koi.entity"])


def test_scope_fails_closed():
    assert _admitted([]) == set()
    assert "garbage" not in _admitted(["Vault-file", "orn:koi-net.vault-file"])
    assert "obsidian-note" not in _admitted(list(ni.DEFAULT_EVENT_TYPES))


def test_no_declared_scope_still_never_relays_another_nodes_protocol_objects():
    """rid_types None means no edge filter (membrane off, no edge). Even then our node serves
    only its own node and edge objects, never another node's, so a stock node's handshake
    bundle is not handed to every poller (divergence 23, narrowed for protocol objects)."""
    admitted = _admitted(None)
    assert {"own-node", "own-edge"} <= admitted
    assert not {"relayed-node", "relayed-edge"} & admitted
    assert "vault-legacy-ns" in admitted  # everything else is unchanged


@pytest.mark.parametrize("rid, contents", [
    # A domain branch must not admit another node's protocol object.
    (OTHER, {"_koi_domain": "entity", "payload": {}}),
    # Unparseable references still name node and edge objects.
    ("orn:koi-net.node:peer", None),
    ("orn:koi-net.edge:", None),
    # Casing does not change what the RID names.
    ("orn:KOI-NET.node:peer+" + "b" * 64, None),
])
@pytest.mark.parametrize("rid_types", [None, ["entity"], ["orn:koi-net.node"], ["Node"]])
def test_foreign_protocol_objects_are_never_admitted(rid, contents, rid_types):
    assert not eq.edge_admits(rid_types, rid, contents, OTHER, OURS)


def test_provides_are_rid_types_and_include_node_and_edge():
    for entry in (*ni.DEFAULT_EVENT_TYPES, *ni.DEFAULT_STATE_TYPES):
        assert ":" in entry, f"{entry!r} is a bare name, not a RID type"
        RIDType.from_string(entry)
    assert {"orn:koi-net.node", "orn:koi-net.edge"} <= set(ni.DEFAULT_EVENT_TYPES)
    assert "orn:koi-net.node" in ni.DEFAULT_STATE_TYPES


def test_own_node_bundle_is_a_rid_lib_bundle_of_our_profile():
    from api.koi_protocol import NodeProfile, NodeProvides

    profile = NodeProfile(
        node_rid=OURS, node_name="conformance-ours", node_type="FULL",
        base_url="http://127.0.0.1:9/koi-net",
        provides=NodeProvides(event=list(ni.DEFAULT_EVENT_TYPES), state=list(ni.DEFAULT_STATE_TYPES)),
        public_key="MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE" + "A" * 60,
        encryption_key="x25519-test-value",
    )
    raw = eq.own_node_bundle(profile)
    json.dumps(raw)  # wire-serialisable
    bundle = Bundle.model_validate(raw)
    assert str(bundle.rid) == OURS
    # The manifest hash is rid-lib's own hash of the contents, so a stock node's check agrees.
    assert bundle.manifest.sha256_hash == Bundle.generate(bundle.rid, bundle.contents).manifest.sha256_hash
    for field in ("base_url", "node_type", "provides", "public_key"):
        assert bundle.contents[field] == raw["contents"][field]
    assert bundle.contents["provides"]["event"] == list(ni.DEFAULT_EVENT_TYPES)
    assert bundle.contents["encryption_key"] == "x25519-test-value"  # our extension survives


@pytest.mark.parametrize("requested, listed", [
    (None, True), ([], True), (["orn:koi-net.node"], True),
    (["orn:koi-net.person"], False), (["Person"], False),
])
def test_own_rid_is_listed_for_node_requests(requested, listed):
    assert eq.lists_own_node(requested) is listed


def test_rids_fetch_query_accepts_both_type_forms():
    from api.koi_net_router import _build_rids_fetch_query

    sql, params = _build_rids_fetch_query(["Person", "orn:koi-net.meeting", "orn:koi-net.node"], None)
    assert params == [["Person"], ["orn:koi-net.meeting"]]
    assert "er.entity_type = ANY($1)" in sql
    assert "er.koi_rid LIKE ANY" in sql or "split_part(er.koi_rid" in sql
    # Only protocol types requested: no entity matches, and no unfiltered listing either.
    sql, params = _build_rids_fetch_query(["orn:koi-net.node"], None)
    assert "\n  AND FALSE" in sql and params == [], sql
    # A context is compared as the koi_rid stores it: lowercase, no whitespace.
    _sql, params = _build_rids_fetch_query([" orn:koi-net.Person "], None)
    assert params == [["orn:koi-net.person"]]
    # No type requested: no type condition at all.
    sql, params = _build_rids_fetch_query(None, None)
    assert "entity_type" not in sql and "koi_rid, ':'" not in sql and params == []
