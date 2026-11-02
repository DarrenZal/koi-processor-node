"""extract_rid_type must parse ORNs by the grammar, not by substring guessing.

The original implementation had two hardcoded branches:

    if "koi-net." in rid:  ... split on "koi-net."
    if "entity:"  in rid:  ... split on "entity:"

Both are substring tests against a structured identifier, and they were wrong in
three ways that mattered once the filter began to FAIL CLOSED (divergence 10):

  * 'orn:personal-koi.entity:abc' contains "entity:", so it returned 'Abc' —
    the SLUG, capitalized, presented as a type. An edge scoped {Abc} would have
    matched it.
  * 'orn:personal-koi.doclink:x' matched neither branch -> None -> now excluded
    from every edge.
  * 'orn:personal-koi.vault-file:x' likewise, which blocked the divergence-1
    namespace migration outright: moving off the squatted koi-net.* namespace
    would have made vault files undeliverable.

The 'orn:entity:{type}/{slug}' form the second branch existed for has ZERO rows
on either node (checked in koi_net_events and entity_registry, with
'orn:personal-koi.entity:' = 81,552 as the positive control), so it was dead.

It now uses rid_lib.core.RID.from_string — the same parser upstream uses — which
is the point: stop hand-rolling a parser for a format someone else specifies.
"""

import pytest

from api.event_queue import extract_rid_type


@pytest.mark.parametrize("rid,expected", [
    # --- unchanged behaviour: these back live edge rid_types ---
    ("orn:koi-net.vault-file:Shared/x.md", "Vault-file"),
    ("orn:koi-net.vault-file:Shared/a/b/deeply/nested.md", "Vault-file"),
    ("orn:koi-net.claim:abc", "Claim"),
    ("orn:koi-net.intent:abc", "Intent"),
    ("orn:koi-net.node:name+hash", "Node"),
    ("orn:koi-net.edge:a>b:poll", "Edge"),
    ("orn:koi-net.specdoc:canary+hash", "Specdoc"),
    # --- fixed: were None or garbage ---
    ("orn:personal-koi.entity:abc", "Entity"),
    ("orn:personal-koi.doclink:abc", "Doclink"),
    ("orn:personal-koi.knowledge-episode:abc", "Knowledge-episode"),
    ("orn:obsidian.note:Shared/x.md", "Note"),
    # --- the migration target must resolve to the SAME type as the legacy one,
    #     which is what makes the divergence-1 cutover safe for edge matching ---
    ("orn:personal-koi.vault-file:Shared/x.md", "Vault-file"),
])
def test_extract_rid_type(rid, expected):
    assert extract_rid_type(rid) == expected


@pytest.mark.parametrize("rid", [
    "", "not-an-rid", "forest-garden", "orn:nocolon", "orn::empty",
    "anthropic-headless-billing-banner-watch", None, 123, "orn:a:b",
])
def test_unparseable_rid_is_none(rid):
    """None means 'no declared type', which the poll filter treats as excluded.

    Malformed input must never raise out of the filter — a crash here stops the
    whole poll for every peer.
    """
    assert extract_rid_type(rid) is None


def test_legacy_and_migrated_namespaces_agree():
    """The divergence-1 cutover must be a no-op for edge scoping."""
    legacy = extract_rid_type("orn:koi-net.vault-file:Shared/note.md")
    migrated = extract_rid_type("orn:personal-koi.vault-file:Shared/note.md")
    assert legacy == migrated == "Vault-file", (
        "an edge listing Vault-file must match both namespaces, or the cutover "
        "silently stops delivering vault files"
    )


def test_registered_types_validate_their_reference():
    """rid-lib enforces reference grammar for types it registers, and we inherit that.

    'orn:koi-net.node:probe' is REJECTED because KoiNetNode requires a
    '<name>+<hash>' reference, so extract_rid_type returns None and the poll
    filter excludes it. The old substring implementation returned 'Node' for
    anything merely containing "koi-net.", malformed or not.

    Failing closed on a malformed protocol RID is the correct default and is
    consistent with divergence 10, but it IS a behaviour change: pinning it here
    so it is a decision rather than a surprise. Real node RIDs on the wire carry
    the '+hash' form and are unaffected.
    """
    assert extract_rid_type("orn:koi-net.node:probe") is None
    assert extract_rid_type("orn:koi-net.node:example-node+3f9c2a71") == "Node"


def test_obsidian_note_rid_survives_nested_paths():
    """rid_lib parses a nested obsidian.note RID even though ObsidianNote.from_reference would not.

    koi-net-obsidian-manager-node's ObsidianNote requires exactly two
    '/'-separated components (<vault_id>/<note_id>), and a vault with folders has
    paths deeper than that. That is the recorded reason divergence 1 does
    NOT migrate to obsidian.note. Namespace-level parsing is unaffected, which
    is all this function needs.
    """
    assert extract_rid_type("orn:obsidian.note:Shared/Projects/Deeply/Nested.md") == "Note"
