"""Divergences 16 and 24: events on the wire survive koi-net's re-serialisation.

koi-net verifies a signature by parsing the payload into its models and dumping it again
(model_dump_json, exclude_none). koi_net.protocol.event.Event ignores unknown fields, so our
event_id extension broke every signed poll reply (16) and push (24). rid-lib's Manifest
keeps extras but dumps its declared fields first and normalises the timestamp, while
koi_net_events stores manifests as jsonb, which reorders keys. The end-to-end proof is the
conformance suite and the interop gate; this pins the shape.
"""
import pytest
from rid_lib.ext import Manifest

from api.koi_protocol import koi_net_event

RID = "orn:koi-net.vault-file:Shared/Projects/a.md"
# A vault-sync manifest as jsonb hands it back: keys reordered, our extensions included.
JSONB_MANIFEST = {"rid": RID, "bytes": 333, "deleted": False, "base_hash": None, "timestamp": "2026-11-06T03:54:05.5Z",
                  "origin_seq": 251, "sha256_hash": "ab" * 32, "content_hash": "ab" * 32, "relative_path": "Projects/a.md"}


def test_koi_net_peers_get_exactly_koi_nets_event_fields():
    ev = koi_net_event(RID, "NEW", JSONB_MANIFEST, {"markdown": "x"}, event_id="e-1")
    assert list(ev) == ["rid", "event_type", "manifest", "contents"]
    assert "event_id" not in ev


def test_manifest_has_declared_fields_first_and_keeps_extensions():
    m = koi_net_event(RID, "NEW", JSONB_MANIFEST, {})["manifest"]
    assert list(m)[:3] == ["rid", "timestamp", "sha256_hash"]
    assert m["timestamp"] == "2026-11-06T03:54:05.500000Z"  # rid-lib's normalised form
    assert m["relative_path"] == "Projects/a.md" and m["origin_seq"] == 251
    # koi-net dumps with exclude_none, which drops a None extra on its model, so none is sent.
    assert "base_hash" not in m


def test_the_manifest_is_a_fixed_point_of_rid_libs_round_trip():
    """What koi-net does on receipt: parse into Manifest, dump again. Nothing may change."""
    m = koi_net_event(RID, "NEW", JSONB_MANIFEST, {})["manifest"]
    again = Manifest.model_validate(m).model_dump(mode="json", exclude_none=True)
    assert again == m and list(again) == list(m)


def test_any_stored_key_order_gives_a_fixed_point():
    """Extras keep the order they arrive in, on our side and on koi-net's, so the signature
    holds for any stored order; the declared fields always come first."""
    reordered = dict(reversed(list(JSONB_MANIFEST.items())))
    m = koi_net_event(RID, "NEW", reordered, {})["manifest"]
    assert list(m)[:3] == ["rid", "timestamp", "sha256_hash"]
    again = Manifest.model_validate(m).model_dump(mode="json", exclude_none=True)
    assert list(again) == list(m) and again == m


def test_peers_running_our_code_still_get_event_id():
    ev = koi_net_event(RID, "NEW", JSONB_MANIFEST, {"markdown": "x"}, event_id="e-1", extensions=True)
    assert list(ev) == ["rid", "event_type", "event_id", "manifest", "contents"]
    assert ev["event_id"] == "e-1"


def test_absent_parts_are_left_out_as_koi_net_excludes_none():
    assert koi_net_event("conformance::task-1", "FORGET") == {"rid": "conformance::task-1", "event_type": "FORGET"}
    # A manifest rid-lib cannot parse is left out rather than sent malformed.
    assert "manifest" not in koi_net_event(RID, "NEW", {"rid": RID, "timestamp": ""}, {"a": 1})


# --- fixes of 2026-11-06 -------------------------------------------------------------

from api.koi_protocol import complete_manifest  # noqa: E402


def test_an_event_koi_net_cannot_represent_is_withheld_not_raised():
    """A task key without a colon as the RID raised and lost a whole poll batch."""
    assert koi_net_event("koi-null-embed-chunks", "NEW", None, {"_koi_domain": "task"}) is None
    assert koi_net_event("orn:koi-net.node:abc", "NEW", None, {}) is None   # malformed node reference
    assert koi_net_event(RID, "BOGUS", None, {}) is None


def test_a_peer_running_our_code_still_gets_it_in_the_raw_form():
    ev = koi_net_event("koi-null-embed-chunks", "NEW", {"x": 1}, {"_koi_domain": "task"}, event_id="e-9", extensions=True)
    assert ev == {"rid": "koi-null-embed-chunks", "event_type": "NEW", "event_id": "e-9",
                  "manifest": {"x": 1}, "contents": {"_koi_domain": "task"}}


def test_complete_manifest_fills_the_three_koi_net_fields():
    """Sensor manifests without rid/timestamp/sha256_hash were dropped on push."""
    m = complete_manifest({"source": "github", "timestamp": "2026-11-06T00:00:00+00:00"}, RID, {"a": 1})
    assert m["rid"] == RID and m["timestamp"].endswith("Z") and len(m["sha256_hash"]) == 64 and m["source"] == "github"
    assert koi_net_event(RID, "NEW", m, {"a": 1})["manifest"]["source"] == "github"
    assert complete_manifest(None, RID) is None


def test_koi_net_verifies_what_we_sign():
    """The end-to-end property, checked with koi-net itself: an events payload built from
    koi_net_event and signed by our signer verifies under koi-net's SignedEnvelope."""
    from cryptography.hazmat.primitives.asymmetric import ec
    from koi_net.protocol.api.models import EventsPayload
    from koi_net.protocol.envelope import SignedEnvelope
    from koi_net.protocol.secure import PublicKey

    from api.koi_envelope import public_key_to_der_b64, sign_envelope

    key = ec.generate_private_key(ec.SECP256R1())
    events = [
        koi_net_event(RID, "NEW", JSONB_MANIFEST, {"markdown": "é ünïcode ", "nested": {"none": None, "list": [1, None]}}),
        koi_net_event("conformance::task-1", "UPDATE", None, {"_koi_domain": "task", "payload": {"k": None}}),
        koi_net_event("orn:koi-net.edge:" + "a" * 64, "FORGET"),
    ]
    body = sign_envelope({"type": "events_payload", "events": events},
                         "orn:koi-net.node:ours+" + "b" * 64, "orn:koi-net.node:peer+" + "c" * 64, key)
    envelope = SignedEnvelope[EventsPayload].model_validate(body)
    envelope.verify_with(PublicKey.from_der(public_key_to_der_b64(key.public_key())))


@pytest.mark.parametrize("stored", ["x", [1], 5, {"timestamp": 1730000000}, {"timestamp": None}, {"rid": 7}])
def test_complete_manifest_never_raises_on_whatever_a_peer_stored(stored):
    """A non-dict stored manifest used to raise before the guard, losing a poll
    batch (poll marks rows delivered first) or stalling a webhook edge."""
    m = complete_manifest(stored, RID, {"a": 1})
    assert m is None or isinstance(m, dict)
    koi_net_event(RID, "NEW", m, {"a": 1})  # and the builder copes with the result


def test_a_null_timestamp_still_gets_the_current_time():
    """A null timestamp used to drop the manifest."""
    m = complete_manifest({"rid": RID, "timestamp": None, "sha256_hash": "ab" * 32}, RID, {})
    assert m["timestamp"].endswith("Z")
    assert "manifest" in koi_net_event(RID, "NEW", m, {})
