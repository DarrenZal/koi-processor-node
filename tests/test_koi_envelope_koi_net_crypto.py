"""Envelopes are signed and verified by koi-net's own koi_net.protocol.secure.

Cross-version fixtures: an envelope signed by the code before the swap (kept as
tests/fixtures/koi_envelope_before_koi_net_signing.py) verifies under the new code, and the reverse. An
envelope koi-net signs with its typed UnsignedEnvelope verifies here, and one we sign
verifies with koi-net's SignedEnvelope. A malformed signature is a refusal, not a 500. Node
RIDs are unchanged. Signing prints nothing: koi-net's debug logging is filtered (koi_net_floor).
"""
import base64
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

import api.koi_net_floor  # noqa: F401
from api import koi_envelope as new
from api.node_identity import derive_node_rid, generate_keypair, get_public_key_der_b64
from koi_net.protocol.api.models import EventsPayload
from koi_net.protocol.envelope import SignedEnvelope, UnsignedEnvelope
from koi_net.protocol.secure import PrivateKey, PublicKey
from rid_lib.types import KoiNetNode

ROOT = Path(__file__).resolve().parents[1]
OLD_SIGNER = ROOT / "tests" / "fixtures" / "koi_envelope_before_koi_net_signing.py"


@pytest.fixture(scope="module")
def old(tmp_path_factory):
    """api/koi_envelope.py as it was before the swap, loaded as its own module."""
    src = OLD_SIGNER.read_text()
    path = tmp_path_factory.mktemp("old_envelope") / "koi_envelope_before_s7c.py"
    path.write_text(src)
    spec = importlib.util.spec_from_file_location("koi_envelope_before_s7c", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # pydantic resolves its postponed annotations through sys.modules
    spec.loader.exec_module(module)
    assert "_der_to_raw_signature" in src  # the pre-swap implementation, not this one
    return module


def _nodes():
    a, b = generate_keypair(), generate_keypair()
    return a, derive_node_rid("s7c-a", a.public_key()), derive_node_rid("s7c-b", b.public_key())


PAYLOAD = {"type": "events_payload", "events": [{"rid": "orn:koi-net.node:x+0", "event_type": "NEW"}]}


def test_old_signed_verifies_under_new_and_the_reverse(old):
    key, a, b = _nodes()
    by_old = old.sign_envelope(PAYLOAD, a, b, key)
    assert new.verify_envelope(by_old, key.public_key(), expected_target_node=b) == (PAYLOAD, a)
    by_new = new.sign_envelope(PAYLOAD, a, b, key)
    assert old.verify_envelope(by_new, key.public_key(), expected_target_node=b) == (PAYLOAD, a)
    assert set(by_new) == set(by_old) and len(base64.b64decode(by_new["signature"])) == 64


def test_koi_net_signed_verifies_here_and_ours_verifies_in_koi_net():
    key, a, b = _nodes()
    typed = EventsPayload.model_validate(PAYLOAD)
    signed = UnsignedEnvelope[EventsPayload](
        payload=typed, source_node=KoiNetNode.from_string(a), target_node=KoiNetNode.from_string(b),
    ).sign_with(PrivateKey(key))
    as_dict = signed.model_dump(mode="json", exclude_none=True)
    assert new.verify_envelope(as_dict, key.public_key())[1] == a
    ours = new.sign_envelope(typed.model_dump(mode="json", exclude_none=True), a, b, key)
    SignedEnvelope[EventsPayload].model_validate(ours).verify_with(PublicKey(key.public_key()))


@pytest.mark.parametrize("signature, code", [
    ("", "MISSING_ENVELOPE_FIELDS"),
    (None, "MISSING_ENVELOPE_FIELDS"),
    (42, "INVALID_SIGNATURE_FORMAT"),
    (["x"], "INVALID_SIGNATURE_FORMAT"),
    (b"bytes", "INVALID_SIGNATURE_FORMAT"),
    ("not base64 !!", "INVALID_SIGNATURE_FORMAT"),
    ("é" * 88, "INVALID_SIGNATURE_FORMAT"),
    (base64.b64encode(b"short").decode(), "INVALID_SIGNATURE_FORMAT"),
    (base64.b64encode(b"\x01" * 64).decode(), "INVALID_SIGNATURE"),  # well formed, wrong
], ids=["empty", "none", "int", "list", "bytes", "bad-base64", "non-ascii", "short", "wrong-64-bytes"])
def test_a_malformed_signature_is_refused_with_its_code(signature, code):
    """A malformed signature is 400 INVALID_SIGNATURE_FORMAT at the router; a well-formed wrong
    one is 401 INVALID_SIGNATURE (api/koi_net_router.py _envelope_error_response)."""
    key, a, b = _nodes()
    env = new.sign_envelope(PAYLOAD, a, b, key)
    env["signature"] = signature
    with pytest.raises(new.EnvelopeError) as err:
        new.verify_envelope(env, key.public_key())
    assert err.value.code == code


def test_a_tampered_payload_is_refused():
    key, a, b = _nodes()
    env = new.sign_envelope(PAYLOAD, a, b, key)
    env["payload"] = {**PAYLOAD, "events": []}
    with pytest.raises(new.EnvelopeError) as err:
        new.verify_envelope(env, key.public_key())
    assert err.value.code == "INVALID_SIGNATURE"


def test_node_rids_are_unchanged_and_match_koi_nets_derivation():
    key = generate_keypair()
    assert derive_node_rid("s7c-node", key.public_key()) == str(PublicKey(key.public_key()).to_node_rid("s7c-node"))
    assert PublicKey(key.public_key()).to_der() == get_public_key_der_b64(key)


def test_signing_and_verifying_print_nothing_in_a_fresh_process():
    """In a fresh process, as a service starts: koi_net_floor's structlog filter, not some
    earlier test's configuration, is what keeps koi-net's debug lines out of stdout."""
    code = (
        "from api.koi_envelope import sign_envelope, verify_envelope\n"
        "from api.node_identity import derive_node_rid, generate_keypair\n"
        "k = generate_keypair(); a = derive_node_rid('p-a', k.public_key()); b = derive_node_rid('p-b', k.public_key())\n"
        "verify_envelope(sign_envelope({'type': 'events_payload', 'events': []}, a, b, k), k.public_key())\n"
    )
    r = subprocess.run([sys.executable, "-B", "-c", code], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    assert r.stdout == "" and r.stderr == "", (r.stdout, r.stderr)


def test_an_envelope_signed_before_the_swap_still_verifies():
    """A golden vector from the signer before the swap, so this direction needs no fixture code."""
    import json
    from api.koi_envelope import load_public_key_from_der_b64
    vector = json.loads((ROOT / "tests" / "fixtures" / "koi_envelope_pre_s7c_vector.json").read_text())
    env = vector["envelope"]
    payload, source = new.verify_envelope(env, load_public_key_from_der_b64(vector["public_key_der_b64"]),
                                          expected_target_node=env["target_node"])
    assert source == env["source_node"] and payload == env["payload"]
    tampered = {**env, "payload": {**env["payload"], "events": []}}
    with pytest.raises(new.EnvelopeError):
        new.verify_envelope(tampered, load_public_key_from_der_b64(vector["public_key_der_b64"]))
