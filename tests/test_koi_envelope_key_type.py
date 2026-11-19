"""Only EC keys are loaded or used to verify (register 26).

A node RID can be bound to any key, because the binding is a hash of the key's DER. An
Ed25519 or RSA key would pass a binding check and then crash ECDSA verification, an
unauthenticated 500 instead of a refusal.
"""
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from api.koi_envelope import EnvelopeError, load_public_key_from_der_b64, public_key_to_der_b64, sign_envelope, verify_envelope
from api.node_identity import generate_keypair


def _ed25519_der_b64():
    return public_key_to_der_b64(ed25519.Ed25519PrivateKey.generate().public_key())


def test_the_loader_refuses_a_non_ec_key():
    with pytest.raises(ValueError):
        load_public_key_from_der_b64(_ed25519_der_b64())


def test_the_loader_accepts_an_ec_key():
    key = generate_keypair()
    assert load_public_key_from_der_b64(public_key_to_der_b64(key.public_key())).public_numbers() == key.public_key().public_numbers()


def test_verification_with_a_non_ec_key_is_a_refusal_not_a_crash():
    key = generate_keypair()
    envelope = sign_envelope({"type": "probe"}, "orn:koi-net.node:a+0", "orn:koi-net.node:b+0", key)
    ed = serialization.load_der_public_key(ed25519.Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo))
    with pytest.raises(EnvelopeError) as err:
        verify_envelope(envelope, ed)
    assert err.value.code == "INVALID_SIGNATURE"
