"""
KOI SignedEnvelope utilities.

ECDSA P-256 with raw r||s base64 signatures, made and checked by koi-net's own
koi_net.protocol.secure (since 2026-11-06): PrivateKey.sign and PublicKey.verify, over the bytes
koi-net signs, UnsignedEnvelope.model_dump_json(exclude_none=True).

The envelope stays a dict here rather than koi-net's SignedEnvelope[T]: that generic only
admits koi-net's request and response models, and our extensions (/handshake,
/events/confirm, the poll and broadcast forms our older peers read) are not among them. A
dict payload dumped from koi-net's model serialises to the same bytes as the typed envelope.

Public keys loaded from koi_net_nodes database table.
"""

from __future__ import annotations

import json
import os
from base64 import b64decode, b64encode
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict

import api.koi_net_floor  # noqa: F401  (Python >= 3.12; quiets koi-net's debug logging)
from koi_net.protocol.secure import PrivateKey as _KoiNetPrivateKey, PublicKey as _KoiNetPublicKey

# koi-net requires cryptography, so it is always present once the import above succeeds.
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec


class EnvelopeError(Exception):
    """Raised when envelope validation fails."""

    def __init__(self, message: str, code: str = "ENVELOPE_ERROR"):
        super().__init__(message)
        self.code = code


# =============================================================================
# Pydantic models matching KOI-net protocol/envelope.py
# CRITICAL: exclude_none=True ensures FORGET events (manifest=None) serialize
# correctly — omitting the field rather than emitting "manifest": null
# =============================================================================

class UnsignedEnvelope(BaseModel):
    """Unsigned envelope for signature computation."""
    model_config = ConfigDict(extra="forbid")

    payload: Dict[str, Any]
    source_node: str
    target_node: str


# =============================================================================
# Signature helpers
# =============================================================================

def _unsigned_envelope_bytes(
    payload: Dict[str, Any], source_node: str, target_node: str
) -> bytes:
    """Compute the bytes to sign/verify using Pydantic serialization.

    Uses model_dump_json(exclude_none=True) to match KOI-net exactly.
    """
    unsigned = UnsignedEnvelope(
        payload=payload,
        source_node=source_node,
        target_node=target_node,
    )
    return unsigned.model_dump_json(exclude_none=True).encode("utf-8")


# =============================================================================
# Key loading
# =============================================================================

def load_private_key_from_file(path: str, password: Optional[str] = None):
    """Load ECDSA private key from PEM file."""
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        pem_data = f.read()
    password_bytes = password.encode() if password else None
    return serialization.load_pem_private_key(data=pem_data, password=password_bytes)


def load_public_key_from_der_b64(der_b64: str):
    """Load ECDSA public key from DER-encoded base64 string.

    Raises ValueError for any other key type. A node RID can be bound to any key (the binding
    is a hash), so an RSA or Ed25519 key would pass a binding check and then crash ECDSA
    verification with a 500 instead of a refusal."""
    der_bytes = b64decode(der_b64)
    key = serialization.load_der_public_key(der_bytes)
    if not isinstance(key, ec.EllipticCurvePublicKey):
        raise ValueError(f"not an EC public key: {type(key).__name__}")
    return key


def public_key_to_der_b64(public_key) -> str:
    """Export public key as DER-encoded base64 string."""
    der_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return b64encode(der_bytes).decode()


# =============================================================================
# Sign / Verify
# =============================================================================

def sign_envelope(
    payload: Dict[str, Any],
    source_node: str,
    target_node: str,
    private_key,
) -> Dict[str, Any]:
    """Sign an envelope payload and return the signed envelope dict."""
    message = _unsigned_envelope_bytes(payload, source_node, target_node)
    return {
        "payload": payload,
        "source_node": source_node,
        "target_node": target_node,
        "signature": _KoiNetPrivateKey(private_key).sign(message),
    }


def verify_envelope(
    envelope: Dict[str, Any],
    public_key,
    expected_source_node: Optional[str] = None,
    expected_target_node: Optional[str] = None,
) -> Tuple[Dict[str, Any], str]:
    """Verify a signed envelope using a single public key.

    Returns (payload, source_node) on success.
    Raises EnvelopeError on failure.
    """

    source_node = envelope.get("source_node")
    target_node = envelope.get("target_node")
    signature = envelope.get("signature")

    if not source_node or not target_node or not signature:
        raise EnvelopeError("Envelope missing required fields", code="MISSING_ENVELOPE_FIELDS")
    if expected_source_node and source_node != expected_source_node:
        raise EnvelopeError(
            f"Envelope source_node mismatch: expected {expected_source_node}, got {source_node}",
            code="SOURCE_NODE_MISMATCH",
        )
    if expected_target_node and target_node != expected_target_node:
        raise EnvelopeError(
            f"Envelope target_node mismatch: expected {expected_target_node}, got {target_node}",
            code="TARGET_NODE_MISMATCH",
        )

    message = _unsigned_envelope_bytes(envelope["payload"], source_node, target_node)

    if not isinstance(public_key, ec.EllipticCurvePublicKey):
        raise EnvelopeError("Signing key is not an EC public key", code="INVALID_SIGNATURE")
    if not isinstance(signature, str):
        raise EnvelopeError("Signature must be a base64 string", code="INVALID_SIGNATURE_FORMAT")
    try:
        _KoiNetPublicKey(public_key).verify(signature, message)
    except InvalidSignature as exc:
        raise EnvelopeError("Invalid envelope signature", code="INVALID_SIGNATURE") from exc
    except (ValueError, TypeError) as exc:  # not base64, or not a raw r||s of the curve's size
        raise EnvelopeError(f"Malformed signature: {exc}", code="INVALID_SIGNATURE_FORMAT") from exc

    return envelope["payload"], source_node


def is_signed_envelope(body: Dict[str, Any]) -> bool:
    """Check if a request body is a SignedEnvelope."""
    return (
        isinstance(body, dict)
        and "signature" in body
        and "payload" in body
        and "source_node" in body
        and "target_node" in body
    )


def unwrap_and_verify_response(
    raw_body: Dict[str, Any],
    expected_source_node: str,
    peer_public_key_b64: Optional[str] = None,
    expected_target_node: Optional[str] = None,
) -> Dict[str, Any]:
    """Unwrap and verify a response that may or may not be a signed envelope.

    1. If raw_body is a signed envelope, verify signature against peer's public key.
    2. Verify envelope.source_node == expected_source_node.
    3. Verify envelope.target_node == expected_target_node (if provided).
    4. If not a signed envelope, return raw_body as-is (permissive mode).
    5. On verification failure, raise EnvelopeError.

    Returns the inner payload dict.
    """
    if not is_signed_envelope(raw_body):
        return raw_body

    if not peer_public_key_b64:
        raise EnvelopeError(
            f"Signed response from {expected_source_node} but no public key available",
            code="UNKNOWN_SOURCE_NODE",
        )

    pub_key = load_public_key_from_der_b64(peer_public_key_b64)
    payload, source = verify_envelope(
        raw_body,
        pub_key,
        expected_source_node=expected_source_node,
        expected_target_node=expected_target_node,
    )
    return payload
