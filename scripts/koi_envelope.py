"""
KOI SignedEnvelope utilities (minimal, protocol-aligned).

Supports signing/verifying envelopes using ECDSA P-256 with raw r||s base64
signatures to match KOI-net reference behavior.

P1b: Uses Pydantic model_dump_json(exclude_none=True) for serialization
to match KOI-net's UnsignedEnvelope.sign_with() behavior exactly.
"""

from __future__ import annotations

import json
import os
from base64 import b64decode, b64encode
from enum import StrEnum
from typing import Any, Dict, Optional, Tuple

from pydantic import BaseModel, ConfigDict

try:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import (
        decode_dss_signature,
        encode_dss_signature
    )
    _CRYPTO_AVAILABLE = True
except Exception:  # pragma: no cover - runtime dependency
    _CRYPTO_AVAILABLE = False
    InvalidSignature = Exception
    ec = None


class EnvelopeError(Exception):
    """Raised when envelope validation fails."""


# ============================================================================
# Pydantic models matching KOI-net protocol/envelope.py
# CRITICAL: exclude_none=True ensures FORGET events (manifest=None) serialize
# correctly - omitting the field rather than emitting "manifest": null
# ============================================================================

class UnsignedEnvelope(BaseModel):
    """Unsigned envelope for signature computation.

    Matches KOI-net's UnsignedEnvelope schema exactly.
    Uses exclude_none=True to omit None fields (critical for FORGET events).
    """
    model_config = ConfigDict(extra="forbid")

    payload: Dict[str, Any]
    source_node: str
    target_node: str


class SignedEnvelope(BaseModel):
    """Signed envelope for wire transmission.

    Matches KOI-net's SignedEnvelope schema exactly.
    """
    model_config = ConfigDict(extra="forbid")

    payload: Dict[str, Any]
    source_node: str
    target_node: str
    signature: str


class ErrorType(StrEnum):
    """BlockScience koi-net protocol error types."""
    UnknownNode = "unknown_node"
    InvalidKey = "invalid_key"
    InvalidSignature = "invalid_signature"
    InvalidTarget = "invalid_target"


class ErrorResponse(BaseModel):
    """KOI-net error response model.

    MUST be used instead of FastAPI's default {"detail": ...} format
    for /koi-net/* endpoints to maintain interoperability.

    The error field is str (not ErrorType) to allow Regen-specific extras
    like invalid_json, internal_error, signing_unavailable.
    """
    error: str
    detail: Optional[str] = None


class AmbiguousNodeError(EnvelopeError):
    """Raised when a short hash matches multiple known peers."""


def _normalize_pem(pem: str) -> str:
    return pem.replace("\\n", "\n")


def _unsigned_envelope_bytes(payload: Dict[str, Any], source_node: str, target_node: str) -> bytes:
    """Compute the bytes to sign/verify using Pydantic serialization.

    CRITICAL: Uses model_dump_json(exclude_none=True) to match KOI-net exactly.
    This ensures:
    - Field ordering matches Pydantic's default (alphabetical by model definition)
    - None fields are omitted, not serialized as null
    - Consistent serialization for signature verification
    """
    unsigned = UnsignedEnvelope(
        payload=payload,
        source_node=source_node,
        target_node=target_node
    )
    return unsigned.model_dump_json(exclude_none=True).encode("utf-8")


def _der_to_raw_signature(der_signature: bytes) -> bytes:
    r, s = decode_dss_signature(der_signature)
    byte_length = (ec.SECP256R1().key_size + 7) // 8
    r_bytes = r.to_bytes(byte_length, byteorder="big")
    s_bytes = s.to_bytes(byte_length, byteorder="big")
    return r_bytes + s_bytes


def _raw_to_der_signature(raw_signature: bytes) -> bytes:
    byte_length = (ec.SECP256R1().key_size + 7) // 8
    if len(raw_signature) != 2 * byte_length:
        raise EnvelopeError(f"Raw signature must be {2 * byte_length} bytes")
    r_bytes = raw_signature[:byte_length]
    s_bytes = raw_signature[byte_length:]
    r = int.from_bytes(r_bytes, byteorder="big")
    s = int.from_bytes(s_bytes, byteorder="big")
    return encode_dss_signature(r, s)


def load_private_key_from_env(prefix: str = "KOI"):
    if not _CRYPTO_AVAILABLE:
        return None

    pem = os.getenv(f"{prefix}_PRIVATE_KEY_PEM")
    pem_path = os.getenv(f"{prefix}_PRIVATE_KEY_PEM_PATH")
    password = os.getenv(f"{prefix}_PRIVATE_KEY_PASSWORD")

    if pem_path:
        with open(pem_path, "r") as f:
            pem = f.read()
    if not pem:
        return None

    pem = _normalize_pem(pem)
    password_bytes = password.encode() if password else None

    return serialization.load_pem_private_key(
        data=pem.encode(),
        password=password_bytes
    )


def load_public_keys_from_env(prefix: str = "KOI") -> Dict[str, Any]:
    if not _CRYPTO_AVAILABLE:
        return {}

    keys_json = os.getenv(f"{prefix}_PUBLIC_KEYS_JSON")
    keys_path = os.getenv(f"{prefix}_PUBLIC_KEYS_PATH")

    if keys_path:
        with open(keys_path, "r") as f:
            keys_json = f.read()
    if not keys_json:
        return {}

    data = json.loads(keys_json)
    if not isinstance(data, dict):
        raise EnvelopeError("Public keys JSON must be an object mapping node_id -> PEM")

    keys: Dict[str, Any] = {}
    for node_id, pem in data.items():
        pem = _normalize_pem(pem)
        keys[node_id] = serialization.load_pem_public_key(pem.encode())
    return keys


def sign_envelope(
    payload: Dict[str, Any],
    source_node: str,
    target_node: str,
    private_key
) -> Dict[str, Any]:
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required for signing envelopes")
    message = _unsigned_envelope_bytes(payload, source_node, target_node)
    der_signature = private_key.sign(message, ec.ECDSA(hashes.SHA256()))
    raw_signature = _der_to_raw_signature(der_signature)
    signature = b64encode(raw_signature).decode()
    return {
        "payload": payload,
        "source_node": source_node,
        "target_node": target_node,
        "signature": signature
    }


def verify_envelope(
    envelope: Dict[str, Any],
    public_keys: Dict[str, Any],
    expected_target: Optional[str] = None,
    enforce_target: bool = False
) -> Tuple[Dict[str, Any], str]:
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required for verifying envelopes")

    if not isinstance(envelope, dict) or "payload" not in envelope:
        raise EnvelopeError("Invalid envelope structure")

    source_node = envelope.get("source_node")
    target_node = envelope.get("target_node")
    signature = envelope.get("signature")

    if enforce_target and expected_target and target_node != expected_target:
        raise EnvelopeError(f"Envelope target {target_node!r} does not match {expected_target!r}")

    if not source_node or not signature:
        raise EnvelopeError("Envelope missing source_node or signature")

    public_key = public_keys.get(source_node)
    if not public_key:
        raise EnvelopeError(f"No public key registered for {source_node}")

    message = _unsigned_envelope_bytes(envelope["payload"], source_node, target_node)
    raw_signature = b64decode(signature)
    der_signature = _raw_to_der_signature(raw_signature)

    try:
        public_key.verify(der_signature, message, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as exc:
        raise EnvelopeError("Invalid envelope signature") from exc

    return envelope["payload"], source_node


def verify_envelope_with_key(envelope: Dict[str, Any], public_key) -> bool:
    """Verify envelope signature using a single public key.

    This matches KOI-net's SignedEnvelope.verify_with() pattern for simpler
    use cases where the public key is already known.

    Args:
        envelope: The signed envelope dict
        public_key: ECDSA P-256 public key object

    Returns:
        True if signature is valid

    Raises:
        EnvelopeError: If signature verification fails
    """
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required for verifying envelopes")

    source_node = envelope.get("source_node")
    target_node = envelope.get("target_node")
    signature = envelope.get("signature")

    if not source_node or not target_node or not signature:
        raise EnvelopeError("Envelope missing required fields")

    message = _unsigned_envelope_bytes(envelope["payload"], source_node, target_node)
    raw_signature = b64decode(signature)
    der_signature = _raw_to_der_signature(raw_signature)

    try:
        public_key.verify(der_signature, message, ec.ECDSA(hashes.SHA256()))
        return True
    except InvalidSignature as exc:
        raise EnvelopeError("Invalid envelope signature") from exc


# ============================================================================
# Key generation and node identity derivation (Phase 5)
# ============================================================================

def generate_keypair():
    """Generate a new ECDSA P-256 keypair.

    Returns:
        (private_key, public_key) tuple
    """
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required for keypair generation")
    private_key = ec.generate_private_key(ec.SECP256R1())
    return private_key, private_key.public_key()


def save_private_key(private_key, path: str, password: Optional[str] = None):
    """Save private key to PEM file.

    Args:
        private_key: ECDSA private key
        path: File path to write PEM
        password: Optional password for encryption
    """
    from pathlib import Path as _Path

    password_bytes = password.encode() if password else None
    encryption = (
        serialization.BestAvailableEncryption(password_bytes)
        if password_bytes
        else serialization.NoEncryption()
    )
    pem_data = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        encryption,
    )
    p = _Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(pem_data)


def load_private_key(path: str, password: Optional[str] = None):
    """Load private key from PEM file.

    Returns:
        ECDSA private key, or None if file doesn't exist
    """
    if not _CRYPTO_AVAILABLE:
        return None
    from pathlib import Path as _Path
    p = _Path(path)
    if not p.exists():
        return None
    pem_data = p.read_bytes()
    password_bytes = password.encode() if password else None
    return serialization.load_pem_private_key(pem_data, password=password_bytes)


def generate_and_save_keypair(path: str, password: Optional[str] = None):
    """Generate ECDSA P-256 keypair and save private key to PEM file.

    If key file already exists, loads existing key instead of generating.

    Returns:
        (private_key, public_key) tuple
    """
    existing = load_private_key(path, password)
    if existing:
        return existing, existing.public_key()
    private_key, public_key = generate_keypair()
    save_private_key(private_key, path, password)
    return private_key, public_key


def public_key_to_b64der(public_key) -> str:
    """Encode public key as base64(DER) string — the canonical wire format."""
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required")
    der_bytes = public_key.public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return b64encode(der_bytes).decode()


def public_key_from_b64der(b64der: str):
    """Decode public key from base64(DER) string."""
    if not _CRYPTO_AVAILABLE:
        raise EnvelopeError("cryptography is required")
    der_bytes = b64decode(b64der)
    return serialization.load_der_public_key(der_bytes)


def derive_node_rid(name: str, public_key) -> str:
    """Derive a node RID from name and public key.

    Matches BlockScience's canonical pattern: sha256(base64(DER(public_key)))
    producing a full 64-character hex hash.

    Args:
        name: Node name (e.g., "regen-coordinator")
        public_key: ECDSA P-256 public key

    Returns:
        Node RID string: "orn:koi-net.node:{name}+{64char_hex}"
    """
    import hashlib
    b64der = public_key_to_b64der(public_key)
    full_hash = hashlib.sha256(b64der.encode()).hexdigest()
    return f"orn:koi-net.node:{name}+{full_hash}"


def _derive_hash_from_public_key(public_key, length: Optional[int] = None) -> str:
    """Derive the hash portion of a node RID from a public key.

    Args:
        public_key: ECDSA P-256 public key
        length: If provided, truncate hash to this many chars (e.g. 16 for legacy)

    Returns:
        Hex hash string
    """
    import hashlib
    b64der = public_key_to_b64der(public_key)
    full_hash = hashlib.sha256(b64der.encode()).hexdigest()
    if length:
        return full_hash[:length]
    return full_hash


def node_rid_matches_public_key(
    node_rid: str,
    public_key,
    allow_legacy16: bool = True,
    allow_der64: bool = True,
) -> bool:
    """Match node RID hash against public key using both hash modes.

    Replicates Octo's node_identity.py pattern for interoperability.
    Supports both 64-char (BlockScience canonical) and 16-char (legacy) hashes.

    Args:
        node_rid: Full node RID (e.g., "orn:koi-net.node:name+hash")
        public_key: ECDSA P-256 public key
        allow_legacy16: Accept 16-char truncated hash match
        allow_der64: Accept 64-char full hash match

    Returns:
        True if the hash in node_rid matches the public key
    """
    suffix = node_rid.rsplit("+", 1)[-1] if "+" in node_rid else ""
    if not suffix:
        return False

    full_hash = _derive_hash_from_public_key(public_key)

    if len(suffix) == 64 and allow_der64:
        return suffix == full_hash
    if len(suffix) == 16 and allow_legacy16:
        return suffix == full_hash[:16]
    return False
