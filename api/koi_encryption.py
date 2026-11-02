"""
E2EE for Vault Sync — X25519 + ChaCha20-Poly1305

Encrypts vault file contents end-to-end between KOI-net peers.
Uses the already-installed `cryptography` library (no new dependencies).

Key exchange: X25519 (Curve25519 ECDH)
Key derivation: HKDF-SHA256
Symmetric cipher: ChaCha20-Poly1305 (AEAD)
"""

from __future__ import annotations

import base64
import logging
import os
from pathlib import Path
from typing import Tuple

from cryptography.hazmat.primitives.asymmetric.x25519 import (
    X25519PrivateKey,
    X25519PublicKey,
)
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

logger = logging.getLogger(__name__)


def generate_encryption_keypair() -> Tuple[X25519PrivateKey, X25519PublicKey]:
    """Generate an X25519 keypair for E2EE."""
    private_key = X25519PrivateKey.generate()
    return private_key, private_key.public_key()


def save_encryption_key(private_key: X25519PrivateKey, path: Path) -> None:
    """Save X25519 private key as hex-encoded 32 bytes, chmod 600."""
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = private_key.private_bytes_raw()
    path.write_text(raw.hex())
    os.chmod(path, 0o600)
    logger.info(f"Saved encryption key to {path}")


def load_encryption_key(path: Path) -> X25519PrivateKey:
    """Load X25519 private key from hex-encoded file."""
    raw = bytes.fromhex(path.read_text().strip())
    return X25519PrivateKey.from_private_bytes(raw)


def get_encryption_public_key_b64(public_key: X25519PublicKey) -> str:
    """Return base64-encoded raw 32-byte X25519 public key."""
    return base64.b64encode(public_key.public_bytes_raw()).decode()


def load_public_key_from_b64(b64: str) -> X25519PublicKey:
    """Load X25519 public key from base64-encoded raw bytes."""
    return X25519PublicKey.from_public_bytes(base64.b64decode(b64))


def derive_shared_key(
    my_private: X25519PrivateKey,
    their_public: X25519PublicKey,
    my_rid: str,
    their_rid: str,
) -> bytes:
    """Derive a 32-byte symmetric key via ECDH + HKDF.

    Canonical RID ordering ensures both sides derive the same key.
    """
    shared_secret = my_private.exchange(their_public)
    rids = ":".join(sorted([my_rid, their_rid]))
    return HKDF(
        algorithm=SHA256(),
        length=32,
        salt=None,
        info=f"koi-net-e2ee-v1:{rids}".encode(),
    ).derive(shared_secret)


def encrypt_content(plaintext: str, shared_key: bytes, rid: str) -> str:
    """Encrypt plaintext with ChaCha20-Poly1305. Returns base64(nonce || ciphertext || tag)."""
    nonce = os.urandom(12)
    ct = ChaCha20Poly1305(shared_key).encrypt(nonce, plaintext.encode("utf-8"), rid.encode("utf-8"))
    return base64.b64encode(nonce + ct).decode()


def decrypt_content(ciphertext_b64: str, shared_key: bytes, rid: str) -> str:
    """Decrypt base64(nonce || ciphertext || tag) with ChaCha20-Poly1305."""
    raw = base64.b64decode(ciphertext_b64)
    nonce, ct = raw[:12], raw[12:]
    return ChaCha20Poly1305(shared_key).decrypt(nonce, ct, rid.encode("utf-8")).decode("utf-8")
