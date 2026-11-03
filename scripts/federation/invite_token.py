#!/usr/bin/env python3
"""
Invite token for KOI-net peer onboarding.

Format: KOI-INVITE-1:<base64url-payload>.<base64url-hmac>
Single line, URL-safe chars only, safe for Signal.

HMAC is admin-side only — peers decode without signature verification.
Identity verification is provided by SAS, not by the token signature.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import time

VERSION_PREFIX = "KOI-INVITE-1:"


def generate_jti() -> str:
    """Generate a 16-byte random hex token ID."""
    return secrets.token_hex(16)


def load_or_create_secret(state_dir: str) -> bytes:
    """Load or auto-generate the 32-byte invite HMAC secret."""
    secret_path = os.path.join(state_dir, "invite_secret")
    if os.path.exists(secret_path):
        with open(secret_path, "rb") as f:
            secret = f.read()
        if len(secret) < 32:
            raise ValueError(f"Invite secret too short ({len(secret)} bytes), expected 32")
        return secret
    os.makedirs(state_dir, exist_ok=True)
    secret = secrets.token_bytes(32)
    old_umask = os.umask(0o077)
    try:
        with open(secret_path, "wb") as f:
            f.write(secret)
    finally:
        os.umask(old_umask)
    return secret


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def create_token(payload: dict, secret: bytes) -> str:
    """Admin-side: create a signed invite token from payload dict."""
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    payload_b64 = _b64url_encode(payload_json.encode())
    mac = hmac.new(secret, payload_json.encode(), hashlib.sha256).digest()
    mac_b64 = _b64url_encode(mac)
    return f"{VERSION_PREFIX}{payload_b64}.{mac_b64}"


def decode_token(token_str: str) -> dict:
    """Peer-side: extract payload WITHOUT signature verification.

    Validates format and expiry only. The peer trusts the token came
    from admin via a secure channel (Signal).
    """
    if not token_str.startswith(VERSION_PREFIX):
        raise ValueError(f"Invalid token prefix (expected {VERSION_PREFIX})")
    body = token_str[len(VERSION_PREFIX):]
    parts = body.split(".")
    if len(parts) != 2:
        raise ValueError("Invalid token format (expected payload.hmac)")
    payload_b64 = parts[0]
    payload_json = _b64url_decode(payload_b64).decode()
    payload = json.loads(payload_json)
    # Validate expiry
    expires_at = payload.get("expires_at")
    if expires_at and time.time() > expires_at:
        raise ValueError(f"Token expired at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(expires_at))}")
    return payload


def verify_token(token_str: str, secret: bytes) -> dict:
    """Admin-side: full HMAC verification + payload extraction."""
    if not token_str.startswith(VERSION_PREFIX):
        raise ValueError(f"Invalid token prefix (expected {VERSION_PREFIX})")
    body = token_str[len(VERSION_PREFIX):]
    parts = body.split(".")
    if len(parts) != 2:
        raise ValueError("Invalid token format (expected payload.hmac)")
    payload_b64, mac_b64 = parts
    payload_json = _b64url_decode(payload_b64).decode()
    expected_mac = hmac.new(secret, payload_json.encode(), hashlib.sha256).digest()
    actual_mac = _b64url_decode(mac_b64)
    if not hmac.compare_digest(expected_mac, actual_mac):
        raise ValueError("Invalid token signature")
    payload = json.loads(payload_json)
    expires_at = payload.get("expires_at")
    if expires_at and time.time() > expires_at:
        raise ValueError(f"Token expired at {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(expires_at))}")
    return payload
