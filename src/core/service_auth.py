#!/usr/bin/env python3
"""Shared service-to-service authentication and request validation for the
internal KOI services (event bridge, coordinator, bge, MCP).

Auth model
----------
- Clients send the current key in the ``X-KOI-Service-Key`` header, taken from
  ``KOI_SERVICE_KEY``.
- Servers accept any key in ``KOI_SERVICE_KEYS`` (comma-separated: current plus
  next, so a rotation needs no outage). This is a new secret, separate from
  ``KOI_INTERNAL_API_KEY``.
- A key counts as usable only if it is non-empty after stripping and at least
  ``MIN_KEY_LENGTH`` characters. Unset, empty, whitespace or too short counts as
  missing.
- A missing or too-short header is rejected before any comparison. A present
  header is compared as UTF-8 bytes with ``hmac.compare_digest`` against every
  configured key, so comparison is constant time and a non-ASCII header is
  rejected rather than raising.
- Mode is per service via ``KOI_AUTH_MODE_<SERVICE>``. Only the exact value
  ``warn`` means warn; anything else, including unset, means enforce. Each
  service reads its own variable so a shared env file cannot couple two
  services' rollouts.
- ``warn`` is a rollout tool, not a safe state: it logs a "would reject" line and
  allows the request. It is only safe with the service bound to loopback and the
  R26 proxy bypass closed.

This module imports only the standard library at import time. The FastAPI
dependency factory imports FastAPI lazily, so importing this module - and the
services that import it, including the semantic bridge - never requires FastAPI
and never reads required configuration at import time.
"""

import hmac
import logging
import os
import re
from datetime import datetime
from typing import Callable, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

logger = logging.getLogger("koi.service_auth")

MIN_KEY_LENGTH = 32
SERVICE_KEY_HEADER = "X-KOI-Service-Key"
SERVER_KEYS_ENV = "KOI_SERVICE_KEYS"
CLIENT_KEY_ENV = "KOI_SERVICE_KEY"

# Event types the bridge pipeline actually handles. The allowlist is applied
# after the content filter, so heartbeats never reach it.
ALLOWED_EVENT_TYPES = frozenset({"NEW", "UPDATE", "FORGET"})

MAX_RID_LENGTH = 256
# Characters that must never appear in an identifier that is later interpolated
# into a SPARQL literal or IRI. Control characters and whitespace are handled
# separately in ``identifier_is_valid``.
_FORBIDDEN_ID_CHARS = frozenset('"\\<>{}|^`')
# A content hash must be a bounded token with no whitespace, control or
# injection characters. The RDF literal escaping is the real injection guard, so
# this is deliberately lenient about FORMAT - bare hex, "sha256:<hex>", or a
# multibase/CID digest all pass - to avoid rejecting a producer's legitimate
# hash (which, in enforce mode, would dead-letter and lose the event).
_CONTENT_HASH_RE = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9:._-]{0,255}\Z")


# --------------------------------------------------------------------------- #
# Key handling (pure)
# --------------------------------------------------------------------------- #
def _usable_key(value: Optional[str]) -> Optional[str]:
    """Return the stripped key if it is usable, else None."""
    if value is None:
        return None
    stripped = value.strip()
    if len(stripped) < MIN_KEY_LENGTH:
        return None
    return stripped


def load_service_keys(env: str = SERVER_KEYS_ENV) -> List[str]:
    """Parse the server's accepted keys from a comma-separated env var.

    Empty, whitespace and too-short entries are dropped. The returned list may be
    empty, meaning no key is configured.
    """
    raw = os.getenv(env, "") or ""
    keys: List[str] = []
    for part in raw.split(","):
        usable = _usable_key(part)
        if usable is not None:
            keys.append(usable)
    return keys


def header_matches_a_key(header_value: Optional[str], keys: List[str]) -> bool:
    """True iff a usable header matches one configured key, in constant time."""
    candidate = _usable_key(header_value)
    if candidate is None or not keys:
        return False
    try:
        candidate_bytes = candidate.encode("utf-8")
    except Exception:
        return False
    matched = False
    # Compare against every key so timing does not reveal which key matched.
    for key in keys:
        if hmac.compare_digest(candidate_bytes, key.encode("utf-8")):
            matched = True
    return matched


def auth_mode(service: str) -> str:
    """Return 'warn' or 'enforce' for a service. Only the exact value 'warn' warns."""
    value = os.getenv(f"KOI_AUTH_MODE_{service.upper()}")
    return "warn" if value == "warn" else "enforce"


def keys_configured(env: str = SERVER_KEYS_ENV) -> bool:
    return len(load_service_keys(env)) > 0


def assert_keys_configured(service: str, env: str = SERVER_KEYS_ENV) -> None:
    """Refuse to start if no usable key is configured, in either mode.

    Call this from a startup hook or ``__main__`` - never at import, because the
    semantic bridge imports the v2 bridge module.
    """
    keys = load_service_keys(env)
    mode = auth_mode(service)
    if not keys:
        raise RuntimeError(
            f"{service}: no usable service key in {env}. Provide a comma-separated "
            f"value with each entry at least {MIN_KEY_LENGTH} characters. "
            f"Refusing to start (mode would be '{mode}')."
        )
    logger.info(
        "%s: service-auth mode=%s, key configured: yes (%d accepted)",
        service, mode, len(keys),
    )


def preflight(service: str, env: str = SERVER_KEYS_ENV) -> Tuple[bool, str]:
    """Return (ok, message) for a ``--preflight`` check without starting a server."""
    keys = load_service_keys(env)
    mode = auth_mode(service)
    if not keys:
        return False, (
            f"{service}: FAIL - no usable key in {env} "
            f"(each entry must be >= {MIN_KEY_LENGTH} chars); mode would be '{mode}'"
        )
    return True, f"{service}: OK - mode={mode}, {len(keys)} key(s) configured"


# --------------------------------------------------------------------------- #
# Identifier / field validation (pure)
# --------------------------------------------------------------------------- #
def identifier_is_valid(value: Optional[str], max_length: int = MAX_RID_LENGTH) -> bool:
    """A RID / source_node is valid if it is a non-empty string within the length
    bound and contains no control character, whitespace or forbidden character."""
    if not isinstance(value, str) or value == "":
        return False
    if len(value) > max_length:
        return False
    for ch in value:
        codepoint = ord(ch)
        if codepoint < 0x20 or codepoint == 0x7F:
            return False
        if ch.isspace():
            return False
        if ch in _FORBIDDEN_ID_CHARS:
            return False
    return True


def timestamp_is_valid(value: Optional[str]) -> bool:
    """True if the value parses as an ISO-8601 timestamp."""
    if not isinstance(value, str) or not value.strip():
        return False
    candidate = value.strip()
    # Accept a trailing Z for UTC, which datetime.fromisoformat handled only from
    # 3.11; normalise it so older interpreters behave the same.
    try:
        datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def content_hash_is_valid(value: Optional[str]) -> bool:
    """True if the value is a bounded hash token: no whitespace, control or
    injection characters, length 1-256. Format-lenient on purpose (bare hex,
    ``alg:hex`` and CID digests all pass); the RDF escaping prevents injection."""
    return isinstance(value, str) and bool(_CONTENT_HASH_RE.match(value))


def event_type_allowed(value: Optional[str]) -> bool:
    return value in ALLOWED_EVENT_TYPES


def validate_koi_event(event: dict) -> List[str]:
    """Return a list of problems with a KOI event (empty means valid).

    Checks the event_type allowlist, that event/bundle/manifest RIDs are valid
    and equal, that source_node is valid, that the timestamp is ISO-8601 and that
    the content hash (when a bundle is present) is hex. FORGET events carry no
    bundle, so only event.rid, source_node and timestamp are checked.
    """
    problems: List[str] = []

    event_type = event.get("event_type")
    if not event_type_allowed(event_type):
        problems.append(f"event_type {event_type!r} not allowed")

    rids = {"event.rid": event.get("rid")}
    bundle = event.get("bundle")
    bundle = bundle if isinstance(bundle, dict) else None
    if bundle is not None:
        rids["bundle.rid"] = bundle.get("rid")
        manifest = bundle.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        rids["manifest.rid"] = manifest.get("rid")

    for name, rid in rids.items():
        if not identifier_is_valid(rid):
            problems.append(f"{name} is not a valid identifier")

    present = [rid for rid in rids.values() if isinstance(rid, str)]
    if len(set(present)) > 1:
        problems.append("event.rid, bundle.rid and manifest.rid are not all equal")

    if not identifier_is_valid(event.get("source_node")):
        problems.append("source_node is not a valid identifier")

    if not timestamp_is_valid(event.get("timestamp")):
        problems.append("timestamp is not ISO-8601")

    if bundle is not None:
        manifest = bundle.get("manifest")
        manifest = manifest if isinstance(manifest, dict) else {}
        if not content_hash_is_valid(manifest.get("content_hash")):
            problems.append("content_hash is not a valid token")

    return problems


# --------------------------------------------------------------------------- #
# Privacy predicate (pure) - mirrors the bridge's own derivation
# --------------------------------------------------------------------------- #
def _meta_is_private(meta) -> bool:
    return isinstance(meta, dict) and (
        bool(meta.get("is_private")) or bool(meta.get("access_source"))
    )


def bundle_is_private(bundle: Optional[dict]) -> bool:
    """True if a bundle should be treated as private: an ``is_private`` truthy
    value or any ``access_source`` in the manifest metadata, the contents
    metadata, or the contents.document metadata. Errs toward private."""
    if not isinstance(bundle, dict):
        return False
    manifest = bundle.get("manifest")
    manifest = manifest if isinstance(manifest, dict) else {}
    contents = bundle.get("contents")
    contents = contents if isinstance(contents, dict) else {}
    document = contents.get("document")
    document = document if isinstance(document, dict) else {}
    return (
        _meta_is_private(manifest.get("metadata"))
        or _meta_is_private(contents.get("metadata"))
        or _meta_is_private(document.get("metadata"))
    )


# --------------------------------------------------------------------------- #
# Secret masking (pure)
# --------------------------------------------------------------------------- #
def mask_dsn(url: Optional[str]) -> str:
    """Return ``scheme://host:port`` for a URL-valued setting, dropping userinfo,
    path and query so a credential is never logged."""
    if not url:
        return "(unset)"
    try:
        parts = urlsplit(url)
        host = parts.hostname or ""
        netloc = f"{host}:{parts.port}" if parts.port else host
        return urlunsplit((parts.scheme, netloc, "", "", "")) or "(unparseable)"
    except Exception:
        return "(unparseable)"


# --------------------------------------------------------------------------- #
# FastAPI dependency (framework layer; FastAPI imported lazily)
# --------------------------------------------------------------------------- #
def make_service_key_dependency(
    service: str,
    server_keys_env: str = SERVER_KEYS_ENV,
) -> Callable:
    """Build a FastAPI dependency that enforces (or, in warn mode, logs) the
    service key. Reads env at request time so a restart can change mode and keys
    and tests can set them per case. FastAPI is imported lazily."""
    from fastapi import Header, HTTPException, Request  # noqa: WPS433 (lazy by design)

    async def _dependency(
        request: Request,
        x_koi_service_key: Optional[str] = Header(default=None, alias=SERVICE_KEY_HEADER),
    ) -> None:
        keys = load_service_keys(server_keys_env)
        if header_matches_a_key(x_koi_service_key, keys):
            return
        mode = auth_mode(service)
        user_agent = request.headers.get("user-agent", "")
        if mode == "warn":
            logger.warning(
                "[service-auth %s] WOULD REJECT (warn): method=%s path=%s ua=%r header_present=%s",
                service, request.method, request.url.path, user_agent,
                x_koi_service_key is not None,
            )
            return
        logger.warning(
            "[service-auth %s] REJECT: method=%s path=%s ua=%r header_present=%s",
            service, request.method, request.url.path, user_agent,
            x_koi_service_key is not None,
        )
        raise HTTPException(status_code=401, detail="missing or invalid service key")

    _dependency.koi_service = service  # marker for the route census
    return _dependency
