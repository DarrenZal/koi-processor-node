"""
KOI-net Protocol Models

Pydantic models matching the BlockScience wire format for KOI-net interoperability.
Uses rid-lib types where possible for JCS-canonical hashing.

Reference: BlockScience koi-net protocol/
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict


# =============================================================================
# Event Types
# =============================================================================

class EventType(StrEnum):
    NEW = "NEW"
    UPDATE = "UPDATE"
    FORGET = "FORGET"


# =============================================================================
# Wire Models (strict P1a/P1b format)
# =============================================================================

KOI_NET_PREFIX = "/koi-net"


def koi_net_url(base_url: str, path: str) -> str:
    """A peer's KOI-net endpoint URL, from the base_url it advertises (divergence 14).

    koi-net advertises the KOI-net API root (`http://host:8000/koi-net`) and joins paths
    straight onto it. Our nodes have advertised the host root (`http://host:8351`) and
    joined `/koi-net/...`. Both forms are accepted: a base_url whose path already ends in
    /koi-net is used as is, any other gets /koi-net appended. `path` is the KOI-net path,
    such as `/events/poll` (koi_net.protocol.api.paths).
    """
    base = base_url.rstrip("/")
    if not base.endswith(KOI_NET_PREFIX):
        base += KOI_NET_PREFIX
    return base + "/" + path.lstrip("/")


_LOOPBACK_NAMES = {"localhost", "127.0.0.1", "::1"}


def _url_key(url: str):
    """(scheme, host, port, path) with defaults filled and loopback names unified, and a
    trailing /koi-net dropped, so two spellings of one URL compare equal."""
    from urllib.parse import urlsplit

    import ipaddress
    import posixpath
    import socket

    parts = urlsplit(url.strip())
    host = (parts.hostname or "").lower().rstrip(".")
    addr = None
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        try:  # IPv4 shorthand a client still resolves: 127.1, 2130706433, 0x7f.1
            addr = ipaddress.ip_address(socket.inet_ntoa(socket.inet_aton(host)))
        except (OSError, ValueError):
            addr = None
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
        addr = addr.ipv4_mapped
    if host in _LOOPBACK_NAMES or (addr is not None and (addr.is_loopback or addr.is_unspecified)):
        host = "127.0.0.1"
    elif addr is not None:
        host = str(addr)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = posixpath.normpath(parts.path or "/").rstrip("/")
    path = "" if path in (".", "/") else path
    if path.endswith(KOI_NET_PREFIX):
        path = path[: -len(KOI_NET_PREFIX)].rstrip("/")
    return (parts.scheme, host, port, path)


def valid_peer_base_url(url: Any, own_base_url: Optional[str] = None) -> Optional[str]:
    """A peer's advertised base_url, if it is safe to send requests to, else None.

    It comes from a profile that /handshake accepts unsigned and that any key pair can
    bootstrap, and our node sends signed requests to it (first-contact pushes, poll, webhook
    push, key refresh). So: http or https, a host, no user info, query, fragment or
    parameters (a `#` or `?` would cut off koi_net_url's path and aim the request anywhere),
    no control, format or separator characters, a valid port, and not our own URL in any
    spelling. Hosts are not filtered by address: our peers sit on private and loopback
    addresses (WireGuard, the interop gate). Reviews of 2026-11-06.
    """
    import unicodedata
    from urllib.parse import urlsplit

    if not isinstance(url, str) or not url.strip():
        return None
    candidate = url.strip()
    if any(c in candidate for c in "?#;\\") or any(unicodedata.category(c)[0] in ("C", "Z") for c in candidate):
        return None
    try:
        parts = urlsplit(candidate)
        _ = parts.port  # raises on a malformed port
    except ValueError:
        return None
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password:
        return None
    if not parts.hostname.isascii():
        return None  # an IDN host could be rewritten into our own (e.g. full-width dots)
    if own_base_url:
        try:
            if _url_key(candidate) == _url_key(own_base_url):
                return None
        except ValueError:
            pass
    return candidate.rstrip("/")


def complete_manifest(manifest: Optional[Dict[str, Any]], rid: str, contents: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """A stored manifest with the three koi-net fields filled in: `rid` defaults to the event's,
    `timestamp` is put in Z form, and `sha256_hash` is derived from the contents (rid-lib's
    JCS hash) when absent. Used by both the poll reply and the webhook push (some sensors store
    manifests without these fields, and the push path used to drop them)."""
    if not manifest or not isinstance(manifest, dict):
        return None  # a peer may have stored anything
    from rid_lib.ext.utils import sha256_hash_json

    try:
        m = dict(manifest)
        m["rid"] = manifest.get("rid") or rid
        ts = manifest.get("timestamp")
        # A missing, null or empty timestamp gets the current time (a change on 2026-11-06
        # briefly dropped the manifest instead); a non-string one (an epoch number) is left
        # for rid-lib to parse.
        m["timestamp"] = timestamp_to_z_format(ts or "") if (ts is None or isinstance(ts, str)) else ts
        if not manifest.get("sha256_hash"):
            m["sha256_hash"] = sha256_hash_json(contents if contents is not None
                                                else {"rid": m["rid"], "timestamp": ts if ts is not None else ""})
        return m
    except Exception:  # never raise: the caller has already marked the row delivered
        return None


def koi_net_event(
    rid: str,
    event_type: str,
    manifest: Optional[Dict[str, Any]] = None,
    contents: Optional[Dict[str, Any]] = None,
    event_id: Optional[str] = None,
    extensions: bool = False,
) -> Optional[Dict[str, Any]]:
    """One event as we put it on the wire (divergences 16 and 24), built by koi-net itself.

    koi-net verifies a signature by re-serialising the payload it parsed with its own models
    (`model_dump_json(exclude_none=True)`), so the event is built with
    koi_net.protocol.event.Event and dumped the same way. That fixes the three things that
    broke signatures when this was a hand-built dict: our `event_id` extension (Event has no
    such field), manifest key order (koi_net_events stores manifests as jsonb, rid-lib's
    Manifest dumps its declared fields first and normalises the timestamp), and None-valued
    manifest extras (dropped by exclude_none on the model, not in a plain dict).

    Never raises (one bad stored row must not lose a poll batch). An event koi-net cannot
    represent (a RID it cannot parse, such as a task key without a colon; an unknown event
    type) returns None for a koi-net peer, which can never accept it. A peer running our code
    (`extensions`) gets it in our older raw form instead, with `event_id`, as before S4. A
    manifest rid-lib cannot parse is left out.
    """
    from pydantic import ValidationError
    from rid_lib.core import RID as _RID
    from rid_lib.ext import Manifest as _Manifest
    from koi_net.protocol.event import Event as _Event, EventType as _EventType

    parsed_manifest = None
    if manifest:
        try:
            parsed_manifest = _Manifest.model_validate(manifest)
        except (ValueError, TypeError, ValidationError):
            parsed_manifest = None
    try:
        wire = _Event(
            rid=_RID.from_string(rid), event_type=_EventType(event_type),
            manifest=parsed_manifest, contents=contents,
        ).model_dump(mode="json", exclude_none=True)
    except (ValueError, TypeError, ValidationError):
        if not extensions:
            return None
        raw: Dict[str, Any] = {"rid": rid, "event_type": event_type}
        if event_id:
            raw["event_id"] = event_id
        if manifest:
            raw["manifest"] = manifest
        if contents is not None:
            raw["contents"] = contents
        return raw
    if extensions and event_id:
        wire = {"rid": wire["rid"], "event_type": wire["event_type"], "event_id": event_id,
                **{k: v for k, v in wire.items() if k not in ("rid", "event_type")}}
    return wire


class WireManifest(BaseModel):
    """KOI-net wire manifest: core fields + passthrough for extension fields."""
    model_config = ConfigDict(extra="allow")

    rid: str
    timestamp: str  # ISO 8601 UTC with Z suffix
    sha256_hash: str  # JCS-canonical hash via rid-lib


class WireEvent(BaseModel):
    """Strict KOI-net wire event."""
    model_config = ConfigDict(extra="forbid")

    rid: str
    event_type: EventType
    event_id: Optional[str] = None
    manifest: Optional[WireManifest] = None
    contents: Optional[Dict[str, Any]] = None


# =============================================================================
# Node Capability Declaration
# =============================================================================

class NodeProvides(BaseModel):
    """What RID types this node broadcasts events for and serves state queries for."""
    event: List[str] = []   # RID types this node broadcasts events for
    state: List[str] = []   # RID types this node serves state queries for


class NodeProfile(BaseModel):
    """Node identity and capability declaration."""
    node_rid: str
    node_name: str
    node_type: str          # "FULL" or "PARTIAL"
    base_url: Optional[str] = None  # None for PARTIAL nodes
    provides: NodeProvides
    public_key: Optional[str] = None  # DER-encoded, base64
    encryption_key: Optional[str] = None  # X25519 public key, base64 (E2EE)
    ontology_uri: Optional[str] = None
    ontology_version: Optional[str] = None


# =============================================================================
# Our extensions' request and response models
# =============================================================================
# The five KOI-net protocol routes parse requests and build replies with koi-net's own
# models (koi_net.protocol.api.models), so they are not redefined here. What stays
# is our own: /handshake (fork 7) and /events/confirm (fork 16).

class HandshakeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["handshake"] = "handshake"
    profile: NodeProfile
    defer_approval: bool = False  # When True, inbound edge stays PROPOSED (for invite flow SAS verification)


class HandshakeResponse(BaseModel):
    type: Literal["handshake_response"] = "handshake_response"
    profile: NodeProfile
    accepted: bool
    edge_status: Optional[str] = None   # "APPROVED" or "PROPOSED" — inbound poll edge
    edge_rid: Optional[str] = None      # inbound poll edge RID for admin reference


class ConfirmEventsResponse(BaseModel):
    type: Literal["confirm_events_response"] = "confirm_events_response"
    confirmed: int


# =============================================================================
# Signed Envelope
# =============================================================================

class SignedEnvelope(BaseModel):
    """Signed envelope for wire transmission."""
    model_config = ConfigDict(exclude_none=True)

    payload: Dict[str, Any]
    source_node: str
    target_node: str
    signature: str


# =============================================================================
# Utilities
# =============================================================================

def timestamp_to_z_format(ts: str) -> str:
    """Convert timestamp to Z suffix format for KOI-net compatibility.

    Returns a valid ISO 8601 UTC timestamp with Z suffix. If the input is
    empty or falsy, returns the current UTC time (koi-net Manifest requires
    a valid datetime for the timestamp field).
    """
    if not ts:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if ts.endswith("+00:00"):
        return ts[:-6] + "Z"
    if "+00:00" in ts:
        return ts.replace("+00:00", "Z")
    return ts
