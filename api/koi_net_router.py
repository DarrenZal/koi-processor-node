"""
KOI-net Protocol Router

FastAPI APIRouter implementing the KOI-net protocol endpoints.
Mounted conditionally via feature flag (KOI_NET_ENABLED=true).

Endpoints:
    POST /koi-net/handshake           — Exchange NodeProfile, establish edges
    POST /koi-net/events/broadcast    — Receive events from peers
    POST /koi-net/events/poll         — Serve queued events to polling nodes
    POST /koi-net/events/confirm      — Acknowledge receipt of events
    POST /koi-net/manifests/fetch     — Serve manifests by RID
    POST /koi-net/bundles/fetch       — Serve bundles by RID
    POST /koi-net/rids/fetch          — List available RIDs
    GET  /koi-net/health              — Node identity and status
    GET  /koi-net/edges              — Active federation edges (dashboard)
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from api.koi_protocol import (
    ConfirmEventsResponse,
    EventType,
    HandshakeRequest,
    HandshakeResponse,
    complete_manifest,
    koi_net_event,
    koi_net_url,
    valid_peer_base_url,
    NodeProfile,
    WireManifest,
    timestamp_to_z_format,
)
from api.koi_poller import KOIPoller
from api.koi_envelope import (
    EnvelopeError,
    is_signed_envelope,
    load_public_key_from_der_b64,
    public_key_to_der_b64,
    sign_envelope,
    verify_envelope,
)
from api.node_identity import (
    derive_node_rid_hash,
    load_or_create_identity,
    node_rid_matches_public_key,
    node_rid_suffix,
)
from api.event_queue import EDGE_CONTEXT, NODE_CONTEXT, EventQueue, is_protocol_rid, lists_own_node, own_node_bundle
from api.koi_net_negotiation import Negotiator, first_contact_allowed
import api.koi_net_floor  # noqa: F401  (Python >= 3.12, before any koi_net import)
from koi_net.protocol.api.models import (  # the protocol routes' requests and replies
    BundlesPayload,
    EventsPayload,
    FetchBundles,
    FetchManifests,
    FetchRids,
    ManifestsPayload,
    RidsPayload,
)
from koi_net.protocol.event import Event as KoiNetEvent
from pydantic import ValidationError
from rid_lib.core import RID
from rid_lib.ext import Bundle, Manifest
from koi_net.protocol.api.paths import (
    BROADCAST_EVENTS_PATH,
    FETCH_BUNDLES_PATH,
    FETCH_MANIFESTS_PATH,
    FETCH_RIDS_PATH,
    POLL_EVENTS_PATH,
)
from api.federation_blocks import FederationBlocksUnavailable, blocked_hashes, is_blocked, rid_sha256
from api.vault_sync import VaultSyncManager, VaultUnavailableError

# RFC 8785 (JCS) canonicalization. Pinned at requirements.txt:91. Imported at
# module scope on purpose: a missing pinned dependency should fail loudly at
# startup rather than silently degrade the hash back to the non-JCS form this
# import replaced (see _jcs_sha256 below).
from rid_lib.ext.utils import sha256_hash_json as _rid_lib_sha256_hash_json

logger = logging.getLogger(__name__)

def _jcs_sha256(data) -> str:
    """JCS-canonical (RFC 8785) SHA-256 hash, via rid-lib.

    Previously this was inlined as ``json.dumps(sort_keys=True,
    separators=(",", ":"), ensure_ascii=False)`` with the comment "inlined from
    rid_lib to avoid dependency" — but rid-lib is a pinned, installed dependency
    (requirements.txt:91) and `koi_protocol.WireManifest` already documents this
    field as "JCS-canonical hash via rid-lib". The inlined form is not JCS:
    measured against the 448 coordinator newsletter bundles, it disagreed with
    the peer-computed `manifest.sha256_hash` on **336 of 448 (75%)**, while
    `rid_lib.ext.utils.sha256_hash_json` agreed on **448 of 448**.

    No code path in this repo verifies a *received* sha256_hash (grep for
    `sha256_hash` across api/ scripts/ shows emit sites only), so correcting the
    implementation changes what this node emits and breaks no comparison.
    """
    return _rid_lib_sha256_hash_json(data)

rid_sha256_hash_json = _jcs_sha256

koi_net_router = APIRouter(tags=["koi-net"])

# Module-level state (initialized in setup_koi_net)
_private_key = None
_encryption_private_key = None  # X25519 private key for E2EE
_node_profile: Optional[NodeProfile] = None
_event_queue: Optional[EventQueue] = None
_db_pool: Optional[asyncpg.Pool] = None
_poller: Optional[KOIPoller] = None
_vault_sync: Optional[VaultSyncManager] = None
_commons_ingest_worker = None  # Optional[CommonsIngestWorker]
_negotiator: Optional[Negotiator] = None  # first contact and edge negotiation (divergence 20)


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _security_policy() -> Dict[str, bool]:
    strict_mode = _bool_env("KOI_STRICT_MODE", False)
    require_signed = _bool_env("KOI_REQUIRE_SIGNED_ENVELOPES", strict_mode)
    enforce_target = _bool_env("KOI_ENFORCE_TARGET_MATCH", strict_mode)
    enforce_source_binding = _bool_env(
        "KOI_ENFORCE_SOURCE_KEY_RID_BINDING",
        strict_mode,
    )
    allow_legacy16 = _bool_env("KOI_ALLOW_LEGACY16_NODE_RID", True)
    allow_der64 = _bool_env("KOI_ALLOW_DER64_NODE_RID", True)
    allow_b64_64 = _bool_env("KOI_ALLOW_B64_64_NODE_RID", True)
    require_approved_edge_for_poll = _bool_env(
        "KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL", strict_mode,
    )
    return {
        "strict_mode": strict_mode,
        "require_signed": require_signed,
        "enforce_target": enforce_target,
        "enforce_source_binding": enforce_source_binding,
        "allow_legacy16": allow_legacy16,
        "allow_der64": allow_der64,
        "allow_b64_64": allow_b64_64,
        "require_approved_edge_for_poll": require_approved_edge_for_poll,
    }


# BlockScience ErrorType mapping (api_models.py:55-57)
_ERROR_TYPE_MAP = {
    "UNKNOWN_SOURCE_NODE": "unknown_node",
    "SOURCE_NODE_KEY_BINDING_FAILED": "invalid_key",
    "INVALID_SIGNATURE": "invalid_signature",
    "INVALID_SIGNATURE_FORMAT": "invalid_signature",
    "TARGET_NODE_MISMATCH": "invalid_target",
    "SOURCE_NODE_MISMATCH": "invalid_target",
    "INVALID_JSON": "invalid_signature",
    "INVALID_PAYLOAD": "invalid_signature",
    "UNSIGNED_ENVELOPE_REQUIRED": "invalid_signature",
    "MISSING_ENVELOPE_FIELDS": "invalid_signature",
    "ENVELOPE_ERROR": "invalid_signature",
    "CRYPTO_UNAVAILABLE": "invalid_signature",
    # UNAPPROVED_EDGE and IDENTITY_REQUIRED (membrane gate refusals) deliberately fall
    # through to the default. koi-net 2.0.x clients re-handshake on "unknown_node", and the
    # handshake broadcast meets the same gate, so mapping them there loops. See divergence
    # 18 in docs/architecture/koi-protocol-conformance.md.
}

_DEFAULT_ERROR_TYPE = "invalid_signature"


def _protocol_error(
    status_code: int,
    code: str,
    message: str,
    **extra: Any,
) -> JSONResponse:
    error_type = _ERROR_TYPE_MAP.get(code, _DEFAULT_ERROR_TYPE)
    body = {
        "type": "error_response",
        "error": error_type,
        "error_code": code,
        "message": message,
    }
    if extra:
        body.update(extra)
    return JSONResponse(status_code=status_code, content=body)


def _requested_rids(payload: Any) -> Optional[List[str]]:
    """The request's `rids`, or None when they are not a list of strings."""
    if not payload:
        return []
    if not isinstance(payload, dict):
        return None
    rids = payload.get("rids", [])
    if not isinstance(rids, list) or not all(isinstance(rid, str) for rid in rids):
        return None
    return rids


def _koi_net_request(model, payload: Any):
    """The request as koi-net's own model parses it, or None when it does not: a
    RID string rid-lib cannot parse, a missing required field, a wrong `type`. koi-net's server
    answers such a request 422; we answer 400 INVALID_PAYLOAD. A null field reads as absent here
    (so `rid_types: null` lists everything); the fetch routes refuse `rids: null` before this,
    in _requested_rids."""
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        return None
    try:
        return model.model_validate({k: v for k, v in payload.items() if v is not None})
    except (ValidationError, ValueError, TypeError):
        return None


def _typed(model, value: Any):
    """`value` as a rid-lib or koi-net model, or None when it does not validate."""
    try:
        return model.model_validate(value)
    except (ValidationError, ValueError, TypeError):
        return None


def _rid_or_none(value: Any) -> Optional[RID]:
    try:
        return RID.from_string(value)
    except (ValueError, TypeError):
        return None


def _stored_manifest_fields(manifest: Any, rid: str, contents: Any) -> Optional[Dict[str, Any]]:
    """A stored event's manifest as the three koi-net fields, or None when whatever a peer
    stored cannot give them (a non-dict manifest, a non-string timestamp): such a record then
    reads as not found instead of failing the request."""
    try:
        if isinstance(manifest, str):
            manifest = json.loads(manifest)
        if not isinstance(manifest, dict):
            return None
        timestamp = manifest.get("timestamp", "")
        if not isinstance(timestamp, str):
            return None
        return {
            "rid": manifest.get("rid", rid),
            "timestamp": timestamp_to_z_format(timestamp),
            "sha256_hash": _manifest_sha256_hash(manifest, contents),
        }
    except Exception:
        return None


def _koi_net_reply(model_instance) -> Dict[str, Any]:
    """A reply built by koi-net's model and dumped as koi-net re-serialises it to verify a
    signature (model_dump_json(exclude_none=True))."""
    return model_instance.model_dump(mode="json", exclude_none=True)


def _envelope_error_response(exc: EnvelopeError) -> JSONResponse:
    code = getattr(exc, "code", "ENVELOPE_ERROR")
    status_code = 400
    if code in {"INVALID_SIGNATURE", "UNSIGNED_ENVELOPE_REQUIRED"}:
        status_code = 401
    elif code == "NODE_REJECTED":
        status_code = 403
    return _protocol_error(status_code, code, str(exc))


# Barred nodes and keys (register 26): api/koi_net_barred.py, shared with the negotiator and
# the poller. Thin aliases keep the call sites here readable.
from api.koi_net_barred import BARRED_NODE_STATUSES, canonical_key_b64 as _canonical_key_b64  # noqa: E402
from api.koi_net_barred import key_is_barred as _key_is_barred, node_is_barred as _node_is_barred  # noqa: E402


def _canonical_sha256_json(data: Any) -> str:
    """Compute JCS-canonical sha256 hash using rid-lib."""
    return rid_sha256_hash_json(data)


def _manifest_sha256_hash(manifest: Dict[str, Any], contents: Optional[Dict[str, Any]] = None) -> str:
    """Return manifest sha256 hash, deriving a canonical value when absent."""
    existing = manifest.get("sha256_hash")
    if existing:
        return existing
    if contents is not None:
        return _canonical_sha256_json(contents)
    return _canonical_sha256_json(
        {
            "rid": manifest.get("rid", ""),
            "timestamp": manifest.get("timestamp", ""),
        }
    )


async def setup_koi_net(pool: asyncpg.Pool, embed_fn=None):
    """Initialize KOI-net subsystem. Called from app startup."""
    global _private_key, _encryption_private_key, _node_profile, _event_queue, _db_pool, _poller, _vault_sync, _commons_ingest_worker, _negotiator
    _db_pool = pool

    node_name = os.getenv("KOI_NODE_NAME", "darren-personal")
    base_url = os.getenv("KOI_BASE_URL")  # e.g. http://127.0.0.1:8351

    _private_key, _node_profile, _encryption_private_key = load_or_create_identity(
        node_name=node_name,
        base_url=base_url,
        node_type="FULL",
    )
    _node_profile.ontology_uri = os.getenv(
        "KOI_ONTOLOGY_URI", "http://bkc.regen.network/ontology"
    )
    _node_profile.ontology_version = os.getenv("KOI_ONTOLOGY_VERSION", "1.0.0")
    _event_queue = EventQueue(pool, _node_profile.node_rid)
    _negotiator = Negotiator(
        pool=pool, own_rid=_node_profile.node_rid, own_profile=_node_profile,
        private_key=_private_key, event_queue=_event_queue,
        own_bundle=lambda: own_node_bundle(_node_profile),
        membrane_on=lambda: bool(_security_policy()["require_approved_edge_for_poll"]),
        defer_new_edges=lambda: _bool_env("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", True),
    )

    # Pipeline integration is not available in personal-koi
    pipeline = None
    use_pipeline = False

    # Start background poller
    _poller = KOIPoller(
        pool=pool,
        node_rid=_node_profile.node_rid,
        private_key=_private_key,
        node_profile=_node_profile,
        pipeline=pipeline,
        use_pipeline=use_pipeline,
        event_queue=_event_queue,
    )
    await _poller.start()

    # Initialize vault sync if enabled
    vault_sync_enabled = _bool_env("VAULT_SYNC_ENABLED", False)
    vault_path = os.getenv("OBSIDIAN_VAULT_PATH", "~/Documents/Notes")
    if vault_sync_enabled and vault_path:
        _vault_sync = VaultSyncManager(
            pool=pool,
            node_rid=_node_profile.node_rid,
            event_queue=_event_queue,
            vault_path=vault_path,
            encryption_private_key=_encryption_private_key,
        )
        _poller.vault_sync = _vault_sync
        await _vault_sync.load_metrics()
        _vault_sync.start_watcher()

        # Persistent peer-folder safety check for KOI_VAULT_MIRROR_PATHS:
        # if any peer other than the home node is subscribed to a top-level folder we mirror,
        # log a single ERROR. Operator removes the peer manually (we don't
        # auto-revoke). Catches drift over time vs the one-shot pre-flight gate.
        from api.vault_sync import _VAULT_MIRROR_PATTERNS as _vmp
        if _vmp:
            try:
                mirror_top_folders = set()
                for pat in _vmp:
                    top = pat.strip("/").split("/", 1)[0]
                    if top:
                        mirror_top_folders.add(top)
                async with pool.acquire() as _conn:
                    rows = await _conn.fetch(
                        "SELECT peer_node_rid, shared_folder FROM vault_sync_peers "
                        "WHERE shared_folder = ANY($1::text[]) "
                        "AND peer_node_rid NOT LIKE 'orn:koi-net.node:darren-personal+%'",
                        list(mirror_top_folders),
                    )
                if rows:
                    for r in rows:
                        logger.error(
                            "vault_sync.unsafe_mirror_peer_subscription "
                            "peer=%s folder=%s — non-MacBook peer subscribed to a "
                            "mirrored folder; mirror mode will accept their writes "
                            "unconditionally. Remove this subscription if unintended.",
                            r["peer_node_rid"], r["shared_folder"],
                        )
            except Exception as e:
                logger.warning("vault_sync.mirror_peer_check_failed reason=%s", e)
        # Wire the trigger endpoint to this manager (was wired via personal.py before,
        # but that path is broken — VaultSyncManager constructor requires node_rid etc.)
        try:
            from api.routers.vault_sync_router import set_vault_sync_manager, _rebuild_pageindex_bg
            set_vault_sync_manager(_vault_sync)
            _vault_sync.post_cycle_hooks.append(_rebuild_pageindex_bg)
        except Exception:
            logger.warning("vault_sync: could not wire trigger endpoint or pageindex hook")
        logger.info(f"Vault sync enabled (vault={vault_path}, e2ee={'yes' if _encryption_private_key else 'no'})")

    policy = _security_policy()
    logger.info(
        "KOI-net validation policy: strict_mode=%s require_signed=%s "
        "enforce_target=%s enforce_source_binding=%s allow_legacy16=%s "
        "allow_der64=%s allow_b64_64=%s require_approved_edge_for_poll=%s",
        policy["strict_mode"],
        policy["require_signed"],
        policy["enforce_target"],
        policy["enforce_source_binding"],
        policy["allow_legacy16"],
        policy["allow_der64"],
        policy["allow_b64_64"],
        policy["require_approved_edge_for_poll"],
    )
    # Start commons ingest worker (for BKC profiles with commons intake)
    commons_ingest_enabled = _bool_env("COMMONS_INGEST_ENABLED", False)
    if commons_ingest_enabled:
        try:
            from api.commons_ingest_worker import CommonsIngestWorker
            _commons_ingest_worker = CommonsIngestWorker(pool)
            await _commons_ingest_worker.start()
            logger.info("Commons ingest worker started")
        except Exception as e:
            logger.warning(f"Commons ingest worker failed to start: {e}")

    logger.info(f"KOI-net initialized: {_node_profile.node_rid}")


async def shutdown_koi_net():
    """Stop poller and clean up. Called from app shutdown."""
    global _poller, _commons_ingest_worker
    logger.info("shutdown_koi_net: stopping background tasks")
    # Stop poller FIRST to prevent new poll/vault-sync cycles during cleanup
    if _poller:
        try:
            await asyncio.wait_for(_poller.stop(), timeout=5)
        except asyncio.TimeoutError:
            logger.warning("shutdown_koi_net: poller stop timed out after 5s")
        _poller = None
    if _commons_ingest_worker:
        await _commons_ingest_worker.stop()
        _commons_ingest_worker = None
    if _vault_sync:
        # Run observer.join in executor to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _vault_sync.stop_watcher)
        await _vault_sync.persist_metrics()
    logger.info("shutdown_koi_net: complete")


# =============================================================================
# Envelope helpers
# =============================================================================

async def _get_peer_key_record(source_node: str):
    """Look up a peer key record from koi_net_nodes."""
    if not _db_pool:
        return None
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT public_key FROM koi_net_nodes WHERE node_rid = $1",
            source_node,
        )
        if row and row["public_key"]:
            return {
                "der_b64": row["public_key"],
                "public_key": load_public_key_from_der_b64(row["public_key"]),
            }
    return None


async def _refresh_peer_public_key(source_node: str):
    """Best-effort key refresh from peer /koi-net/health when key is missing."""
    if not _db_pool:
        return None

    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT base_url FROM koi_net_nodes WHERE node_rid = $1",
            source_node,
        )
    base_url = valid_peer_base_url(row["base_url"] if row else None,
                                   _node_profile.base_url if _node_profile else None)
    if not base_url:
        return None
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(koi_net_url(base_url, "/health"))
        if resp.status_code != 200:
            return None
        health = resp.json()
    except Exception:
        return None

    node = health.get("node") if isinstance(health, dict) else None
    if not isinstance(node, dict):
        return None

    detected_rid = node.get("node_rid")
    if detected_rid and detected_rid != source_node:
        logger.warning(
            f"Peer health RID mismatch for {base_url}: expected {source_node}, got {detected_rid}"
        )
        return None

    public_key = node.get("public_key")
    if not public_key:
        return None
    # The key served at the stored URL must be the one the RID is bound to; the URL may point
    # anywhere (a stranger could otherwise serve its own key for someone else's RID).
    try:
        served_key = load_public_key_from_der_b64(public_key)
    except Exception:
        served_key = None
    policy = _security_policy()
    if served_key is None or not node_rid_matches_public_key(
        source_node, served_key, allow_legacy16=policy["allow_legacy16"],
        allow_der64=policy["allow_der64"], allow_b64_64=policy["allow_b64_64"],
    ):
        logger.warning(f"Key refresh refused for {source_node}: the key at {base_url} is not bound to its RID")
        return None

    node_name = node.get("node_name") or source_node
    node_type = node.get("node_type") or "FULL"
    ontology_uri = node.get("ontology_uri")
    ontology_version = node.get("ontology_version")
    async with _db_pool.acquire() as conn:
        if await _node_is_barred(conn, source_node) or await _key_is_barred(conn, public_key):
            logger.warning("Key refresh refused: %s or its key was rejected by an operator", source_node)
            return None
        # Key pinning: check for mismatch before upsert
        existing_key = await conn.fetchval(
            "SELECT public_key FROM koi_net_nodes WHERE node_rid = $1",
            source_node,
        )
        if existing_key and existing_key != public_key:
            logger.warning(
                "KEY MISMATCH during key refresh for %s — "
                "pinned key starts with: %s... refreshed key starts with: %s... "
                "Refusing to update. Manual re-approval required.",
                source_node, existing_key[:20], public_key[:20],
            )
            return None

        await conn.execute(
            """
            INSERT INTO koi_net_nodes
                (node_rid, node_name, node_type, base_url, public_key,
                 ontology_uri, ontology_version, status, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', NOW())
            ON CONFLICT (node_rid) DO UPDATE SET
                -- a keyless row takes the first key it is given and keeps it
                public_key = COALESCE(koi_net_nodes.public_key, EXCLUDED.public_key),
                node_name = EXCLUDED.node_name,
                node_type = EXCLUDED.node_type,
                base_url = EXCLUDED.base_url,
                ontology_uri = COALESCE(EXCLUDED.ontology_uri, koi_net_nodes.ontology_uri),
                ontology_version = COALESCE(EXCLUDED.ontology_version, koi_net_nodes.ontology_version),
                -- never reactivate a node an operator rejected or revoked (register 26)
                status = CASE WHEN koi_net_nodes.status IN ('rejected', 'revoked') THEN koi_net_nodes.status ELSE 'active' END,
                last_seen = NOW()
            """,
            source_node,
            node_name,
            node_type,
            base_url,
            public_key,
            ontology_uri,
            ontology_version,
        )

    logger.info(f"Refreshed public key for {source_node} from {koi_net_url(base_url, '/health')}")
    return {
        "der_b64": public_key,
        "public_key": load_public_key_from_der_b64(public_key),
    }


def _extract_bootstrap_key(
    body: Dict[str, Any], source_node: str
) -> Optional[Dict[str, Any]]:
    """Extract and validate a bootstrap key from a broadcast payload."""
    payload = body.get("payload")
    if not isinstance(payload, dict):
        return None

    events = payload.get("events")
    if not isinstance(events, list):
        return None

    for ev in events:
        if not isinstance(ev, dict):
            continue
        if ev.get("event_type") != "NEW":
            continue
        if ev.get("rid") != source_node:
            continue
        contents = ev.get("contents")
        if not isinstance(contents, dict):
            continue
        public_key_b64 = contents.get("public_key")
        if not public_key_b64:
            continue

        try:
            pub_key = load_public_key_from_der_b64(public_key_b64)
        except Exception:
            logger.warning(
                f"Bootstrap: invalid public key in NEW event for {source_node}"
            )
            return None

        suffix = node_rid_suffix(source_node)
        if not suffix:
            return None

        b64_hash = derive_node_rid_hash(pub_key, "b64_64")
        legacy_hash = derive_node_rid_hash(pub_key, "legacy16")
        if suffix != b64_hash and suffix != legacy_hash:
            logger.warning(
                f"Bootstrap: RID hash mismatch for {source_node} — "
                f"expected {b64_hash[:16]}... or {legacy_hash}, got {suffix}"
            )
            return None

        return {
            "der_b64": public_key_b64,
            "public_key": pub_key,
            "bootstrap_contents": contents,
        }

    return None


async def _persist_bootstrap_peer(
    source_node: str, key_record: Dict[str, Any]
) -> bool:
    """Persist a bootstrapped peer to koi_net_nodes after signature verification. Returns
    False when the node or its key is barred, and the request must then be refused."""
    contents = key_record.get("bootstrap_contents")
    if not contents or not _db_pool:
        return True

    node_name = contents.get("node_name") or source_node
    node_type = contents.get("node_type") or "FULL"
    # Not stored unless safe to call: any key pair can bootstrap.
    base_url = valid_peer_base_url(contents.get("base_url"), _node_profile.base_url if _node_profile else None)
    public_key_b64 = key_record["der_b64"]
    ontology_uri = contents.get("ontology_uri")
    ontology_version = contents.get("ontology_version")

    async with _db_pool.acquire() as conn:
        # A node or key an operator rejected is not registered again under another RID.
        if await _node_is_barred(conn, source_node) or await _key_is_barred(conn, public_key_b64):
            logger.warning("Bootstrap refused: %s or its public key was rejected by an operator", source_node)
            return False
        # Key pinning: check for mismatch before bootstrap upsert
        existing_key = await conn.fetchval(
            "SELECT public_key FROM koi_net_nodes WHERE node_rid = $1",
            source_node,
        )
        if existing_key and existing_key != public_key_b64:
            logger.warning(
                "KEY MISMATCH during bootstrap for %s — "
                "pinned key starts with: %s... bootstrap key starts with: %s... "
                "Refusing to update. Manual re-approval required.",
                source_node, existing_key[:20], public_key_b64[:20],
            )
            return True

        await conn.execute(
            """
            INSERT INTO koi_net_nodes
                (node_rid, node_name, node_type, base_url,
                 public_key, ontology_uri, ontology_version,
                 status, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', NOW())
            ON CONFLICT (node_rid) DO UPDATE SET
                -- a keyless row takes the first key it is given and keeps it
                public_key = COALESCE(koi_net_nodes.public_key, EXCLUDED.public_key),
                node_name = EXCLUDED.node_name,
                node_type = EXCLUDED.node_type,
                base_url = EXCLUDED.base_url,
                ontology_uri = COALESCE(EXCLUDED.ontology_uri, koi_net_nodes.ontology_uri),
                ontology_version = COALESCE(EXCLUDED.ontology_version, koi_net_nodes.ontology_version),
                -- never reactivate a node an operator rejected or revoked (register 26)
                status = CASE WHEN koi_net_nodes.status IN ('rejected', 'revoked') THEN koi_net_nodes.status ELSE 'active' END,
                last_seen = NOW()
            """,
            source_node,
            node_name,
            node_type,
            base_url,
            public_key_b64,
            ontology_uri,
            ontology_version,
        )
    logger.info(
        f"Bootstrap: registered new peer {source_node} ({node_name}) "
        f"via broadcast NEW event"
    )
    return True


def _handshake_profile_key(body: Dict[str, Any], source_node: str) -> Optional[Dict[str, Any]]:
    """The key a signed handshake's own profile carries, used to verify that handshake when we
    hold no key for its sender: a new node, or an existing keyless row. Accepted only when the
    sender's RID is bound to it, as for a bootstrap key, so the signature proves the sender
    holds the RID's key. Nothing is written here: /handshake writes the row after its own
    checks."""
    payload = body.get("payload")
    profile = payload.get("profile") if isinstance(payload, dict) else None
    if not isinstance(profile, dict) or profile.get("node_rid") != source_node or not profile.get("public_key"):
        return None
    try:
        key = load_public_key_from_der_b64(profile["public_key"])
    except Exception:
        return None
    policy = _security_policy()
    if key is None or not node_rid_matches_public_key(
        source_node, key, allow_legacy16=policy["allow_legacy16"],
        allow_der64=policy["allow_der64"], allow_b64_64=policy["allow_b64_64"],
    ):
        return None
    return {"der_b64": profile["public_key"], "public_key": key}


async def _unwrap_request(request: Request, allow_unsigned: bool = False):
    """Parse request body, optionally unwrapping SignedEnvelope.

    Returns (payload_dict, source_node_or_none, was_signed).
    """
    try:
        body = await request.json()
    except Exception as exc:
        raise EnvelopeError("Invalid JSON payload", code="INVALID_JSON") from exc

    if not isinstance(body, dict):
        raise EnvelopeError("Payload must be a JSON object", code="INVALID_PAYLOAD")

    policy = _security_policy()
    if not is_signed_envelope(body):
        if policy["require_signed"] and not allow_unsigned:
            raise EnvelopeError(
                "Signed envelope required by KOI policy",
                code="UNSIGNED_ENVELOPE_REQUIRED",
            )
        return body, None, False

    source_node = body.get("source_node")
    target_node = body.get("target_node")
    if not source_node:
        raise EnvelopeError("Signed envelope missing source_node", code="MISSING_ENVELOPE_FIELDS")

    if policy["enforce_target"] and _node_profile and target_node != _node_profile.node_rid:
        raise EnvelopeError(
            f"Envelope target_node mismatch: expected {_node_profile.node_rid}, got {target_node}",
            code="TARGET_NODE_MISMATCH",
        )

    key_record = await _get_peer_key_record(source_node)
    if not key_record:
        key_record = await _refresh_peer_public_key(source_node)
    is_bootstrap = False
    if not key_record or not key_record.get("public_key"):
        key_record = _extract_bootstrap_key(body, source_node)
        is_bootstrap = True
    if (not key_record or not key_record.get("public_key")) and allow_unsigned:
        # Only /handshake passes allow_unsigned. Its profile's key verifies it; not persisted.
        key_record = _handshake_profile_key(body, source_node)
        is_bootstrap = False
    if not key_record or not key_record.get("public_key"):
        raise EnvelopeError(f"No public key for {source_node}", code="UNKNOWN_SOURCE_NODE")

    if policy["enforce_source_binding"]:
        bound = node_rid_matches_public_key(
            source_node,
            key_record["public_key"],
            allow_legacy16=policy["allow_legacy16"],
            allow_der64=policy["allow_der64"],
            allow_b64_64=policy["allow_b64_64"],
        )
        if not bound:
            raise EnvelopeError(
                f"Source node RID does not match stored public key: {source_node}",
                code="SOURCE_NODE_KEY_BINDING_FAILED",
            )

    payload, source = verify_envelope(
        body,
        key_record["public_key"],
        expected_target_node=_node_profile.node_rid
        if policy["enforce_target"] and _node_profile
        else None,
    )

    if is_bootstrap and not await _persist_bootstrap_peer(source_node, key_record):
        raise EnvelopeError(f"{source_node} or its key was rejected by an operator", code="NODE_REJECTED")
    # A rejected or revoked node (or one holding a barred key) is refused on every signed
    # request, with the membrane on or off.
    if _db_pool and source:
        async with _db_pool.acquire() as conn:
            if await _node_is_barred(conn, source):
                raise EnvelopeError(f"{source} was rejected by an operator", code="NODE_REJECTED")

    return payload, source, True


def _wrap_response(payload: Dict[str, Any], target_node: Optional[str], signed: bool):
    """Optionally sign a response payload."""
    if signed and _private_key and target_node and _node_profile:
        return sign_envelope(payload, _node_profile.node_rid, target_node, _private_key)
    return payload


def _stored_base_url(url) -> Optional[str]:
    """A peer URL as /handshake stores it (valid_peer_base_url), for comparisons."""
    return valid_peer_base_url(url, _node_profile.base_url if _node_profile else None)


HANDSHAKE_WINDOW_SECONDS = 600  # issued_at within ±10 minutes of now


def _handshake_issued_at(payload: Any):
    """(issued_at as an aware UTC datetime or None, error text or None) from a handshake's
    profile. Absent is (None, None); present but unparseable is (None, reason)."""
    raw = (payload or {}).get("profile", {}).get("issued_at") if isinstance(payload, dict) else None
    if raw is None:
        return None, None
    if not isinstance(raw, str):
        return None, "issued_at is not a string"
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None, "issued_at is not ISO-8601"
    if parsed.tzinfo is None:
        return None, "issued_at has no time zone"
    return parsed.astimezone(timezone.utc), None


def _issued_at_in_window(issued_at) -> bool:
    return abs((datetime.now(timezone.utc) - issued_at).total_seconds()) <= HANDSHAKE_WINDOW_SECONDS


def _stale_handshake_reason(issued_at, issued_at_err, stored: Optional[str]) -> Optional[str]:
    """Why a signed handshake that would change a peer's row is refused, or None to accept it.

    With a stored value, the handshake must carry a newer issued_at, so a replay of an older
    one (including one from before issued_at existed) is refused. Without one, a handshake
    without issued_at is accepted as before; with one, it must be inside the window."""
    if issued_at_err:
        return issued_at_err
    if issued_at is not None and not _issued_at_in_window(issued_at):
        skew = abs((datetime.now(timezone.utc) - issued_at).total_seconds())
        return f"issued_at is {int(skew)}s from now (window {HANDSHAKE_WINDOW_SECONDS}s)"
    if stored:
        if issued_at is None:
            return "no issued_at, and a newer signed handshake was already accepted"
        try:
            last = datetime.fromisoformat(stored.replace("Z", "+00:00"))
        except ValueError:
            return None  # an unreadable stored value does not lock the peer out; it is overwritten
        if issued_at <= last:
            return "issued_at is not newer than the last accepted handshake"
    return None


def _handshake_changes_peer(known, peer) -> bool:
    """True if applying this handshake would alter the stored record of a known peer."""
    return (
        known["status"] != "active"
        or peer.node_name != known["node_name"]
        or peer.node_type != known["node_type"]
        # Compared as stored: the URL is normalised on the way in (valid_peer_base_url), so a raw
        # comparison would read a trailing slash as a change and refuse an unsigned re-handshake.
        or _stored_base_url(peer.base_url) != _stored_base_url(known["base_url"])
        or (peer.encryption_key is not None and peer.encryption_key != known["encryption_key"])
        or (peer.ontology_uri is not None and peer.ontology_uri != known["ontology_uri"])
        or (peer.ontology_version is not None and peer.ontology_version != known["ontology_version"])
        or sorted(peer.provides.event or []) != sorted(known["provides_event"] or [])
        or sorted(peer.provides.state or []) != sorted(known["provides_state"] or [])
    )


# The admin rule lives in api/auth.py, shared with the routers that use api.auth directly.
from api.auth import (  # noqa: E402
    bearer_matches as _bearer_matches,
    is_loopback_client as _is_loopback_client,
    local_admin_refusal as _local_admin_refusal,
    read_admin_token as _read_admin_token,
)


def _enforce_local_admin(request: Request) -> Optional[JSONResponse]:
    """None for a loopback caller presenting the configured admin bearer, else a refusal.

    Fails closed (api.auth.local_admin_refusal, ported 2026-11-06 from the federation security
    lane's guard, so both lines share one rule): with no admin token configured, every caller
    is refused with 503, loopback included. Loopback alone is not authority; PR #15's "local
    operator" check (`_enforce_local_admin(request) is None`) inherits this.
    """
    refusal = _local_admin_refusal(request)
    return None if refusal is None else _protocol_error(*refusal)


def _enforce_commons_admin(request: Request) -> Optional[JSONResponse]:
    """Return protocol error if request is not authorized for commons admin endpoints.

    Accepts:
    - a valid KOI_COMMONS_SERVICE_TOKEN bearer (for BFF access from a remote dashboard);
    - a loopback caller presenting the admin bearer, as _enforce_local_admin, failing closed
      when no admin token is configured.

    This guard is ONLY for /koi-net/commons/* endpoints. All other admin
    endpoints continue using _enforce_local_admin (localhost-only).
    """
    if _bearer_matches(request, os.getenv("KOI_COMMONS_SERVICE_TOKEN")):
        return None
    refusal = _local_admin_refusal(request, "Endpoint requires localhost or valid commons service token")
    return None if refusal is None else _protocol_error(*refusal)


# =============================================================================
# Membrane Governance Helpers
# =============================================================================

async def _check_approved_edge(source_node: str, direction: str = "any") -> bool:
    """Return True if source_node has an APPROVED edge relationship with us.

    direction="poll": we serve the caller (source_node=us, target_node=caller), the edge
    /events/poll checks and /edges/approve opens — for fetch gating
    direction="any": any approved edge in either direction — for broadcast/confirm gating
    """
    if not _db_pool or not _node_profile:
        return False
    async with _db_pool.acquire() as conn:
        if direction == "poll":
            edge = await conn.fetchrow(
                "SELECT 1 FROM koi_net_edges WHERE source_node = $2 AND target_node = $1 AND status = 'APPROVED'",
                source_node, _node_profile.node_rid,
            )
        else:  # "any" — either direction
            edge = await conn.fetchrow(
                """SELECT 1 FROM koi_net_edges
                   WHERE status = 'APPROVED'
                   AND ((source_node = $1 AND target_node = $2) OR (source_node = $2 AND target_node = $1))""",
                source_node, _node_profile.node_rid,
            )
        # A rejected or revoked node has no approved edge, whatever the edge table says.
        return edge is not None and not await _node_is_barred(conn, source_node)


# =============================================================================
# Endpoints
# =============================================================================

@koi_net_router.post("/handshake")
async def handshake(request: Request):
    """Exchange NodeProfile and establish edges."""
    try:
        payload, source_node, signed = await _unwrap_request(request, allow_unsigned=True)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    if not payload or "profile" not in payload:
        return _protocol_error(400, "MISSING_PROFILE", "Missing profile")

    try:
        req = HandshakeRequest(**payload)
    except Exception as exc:
        return _protocol_error(400, "INVALID_HANDSHAKE", f"Invalid handshake: {exc}")

    peer = req.profile

    # Store/update peer in koi_net_nodes
    is_local_operator = _enforce_local_admin(request) is None
    async with _db_pool.acquire() as conn:
        known = await conn.fetchrow(
            "SELECT node_name, node_type, base_url, public_key, encryption_key, ontology_uri, "
            "ontology_version, provides_event, provides_state, status, "
            "metadata->>'handshake_issued_at' AS handshake_issued_at "
            "FROM koi_net_nodes WHERE node_rid = $1",
            peer.node_rid,
        )
        # A node an operator rejected or revoked stays out, whoever signs, and so does its key
        # under any other RID, hash suffix or encoding (register 26).
        if known and known["status"] in BARRED_NODE_STATUSES:
            return _protocol_error(403, "NODE_REJECTED", f"{peer.node_rid} was {known['status']}; "
                                   "an operator must re-admit it (POST /koi-net/nodes/readmit)")
        # A row registered before its key was barred under another RID.
        if known and await _node_is_barred(conn, peer.node_rid):
            return _protocol_error(403, "NODE_REJECTED", "This node's public key belongs to a node an operator rejected")
        if peer.public_key and await _key_is_barred(conn, peer.public_key):
            return _protocol_error(403, "NODE_REJECTED", "This public key belongs to a node an operator rejected")
        # A node seen for the first time must present a key its RID is bound to. Otherwise it
        # would leave a keyless row whose key a later request fetches from wherever the stranger
        # points base_url.
        if not known or not known["public_key"]:
            # Also for an existing keyless row: it would otherwise be anyone's to claim, and a
            # later request would take its key from wherever base_url points.
            policy = _security_policy()
            try:
                new_key = load_public_key_from_der_b64(peer.public_key) if peer.public_key else None
            except Exception:
                return _protocol_error(400, "INVALID_PUBLIC_KEY", "The profile's public key does not parse")
            if new_key is None:
                return _protocol_error(400, "MISSING_PUBLIC_KEY", "A first handshake must carry the node's public key")
            if not node_rid_matches_public_key(
                peer.node_rid, new_key, allow_legacy16=policy["allow_legacy16"],
                allow_der64=policy["allow_der64"], allow_b64_64=policy["allow_b64_64"],
            ):
                return _protocol_error(403, "SOURCE_NODE_KEY_BINDING_FAILED",
                                       f"{peer.node_rid} is not bound to the profile's public key")
        # Unsigned handshakes are for first contact. A known peer with a pinned key changes only
        # under that key's signature or from the local operator; presenting the matching public
        # key proves nothing, because the key is public. An unsigned handshake that changes
        # nothing stays harmless, so re-running registration and creating missing edges still
        # work. A mismatched key is refused just below as KEY_MISMATCH.
        # An existing keyless row is no exception: its bound
        # key is public, so presenting it unsigned proves nothing, and the row may already hold
        # an APPROVED edge that vault sync encrypts to. Unchanged, it still takes that key.
        may_update = (
            not known
            or (signed and source_node == peer.node_rid)
            or is_local_operator
        )
        if (
            not may_update
            and not (known["public_key"] and peer.public_key and peer.public_key != known["public_key"])
            and _handshake_changes_peer(known, peer)
        ):
            return _protocol_error(
                401, "SIGNED_HANDSHAKE_REQUIRED",
                f"Changing {peer.node_rid} requires an envelope signed by its pinned key "
                "or the local operator",
            )

        # Replay of a signed handshake (register 26). A signature
        # proves who signed, not when, so an old signed handshake could roll a peer's base_url
        # or encryption_key back. The signed profile carries issued_at (inside the profile, which
        # every version of ours ignores when unknown; HandshakeRequest itself forbids extra
        # fields). A signed handshake that would change the row must be newer than the last one
        # accepted and within the window. Unsigned, unchanged and operator handshakes are as before.
        issued_at, issued_at_err = _handshake_issued_at(payload)
        self_signed = signed and source_node == peer.node_rid and not is_local_operator
        changes_peer = bool(known) and _handshake_changes_peer(known, peer)
        if self_signed and changes_peer:
            stale = _stale_handshake_reason(issued_at, issued_at_err, known["handshake_issued_at"])
            if stale:
                return _protocol_error(409, "STALE_HANDSHAKE", f"Refusing a replayed or stale handshake: {stale}")
        # The decision above reads the stored value; the upsert below repeats it atomically, so
        # two signed changes racing (B newer, committing first) cannot leave the older one's
        # profile under the newer timestamp. Fixed-width UTC, so text order
        # is time order, and no cast a malformed stored value could make fail.
        # Stored only when inside the window at receive time, on every path (unchanged and first
        # contact included): one far-future stamp would otherwise lock the node's later changes
        # out for as long as it lies ahead. A change outside the
        # window was refused above; an unchanged or first-contact one is accepted, unstored.
        issued_at_text = (issued_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                          if self_signed and issued_at is not None and not issued_at_err
                          and _issued_at_in_window(issued_at) else None)
        # Every self-signed handshake with an in-window issued_at must be newer than the stored
        # one, changed or not: a replay that reads "unchanged" before a newer change lands would
        # otherwise write the older profile back (a stale unchanged one gets a harmless 409).
        require_newer = bool(self_signed and issued_at_text)
        # Who may write the profile fields of an existing row: the operator; a self-signed
        # handshake that passes the freshness condition in the upsert; a self-signed legacy one
        # (no issued_at) only while no value is stored, judged in the write itself. Never an
        # unsigned caller: its "unchanged" was judged on a read a newer change may since have
        # replaced. A first contact (the INSERT) writes its profile as before.
        write_fresh = bool(is_local_operator or (self_signed and issued_at_text))
        write_legacy = bool(self_signed and issued_at is None and not issued_at_err)
        legacy_change = bool(write_legacy and changes_peer)

        # Key pinning check: reject if peer presents a different key than what's pinned
        if peer.public_key:
            existing_key = await conn.fetchval(
                "SELECT public_key FROM koi_net_nodes WHERE node_rid = $1",
                peer.node_rid,
            )
            if existing_key and existing_key != peer.public_key:
                logger.warning(
                    "KEY ROTATION REJECTED: node %s (%s) presented different public key. "
                    "Existing key starts with: %s... New key starts with: %s... "
                    "Manual re-approval required via UPDATE koi_net_nodes SET public_key = ... WHERE node_rid = ...",
                    peer.node_rid, peer.node_name,
                    existing_key[:20], peer.public_key[:20],
                )
                return _protocol_error(
                    403, "KEY_MISMATCH",
                    f"Public key for {peer.node_rid} does not match pinned key. "
                    "Manual re-approval required."
                )

        written = await conn.fetchval(
            """
            INSERT INTO koi_net_nodes
                (node_rid, node_name, node_type, base_url, public_key, encryption_key,
                 provides_event, provides_state, ontology_uri, ontology_version,
                 last_seen, status, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), 'active',
                    CASE WHEN $12::text IS NULL THEN '{}'::jsonb
                         ELSE jsonb_build_object('handshake_issued_at', $12::text) END)
            ON CONFLICT (node_rid) DO UPDATE SET
                -- a keyless row takes the first key it is given and keeps it
                public_key = COALESCE(koi_net_nodes.public_key, EXCLUDED.public_key),
                -- profile fields only for a caller that may write them
                node_name = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN EXCLUDED.node_name ELSE koi_net_nodes.node_name END,
                node_type = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN EXCLUDED.node_type ELSE koi_net_nodes.node_type END,
                base_url = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN EXCLUDED.base_url ELSE koi_net_nodes.base_url END,
                provides_event = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN EXCLUDED.provides_event ELSE koi_net_nodes.provides_event END,
                provides_state = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN EXCLUDED.provides_state ELSE koi_net_nodes.provides_state END,
                encryption_key = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN COALESCE(EXCLUDED.encryption_key, koi_net_nodes.encryption_key) ELSE koi_net_nodes.encryption_key END,
                ontology_uri = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN COALESCE(EXCLUDED.ontology_uri, koi_net_nodes.ontology_uri) ELSE koi_net_nodes.ontology_uri END,
                ontology_version = CASE WHEN $14::boolean OR ($15::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NULL)
                             THEN COALESCE(EXCLUDED.ontology_version, koi_net_nodes.ontology_version) ELSE koi_net_nodes.ontology_version END,
                last_seen = CASE WHEN $11::boolean THEN NOW() ELSE koi_net_nodes.last_seen END,
                status = CASE WHEN koi_net_nodes.status IN ('rejected', 'revoked') THEN koi_net_nodes.status ELSE 'active' END,
                -- the newest accepted signed handshake's issued_at
                metadata = CASE WHEN $12::text IS NOT NULL
                                     AND (koi_net_nodes.metadata->>'handshake_issued_at' IS NULL
                                          OR koi_net_nodes.metadata->>'handshake_issued_at' < $12::text)
                                THEN COALESCE(koi_net_nodes.metadata, '{}'::jsonb)
                                     || jsonb_build_object('handshake_issued_at', $12::text)
                                ELSE koi_net_nodes.metadata END
            -- a self-signed handshake with a stamp applies only while newer than the stored one
            -- a legacy change is refused once a value is stored
            WHERE (NOT $13::boolean
                   OR koi_net_nodes.metadata->>'handshake_issued_at' IS NULL
                   OR koi_net_nodes.metadata->>'handshake_issued_at' < $12::text)
              AND NOT ($16::boolean AND koi_net_nodes.metadata->>'handshake_issued_at' IS NOT NULL)
            RETURNING node_rid
            """,
            peer.node_rid,
            peer.node_name,
            peer.node_type,
            valid_peer_base_url(peer.base_url, _node_profile.base_url if _node_profile else None),
            peer.public_key,
            peer.encryption_key,
            peer.provides.event if peer.provides else [],
            peer.provides.state if peer.provides else [],
            peer.ontology_uri,
            peer.ontology_version,
            may_update,
            issued_at_text,
            require_newer,
            write_fresh,
            write_legacy,
            legacy_change,
        )
        if not written:
            # A newer signed handshake committed between our check and this write: refuse
            # before any edge or alias write, and log no success.
            return _protocol_error(409, "STALE_HANDSHAKE",
                                   "Refusing a stale handshake: a newer one was accepted meanwhile")

        # Admission (register entry 26). Every NEW inbound edge is PROPOSED until an operator
        # approves it (POST /koi-net/edges/approve). Existing edges keep their status: the upsert
        # below never changes one. No "known" test is involved, so neither a bootstrap row nor
        # an edge in the other direction can approve one. KOI_NET_DEFER_UNKNOWN_HANDSHAKE=false
        # restores the old auto-approval of a new inbound edge. A peer we already serve (an
        # APPROVED edge the other way) gets no exception: serving a peer does not admit it.
        effective_defer = req.defer_approval or _bool_env("KOI_NET_DEFER_UNKNOWN_HANDSHAKE", True)

        # Create inbound POLL edge (APPROVED by default, PROPOSED when deferred)
        inbound_status = 'PROPOSED' if effective_defer else 'APPROVED'
        edge_rid_inbound = f"orn:koi-net.edge:{peer.node_rid}>{_node_profile.node_rid}:poll"
        await conn.execute(
            """
            INSERT INTO koi_net_edges
                (edge_rid, source_node, target_node, edge_type, status, rid_types)
            VALUES ($1, $2, $3, 'POLL', $5, $4)
            ON CONFLICT (edge_rid) DO UPDATE SET
                updated_at = NOW(),
                -- Only a PROPOSED edge takes the peer's provides: an operator's grant on an APPROVED
                -- edge, and a REVOKED or REJECTED one, keep their rid_types (PR #15's may_update
                -- CASE rewrote them).
                rid_types = CASE WHEN $6::boolean AND koi_net_edges.status = 'PROPOSED'
                                 THEN EXCLUDED.rid_types ELSE koi_net_edges.rid_types END
            """,
            edge_rid_inbound,
            peer.node_rid,
            _node_profile.node_rid,
            peer.provides.event if peer.provides else [],
            inbound_status,
            may_update,
        )

        # Create outbound POLL edge (PROPOSED — peer wants to poll us, requires approval)
        edge_rid_outbound = f"orn:koi-net.edge:{_node_profile.node_rid}>{peer.node_rid}:poll"
        await conn.execute(
            """
            INSERT INTO koi_net_edges
                (edge_rid, source_node, target_node, edge_type, status, rid_types)
            VALUES ($1, $2, $3, 'POLL', 'PROPOSED', $4)
            ON CONFLICT (edge_rid) DO UPDATE SET
                updated_at = NOW(),
                -- What we serve the peer: an operator-narrowed APPROVED grant keeps its rid_types.
                rid_types = CASE WHEN $5::boolean AND koi_net_edges.status = 'PROPOSED'
                                 THEN EXCLUDED.rid_types ELSE koi_net_edges.rid_types END
            """,
            edge_rid_outbound,
            _node_profile.node_rid,
            peer.node_rid,
            _node_profile.provides.event if _node_profile.provides else [],
            may_update,
        )

        # Auto-create alias using full node_name (short aliases are manual)
        if peer.node_name:
            await conn.execute(
                """
                INSERT INTO koi_net_peer_aliases (alias, node_rid)
                VALUES ($1, $2)
                ON CONFLICT (alias) DO UPDATE SET
                    node_rid = CASE WHEN $3::boolean THEN EXCLUDED.node_rid ELSE koi_net_peer_aliases.node_rid END
                """,
                peer.node_name.lower(),
                peer.node_rid,
                is_local_operator,  # only the local operator moves an existing name to another node
            )

    # Invalidate cached E2EE shared key on handshake (key may have rotated)
    if _vault_sync:
        _vault_sync.invalidate_shared_key(peer.node_rid)

    logger.info(
        f"Handshake with {peer.node_rid} ({peer.node_name}) — "
        f"inbound edge {inbound_status}, outbound edge PROPOSED, alias '{peer.node_name}'"
        f"{', e2ee=yes' if peer.encryption_key else ''}"
    )

    # Report the inbound edge as stored, not as computed: an existing edge keeps its status
    # on conflict, so a PROPOSED or REJECTED edge must not be reported APPROVED.
    async with _db_pool.acquire() as conn:
        stored_status = await conn.fetchval("SELECT status FROM koi_net_edges WHERE edge_rid = $1", edge_rid_inbound)
    response = HandshakeResponse(
        profile=_node_profile,
        accepted=True,
        edge_status=stored_status or inbound_status,
        edge_rid=edge_rid_inbound,
    )
    return JSONResponse(
        content=_wrap_response(response.model_dump(), peer.node_rid, signed)
    )


@koi_net_router.post(BROADCAST_EVENTS_PATH)
async def events_broadcast(request: Request):
    """Receive events from peers."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    if not payload:
        return _protocol_error(400, "EMPTY_PAYLOAD", "Empty payload")

    events = payload.get("events", [])
    if not isinstance(events, list):
        return _protocol_error(400, "INVALID_EVENTS", "events must be a list")

    # Node and edge objects go to the negotiator and never into the event table
    # (divergences 20, 23). Only a signed sender can be answered.
    protocol_events = [e for e in events if isinstance(e, dict) and is_protocol_rid(e.get("rid"))]
    events = [e for e in events if not (isinstance(e, dict) and is_protocol_rid(e.get("rid")))]

    # Membrane governance (divergence 18, decision D1): a node without an approved edge may
    # still introduce itself and negotiate an edge with us, and nothing else. Every other
    # event it sends is dropped, and the reply is HTTP 200 with an empty body (spec 4.1.1),
    # so a refused peer is not told its signature is bad.
    policy = _security_policy()
    if policy["require_approved_edge_for_poll"]:
        if not source_node or not await _check_approved_edge(source_node, direction="any"):
            allowed = [e for e in protocol_events
                       if source_node and first_contact_allowed(e, source_node, _node_profile.node_rid)]
            if allowed and _negotiator:
                await _negotiator.handle(allowed, source_node)
            dropped = len(protocol_events) + len(events) - len(allowed)
            logger.info(f"Broadcast: {source_node or 'unsigned sender'} has no approved edge; "
                        f"handled {len(allowed)} first-contact object(s), dropped {dropped} event(s)")
            return Response(status_code=200)

    if protocol_events and source_node and _negotiator:
        await _negotiator.handle(protocol_events, source_node)

    queued = 0
    for event_data in events:
        if not isinstance(event_data, dict):
            continue
        rid = event_data.get("rid")
        event_type = event_data.get("event_type", "NEW")
        if not rid:
            continue

        try:
            result = await _event_queue.add(
                event_type=event_type,
                rid=rid,
                manifest=event_data.get("manifest"),
                contents=event_data.get("contents"),
                source_node=source_node or "unknown",
                event_id=event_data.get("event_id"),
            )
            if result is not None:
                queued += 1
        except FederationBlocksUnavailable:
            logger.error(f"Broadcast: federation_rid_blocks is missing, not queuing {rid_sha256(str(rid))[:12]}")
        except Exception as exc:
            logger.warning(f"Failed to queue event {rid}: {exc}")

    logger.info(f"Broadcast: queued {queued}/{len(events)} events from {source_node}"
                f" ({len(protocol_events)} node/edge object(s) to negotiation)")
    # Divergence 22: spec 4.1.1 requires an empty body. A peer running our code still gets
    # the signed {"status", "queued"} reply its webhook pusher expects (see 24).
    if not await _peer_reads_extensions(source_node):
        return Response(status_code=200)
    resp = {"status": "ok", "queued": queued}
    return JSONResponse(content=_wrap_response(resp, source_node, signed))


@koi_net_router.post(POLL_EVENTS_PATH)
async def events_poll(request: Request):
    """Serve queued events to a polling node."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    if not payload:
        return _protocol_error(400, "EMPTY_PAYLOAD", "Empty payload")

    requesting_node = source_node or payload.get("node_id")
    if not requesting_node:
        return _protocol_error(
            400,
            "MISSING_REQUESTING_NODE",
            "Cannot identify requesting node (use SignedEnvelope or include node_id)",
        )

    limit = payload.get("limit", 50)
    if not isinstance(limit, int) or limit <= 0:
        limit = 50

    # Same identity rule as the other gated routes: while the gate is on, an unsigned caller
    # is unidentified, whatever node_id it names.
    if not source_node and _security_policy()["require_approved_edge_for_poll"]:
        logger.info("Poll: unsigned request, returning empty (gate on)")
        return JSONResponse(content=_wrap_response(_koi_net_reply(EventsPayload(events=[])), None, False))

    rid_types = None
    has_approved_edge = False
    async with _db_pool.acquire() as conn:
        edge = await conn.fetchrow(
            """
            SELECT rid_types FROM koi_net_edges
            WHERE target_node = $1 AND source_node = $2 AND status = 'APPROVED'
            """,
            requesting_node,
            _node_profile.node_rid,
        )
        if edge:
            has_approved_edge = True
            # Pass rid_types through verbatim, INCLUDING an empty list. Collapsing
            # empty -> None made the filter fail OPEN: an edge declaring nothing
            # received everything. poll() distinguishes None (no filter declared)
            # from [] (declares nothing, so receives nothing).
            rid_types = edge["rid_types"]
            # Divergence 6: last_seen was written only by handshake/key-bootstrap,
            # so every value went stale in March while peers polled continuously —
            # and a peer was wrongly reported offline for three months on that
            # basis. A poll is proof of life; record it.
            try:
                await conn.execute(
                    "UPDATE koi_net_nodes SET last_seen = NOW() WHERE node_rid = $1",
                    requesting_node,
                )
            except Exception:
                pass  # liveness bookkeeping must never fail a poll

    policy = _security_policy()
    if not has_approved_edge and policy["require_approved_edge_for_poll"]:
        # D1: a partial node with no approved edge still receives the node and edge objects
        # we queued for it (our bundle on first contact, edge negotiation), nothing else.
        events = await _event_queue.poll_own_protocol_objects(requesting_node, limit) if signed else []
        logger.info(
            f"Poll: no approved edge for {requesting_node}, serving {len(events)} first-contact object(s) (strict)"
        )
    else:
        events = await _event_queue.poll(requesting_node, limit, rid_types)

    # Divergence 16: built so koi-net's re-serialisation reproduces it byte for byte.
    extensions = await _peer_reads_extensions(requesting_node)
    wire_events = []
    unrepresentable = 0
    for ev in events:
        # All stored manifest fields are kept (e.g. vault-sync content_hash, relative_path);
        # the three koi-net fields are filled in.
        we = koi_net_event(
            ev["rid"], ev["event_type"], complete_manifest(ev.get("manifest"), ev["rid"], ev.get("contents")),
            ev.get("contents"), event_id=ev.get("event_id"), extensions=extensions,
        )
        if we is None:
            # poll() has marked it delivered: a koi-net peer can never accept it (e.g. a task key
            # without a colon as its RID, divergence 17), so it is withheld for good.
            unrepresentable += 1
            continue
        wire_events.append(we)
    if unrepresentable:
        logger.info(f"Poll: withheld {unrepresentable} event(s) koi-net cannot represent from {requesting_node}")

    if extensions:
        # A peer running our code: our events may carry `event_id` and RIDs koi-net cannot
        # parse (divergences 16, 17), so koi-net's EventsPayload cannot hold them.
        payload = {"type": "events_payload", "events": wire_events}
    else:
        payload = _koi_net_reply(EventsPayload(events=[KoiNetEvent.model_validate(we) for we in wire_events]))
    return JSONResponse(content=_wrap_response(payload, requesting_node, signed))


@koi_net_router.post("/events/confirm")
async def events_confirm(request: Request):
    """Acknowledge receipt of events."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    if not payload:
        return _protocol_error(400, "EMPTY_PAYLOAD", "Empty payload")

    confirming_node = source_node or payload.get("node_id")
    event_ids = payload.get("event_ids", [])

    if not confirming_node:
        return _protocol_error(400, "MISSING_CONFIRMING_NODE", "Cannot identify confirming node")

    # Membrane governance: gate confirm for unapproved/unknown nodes
    policy = _security_policy()
    if policy["require_approved_edge_for_poll"]:
        if not source_node:
            logger.info("Confirm: unsigned request rejected — signed envelope required in strict mode")
            return _protocol_error(403, "IDENTITY_REQUIRED", "Signed envelope required for confirm in strict mode")
        if not await _check_approved_edge(confirming_node, direction="any"):
            return _protocol_error(403, "UNAPPROVED_EDGE", f"No approved edge for {confirming_node}")

    confirmed = await _event_queue.confirm(event_ids, confirming_node)
    resp = ConfirmEventsResponse(confirmed=confirmed)
    return JSONResponse(
        content=_wrap_response(resp.model_dump(), confirming_node, signed)
    )


async def _peer_reads_extensions(node_rid: Optional[str]) -> bool:
    """Whether a peer runs our code and reads our wire extensions (`event_id` on events,
    the signed `{"status", "queued"}` broadcast reply).

    koi-net nodes never send `encryption_key` or `ontology_uri` in their profile; our nodes
    always do (`ontology_uri` has a default), through /handshake or a broadcast bootstrap. A
    peer we cannot identify is treated as a koi-net node, which only loses our extensions.
    """
    if not node_rid or not _db_pool:
        return False
    try:
        async with _db_pool.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT encryption_key IS NOT NULL OR ontology_uri IS NOT NULL FROM koi_net_nodes WHERE node_rid = $1",
                node_rid,
            ))
    except asyncpg.PostgresError as exc:
        # A failed lookup must not fail the request; the peer just gets the koi-net form.
        logger.warning(f"Peer extension lookup failed ({type(exc).__name__}); answering in koi-net form")
        return False


def _own_and_rest(rids: List[Any]) -> tuple:
    """Split requested RIDs into (our own node RID was requested, every other RID).

    Our own KoiNetNode bundle is served to any caller, membrane or not (divergence 15):
    it is the profile /koi-net/health already publishes, and a stock node needs it to
    verify us. Another node's node or edge object is never served from our event table,
    where peers' pushed events also land (divergence 23); it reads as not found.
    """
    own = _node_profile.node_rid if _node_profile else None
    own_requested = any(r == own for r in rids)
    rest = [r for r in rids if r != own]
    return own_requested, rest


def _own_rid_listing(rid_types: Optional[List[str]]) -> List[str]:
    """Our own node RID, when a /rids/fetch request asks for node objects or for everything."""
    return [_node_profile.node_rid] if _node_profile and lists_own_node(rid_types) else []


def _is_foreign_protocol_object(rid: str) -> bool:
    """Any node or edge object other than our own node, which _own_and_rest() has split off.
    Our own edge objects are not served from here yet: S3 serves them from koi_net_edges."""
    return is_protocol_rid(rid)


@koi_net_router.post(FETCH_MANIFESTS_PATH)
async def manifests_fetch(request: Request):
    """Serve manifests by RID."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    # Without this check a null or non-string RID reaches not_found and fails response validation.
    rids = _requested_rids(payload)
    if rids is None or _koi_net_request(FetchManifests, payload) is None:
        return _protocol_error(400, "INVALID_PAYLOAD", "rids must be a list of RID strings")
    own_requested, rids = _own_and_rest(rids)
    own = [Manifest.model_validate(own_node_bundle(_node_profile)["manifest"])] if own_requested else []

    def reply(manifests, not_found=()):
        return _koi_net_reply(ManifestsPayload(manifests=manifests, not_found=[RID.from_string(r) for r in not_found]))

    # Membrane governance: gate fetch for unapproved/unknown nodes
    policy = _security_policy()
    if policy["require_approved_edge_for_poll"]:
        if not source_node:
            logger.info("manifests/fetch: unsigned request, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(own), None, False))
        if not await _check_approved_edge(source_node, direction="poll"):
            logger.info(f"manifests/fetch: no approved poll edge for {source_node}, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(own), source_node, signed))

    # Unicast scoping. Divergence 3 is an intentional fork: upstream buffers
    # events per peer at WRITE time; we store one broadcast row and filter per
    # peer at READ time. So `target_node` is this fork's representation of "in
    # that peer's buffer", and serving a peer an event unicast to a different
    # node is reading someone else's buffer. poll() has always enforced this;
    # the fetch path never did.
    #
    # Fail-closed by construction, per the register's rule for anything on the
    # read path: an unidentified caller is served nothing, rather than falling
    # through to broadcast-only via SQL NULL semantics.
    if not source_node:
        logger.info("manifests/fetch: unidentified caller, serving nothing (unicast scoping)")
        return JSONResponse(content=_wrap_response(reply(own), None, False))

    manifests = list(own)
    not_found = []
    async with _db_pool.acquire() as conn:
        try:
            blocked = await blocked_hashes(conn)
        except FederationBlocksUnavailable:
            logger.error("manifests/fetch: federation_rid_blocks is missing, serving nothing")
            blocked, rids = frozenset(), []
        for rid in rids:
            if _is_foreign_protocol_object(rid):
                not_found.append(rid)
                continue
            row = await conn.fetchrow(
                """
                SELECT manifest, contents FROM koi_net_events
                WHERE rid = $1 AND manifest IS NOT NULL
                  AND (target_node IS NULL OR target_node = $2)
                ORDER BY queued_at DESC LIMIT 1
                """,
                rid,
                source_node,
            )
            # Blocked records (migration 137) are never served, nor a stored event that refers to one.
            if row and row["manifest"] and not is_blocked(blocked, rid, row["contents"], row["manifest"]):
                try:
                    c = json.loads(row["contents"]) if isinstance(row["contents"], str) else (row["contents"] or None)
                except ValueError:
                    c = None
                fields = _stored_manifest_fields(row["manifest"], rid, c)
                manifest = _typed(Manifest, fields) if fields else None
                if manifest is not None:
                    manifests.append(manifest)
                else:  # a stored manifest rid-lib cannot represent reads as not found
                    not_found.append(rid)
            else:
                # koi_net ManifestsPayload.not_found: requested RIDs this node has no manifest
                # for (a blocked record is reported the same way as an absent one).
                not_found.append(rid)

    return JSONResponse(content=_wrap_response(reply(manifests, not_found), source_node, signed))


@koi_net_router.post(FETCH_BUNDLES_PATH)
async def bundles_fetch(request: Request):
    """Serve bundles by RID."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    rids = _requested_rids(payload)
    if rids is None or _koi_net_request(FetchBundles, payload) is None:
        return _protocol_error(400, "INVALID_PAYLOAD", "rids must be a list of RID strings")
    own_requested, rids = _own_and_rest(rids)
    own = [Bundle.model_validate(own_node_bundle(_node_profile))] if own_requested else []

    def reply(bundles, not_found=()):
        return _koi_net_reply(BundlesPayload(bundles=bundles, not_found=[RID.from_string(r) for r in not_found]))

    # Membrane governance: gate fetch for unapproved/unknown nodes
    policy = _security_policy()
    if policy["require_approved_edge_for_poll"]:
        if not source_node:
            logger.info("bundles/fetch: unsigned request, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(own), None, False))
        if not await _check_approved_edge(source_node, direction="poll"):
            logger.info(f"bundles/fetch: no approved poll edge for {source_node}, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(own), source_node, signed))

    # Unicast scoping — see manifests_fetch for the divergence-3 rationale.
    # Fail-closed by construction: unidentified caller is served nothing.
    if not source_node:
        logger.info("bundles/fetch: unidentified caller, serving nothing (unicast scoping)")
        return JSONResponse(content=_wrap_response(reply(own, rids), None, False))

    bundles = list(own)
    not_found = []

    async with _db_pool.acquire() as conn:
        try:
            blocked = await blocked_hashes(conn)
        except FederationBlocksUnavailable:
            logger.error("bundles/fetch: federation_rid_blocks is missing, serving only our own node")
            return JSONResponse(content=_wrap_response(reply(own, rids), source_node, signed))
        for rid in rids:
            if _is_foreign_protocol_object(rid):
                not_found.append(rid)
                continue
            row = await conn.fetchrow(
                """
                SELECT manifest, contents FROM koi_net_events
                WHERE rid = $1 AND contents IS NOT NULL
                  AND (target_node IS NULL OR target_node = $2)
                ORDER BY queued_at DESC LIMIT 1
                """,
                rid,
                source_node,
            )
            # Blocked records (migration 137) read as not found, as does a stored event that refers to one.
            if row and not is_blocked(blocked, rid, row["contents"], row["manifest"]):
                try:
                    c = json.loads(row["contents"]) if isinstance(row["contents"], str) else (row["contents"] or {})
                except ValueError:
                    c = None
                fields = _stored_manifest_fields(row["manifest"] or {}, rid, c)
                bundle = _typed(Bundle, {"manifest": fields, "contents": c}) if fields and isinstance(c, dict) else None
                if bundle is not None:
                    bundles.append(bundle)
                else:  # a stored record rid-lib cannot represent reads as not found
                    not_found.append(rid)
            else:
                not_found.append(rid)

    return JSONResponse(content=_wrap_response(reply(bundles, not_found), source_node, signed))


def _federation_rid_groups() -> Optional[List[str]]:
    """Optional group_id allowlist for the federation RID directory.

    KOI_FEDERATION_RID_GROUPS is a comma-separated list of group_ids
    (e.g. "bioregional-coordination,biofi"). When set, /rids/fetch only
    lists entities whose metadata.group_id is in the list. Unset/empty
    means no group restriction.
    """
    raw = os.getenv("KOI_FEDERATION_RID_GROUPS", "").strip()
    if not raw:
        return None
    groups = [g.strip() for g in raw.split(",") if g.strip()]
    return groups or None


def _build_rids_fetch_query(
    rid_types: Optional[List[str]],
    allowed_groups: Optional[List[str]],
) -> tuple:
    """Build the /rids/fetch directory query.

    The RID directory is peer-visible, so it must never list entities that
    are node-private or that were tombstoned by a merge (merged_into set) —
    peers should only discover live, shareable identities.
    """
    conditions = [
        "er.koi_rid IS NOT NULL",
        "er.merged_into IS NULL",
        "NOT COALESCE(er.node_private, FALSE)",
    ]
    params: List[Any] = []
    if rid_types:
        # Bare names ("Person") match entity_type, as before. RID types
        # ("orn:koi-net.person", divergence 21) match the koi_rid's own type, the part of
        # orn:<namespace>:<reference> before the reference. Node and edge types are not
        # entities: lists_own_node() decides those.
        names = [t for t in rid_types if isinstance(t, str) and ":" not in t]
        contexts = [t.strip().lower() for t in rid_types if isinstance(t, str) and ":" in t
                    and t.strip().lower() not in (NODE_CONTEXT, EDGE_CONTEXT)]
        alternatives = []
        if names:
            params.append(names)
            alternatives.append(f"er.entity_type = ANY(${len(params)})")
        if contexts:
            params.append(contexts)
            alternatives.append(
                f"split_part(er.koi_rid, ':', 1) || ':' || split_part(er.koi_rid, ':', 2) = ANY(${len(params)})")
        conditions.append("(" + " OR ".join(alternatives) + ")" if alternatives else "FALSE")
    if allowed_groups:
        params.append(allowed_groups)
        conditions.append(f"er.metadata->>'group_id' = ANY(${len(params)})")
    sql = (
        "SELECT DISTINCT er.koi_rid\n"
        "FROM entity_registry er\n"
        "WHERE " + "\n  AND ".join(conditions) + "\n"
        "ORDER BY er.koi_rid"
    )
    return sql, params


@koi_net_router.post(FETCH_RIDS_PATH)
async def rids_fetch(request: Request):
    """List available RIDs, optionally filtered by type."""
    try:
        payload, source_node, signed = await _unwrap_request(request)
    except EnvelopeError as exc:
        return _envelope_error_response(exc)

    if _koi_net_request(FetchRids, payload) is None:
        return _protocol_error(400, "INVALID_PAYLOAD", "rid_types must be a list of RID types")
    rid_types = (payload or {}).get("rid_types")

    def reply(listed):
        parsed = [_rid_or_none(str(r)) for r in listed]
        if None in parsed:  # a stored koi_rid rid-lib cannot parse is not listed
            logger.info(f"rids/fetch: left out {parsed.count(None)} RID(s) koi-net cannot represent")
        return _koi_net_reply(RidsPayload(rids=[r for r in parsed if r is not None]))

    # Membrane governance: gate fetch for unapproved/unknown nodes
    policy = _security_policy()
    if policy["require_approved_edge_for_poll"]:
        if not source_node:
            logger.info("rids/fetch: unsigned request, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(_own_rid_listing(rid_types)), None, False))
        if not await _check_approved_edge(source_node, direction="poll"):
            logger.info(f"rids/fetch: no approved poll edge for {source_node}, returning empty (strict)")
            return JSONResponse(content=_wrap_response(reply(_own_rid_listing(rid_types)), source_node, signed))

    sql, params = _build_rids_fetch_query(rid_types, _federation_rid_groups())
    async with _db_pool.acquire() as conn:
        try:
            blocked = await blocked_hashes(conn)
        except FederationBlocksUnavailable:
            logger.error("rids/fetch: federation_rid_blocks is missing, listing only our own node")
            return JSONResponse(content=_wrap_response(reply(_own_rid_listing(rid_types)), source_node, signed))
        rows = await conn.fetch(sql, *params)
        # Blocked entities (migration 137) are not listed, whether blocked by koi_rid or by entity URI.
        hidden = set()
        if blocked:
            hidden = {r["koi_rid"] for r in await conn.fetch(
                "SELECT koi_rid FROM entity_registry"
                " WHERE encode(sha256(convert_to(fuseki_uri, 'UTF8')), 'hex') = ANY($1::text[])",
                list(blocked),
            )}

    rids = [row["koi_rid"] for row in rows if row["koi_rid"] not in hidden and not is_blocked(blocked, row["koi_rid"])]
    rids = _own_rid_listing(rid_types) + rids
    return JSONResponse(content=_wrap_response(reply(rids), source_node, signed))


@koi_net_router.get("/edges")
async def koi_net_edges(request: Request, status: Optional[str] = None):
    """Federation edges. Unauthenticated: APPROVED only. Admin: filter by status."""
    if not _db_pool:
        return {"edges": []}

    # Check if admin request (localhost + token)
    is_admin = _enforce_local_admin(request) is None

    if is_admin and status:
        if status == "all":
            query = (
                "SELECT edge_rid, source_node, target_node, edge_type, status, "
                "created_at, updated_at FROM koi_net_edges"
            )
            params = []
        else:
            query = (
                "SELECT edge_rid, source_node, target_node, edge_type, status, "
                "created_at, updated_at FROM koi_net_edges WHERE status = $1"
            )
            params = [status.upper()]
    else:
        # Unauthenticated: APPROVED only (backward compatible)
        query = (
            "SELECT edge_rid, source_node, target_node, edge_type, status "
            "FROM koi_net_edges WHERE status = 'APPROVED'"
        )
        params = []

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    edges = []
    for r in rows:
        edge = dict(r)
        # Serialize datetime fields for JSON
        for field in ("created_at", "updated_at"):
            if field in edge and edge[field] is not None:
                edge[field] = edge[field].isoformat()
        edges.append(edge)
    return {"edges": edges}


def _key_fingerprint(b64: Optional[str]) -> Optional[str]:
    """"SHA256:" + base64 (no padding) of SHA-256 over the key's decoded bytes: the format of
    scripts/federation/fingerprints.py and lib.sh's compute_koi_fingerprint."""
    if not b64:
        return None
    try:
        raw = base64.b64decode("".join(str(b64).split()), validate=True)
    except (binascii.Error, ValueError):
        return "(unparseable)"
    return "SHA256:" + base64.b64encode(hashlib.sha256(raw).digest()).decode().rstrip("=")


def _peer_identity(row, node_rid: Optional[str]) -> Dict[str, Any]:
    """What an approval admits, for the operator to compare with the peer's own values given
    out of band (register 26): an unsigned first handshake can register
    an unused RID with the caller's own address and encryption key."""
    row = row or {}
    return {
        "node_rid": node_rid,
        "node_name": row.get("node_name"),
        "base_url": row.get("base_url"),
        "public_key_fingerprint": _key_fingerprint(row.get("public_key")),
        "encryption_key_fingerprint": _key_fingerprint(row.get("encryption_key")),
    }


@koi_net_router.post("/edges/approve")
async def approve_edge(request: Request):
    """Approve a PROPOSED edge to allow a peer to poll our events.

    Admin-only: requires localhost + admin token.
    """
    auth_err = _enforce_local_admin(request)
    if auth_err:
        return auth_err

    try:
        body = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")

    edge_rid = body.get("edge_rid")
    if not edge_rid:
        return _protocol_error(400, "MISSING_EDGE_RID", "edge_rid is required")

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    peer_rid, peer_row = None, None
    async with _db_pool.acquire() as conn:
        ends = await conn.fetchrow("SELECT source_node, target_node FROM koi_net_edges WHERE edge_rid = $1", edge_rid)
        if ends:
            peer_rid = ends["target_node"] if ends["source_node"] == _node_profile.node_rid else ends["source_node"]
            peer_row = await conn.fetchrow(
                "SELECT node_name, base_url, public_key, encryption_key FROM koi_net_nodes WHERE node_rid = $1", peer_rid)
            if await _node_is_barred(conn, peer_rid):
                return _protocol_error(409, "NODE_REJECTED", f"{peer_rid} is rejected; re-admit it first "
                                       "(POST /koi-net/nodes/readmit)")
        row = await conn.fetchrow(
            """
            UPDATE koi_net_edges SET status = 'APPROVED', updated_at = NOW()
            WHERE edge_rid = $1 AND status = 'PROPOSED'
            RETURNING source_node, target_node, edge_type, status, rid_types, metadata
            """,
            edge_rid,
        )
        count = 1 if row else 0

    if count == 0:
        return _protocol_error(
            404, "EDGE_NOT_FOUND",
            f"No PROPOSED edge found with rid '{edge_rid}'"
        )

    if _negotiator:
        try:
            await _negotiator.send_edge_state(dict(row))
        except Exception as exc:  # the approval stands; tell the operator the notice failed
            logger.warning(f"Approved edge {edge_rid}, but notifying the subscriber failed: {exc!r}")
    logger.info(f"Approved edge: {edge_rid}")
    # What was approved, as stored: compare it with the peer's own values (fingerprints.py own).
    return {"status": "approved", "edge_rid": edge_rid,
            "peer": _peer_identity(dict(peer_row) if peer_row else None, peer_rid)}


@koi_net_router.post("/edges/reject")
async def reject_edge(request: Request):
    """Reject a PROPOSED edge. Optionally deactivate the peer node.

    Admin-only: requires localhost + admin token.
    """
    auth_err = _enforce_local_admin(request)
    if auth_err:
        return auth_err

    try:
        body = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")

    edge_rid = body.get("edge_rid")
    if not edge_rid:
        return _protocol_error(400, "MISSING_EDGE_RID", "edge_rid is required")

    deactivate_node = body.get("deactivate_node", False)

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE koi_net_edges SET status = 'REJECTED', updated_at = NOW()
            WHERE edge_rid = $1 AND status = 'PROPOSED'
            RETURNING source_node, target_node, edge_type, status, rid_types, metadata
            """,
            edge_rid,
        )
        count = 1 if row else 0

        if count == 0:
            return _protocol_error(
                404, "EDGE_NOT_FOUND",
                f"No PROPOSED edge found with rid '{edge_rid}'"
            )

        if deactivate_node:
            # The peer is the endpoint that is not us. It used to take source_node, which for a
            # subscription to us (source = us) is our own RID.
            ends = await conn.fetchrow(
                "SELECT source_node, target_node FROM koi_net_edges WHERE edge_rid = $1",
                edge_rid,
            )
            source = None
            if ends:
                source = ends["target_node"] if ends["source_node"] == _node_profile.node_rid else ends["source_node"]
            if source and source != _node_profile.node_rid:
                await conn.execute(
                    "UPDATE koi_net_nodes SET status = 'rejected' WHERE node_rid = $1",
                    source,
                )
                logger.info(f"Deactivated node: {source}")

    if _negotiator:
        try:
            await _negotiator.send_edge_state(dict(row))
        except Exception as exc:
            logger.warning(f"Rejected edge {edge_rid}, but notifying the subscriber failed: {exc!r}")
    logger.info(f"Rejected edge: {edge_rid}")
    return {"status": "rejected", "edge_rid": edge_rid}


@koi_net_router.post("/edges/reopen")
async def reopen_edge(request: Request):
    """Move a REVOKED or REJECTED edge back to PROPOSED, so /edges/approve, with its display of
    what it approves, can then approve it. A peer can never move such an edge (register 26);
    this is the operator's way back short of SQL.

    Admin-only (_enforce_local_admin). Refused (409) for a barred node: re-admit it first.
    """
    auth_err = _enforce_local_admin(request)
    if auth_err:
        return auth_err
    try:
        body = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")
    edge_rid = body.get("edge_rid") if isinstance(body, dict) else None
    if not isinstance(edge_rid, str) or not edge_rid:
        return _protocol_error(400, "MISSING_EDGE_RID", "edge_rid is required")
    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")
    async with _db_pool.acquire() as conn:
        ends = await conn.fetchrow("SELECT source_node, target_node FROM koi_net_edges WHERE edge_rid = $1", edge_rid)
        if not ends:
            return _protocol_error(404, "EDGE_NOT_FOUND", f"No edge with rid '{edge_rid}'")
        peer_rid = ends["target_node"] if ends["source_node"] == _node_profile.node_rid else ends["source_node"]
        if await _node_is_barred(conn, peer_rid):
            return _protocol_error(409, "NODE_REJECTED", f"{peer_rid} is rejected; re-admit it first "
                                   "(POST /koi-net/nodes/readmit)")
        peer_row = await conn.fetchrow(
            "SELECT node_name, base_url, public_key, encryption_key FROM koi_net_nodes WHERE node_rid = $1", peer_rid)
        row = await conn.fetchrow(
            """UPDATE koi_net_edges SET status = 'PROPOSED', updated_at = NOW()
               WHERE edge_rid = $1 AND status IN ('REVOKED', 'REJECTED') RETURNING edge_rid""",
            edge_rid,
        )
    if not row:
        return _protocol_error(404, "EDGE_NOT_HELD", f"Edge '{edge_rid}' is not REVOKED or REJECTED")
    logger.info(f"Reopened edge: {edge_rid} (now PROPOSED)")
    return {"status": "reopened", "edge_rid": edge_rid,
            "peer": _peer_identity(dict(peer_row) if peer_row else None, peer_rid)}


@koi_net_router.post("/nodes/readmit")
async def readmit_node(request: Request):
    """Re-admit a node an operator rejected or revoked: its status becomes active again. Its
    edges keep their status; approve the ones it should have (POST /koi-net/edges/approve).

    Admin-only (_enforce_local_admin).
    """
    auth_err = _enforce_local_admin(request)
    if auth_err:
        return auth_err
    try:
        body = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")
    node_rid = body.get("node_rid") if isinstance(body, dict) else None
    if not isinstance(node_rid, str) or not node_rid:
        return _protocol_error(400, "MISSING_NODE_RID", "node_rid is required")
    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")
    async with _db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE koi_net_nodes SET status = 'active' WHERE node_rid = $1 AND status = ANY($2::text[]) RETURNING node_rid",
            node_rid, list(BARRED_NODE_STATUSES),
        )
    if not row:
        return _protocol_error(404, "NODE_NOT_BARRED", f"No rejected or revoked node '{node_rid}'")
    logger.info(f"Re-admitted node: {node_rid}")
    return {"status": "readmitted", "node_rid": node_rid}


@koi_net_router.get("/health")
async def koi_net_health():
    """Node identity, connected peers, capabilities."""
    peers = []
    queue_size = 0
    policy = _security_policy()

    if _db_pool:
        async with _db_pool.acquire() as conn:
            peer_rows = await conn.fetch(
                "SELECT node_rid, node_name, status, last_seen FROM koi_net_nodes WHERE status = 'active'"
            )
            peers = [
                {
                    "node_rid": r["node_rid"],
                    "node_name": r["node_name"],
                    "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                }
                for r in peer_rows
            ]

    if _event_queue:
        queue_size = await _event_queue.get_queue_size()

    return {
        "status": "healthy",
        "node": _node_profile.model_dump() if _node_profile else None,
        "peers": peers,
        "event_queue_size": queue_size,
        "pipeline_enabled": _poller is not None and _poller.use_pipeline and _poller.pipeline is not None,
        "protocol": {
            "strict_mode": policy["strict_mode"],
            "require_signed_envelopes": policy["require_signed"],
            "enforce_target_match": policy["enforce_target"],
            "enforce_source_key_rid_binding": policy["enforce_source_binding"],
            "core_endpoints": [
                "/koi-net/events/broadcast",
                "/koi-net/events/poll",
                "/koi-net/manifests/fetch",
                "/koi-net/bundles/fetch",
                "/koi-net/rids/fetch",
            ],
            "extensions": [
                "/koi-net/handshake",
                "/koi-net/events/confirm",
                "/koi-net/health",
                "/koi-net/commons/intake",
                "/koi-net/commons/intake/decide",
            ],
        },
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


# =============================================================================
# Document Sharing Endpoints
# =============================================================================

import uuid as _uuid
from pydantic import BaseModel as _ShareBaseModel


class ShareDocumentRequest(_ShareBaseModel):
    """Request to share a document with a peer."""
    document_rid: str  # e.g. "orn:obsidian.note:Articles/My Research.md"
    recipient: str     # Human-friendly alias (e.g. "shawn") or node_rid
    recipient_type: str = "peer"  # peer | commons
    message: Optional[str] = None
    share_mode: str = "root_plus_required"  # root_only | root_plus_required | context_pack
    context_depth: Optional[int] = None
    references: Optional[List[Dict[str, Any]]] = None
    contents: Optional[Dict[str, Any]] = None  # Document content (if not provided, will try to read from vault)


class ShareDocumentResponse(_ShareBaseModel):
    """Response after sharing a document."""
    status: str
    event_id: str
    document_rid: str
    recipient_node_rid: str
    recipient_type: str = "peer"
    share_mode: str
    context_depth: int = 1
    references_total: int = 0
    references_included: int = 0
    references_missing: int = 0
    references_excluded: int = 0
    message: Optional[str] = None


class CommonsIntakeDecisionRequest(_ShareBaseModel):
    """Approve/reject staged commons intake entries."""
    share_id: Optional[int] = None
    event_id: Optional[str] = None
    action: str  # approve | reject
    reviewer: Optional[str] = None
    note: Optional[str] = None


@koi_net_router.post("/share")
async def share_document(req: ShareDocumentRequest):
    """Share a document with a peer via KOI-net FUN event.

    Resolves recipient alias → node_rid, creates a FUN NEW/UPDATE event,
    signs it, and queues it for delivery.
    """
    allowed_modes = {"root_only", "root_plus_required", "context_pack"}
    allowed_recipient_types = {"peer", "commons"}
    share_mode = (req.share_mode or "root_plus_required").strip().lower()
    recipient_type = (req.recipient_type or "peer").strip().lower()
    if share_mode not in allowed_modes:
        return _protocol_error(
            400,
            "INVALID_SHARE_MODE",
            f"Invalid share_mode '{req.share_mode}'. Valid modes: root_only, root_plus_required, context_pack",
        )
    if recipient_type not in allowed_recipient_types:
        return _protocol_error(
            400,
            "INVALID_RECIPIENT_TYPE",
            f"Invalid recipient_type '{req.recipient_type}'. Valid types: peer, commons",
        )
    default_context_depth = 2 if share_mode == "context_pack" else 1
    context_depth = req.context_depth if req.context_depth is not None else default_context_depth
    if context_depth < 1 or context_depth > 4:
        return _protocol_error(
            400,
            "INVALID_CONTEXT_DEPTH",
            "context_depth must be an integer between 1 and 4",
        )

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")
    if not _event_queue:
        return _protocol_error(503, "NOT_INITIALIZED", "Event queue not initialized")
    if not _node_profile:
        return _protocol_error(503, "NOT_INITIALIZED", "Node identity not initialized")

    async with _db_pool.acquire() as conn:
        # Resolve recipient to node_rid
        recipient_node_rid = await _resolve_recipient(conn, req.recipient)
        if not recipient_node_rid:
            return _protocol_error(
                404, "RECIPIENT_NOT_FOUND",
                f"No peer found for '{req.recipient}'. Register peer via /koi-net/handshake first, "
                f"then add alias: INSERT INTO koi_net_peer_aliases (alias, node_rid) VALUES ('{req.recipient}', '<node_rid>')"
            )

        # Check if this document was previously shared (UPDATE vs NEW)
        prev = await conn.fetchrow(
            """SELECT id FROM koi_net_events
               WHERE rid = $1 AND source_node = $2 AND $3 = ANY(delivered_to)
               ORDER BY queued_at DESC LIMIT 1""",
            req.document_rid, _node_profile.node_rid, recipient_node_rid,
        )
        event_type = EventType.UPDATE if prev else EventType.NEW

        # Build manifest
        now_z = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        contents_payload = req.contents or {"message": req.message or "", "document_rid": req.document_rid}
        if req.message and not contents_payload.get("message"):
            contents_payload["message"] = req.message

        # References can be provided explicitly or embedded in contents.
        references_payload: List[Dict[str, Any]] = []
        if req.references:
            references_payload = [
                r for r in req.references
                if isinstance(r, dict)
            ]
        elif isinstance(contents_payload.get("references"), list):
            references_payload = [
                r for r in contents_payload.get("references", [])
                if isinstance(r, dict)
            ]

        refs_total = len(references_payload)
        refs_included = 0
        refs_required = 0
        refs_missing = 0
        refs_excluded = 0
        for ref in references_payload:
            if bool(ref.get("required")):
                refs_required += 1
            if bool(ref.get("included")) or bool(ref.get("include")):
                refs_included += 1
            if not bool(ref.get("exists")):
                refs_missing += 1
            elif not (bool(ref.get("included")) or bool(ref.get("include"))):
                refs_excluded += 1

        dependency_graph = contents_payload.get("dependency_graph")
        dependency_graph_summary: Optional[Dict[str, Any]] = None
        missing_references: List[Dict[str, Any]] = []
        if isinstance(dependency_graph, dict):
            maybe_summary = dependency_graph.get("summary")
            if isinstance(maybe_summary, dict):
                dependency_graph_summary = maybe_summary
            maybe_missing = dependency_graph.get("missing_references")
            if isinstance(maybe_missing, list):
                missing_references = [m for m in maybe_missing if isinstance(m, dict)]

        if dependency_graph_summary:
            try:
                refs_missing = int(dependency_graph_summary.get("unresolved_references", refs_missing))
            except Exception:
                pass
            try:
                refs_excluded = int(
                    dependency_graph_summary.get("missing_references", refs_missing + refs_excluded)
                ) - refs_missing
                refs_excluded = max(refs_excluded, 0)
            except Exception:
                pass

        # Set explicit share marker so the receiver can identify this as a share
        contents_payload["_koi_share"] = True
        contents_payload["_koi_share_meta"] = {
            "recipient_type": recipient_type,
            "share_mode": share_mode,
            "context_depth": context_depth,
            "references_total": refs_total,
            "references_included": refs_included,
            "references_missing": refs_missing,
            "references_excluded": refs_excluded,
        }
        content_hash = _jcs_sha256(contents_payload)

        manifest_data = {
            "rid": req.document_rid,
            "timestamp": now_z,
            "sha256_hash": content_hash,
            "kind": "document_share",
            "recipient_type": recipient_type,
            "share_mode": share_mode,
            "context_depth": context_depth,
            "references_summary": {
                "total": refs_total,
                "required": refs_required,
                "included": refs_included,
                "missing_unresolved": refs_missing,
                "excluded_not_included": refs_excluded,
                "optional": max(refs_total - refs_required, 0),
            },
        }
        if references_payload:
            manifest_data["references"] = references_payload
        if dependency_graph_summary:
            manifest_data["dependency_graph_summary"] = dependency_graph_summary
        if missing_references:
            manifest_data["missing_references"] = missing_references[:200]
        if recipient_type == "commons":
            manifest_data["intake_policy"] = {
                "mode": "staged",
                "requires_manual_approval": True,
            }
        manifest = WireManifest(
            rid=req.document_rid,
            timestamp=now_z,
            sha256_hash=content_hash,
        )

        event_id = str(_uuid.uuid4())

        # Queue for delivery with 7-day TTL, scoped to recipient only. add() returns None when the
        # document or anything it carries is blocked from federation (migration 137): nothing is
        # queued, so nothing is recorded as shared.
        queued = await _event_queue.add(
            event_type=event_type.value,
            rid=req.document_rid,
            manifest=manifest_data,
            contents=contents_payload,
            ttl_hours=168,  # 7 days
            event_id=event_id,
            target_node=recipient_node_rid,
        )
        if queued is None:
            return _protocol_error(409, "RECORD_BLOCKED", "Not shared: the document or a record it carries is blocked from federation")

        # Record in outbound share ledger for reliable retraction during offboarding
        try:
            await conn.execute(
                """INSERT INTO koi_outbound_shares (document_rid, target_node)
                   VALUES ($1, $2)
                   ON CONFLICT (document_rid, target_node)
                   DO UPDATE SET shared_at = NOW(), retracted_at = NULL""",
                req.document_rid, recipient_node_rid,
            )
        except Exception as ledger_err:
            # Non-fatal: ledger table may not exist yet (pre-migration 045)
            logger.warning("Could not record outbound share in ledger: %s", ledger_err)

        logger.info(
            "Shared document %s with %s (event_id=%s, type=%s)",
            req.document_rid, recipient_node_rid, event_id, event_type.value,
        )

    return ShareDocumentResponse(
        status="queued",
        event_id=event_id,
        document_rid=req.document_rid,
        recipient_node_rid=recipient_node_rid,
        recipient_type=recipient_type,
        share_mode=share_mode,
        context_depth=context_depth,
        references_total=refs_total,
        references_included=refs_included,
        references_missing=refs_missing,
        references_excluded=refs_excluded,
        message=f"Document queued for delivery to {req.recipient} ({event_type.value})",
    )


@koi_net_router.get("/shared-with-me")
async def shared_with_me(
    since: Optional[datetime] = None,
    from_peer: Optional[str] = None,
    limit: int = 50,
):
    """List documents shared with this node by peers.

    Reads from koi_shared_documents (persisted by the poller on inbound shares).
    """
    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")
    if not _node_profile:
        return _protocol_error(503, "NOT_INITIALIZED", "Node identity not initialized")

    async with _db_pool.acquire() as conn:
        conditions = ["status != 'retracted'"]
        params: list = []
        idx = 1

        if since:
            conditions.append(f"received_at >= ${idx}::timestamptz")
            params.append(since)
            idx += 1

        if from_peer:
            # Resolve alias to node_rid — return empty if unresolvable
            resolved = await _resolve_recipient(conn, from_peer)
            if not resolved:
                return {"documents": [], "count": 0, "error": f"Unknown peer: {from_peer}"}
            conditions.append(f"sender_node = ${idx}")
            params.append(resolved)
            idx += 1

        where = " AND ".join(conditions)
        try:
            rows = await conn.fetch(
                f"""SELECT event_id, event_type, document_rid, sender_node, sender_name,
                           manifest, contents, message, received_at, status,
                           recipient_type, intake_status, reviewed_at, reviewed_by, review_notes
                    FROM koi_shared_documents
                    WHERE {where}
                    ORDER BY received_at DESC
                    LIMIT ${idx}""",
                *params, limit,
            )
        except Exception as exc:
            # Backward compatibility for pre-047 schema.
            if "recipient_type" not in str(exc) and "intake_status" not in str(exc):
                raise
            rows = await conn.fetch(
                f"""SELECT event_id, event_type, document_rid, sender_node, sender_name,
                           manifest, contents, message, received_at, status
                    FROM koi_shared_documents
                    WHERE {where}
                    ORDER BY received_at DESC
                    LIMIT ${idx}""",
                *params, limit,
            )

        items = []
        for r in rows:
            contents = json.loads(r["contents"]) if isinstance(r["contents"], str) else r["contents"]
            manifest = json.loads(r["manifest"]) if isinstance(r["manifest"], str) else r["manifest"]
            share_meta = contents.get("_koi_share_meta", {}) if isinstance(contents, dict) else {}
            share_mode = (
                share_meta.get("share_mode")
                if isinstance(share_meta, dict)
                else None
            ) or ((manifest or {}).get("share_mode") if isinstance(manifest, dict) else None)
            context_depth = (
                share_meta.get("context_depth")
                if isinstance(share_meta, dict)
                else None
            ) or ((manifest or {}).get("context_depth") if isinstance(manifest, dict) else None)
            recipient_type = (
                share_meta.get("recipient_type")
                if isinstance(share_meta, dict)
                else None
            ) or ((manifest or {}).get("recipient_type") if isinstance(manifest, dict) else None)
            if not recipient_type:
                recipient_type = r.get("recipient_type") or "peer"
            references_summary = (
                (manifest or {}).get("references_summary")
                if isinstance(manifest, dict)
                else None
            )
            dependency_graph_summary = (
                (manifest or {}).get("dependency_graph_summary")
                if isinstance(manifest, dict)
                else None
            )
            missing_references = (
                (manifest or {}).get("missing_references")
                if isinstance(manifest, dict)
                else None
            )
            if not missing_references and isinstance(manifest, dict):
                dep_graph = manifest.get("dependency_graph")
                if isinstance(dep_graph, dict):
                    maybe_missing = dep_graph.get("missing_references")
                    if isinstance(maybe_missing, list):
                        missing_references = maybe_missing
            items.append({
                "event_id": str(r["event_id"]) if r["event_id"] else None,
                "event_type": r["event_type"],
                "document_rid": r["document_rid"],
                "sender": r["sender_name"] or r["sender_node"],
                "sender_node_rid": r["sender_node"],
                "manifest": manifest,
                "recipient_type": recipient_type,
                "share_mode": share_mode,
                "context_depth": context_depth,
                "references_summary": references_summary,
                "dependency_graph_summary": dependency_graph_summary,
                "missing_references_count": len(missing_references)
                if isinstance(missing_references, list)
                else 0,
                "missing_references": missing_references[:50]
                if isinstance(missing_references, list)
                else [],
                "has_contents": contents is not None,
                "message": r["message"],
                "received_at": r["received_at"].isoformat() if r["received_at"] else None,
                "status": r["status"],
                "intake_status": r.get("intake_status"),
                "reviewed_at": (
                    r["reviewed_at"].isoformat() if r.get("reviewed_at") else None
                ),
                "reviewed_by": r.get("reviewed_by"),
                "review_notes": r.get("review_notes"),
            })

    return {"documents": items, "count": len(items)}


@koi_net_router.get("/commons/intake")
async def commons_intake(
    request: Request,
    status: str = "staged",
    from_peer: Optional[str] = None,
    limit: int = 50,
):
    """List incoming commons shares and their intake status."""
    auth_err = _enforce_commons_admin(request)
    if auth_err:
        return auth_err

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    normalized_status = (status or "staged").strip().lower()
    allowed_status = {"staged", "approved", "ingesting", "needs_merge_review", "ingested", "failed", "rejected", "all"}
    if normalized_status not in allowed_status:
        return _protocol_error(
            400,
            "INVALID_INTAKE_STATUS",
            f"Invalid status '{status}'. Valid: staged, approved, ingesting, needs_merge_review, ingested, failed, rejected, all",
        )

    async with _db_pool.acquire() as conn:
        conditions = ["recipient_type = 'commons'", "status != 'retracted'"]
        params: list = []
        idx = 1

        if normalized_status != "all":
            conditions.append(f"intake_status = ${idx}")
            params.append(normalized_status)
            idx += 1

        if from_peer:
            resolved = await _resolve_recipient(conn, from_peer)
            if not resolved:
                return {"documents": [], "count": 0, "error": f"Unknown peer: {from_peer}"}
            conditions.append(f"sender_node = ${idx}")
            params.append(resolved)
            idx += 1

        where = " AND ".join(conditions)
        try:
            rows = await conn.fetch(
                f"""SELECT id, event_id, event_type, document_rid, sender_node, sender_name,
                           manifest, contents, message, received_at, status,
                           intake_status, reviewed_at, reviewed_by, review_notes
                    FROM koi_shared_documents
                    WHERE {where}
                    ORDER BY received_at DESC
                    LIMIT ${idx}""",
                *params, limit,
            )
        except asyncpg.PostgresError as exc:
            if isinstance(
                exc,
                (asyncpg.exceptions.UndefinedColumnError, asyncpg.exceptions.UndefinedTableError),
            ):
                return _protocol_error(
                    503,
                    "COMMONS_INTAKE_SCHEMA_MISSING",
                    "Commons intake fields are missing. Apply migration 047_shared_documents_intake.sql",
                )
            raise

    docs = []
    for r in rows:
        manifest = json.loads(r["manifest"]) if isinstance(r["manifest"], str) else r["manifest"]
        contents = json.loads(r["contents"]) if isinstance(r["contents"], str) else r["contents"]
        docs.append(
            {
                "id": r["id"],
                "event_id": str(r["event_id"]) if r["event_id"] else None,
                "event_type": r["event_type"],
                "document_rid": r["document_rid"],
                "sender": r["sender_name"] or r["sender_node"],
                "sender_node_rid": r["sender_node"],
                "message": r["message"],
                "recipient_type": "commons",
                "status": r["status"],
                "intake_status": r["intake_status"],
                "received_at": r["received_at"].isoformat() if r["received_at"] else None,
                "reviewed_at": r["reviewed_at"].isoformat() if r["reviewed_at"] else None,
                "reviewed_by": r["reviewed_by"],
                "review_notes": r["review_notes"],
                "manifest": manifest,
                "has_contents": contents is not None,
            }
        )

    return {"documents": docs, "count": len(docs), "status_filter": normalized_status}


@koi_net_router.post("/commons/intake/decide")
async def commons_intake_decide(request: Request):
    """Approve/reject a staged commons share entry.

    Records an immutable decision in koi_commons_decisions, then updates
    the mutable intake_status on koi_shared_documents. On approve, status
    becomes 'approved' (async ingest worker picks it up later).
    """
    auth_err = _enforce_commons_admin(request)
    if auth_err:
        return auth_err

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    try:
        raw = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")

    try:
        req = CommonsIntakeDecisionRequest(**raw)
    except Exception as exc:
        return _protocol_error(400, "INVALID_REQUEST", f"Invalid request: {exc}")

    if not req.share_id and not req.event_id:
        return _protocol_error(
            400,
            "MISSING_IDENTIFIER",
            "Provide either share_id or event_id",
        )

    parsed_event_id: Optional[str] = None
    if req.event_id:
        try:
            parsed_event_id = str(_uuid.UUID(req.event_id))
        except Exception:
            return _protocol_error(400, "INVALID_EVENT_ID", "event_id must be a valid UUID")

    action = (req.action or "").strip().lower()
    if action not in {"approve", "reject"}:
        return _protocol_error(
            400,
            "INVALID_ACTION",
            "action must be 'approve' or 'reject'",
        )

    # approved = async ingest worker picks it up; rejected = terminal
    next_intake_status = "approved" if action == "approve" else "rejected"

    row = None
    try:
        async with _db_pool.acquire() as conn:
            async with conn.transaction():
                # 1. Update mutable status on shared document
                if req.share_id:
                    row = await conn.fetchrow(
                        """
                        UPDATE koi_shared_documents
                        SET intake_status = $2,
                            reviewed_at = NOW(),
                            reviewed_by = COALESCE($3, reviewed_by),
                            review_notes = $4
                        WHERE id = $1
                          AND recipient_type = 'commons'
                          AND status != 'retracted'
                        RETURNING id, event_id, document_rid, sender_node, intake_status, status
                        """,
                        req.share_id,
                        next_intake_status,
                        req.reviewer,
                        req.note,
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        UPDATE koi_shared_documents
                        SET intake_status = $2,
                            reviewed_at = NOW(),
                            reviewed_by = COALESCE($3, reviewed_by),
                            review_notes = $4
                        WHERE event_id = $1::UUID
                          AND recipient_type = 'commons'
                          AND status != 'retracted'
                        RETURNING id, event_id, document_rid, sender_node, intake_status, status
                        """,
                        parsed_event_id,
                        next_intake_status,
                        req.reviewer,
                        req.note,
                    )

                if not row:
                    # No-op UPDATE — raise to trigger rollback (nothing to commit)
                    raise ValueError("INTAKE_NOT_FOUND")

                # 2. INSERT immutable decision record (audit trail)
                await conn.execute(
                    """
                    INSERT INTO koi_commons_decisions (share_id, event_id, action, reviewer, note)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    row["id"],
                    row["event_id"],
                    action,
                    req.reviewer,
                    req.note,
                )
    except ValueError as exc:
        if str(exc) == "INTAKE_NOT_FOUND":
            return _protocol_error(404, "INTAKE_NOT_FOUND", "No matching commons intake entry found")
        raise
    except asyncpg.PostgresError as exc:
        if isinstance(
            exc,
            (asyncpg.exceptions.UndefinedColumnError, asyncpg.exceptions.UndefinedTableError),
        ):
            return _protocol_error(
                503,
                "COMMONS_INTAKE_SCHEMA_MISSING",
                "Commons intake/decision tables are missing. Apply migrations 051 + 053.",
            )
        raise

    return {
        "status": "ok",
        "action": action,
        "id": row["id"],
        "event_id": str(row["event_id"]) if row["event_id"] else None,
        "document_rid": row["document_rid"],
        "sender_node": row["sender_node"],
        "intake_status": row["intake_status"],
        "record_status": row["status"],
    }


@koi_net_router.get("/commons/intake/{share_id}/decisions")
async def commons_intake_decisions(request: Request, share_id: int):
    """Get the immutable decision audit trail for a commons share."""
    auth_err = _enforce_commons_admin(request)
    if auth_err:
        return auth_err

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    async with _db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT id, share_id, event_id, action, reviewer, note, decided_at
                FROM koi_commons_decisions
                WHERE share_id = $1
                ORDER BY decided_at ASC
                """,
                share_id,
            )
        except asyncpg.PostgresError as exc:
            if isinstance(exc, asyncpg.exceptions.UndefinedTableError):
                return _protocol_error(
                    503,
                    "COMMONS_DECISIONS_SCHEMA_MISSING",
                    "koi_commons_decisions table missing. Apply migration 053.",
                )
            raise

    decisions = [
        {
            "id": str(r["id"]),
            "share_id": r["share_id"],
            "event_id": str(r["event_id"]) if r["event_id"] else None,
            "action": r["action"],
            "reviewer": r["reviewer"],
            "note": r["note"],
            "decided_at": r["decided_at"].isoformat() if r["decided_at"] else None,
        }
        for r in rows
    ]

    return {"decisions": decisions, "count": len(decisions), "share_id": share_id}


@koi_net_router.get("/commons/intake/{share_id}/merge-candidates")
async def commons_merge_candidates(request: Request, share_id: int):
    """Get merge candidates for a commons share that needs merge review."""
    auth_err = _enforce_commons_admin(request)
    if auth_err:
        return auth_err

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    try:
        async with _db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, share_id, remote_entity_label, remote_entity_type,
                       local_entity_uri, local_entity_label, confidence,
                       resolution, resolved_by, resolved_at, created_at
                FROM koi_commons_merge_candidates
                WHERE share_id = $1
                ORDER BY resolution IS NOT NULL, confidence DESC
                """,
                share_id,
            )
    except asyncpg.PostgresError as exc:
        if isinstance(exc, asyncpg.exceptions.UndefinedTableError):
            return _protocol_error(
                503,
                "MERGE_CANDIDATES_SCHEMA_MISSING",
                "koi_commons_merge_candidates table missing. Apply migration 054.",
            )
        raise

    candidates = [
        {
            "id": r["id"],
            "share_id": r["share_id"],
            "remote_entity_label": r["remote_entity_label"],
            "remote_entity_type": r["remote_entity_type"],
            "local_entity_uri": r["local_entity_uri"],
            "local_entity_label": r["local_entity_label"],
            "confidence": r["confidence"],
            "resolution": r["resolution"],
            "resolved_by": r["resolved_by"],
            "resolved_at": r["resolved_at"].isoformat() if r["resolved_at"] else None,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]

    unresolved = sum(1 for c in candidates if c["resolution"] is None)

    return {
        "candidates": candidates,
        "count": len(candidates),
        "unresolved": unresolved,
        "share_id": share_id,
    }


@koi_net_router.post("/commons/intake/{share_id}/resolve-merges")
async def commons_resolve_merges(request: Request, share_id: int):
    """Resolve ambiguous entity merge candidates for a commons share.

    After the ingest worker sets intake_status = 'needs_merge_review',
    an admin resolves each candidate (merge, keep_separate, cross_ref).
    When all candidates are resolved, the share transitions to 'ingested'.
    """
    auth_err = _enforce_commons_admin(request)
    if auth_err:
        return auth_err

    if not _db_pool:
        return _protocol_error(503, "NOT_INITIALIZED", "KOI-net not initialized")

    try:
        raw = await request.json()
    except Exception:
        return _protocol_error(400, "INVALID_JSON", "Invalid JSON body")

    resolutions = raw.get("resolutions", [])
    if not resolutions:
        return _protocol_error(400, "NO_RESOLUTIONS", "Provide a list of resolutions")

    # Validate all resolutions before entering transaction
    valid_resolutions = ("merge", "keep_separate", "cross_ref")
    for res in resolutions:
        resolution = res.get("resolution")
        if resolution not in valid_resolutions:
            return _protocol_error(
                400, "INVALID_RESOLUTION",
                f"resolution must be merge, keep_separate, or cross_ref (got: {resolution})",
            )

    resolved_count = 0
    unresolved = 0
    async with _db_pool.acquire() as conn:
        async with conn.transaction():
            for res in resolutions:
                candidate_id = res.get("candidate_id")
                resolution = res.get("resolution")
                resolved_by = res.get("resolved_by")

                updated = await conn.execute(
                    """
                    UPDATE koi_commons_merge_candidates
                    SET resolution = $2, resolved_by = $3, resolved_at = NOW()
                    WHERE id = $1 AND share_id = $4 AND resolution IS NULL
                    """,
                    candidate_id, resolution, resolved_by, share_id,
                )
                if "UPDATE 1" in updated:
                    resolved_count += 1

            # Check if all candidates are now resolved
            unresolved = await conn.fetchval(
                """
                SELECT COUNT(*) FROM koi_commons_merge_candidates
                WHERE share_id = $1 AND resolution IS NULL
                """,
                share_id,
            )

            share_finalized = False
            if unresolved == 0:
                # All resolved — transition share to ingested
                transition_result = await conn.execute(
                    """
                    UPDATE koi_shared_documents
                    SET intake_status = 'ingested'
                    WHERE id = $1 AND intake_status = 'needs_merge_review'
                    """,
                    share_id,
                )
                share_finalized = "UPDATE 1" in transition_result

    return {
        "status": "ok",
        "share_id": share_id,
        "resolved_count": resolved_count,
        "remaining_unresolved": unresolved,
        "share_finalized": share_finalized,
    }


async def _resolve_recipient(conn: asyncpg.Connection, recipient: str) -> Optional[str]:
    """Resolve a human-friendly name or alias to a node_rid.

    Search order:
    1. Exact match in koi_net_peer_aliases
    2. Exact match in koi_net_nodes.node_name
    3. If recipient looks like a full node_rid, use directly
    """
    # 1. Check aliases table
    alias_rid = await conn.fetchval(
        "SELECT node_rid FROM koi_net_peer_aliases WHERE LOWER(alias) = LOWER($1)",
        recipient,
    )
    if alias_rid:
        return alias_rid

    # 2. Check node_name
    node_rid = await conn.fetchval(
        "SELECT node_rid FROM koi_net_nodes WHERE LOWER(node_name) = LOWER($1) AND status = 'active'",
        recipient,
    )
    if node_rid:
        return node_rid

    # 3. If it looks like a full RID, use directly
    if recipient.startswith("orn:koi-net.node:"):
        exists = await conn.fetchval(
            "SELECT 1 FROM koi_net_nodes WHERE node_rid = $1", recipient,
        )
        if exists:
            return recipient

    return None


# =====================================================================
# VAULT SYNC ENDPOINTS
# =====================================================================


@koi_net_router.post("/vault-sync/configure")
async def vault_sync_configure(request: Request):
    """Configure vault sync for a peer."""
    err = _enforce_local_admin(request)
    if err:
        return err

    if not _vault_sync:
        return JSONResponse(
            status_code=400,
            content={"error": "Vault sync is not enabled (set VAULT_SYNC_ENABLED=true)"},
        )

    body = await request.json()
    peer = body.get("peer")
    shared_folder = body.get("shared_folder", "Shared")
    enabled = body.get("enabled", True)

    if not peer:
        return JSONResponse(status_code=400, content={"error": "peer is required"})

    result = await _vault_sync.configure(peer, shared_folder, enabled)
    if "error" in result:
        return JSONResponse(status_code=404, content=result)
    return JSONResponse(content=result)


@koi_net_router.delete("/vault-sync/peers/{peer_name}")
async def vault_sync_remove_peer(request: Request, peer_name: str):
    """Disable/remove a vault sync peer."""
    err = _enforce_local_admin(request)
    if err:
        return err

    if not _vault_sync:
        return JSONResponse(
            status_code=400,
            content={"error": "Vault sync is not enabled (set VAULT_SYNC_ENABLED=true)"},
        )

    result = await _vault_sync.unconfigure(peer_name)
    if "error" in result:
        return JSONResponse(status_code=404, content=result)
    return JSONResponse(content=result)


@koi_net_router.get("/vault-sync/status")
async def vault_sync_status(request: Request):
    """Get vault sync dashboard info."""
    err = _enforce_local_admin(request)
    if err:
        return err

    if not _vault_sync:
        return JSONResponse(content={"enabled": False, "reason": "VAULT_SYNC_ENABLED not set"})

    status = await _vault_sync.get_status()
    return JSONResponse(content=status)


@koi_net_router.post("/vault-sync/trigger")
async def vault_sync_trigger(request: Request):
    """Force an immediate sync cycle (for testing). Scans all configured peers."""
    err = _enforce_local_admin(request)
    if err:
        return err

    if not _vault_sync:
        return JSONResponse(
            status_code=400,
            content={"error": "Vault sync is not enabled"},
        )

    result = await _vault_sync.trigger_sync()
    return JSONResponse(content=result)


def _bool_env_raw(name: str, default: bool = False) -> bool:
    """Check env var as bool (standalone, no side effects)."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@koi_net_router.post("/vault-sync/reconcile")
async def vault_sync_reconcile(request: Request):
    """Run reconciliation: detect or repair drift between DB and filesystem."""
    err = _enforce_local_admin(request)
    if err:
        return err

    if not _vault_sync:
        return JSONResponse(
            status_code=400,
            content={"error": "Vault sync is not enabled"},
        )

    body = await request.json()
    mode = body.get("mode", "detect")

    if mode == "repair":
        if not _bool_env_raw("VAULT_SYNC_REPAIR_ENABLED", False):
            return JSONResponse(
                status_code=403,
                content={"error": "repair mode disabled"},
            )

    if mode not in ("detect", "repair"):
        return JSONResponse(
            status_code=400,
            content={"error": f"invalid mode: {mode}. Use 'detect' or 'repair'."},
        )

    try:
        result = await _vault_sync.reconcile(
            mode=mode,
            confirm=body.get("confirm", False),
            paths=body.get("paths"),
            max_actions=body.get("max_actions", 50),
            peer_rid=body.get("peer"),
        )
    except VaultUnavailableError as e:
        return JSONResponse(
            status_code=503,
            content={"error": str(e)},
        )

    return JSONResponse(content=result)
