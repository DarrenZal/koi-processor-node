"""
Database-backed KOI-net Event Queue

Uses the koi_net_events table (migration 039) for event persistence.
Supports add, poll, peek, mark_delivered, confirm, and cleanup operations with per-edge TTL.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg
from rid_lib.core import RID as _RidLibRID
from rid_lib.ext import Bundle as _RidLibBundle

from api.federation_blocks import FederationBlocksUnavailable, blocked_hashes, is_blocked, rid_sha256
from api.koi_protocol import EventType, WireEvent, WireManifest, timestamp_to_z_format

logger = logging.getLogger(__name__)

# Default TTL for events (hours)
DEFAULT_TTL_HOURS = 24
REMOTE_TTL_HOURS = 72


class EventQueue:
    """Database-backed event queue for KOI-net protocol."""

    def __init__(self, pool: asyncpg.Pool, node_rid: str):
        self.pool = pool
        self.node_rid = node_rid

    async def add(
        self,
        event_type: str,
        rid: str,
        manifest: Optional[Dict[str, Any]] = None,
        contents: Optional[Dict[str, Any]] = None,
        source_node: Optional[str] = None,
        ttl_hours: int = DEFAULT_TTL_HOURS,
        event_id: Optional[str] = None,
        target_node: Optional[str] = None,
    ) -> Optional[str]:
        """Add an event to the queue. Returns the event_id.

        If event_id is provided (inbound from a peer), it is preserved and
        used for dedup via the UNIQUE(source_node, event_id, target_node) index.
        Returns None if the event was a duplicate (ON CONFLICT DO NOTHING).

        target_node: If set, only this node can receive the event (unicast).
                     If None, event is available to all polling nodes (broadcast).

        Returns None without queuing when the record is blocked from federation
        (migration 137), and raises FederationBlocksUnavailable when the block
        list is missing.
        """
        effective_source = source_node or self.node_rid
        async with self.pool.acquire() as conn:
            if is_blocked(await blocked_hashes(conn), rid, contents, manifest):
                logger.warning(f"Withheld {event_type} event: record {rid_sha256(rid)[:12]} is blocked from federation")
                return None
            if event_id:
                # Inbound event with sender-assigned event_id — dedup on insert
                row = await conn.fetchrow(
                    """
                    INSERT INTO koi_net_events
                        (event_id, event_type, rid, manifest, contents, source_node, target_node, expires_at)
                    VALUES
                        ($1::UUID, $2, $3, $4, $5, $6, $7, NOW() + ($8 || ' hours')::INTERVAL)
                    ON CONFLICT (source_node, event_id, COALESCE(target_node, ''))
                        WHERE event_id IS NOT NULL DO NOTHING
                    RETURNING event_id::TEXT
                    """,
                    event_id,
                    event_type,
                    rid,
                    json.dumps(manifest) if manifest else None,
                    json.dumps(contents) if contents else None,
                    effective_source,
                    target_node,
                    str(ttl_hours),
                )
                if row is None:
                    logger.debug(f"Duplicate event {event_id} from {effective_source}, skipped")
                    return None
                logger.info(f"Queued {event_type} event for {rid} (id={event_id}, target={target_node})")
                return event_id
            else:
                # Locally generated event — DB assigns event_id
                row = await conn.fetchrow(
                    """
                    INSERT INTO koi_net_events
                        (event_type, rid, manifest, contents, source_node, target_node, expires_at)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, NOW() + ($7 || ' hours')::INTERVAL)
                    RETURNING event_id::TEXT
                    """,
                    event_type,
                    rid,
                    json.dumps(manifest) if manifest else None,
                    json.dumps(contents) if contents else None,
                    effective_source,
                    target_node,
                    str(ttl_hours),
                )
                new_id = row["event_id"]
                logger.info(f"Queued {event_type} event for {rid} (id={new_id}, target={target_node})")
                return new_id

    async def poll(
        self,
        requesting_node: str,
        limit: int = 50,
        rid_types: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Poll for events not yet delivered to the requesting node.

        Returns list of event dicts with event_id, event_type, rid, manifest, contents.
        Marks events as delivered_to this node. Blocked records (migration 137) are
        withheld, and nothing is served while the block list is missing.
        """
        async with self.pool.acquire() as conn:
            try:
                blocked = await blocked_hashes(conn)
            except FederationBlocksUnavailable:
                logger.error(f"Poll from {requesting_node}: federation_rid_blocks is missing, serving nothing")
                return []
            # Fetch events not yet delivered to this node and not expired.
            # target_node scoping: NULL = broadcast (visible to all), non-NULL = unicast.
            rows = await conn.fetch(
                """
                SELECT id, event_id::TEXT, event_type, rid, manifest, contents, source_node, target_node, queued_at
                FROM koi_net_events
                WHERE NOT ($1 = ANY(delivered_to))
                  AND expires_at > NOW()
                  AND (target_node IS NULL OR target_node = $1)
                ORDER BY queued_at ASC
                LIMIT $2
                """,
                requesting_node,
                limit,
            )

            if not rows:
                return []

            events = []
            ids_to_mark = []
            withheld = 0

            for row in rows:
                # A blocked record is withheld for good, like an edge-filtered event:
                # marked so it cannot sit at the head of this peer's queue, and not
                # transmitted.
                if is_blocked(blocked, row["rid"], row["contents"], row["manifest"]):
                    ids_to_mark.append(row["id"])
                    withheld += 1
                    continue

                # Edge scope: api.event_queue.edge_admits (divergences 4, 9, 10, 21, 23).
                # It runs for every row, also when the edge declares no rid_types,
                # because another node's node and edge objects are never relayed.
                excluded = not (self._own_unicast_protocol_object(row, requesting_node)
                                or edge_admits(rid_types, row["rid"], row["contents"], row["source_node"], self.node_rid))
                if excluded:
                    # Mark excluded events delivered rather than skipping
                    # them. A filter decision is PERMANENT for this peer,
                    # not a deferral — if we leave them unmarked they stay
                    # at the head of the peer's queue (ORDER BY queued_at
                    # ASC LIMIT n) and every subsequent poll re-reads and
                    # re-drops the same rows, returning an empty list
                    # forever. That starves the peer of everything behind
                    # them until the 24h TTL lapses.
                    ids_to_mark.append(row["id"])
                    continue

                event = {
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "rid": row["rid"],
                    "manifest": json.loads(row["manifest"]) if row["manifest"] else None,
                    "contents": json.loads(row["contents"]) if row["contents"] else None,
                    "source_node": row["source_node"],
                    "queued_at": row["queued_at"].isoformat() if row["queued_at"] else None,
                }
                events.append(event)
                ids_to_mark.append(row["id"])

            # Mark as delivered to this node
            if ids_to_mark:
                await conn.execute(
                    """
                    UPDATE koi_net_events
                    SET delivered_to = array_append(delivered_to, $1)
                    WHERE id = ANY($2)
                    """,
                    requesting_node,
                    ids_to_mark,
                )
                logger.info(
                    f"Delivered {len(events)} events to {requesting_node}"
                    + (f", withheld {withheld} blocked" if withheld else "")
                )

            return events

    async def peek_undelivered(
        self,
        target_node: str,
        limit: int = 50,
        rid_types: Optional[List[str]] = None,
        mark_excluded: bool = False,
    ) -> List[Dict[str, Any]]:
        """Get events not yet delivered to target_node WITHOUT marking them.

        Returns list of event dicts with event_id for later marking.
        Used by WEBHOOK push delivery (peek -> push -> mark pattern). Blocked
        records (migration 137) are skipped, and nothing is returned while the
        block list is missing.
        """
        async with self.pool.acquire() as conn:
            try:
                blocked = await blocked_hashes(conn)
            except FederationBlocksUnavailable:
                logger.error(f"Peek for {target_node}: federation_rid_blocks is missing, pushing nothing")
                return []
            # Same target_node scoping as poll(): NULL = broadcast, non-NULL = unicast.
            rows = await conn.fetch(
                """
                SELECT id, event_id::TEXT, event_type, rid, manifest, contents, source_node, target_node, queued_at
                FROM koi_net_events
                WHERE NOT ($1 = ANY(delivered_to))
                  AND expires_at > NOW()
                  AND (target_node IS NULL OR target_node = $1)
                ORDER BY queued_at ASC
                LIMIT $2
                """,
                target_node,
                limit,
            )

            if not rows:
                return []

            events = []
            excluded_ids = []
            for row in rows:
                # Same scope rule as poll(). peek is read-only by default: nothing is marked.
                # The WEBHOOK pusher passes mark_excluded=True, so a blocked or excluded row is
                # marked delivered as poll() marks it, and cannot sit at the head of a narrow
                # edge's queue and starve it. The rows returned are never marked here; the
                # caller marks them after the push succeeds (peek -> push -> mark).
                if is_blocked(blocked, row["rid"], row["contents"], row["manifest"]) or not (
                    self._own_unicast_protocol_object(row, target_node)
                    or edge_admits(rid_types, row["rid"], row["contents"], row["source_node"], self.node_rid)
                ):
                    excluded_ids.append(row["id"])
                    continue

                events.append({
                    "event_id": row["event_id"],
                    "event_type": row["event_type"],
                    "rid": row["rid"],
                    "manifest": json.loads(row["manifest"]) if row["manifest"] else None,
                    "contents": json.loads(row["contents"]) if row["contents"] else None,
                    "source_node": row["source_node"],
                    "queued_at": row["queued_at"].isoformat() if row["queued_at"] else None,
                })

            if mark_excluded and excluded_ids:
                await conn.execute(
                    "UPDATE koi_net_events SET delivered_to = array_append(delivered_to, $1) WHERE id = ANY($2)",
                    target_node,
                    excluded_ids,
                )
            return events

    def _own_unicast_protocol_object(self, row, requesting_node: str) -> bool:
        """Our own node or edge object queued for this peer alone: first contact and edge
        negotiation for a partial node (api/koi_net_negotiation.py). Delivered whatever the
        edge's rid_types declare, as koi-net sends negotiation directly."""
        return (row["target_node"] == requesting_node and row["source_node"] == self.node_rid
                and is_protocol_rid(row["rid"]))

    async def poll_own_protocol_objects(self, requesting_node: str, limit: int = 50) -> List[Dict[str, Any]]:
        """For a peer with no approved edge under the membrane (decision D1): only our own
        node and edge objects queued for it alone, marked delivered. Nothing else is read or
        marked, so its other events wait for an approved edge."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, event_id::TEXT, event_type, rid, manifest, contents, source_node, queued_at
                FROM koi_net_events
                WHERE target_node = $1 AND source_node = $2
                  AND NOT ($1 = ANY(delivered_to)) AND expires_at > NOW()
                  AND (lower(rid) LIKE 'orn:koi-net.node:%' OR lower(rid) LIKE 'orn:koi-net.edge:%')
                ORDER BY queued_at ASC
                LIMIT $3
                """,
                requesting_node, self.node_rid, limit,
            )
            events = [{
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "rid": row["rid"],
                "manifest": json.loads(row["manifest"]) if row["manifest"] else None,
                "contents": json.loads(row["contents"]) if row["contents"] else None,
                "source_node": row["source_node"],
                "queued_at": row["queued_at"].isoformat() if row["queued_at"] else None,
            } for row in rows if is_protocol_rid(row["rid"])]
            if rows:
                await conn.execute(
                    "UPDATE koi_net_events SET delivered_to = array_append(delivered_to, $1) WHERE id = ANY($2)",
                    requesting_node, [row["id"] for row in rows],
                )
            return events

    async def mark_delivered(self, event_ids: List[str], target_node: str) -> int:
        """Mark specific events as delivered to target_node.

        Returns count of events actually marked (for verification).
        Idempotent — marking an already-delivered event is a no-op.
        """
        if not event_ids:
            return 0

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE koi_net_events
                SET delivered_to = array_append(delivered_to, $1)
                WHERE event_id::TEXT = ANY($2)
                  AND NOT ($1 = ANY(delivered_to))
                  AND expires_at > NOW()
                """,
                target_node,
                event_ids,
            )
            count = int(result.split()[-1])
            if count > 0:
                logger.info(f"Marked {count} events as delivered to {target_node}")
            return count

    async def confirm(
        self,
        event_ids: List[str],
        confirming_node: str,
    ) -> int:
        """Confirm receipt of events by a node. Returns count confirmed."""
        if not event_ids:
            return 0

        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE koi_net_events
                SET confirmed_by = array_append(confirmed_by, $1)
                WHERE event_id::TEXT = ANY($2)
                  AND NOT ($1 = ANY(confirmed_by))
                """,
                confirming_node,
                event_ids,
            )
            count = int(result.split()[-1])
            logger.info(f"Confirmed {count} events from {confirming_node}")
            return count

    async def cleanup(self) -> int:
        """Delete expired events. Returns count deleted."""
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM koi_net_events
                WHERE expires_at < NOW()
                """
            )
            count = int(result.split()[-1])
            if count > 0:
                logger.info(f"Cleaned up {count} expired events")
            return count

    async def get_queue_size(self) -> int:
        """Get current number of active (non-expired) events."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) as cnt FROM koi_net_events WHERE expires_at > NOW()"
            )
            return row["cnt"]


# The two vault-file namespaces are one RID type while divergence 1's migration
# (koi-net.vault-file -> personal-koi.vault-file) is open, so an edge naming either
# receives both, as the bare name "Vault-file" always has.
_VAULT_FILE_CONTEXTS = frozenset({"orn:koi-net.vault-file", "orn:personal-koi.vault-file"})
NODE_CONTEXT = "orn:koi-net.node"
EDGE_CONTEXT = "orn:koi-net.edge"
_PROTOCOL_PREFIXES = (NODE_CONTEXT + ":", EDGE_CONTEXT + ":")


def is_protocol_rid(rid: Any) -> bool:
    """Whether `rid` names a node or edge object, decided on the string itself.

    A prefix test rather than rid-lib's parse, so a malformed reference
    (`orn:koi-net.node:peer`, no hash) or odd casing (`orn:KOI-NET.node:...`) still counts.
    """
    return isinstance(rid, str) and rid.strip().lower().startswith(_PROTOCOL_PREFIXES)


def rid_context(rid: Any) -> Optional[str]:
    """The RID type of `rid` as rid-lib names it (`orn:koi-net.vault-file`), or None."""
    try:
        return _RidLibRID.from_string(rid).context
    except (ValueError, TypeError, AttributeError):
        return None


def _declared_type_matches(declared: str, context: str, legacy_type: Optional[str]) -> bool:
    """One entry of an edge's rid_types against one event's RID type.

    Two forms are accepted. An upstream RID type (`orn:koi-net.vault-file`, as koi-net
    edges carry and as our provides.event now advertises) must match the event's RID type
    exactly. A bare name (`Vault-file`, `Person`: what edges negotiated before this change
    hold) matches the last segment of the namespace, as before (divergence 13).
    """
    entry = declared.strip()
    if ":" in entry:
        entry = entry.lower()
        if entry == context.lower():
            return True
        return entry in _VAULT_FILE_CONTEXTS and context in _VAULT_FILE_CONTEXTS
    return legacy_type is not None and legacy_type.lower() == entry.lower()


def edge_admits(
    rid_types: Optional[List[str]],
    rid: Any,
    contents: Any,
    source_node: Optional[str],
    own_node: str,
) -> bool:
    """Whether an edge declaring `rid_types` may be served this event. Fails closed.

    - Domain events (contents carry `_koi_domain`) match on the domain name (divergence 9).
    - `rid_types is None` means the edge declares no scope; `[]` declares nothing (4).
    - A RID that does not parse matches nothing (10, 13).
    - Node and edge objects (`orn:koi-net.node`, `orn:koi-net.edge`) are served only when
      this node is their source, whatever the edge declares. Our broadcast table also
      holds events peers pushed to us (divergence 23), so without this a stock node's
      handshake bundle would be relayed to every peer whose edge names node objects.
    """
    # First, before anything else can admit it: another node's node or edge object.
    if is_protocol_rid(rid) and source_node != own_node:
        return False
    c = contents
    if isinstance(c, str):
        try:
            c = json.loads(c)
        except ValueError:
            c = None
    if isinstance(c, dict) and "_koi_domain" in c:
        if rid_types is None:
            return True
        domain = c.get("_koi_domain")
        return bool(domain) and str(domain).lower() in [str(rt).lower() for rt in rid_types]

    context = rid_context(rid)
    if rid_types is None:
        return True
    if context is None:
        return False
    legacy_type = extract_rid_type(rid)
    return any(isinstance(d, str) and _declared_type_matches(d, context, legacy_type) for d in rid_types)


def lists_own_node(rid_types: Optional[List[str]]) -> bool:
    """Whether a /rids/fetch request for `rid_types` should list our own node RID."""
    if not rid_types:
        return True
    return any(isinstance(t, str) and t.strip().lower() == NODE_CONTEXT for t in rid_types)


def own_node_bundle(profile: Any) -> Dict[str, Any]:
    """Our node's own KoiNetNode bundle, as rid-lib builds it (divergence 15).

    Contents are the profile as we advertise it: the koi_net.protocol.node.NodeProfile
    fields (base_url, node_type, provides, public_key) plus our extensions
    (node_rid, node_name, encryption_key, ontology_uri, ontology_version), which upstream
    ignores. The manifest hash is rid-lib's hash of exactly those contents.
    """
    contents = profile.model_dump(exclude_none=True)
    bundle = _RidLibBundle.generate(_RidLibRID.from_string(profile.node_rid), contents)
    return bundle.model_dump(mode="json")


def extract_rid_type(rid: str) -> Optional[str]:
    """Extract the type segment of an ORN, using rid-lib's own parser.

    An ORN is `orn:<namespace>:<reference>`, and the type is the last
    dot-separated segment of the namespace:

        orn:koi-net.vault-file:Shared/x.md      -> Vault-file
        orn:personal-koi.entity:abc             -> Entity
        orn:obsidian.note:Shared/x.md           -> Note

    This replaced two hardcoded substring branches (`"koi-net." in rid`,
    `"entity:" in rid`). Substring-matching a structured identifier was wrong in
    three ways that only became dangerous once the poll filter began failing
    closed (divergence 10):

      * `orn:personal-koi.entity:abc` contains "entity:", so the old code
        returned 'Abc' — the slug, capitalized, presented as a type.
      * `orn:personal-koi.doclink:*` matched neither branch and returned None,
        which now means "excluded from every edge".
      * `orn:personal-koi.vault-file:*` likewise, which blocked the divergence-1
        migration off the squatted koi-net.* namespace: vault files would have
        become undeliverable the moment the namespace changed.

    Both the legacy `koi-net.vault-file` and the migration target
    `personal-koi.vault-file` resolve to `Vault-file`, so the cutover is a no-op
    for edge scoping.

    The `orn:entity:{type}/{slug}` form the second branch handled has zero rows
    on either node, so nothing depended on it.

    Returns None for anything unparseable — including non-str input, which the
    old code raised on. A raise here aborts the poll for every peer.
    """
    try:
        parsed = _RidLibRID.from_string(rid)
    except (ValueError, TypeError, AttributeError):
        return None
    namespace = getattr(parsed, "namespace", None)
    if not namespace or "." not in namespace:
        return None
    return namespace.rsplit(".", 1)[1].capitalize()
