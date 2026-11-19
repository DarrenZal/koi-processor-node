"""KOI-net first contact and edge negotiation as bundles.

Divergences 5, 7, 12, 18 and 20 in docs/architecture/koi-protocol-conformance.md.

A koi-net node joins by broadcasting FORGET + NEW of its own node bundle to its first
contact. It learns the contact's profile only when the contact sends it back, and it
subscribes by proposing a `koi-net.edge` bundle that the publisher approves. Before this
module our node queued those events as data, never answered, and never negotiated an edge
as a bundle (20). Here:

- A NEW/UPDATE of the sender's own node bundle is a first contact. We answer with our own
  node bundle (pushed to a full node, queued for a partial node's next poll), and, when
  KOI_NET_RID_TYPES_OF_INTEREST names a type the contact provides (D4), with a POLL
  subscription proposal.
- An edge bundle in which we are the publisher and the sender the subscriber is a
  subscription request. It gets koi-net's checks (rid_types within what we provide, plus
  node and edge objects; no webhook to a partial node), then stays PROPOSED until an
  operator approves it (`/koi-net/edges/approve`) unless KOI_NET_AUTO_APPROVE_EDGES is set
  (D1). The approval goes back as an UPDATE of the approved edge bundle, a refusal as a
  FORGET.
- An edge bundle approving a subscription we proposed marks it APPROVED.
- Node and edge objects never enter koi_net_events as data (divergence 23).

On the wire, edges are koi_net.protocol.edge.EdgeProfile: source, target, edge_type,
status in {PROPOSED, APPROVED}, rid_types; the edge RID is the section 3.2.1 hash,
sha256(source + target) (5, 12). Edges negotiated by /handshake before this keep their
local `edge_rid` strings; the wire RID is always derived from source and target.

Every wire object here is built with koi_net.protocol's own models (EdgeProfile,
generate_edge_bundle, Event, EventsPayload), because koi-net verifies a signature by
re-serialising the payload it parsed: what it builds, it reproduces.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional

import httpx
import api.koi_net_floor  # noqa: F401  (Python >= 3.12, before any koi_net import)
from koi_net.protocol.api.models import EventsPayload
from koi_net.protocol.api.paths import BROADCAST_EVENTS_PATH
from koi_net.protocol.edge import EdgeProfile, EdgeStatus, EdgeType, generate_edge_bundle
from koi_net.protocol.event import Event, EventType
from rid_lib.core import RID
from rid_lib.ext import Bundle
from rid_lib.ext.utils import sha256_hash
from rid_lib.types import KoiNetEdge, KoiNetNode

from api.event_queue import EDGE_CONTEXT, NODE_CONTEXT, is_protocol_rid
from api.koi_envelope import sign_envelope
from api.koi_net_barred import node_is_barred
from api.koi_protocol import koi_net_url, valid_peer_base_url

logger = logging.getLogger(__name__)

EDGE_PROFILE_CONTEXT = "orn:koi-net.context:koi-net.edge"
EDGE_TYPES = ("WEBHOOK", "POLL")
WIRE_STATUSES = ("PROPOSED", "APPROVED")
IMPLICIT_TYPES = (NODE_CONTEXT, EDGE_CONTEXT)  # spec 5.2: provided by every node
# An edge an operator revoked (remove-peer.sh, containment) or rejected, or a peer's FORGET
# revoked: a peer-initiated negotiation write never moves it (register 26).
HELD_EDGE_STATUSES = ("REVOKED", "REJECTED")
NEGOTIATION_BUNDLE = "bundle"


# -- configuration ------------------------------------------------------------------

def auto_approve_edges() -> bool:
    """KOI_NET_AUTO_APPROVE_EDGES: approve valid subscription proposals without an operator.
    Off by default (D1); the interop gate sets it for its membrane-off cases."""
    return os.getenv("KOI_NET_AUTO_APPROVE_EDGES", "false").strip().lower() in ("1", "true", "yes", "on")


def rid_types_of_interest() -> List[str]:
    """KOI_NET_RID_TYPES_OF_INTEREST: comma-separated RID types this node subscribes to
    when a full node contacts it (D4). Empty by default: the node proposes nothing."""
    raw = os.getenv("KOI_NET_RID_TYPES_OF_INTEREST", "")
    return [t.strip() for t in raw.split(",") if t.strip()]


# -- wire objects -------------------------------------------------------------------

def edge_rid_for(source: str, target: str) -> str:
    """Spec section 3.2.1, as koi_net.protocol.edge.generate_edge_bundle derives it."""
    return str(KoiNetEdge(sha256_hash(str(source) + str(target))))


def _rid_types(rid_types: List[str]) -> list:
    from rid_lib import RIDType
    return [RIDType.from_string(str(t)) for t in rid_types]


def edge_profile(source: str, target: str, edge_type: str, status: str, rid_types: List[str]) -> EdgeProfile:
    if status not in WIRE_STATUSES:
        raise ValueError(f"edge status {status!r} has no wire form (divergence 12)")
    if edge_type not in EDGE_TYPES:
        raise ValueError(f"edge type {edge_type!r} is not WEBHOOK or POLL")
    return EdgeProfile(source=KoiNetNode.from_string(str(source)), target=KoiNetNode.from_string(str(target)),
                       edge_type=EdgeType(edge_type), status=EdgeStatus(status), rid_types=_rid_types(rid_types))


def edge_contents(source: str, target: str, edge_type: str, status: str, rid_types: List[str]) -> Dict[str, Any]:
    """koi_net.protocol.edge.EdgeProfile as it goes on the wire."""
    return edge_profile(source, target, edge_type, status, rid_types).model_dump(mode="json")


def edge_bundle(source: str, target: str, edge_type: str, status: str, rid_types: List[str]) -> Dict[str, Any]:
    """A PROPOSED edge is koi-net's generate_edge_bundle; any other status is the EdgeProfile
    re-generated under the same RID, as koi-net's EdgeNegotiationHandler approves one."""
    if status == "PROPOSED":
        bundle = generate_edge_bundle(source=KoiNetNode.from_string(str(source)), target=KoiNetNode.from_string(str(target)),
                                      rid_types=_rid_types(rid_types), edge_type=EdgeType(edge_type))
    else:
        profile = edge_profile(source, target, edge_type, status, rid_types)
        bundle = Bundle.generate(RID.from_string(edge_rid_for(source, target)), profile.model_dump())
    return bundle.model_dump(mode="json")


def bundle_event(event_type: str, bundle: Dict[str, Any]) -> Dict[str, Any]:
    """koi_net.protocol.event.Event.from_bundle, dumped as koi-net dumps it."""
    return Event.from_bundle(EventType(event_type), Bundle.model_validate(bundle)).model_dump(mode="json", exclude_none=True)


def forget_event(rid: str) -> Dict[str, Any]:
    """koi_net.protocol.event.Event.from_rid(FORGET, rid)."""
    return Event.from_rid(EventType.FORGET, RID.from_string(str(rid))).model_dump(mode="json", exclude_none=True)


def events_payload(events: List[Dict[str, Any]]) -> Dict[str, Any]:
    return EventsPayload(events=[Event.model_validate(e) for e in events]).model_dump(mode="json", exclude_none=True)


def parse_edge_contents(contents: Any) -> Optional[Dict[str, Any]]:
    """An EdgeProfile from an event's contents (koi_net.protocol.edge.EdgeProfile validates
    it), as plain strings, or None when it is not one."""
    from pydantic import ValidationError

    if isinstance(contents, str):
        try:
            contents = json.loads(contents)
        except ValueError:
            return None
    if not isinstance(contents, dict):
        return None
    try:
        p = EdgeProfile.model_validate(contents)
    except ValidationError:
        return None
    return {"source": str(p.source), "target": str(p.target), "edge_type": p.edge_type.value,
            "status": p.status.value, "rid_types": [str(r) for r in p.rid_types]}


# -- decisions (pure) ---------------------------------------------------------------

def first_contact_allowed(event: Dict[str, Any], sender: str, own: str) -> bool:
    """What a node with no approved edge may broadcast under the membrane (D1): events for
    its own node object, and edge bundles between it and us."""
    rid = event.get("rid")
    if not isinstance(rid, str) or not is_protocol_rid(rid):
        return False
    if rid.lower().startswith(NODE_CONTEXT + ":"):
        return rid == sender
    if event.get("event_type") == "FORGET":
        return True  # resolved against edges between the sender and us, nothing else
    edge = parse_edge_contents(event.get("contents"))
    return bool(edge) and {edge["source"], edge["target"]} == {sender, own}


def _normalised(types: List[str]) -> set:
    return {str(t).strip().lower() for t in types}


def subscription_refusal(edge: Dict[str, Any], provides_event: List[str], peer: Dict[str, Any]) -> Optional[str]:
    """koi-net's EdgeNegotiationHandler checks, as a reason string, or None when acceptable."""
    if edge["edge_type"] == "WEBHOOK" and (peer.get("node_type") != "FULL" or not peer.get("base_url")):
        return "partial nodes cannot use webhooks"
    provided = _normalised(list(provides_event) + list(IMPLICIT_TYPES))
    missing = sorted(_normalised(edge["rid_types"]) - provided)
    if missing:
        return f"requested RID types not provided by this node: {missing}"
    return None


def _manifest_stamp(manifest: Any) -> Optional[str]:
    """A manifest's timestamp as fixed-width UTC text (text order is time order), or None."""
    from datetime import datetime, timezone
    raw = manifest.get("timestamp") if isinstance(manifest, dict) else None
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)  # rid-lib writes naive UTC timestamps too
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def interest_overlap(interest: List[str], contact_provides_event: List[str]) -> List[str]:
    """The RID types to subscribe to on a contact: our interest within what it provides,
    counting node and edge objects as provided by every node (spec 5.2)."""
    provided = _normalised(list(contact_provides_event or []) + list(IMPLICIT_TYPES))
    seen, out = set(), []
    for t in interest:
        key = t.strip().lower()
        if key in provided and key not in seen:
            seen.add(key)
            out.append(t.strip())
    return out


class ProtocolRateLimiter:
    """At most `limit` protocol events per source node per `window` seconds."""

    def __init__(self, limit: int = 20, window: float = 60.0):
        self.limit, self.window = limit, window
        self._seen: Dict[str, Deque[float]] = defaultdict(deque)

    def allow(self, node: str, n: int = 1, now: Optional[float] = None) -> bool:
        now = time.monotonic() if now is None else now
        q = self._seen[node]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) + n > self.limit:
            return False
        q.extend([now] * n)
        return True


# -- peer URLs: api.koi_protocol.valid_peer_base_url (shared with the poller and router) --


# -- effects ------------------------------------------------------------------------

REPLY_COOLDOWN_S = 10.0     # at most one first-contact reply per sender in this window
MAX_CONCURRENT_PUSHES = 8   # pushes in flight across all peers
MAX_PENDING_PUSHES = 64     # pushes queued behind them; beyond this a push is dropped
GLOBAL_PUSHES_PER_MINUTE = 120  # every identity is free to mint, so the budget is also global


@dataclass
class Negotiator:
    """Handles node and edge objects a peer broadcasts to us. One per router instance.

    Nothing is sent while a database connection is held: handlers collect what to send in
    an outbox and it is delivered after the connection is released (a queued delivery takes a
    second connection, and enough concurrent first contacts exhausted the pool).
    """

    pool: Any
    own_rid: str
    own_profile: Any
    private_key: Any
    event_queue: Any
    own_bundle: Any  # () -> dict, our KoiNetNode bundle
    membrane_on: Any = lambda: False  # () -> bool, KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL
    # () -> bool, KOI_NET_DEFER_UNKNOWN_HANDSHAKE (default true): a new edge waits for an operator.
    defer_new_edges: Any = lambda: True
    limiter: ProtocolRateLimiter = field(default_factory=ProtocolRateLimiter)
    push_budget: ProtocolRateLimiter = field(
        default_factory=lambda: ProtocolRateLimiter(limit=GLOBAL_PUSHES_PER_MINUTE, window=60.0))
    send_timeout: float = 15.0
    _tasks: set = field(default_factory=set)
    _last_reply: Dict[str, float] = field(default_factory=dict)
    _push_slots: Any = None

    # delivery

    async def _peer(self, conn, node_rid: str) -> Optional[Dict[str, Any]]:
        row = await conn.fetchrow(
            "SELECT node_rid, node_type, base_url, public_key, provides_event, status FROM koi_net_nodes WHERE node_rid = $1",
            node_rid,
        )
        return dict(row) if row else None

    async def deliver(self, peer: Dict[str, Any], events: List[Dict[str, Any]], operator: bool = False) -> None:
        """Send node and edge objects to a peer: pushed to a full node's /events/broadcast,
        queued for a partial node's next poll (koi-net partial nodes have no URL). Never call
        this while holding a connection."""
        if not events:
            return
        base_url = valid_peer_base_url(peer.get("base_url"), getattr(self.own_profile, "base_url", None))
        if peer.get("node_type") == "FULL" and base_url:
            # An operator's approval or rejection notice is not subject to the budget, so a
            # flood of fresh identities cannot block it.
            if not operator and (len(self._tasks) >= MAX_PENDING_PUSHES or not self.push_budget.allow("*")):
                logger.warning("negotiation: push budget spent; not sending %d event(s) to %s", len(events), peer["node_rid"])
                return
            task = asyncio.create_task(self._push(dict(peer, base_url=base_url), events))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
            return
        async with self.pool.acquire() as conn:
            pending = {(r["rid"], r["event_type"]) for r in await conn.fetch(
                """SELECT rid, event_type FROM koi_net_events
                   WHERE target_node = $1 AND source_node = $2 AND NOT ($1 = ANY(delivered_to)) AND expires_at > NOW()""",
                peer["node_rid"], self.own_rid)}
        for ev in events:
            if (ev["rid"], ev["event_type"]) in pending:
                continue  # already waiting for this peer's next poll
            await self.event_queue.add(
                event_type=ev["event_type"], rid=ev["rid"], manifest=ev.get("manifest"),
                contents=ev.get("contents"), target_node=peer["node_rid"],
            )

    async def _push(self, peer: Dict[str, Any], events: List[Dict[str, Any]]) -> None:
        if self._push_slots is None:
            self._push_slots = asyncio.Semaphore(MAX_CONCURRENT_PUSHES)
        async with self._push_slots:
            try:
                url = koi_net_url(peer["base_url"], BROADCAST_EVENTS_PATH)
                body = sign_envelope(events_payload(events), self.own_rid, peer["node_rid"], self.private_key)
                async with httpx.AsyncClient(timeout=self.send_timeout) as client:
                    resp = await client.post(url, content=json.dumps(body), headers={"Content-Type": "application/json"})
                if resp.status_code != 200:
                    logger.warning("negotiation: push of %d event(s) to %s: HTTP %s", len(events), peer["node_rid"], resp.status_code)
                else:
                    logger.info("negotiation: pushed %d event(s) to %s", len(events), peer["node_rid"])
            except Exception as exc:  # a background task must not die with an unhandled error
                logger.warning("negotiation: push to %s failed: %r", peer["node_rid"], exc)

    async def drain(self) -> None:
        """Wait for pushes in flight (tests and shutdown)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    # edges

    async def _edge_row(self, conn, source: str, target: str):
        return await conn.fetchrow(
            """SELECT id, edge_rid, edge_type, status, rid_types, metadata FROM koi_net_edges
               WHERE source_node = $1 AND target_node = $2
               ORDER BY (status = 'APPROVED') DESC, updated_at DESC NULLS LAST LIMIT 1""",
            source, target,
        )

    # Every write to koi_net_edges a peer can drive goes through the three methods below
    # (tests/test_koi_net_edge_writers.py fails on any other writer in api/ outside the operator
    # routes). Nothing serialises a peer's read-then-write against an operator's revoke
    # (remove-peer.sh, /edges/reject), so each write re-checks the stored state itself and
    # returns None when it no longer matches; a None never logs success or claims an approval.

    async def _peer_edge_update(self, conn, edge_id: int, set_sql: str, *values,
                                expect_status: Optional[str] = None) -> Optional[str]:
        """UPDATE one edge row for a peer: `set_sql` uses $3 onward for `values`. The row must
        still be `expect_status`, or, without one, not REVOKED or REJECTED. Returns its edge_rid,
        or None when the stored state no longer matches."""
        return await conn.fetchval(
            f"""UPDATE koi_net_edges SET {set_sql}, updated_at = NOW()
                WHERE id = $1
                  AND CASE WHEN $2::text IS NULL THEN status NOT IN ('REVOKED', 'REJECTED') ELSE status = $2::text END
                RETURNING edge_rid""",
            edge_id, expect_status, *values,
        )

    async def _peer_edge_insert(self, conn, source: str, target: str, edge_type: str, status: str,
                                rid_types: List[str], meta: str) -> Optional[str]:
        """INSERT a negotiated edge for a peer. A row that appeared meanwhile under the same RID
        is updated only if it is not REVOKED or REJECTED. Returns its edge_rid, or None."""
        return await conn.fetchval(
            """INSERT INTO koi_net_edges (edge_rid, source_node, target_node, edge_type, status, rid_types, metadata)
               VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
               ON CONFLICT (edge_rid) DO UPDATE SET edge_type = EXCLUDED.edge_type, status = EXCLUDED.status,
                   rid_types = EXCLUDED.rid_types,
                   metadata = COALESCE(koi_net_edges.metadata, '{}'::jsonb) || EXCLUDED.metadata, updated_at = NOW()
               WHERE koi_net_edges.status NOT IN ('REVOKED', 'REJECTED')
               RETURNING edge_rid""",
            edge_rid_for(source, target), source, target, edge_type, status, list(rid_types), meta,
        )

    async def _peer_forget_pair(self, conn, source: str, target: str) -> str:
        """A peer's FORGET of an edge revokes every row for the pair: /handshake may have added
        one under its own key. It only narrows, and an operator's REJECTED stays: a
        peer's FORGET must not turn it into a REVOKED it could then re-propose past."""
        return await conn.execute(
            """UPDATE koi_net_edges SET status = 'REVOKED', updated_at = NOW()
               WHERE source_node = $1 AND target_node = $2 AND status NOT IN ('REVOKED', 'REJECTED')""",
            source, target,
        )

    async def _upsert_edge(self, conn, source: str, target: str, edge_type: str, status: str,
                           rid_types: List[str], extra_meta: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Write a negotiated edge for the pair, or None when the pair is held.

        Every caller acts for a peer, so this never moves an edge out of REVOKED or REJECTED
       : with no APPROVED row for the pair, any REVOKED or REJECTED row holds it, since
        remove-peer.sh and a peer's FORGET revoke every row and /handshake may add another.
        Only an operator moves a held edge. An APPROVED row may still be rewritten, so a type
        change demotes it to PROPOSED (or keeps it APPROVED under auto-approve).
        """
        statuses = {r["status"] for r in await conn.fetch(
            "SELECT DISTINCT status FROM koi_net_edges WHERE source_node = $1 AND target_node = $2", source, target)}
        if "APPROVED" not in statuses and statuses & set(HELD_EDGE_STATUSES):
            return None
        row = await self._edge_row(conn, source, target)
        meta = json.dumps({"negotiation": NEGOTIATION_BUNDLE, **(extra_meta or {})})
        if row:
            # Atomic against an operator's concurrent revoke: the UPDATE itself refuses a held row.
            # Under READ COMMITTED the predicate is re-checked on the row version an operator
            # committed first; if the peer commits first, the operator's later UPDATE wins.
            # Either way the row ends held (T12).
            return await self._peer_edge_update(
                conn, row["id"], "edge_type = $3, status = $4, rid_types = $5, "
                "metadata = COALESCE(metadata, '{}'::jsonb) || $6::jsonb",
                edge_type, status, list(rid_types), meta,
            )
        # A row that appeared since the check above (a race) is held the same way.
        return await self._peer_edge_insert(conn, source, target, edge_type, status, rid_types, meta)

    async def send_edge_state(self, edge_row: Dict[str, Any]) -> None:
        """After an operator approves or rejects a bundle-negotiated subscription to us, tell
        the subscriber: UPDATE of the approved edge, or FORGET of its RID. Call it after the
        caller has released its connection."""
        meta = edge_row.get("metadata") or {}
        if isinstance(meta, str):
            meta = json.loads(meta or "{}")
        if meta.get("negotiation") != NEGOTIATION_BUNDLE or edge_row["source_node"] != self.own_rid:
            return
        async with self.pool.acquire() as conn:
            peer = await self._peer(conn, edge_row["target_node"])
        if not peer:
            return
        if edge_row["status"] == "APPROVED":
            ev = bundle_event("UPDATE", edge_bundle(self.own_rid, peer["node_rid"], edge_row["edge_type"],
                                                    "APPROVED", list(edge_row["rid_types"] or [])))
        else:
            ev = forget_event(edge_rid_for(self.own_rid, peer["node_rid"]))
        await self.deliver(peer, [ev], operator=True)

    # entry point

    async def handle(self, events: List[Dict[str, Any]], sender: str) -> int:
        """Handle the node and edge objects among a sender's broadcast events. Returns the
        number handled. Never queues them as data."""
        protocol = [e for e in events if isinstance(e, dict) and is_protocol_rid(e.get("rid"))]
        if not protocol:
            return 0
        async with self.pool.acquire() as conn:
            barred = await node_is_barred(conn, sender)
        if barred:
            # A node an operator rejected or revoked, or one holding such a node's key, is neither
            # answered nor negotiated with (register 26).
            logger.info("negotiation: ignoring %d protocol event(s) from barred node %s", len(protocol), sender)
            return 0
        if not self.limiter.allow(sender, len(protocol)):
            logger.warning("negotiation: rate limit, dropping %d protocol event(s) from %s", len(protocol), sender)
            return 0
        handled = 0
        outbox: List[tuple] = []
        replied = False
        async with self.pool.acquire() as conn:
            for ev in protocol:
                rid = ev["rid"]
                try:
                    if rid.lower().startswith(NODE_CONTEXT + ":"):
                        n, reply = await self._on_node_event(conn, ev, sender, may_reply=not replied)
                        replied = replied or reply is not None
                        if reply:
                            outbox.append(reply)
                        handled += n
                    else:
                        handled += await self._on_edge_event(conn, ev, sender, outbox)
                except Exception as exc:  # one bad object must not drop the rest
                    logger.warning("negotiation: %s event for %s from %s failed: %r", ev.get("event_type"), rid, sender, exc)
        for peer, out in outbox:  # connection released
            await self.deliver(peer, out)
        return handled

    async def _on_node_event(self, conn, ev: Dict[str, Any], sender: str, may_reply: bool = True):
        """Returns (handled, reply) where reply is (peer, events) or None."""
        if ev["rid"] != sender:
            logger.info("negotiation: ignoring %s of another node's bundle %s from %s (no relaying)",
                        ev.get("event_type"), ev["rid"], sender)
            return 0, None
        if ev.get("event_type") == "FORGET":
            # A handshake opens with FORGET of the sender's own node; the NEW that follows
            # refreshes the profile. The node's public key is never changed here.
            return 1, None
        contents = ev.get("contents")
        if not isinstance(contents, dict):
            return 0, None
        peer = await self._peer(conn, sender)
        if not peer:
            logger.warning("negotiation: first contact from unregistered %s", sender)
            return 0, None
        provides = contents.get("provides") or {}
        node_type = contents.get("node_type") or peer.get("node_type") or "FULL"
        base_url = None
        if node_type == "FULL":
            base_url = valid_peer_base_url(contents.get("base_url"), getattr(self.own_profile, "base_url", None))
            if contents.get("base_url") and not base_url:
                logger.warning("negotiation: %s advertised a base_url we will not call; stored without one", sender)
        # A bundle applies only if its manifest is newer than the last one applied, as koi-net's
        # BasicManifestHandler decides (basic_manifest_handler.py:31): a replayed older signed
        # bundle would otherwise roll the sender's base_url, node_type and provides back (register
        # 26). Without a stored timestamp a bundle applies, and a
        # bundle without one applies only while none is stored. One conditional UPDATE, so a
        # concurrent replay cannot slip between a check and the write.
        stamp = _manifest_stamp(ev.get("manifest"))
        applied = await conn.fetchval(
            """UPDATE koi_net_nodes SET node_type = $2, base_url = $3, provides_event = $4, provides_state = $5,
                      last_seen = NOW(),
                      metadata = CASE WHEN $6::text IS NULL THEN metadata
                                      ELSE COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('node_bundle_timestamp', $6::text) END
                WHERE node_rid = $1
                  AND (metadata->>'node_bundle_timestamp' IS NULL
                       OR ($6::text IS NOT NULL AND metadata->>'node_bundle_timestamp' < $6::text))
                RETURNING node_rid""",
            sender, node_type, base_url, list(provides.get("event") or []), list(provides.get("state") or []), stamp,
        )
        if not applied:
            logger.info("negotiation: ignoring a node bundle from %s not newer than the last one applied", sender)
            return 1, None
        peer.update(node_type=node_type, base_url=base_url)
        now = time.monotonic()
        if not may_reply or now - self._last_reply.get(sender, -REPLY_COOLDOWN_S) < REPLY_COOLDOWN_S:
            return 1, None  # one reply per request and per cooldown window
        self._last_reply[sender] = now

        reply = [bundle_event("NEW", self.own_bundle())]
        interest = rid_types_of_interest()
        if interest and node_type == "FULL" and base_url:
            wanted = interest_overlap(interest, list(provides.get("event") or []))
            rejected = await conn.fetchval(
                "SELECT 1 FROM koi_net_edges WHERE source_node = $1 AND target_node = $2 AND status = 'REJECTED'",
                sender, self.own_rid,
            )
            if wanted and not rejected:  # an operator's rejection of our subscription sticks
                row = await self._edge_row(conn, sender, self.own_rid)
                if not (row and row["status"] == "APPROVED" and _normalised(row["rid_types"] or []) == _normalised(wanted)):
                    if await self._upsert_edge(conn, sender, self.own_rid, "POLL", "PROPOSED", wanted):
                        reply.append(bundle_event("NEW", edge_bundle(sender, self.own_rid, "POLL", "PROPOSED", wanted)))
                    else:  # an operator revoked our subscription, or the peer forgot it
                        logger.info("negotiation: not re-proposing our held subscription to %s", sender)
        logger.info("negotiation: first contact from %s (%s); answering with our bundle%s", sender, node_type,
                    " and a subscription proposal" if len(reply) > 1 else "")
        return 1, (peer, reply)

    async def _on_edge_event(self, conn, ev: Dict[str, Any], sender: str, outbox: List[tuple]) -> int:
        if ev.get("event_type") == "FORGET":
            for source, target in ((sender, self.own_rid), (self.own_rid, sender)):
                if edge_rid_for(source, target) == ev["rid"]:
                    # Every row for the pair: /handshake may have added one under its own key.
                    # An operator's REJECTED stays: a peer's FORGET must not turn it into a
                    # REVOKED it could then re-propose past.
                    result = await self._peer_forget_pair(conn, source, target)
                    logger.info("negotiation: %s forgot edge %s; revoked locally (%s)", sender, ev["rid"], result)
                    return 1
            return 0
        edge = parse_edge_contents(ev.get("contents"))
        if not edge or ev["rid"] != edge_rid_for(edge["source"], edge["target"]):
            logger.info("negotiation: ignoring malformed edge bundle %s from %s", ev.get("rid"), sender)
            return 0
        if {edge["source"], edge["target"]} != {sender, self.own_rid}:
            logger.info("negotiation: ignoring edge %s that is not between %s and us", ev["rid"], sender)
            return 0

        if edge["source"] == self.own_rid and edge["status"] == "PROPOSED":
            # The sender subscribes to us.
            peer = await self._peer(conn, sender)
            if not peer:
                return 0
            reason = subscription_refusal(edge, list(self.own_profile.provides.event), peer)
            row = await self._edge_row(conn, self.own_rid, sender)
            rejected = await conn.fetchval(
                "SELECT 1 FROM koi_net_edges WHERE source_node = $1 AND target_node = $2 AND status = 'REJECTED'",
                self.own_rid, sender,
            )
            if not reason and rejected:
                # Any rejected row for the pair, not only the one _edge_row picks: /handshake
                # may have added a newer row under its own key.
                reason = "an operator rejected this subscription"
            if reason:
                logger.info("negotiation: refusing edge %s from %s: %s", ev["rid"], sender, reason)
                outbox.append((peer, [forget_event(ev["rid"])]))
                return 1
            approved_already = bool(row and row["status"] == "APPROVED" and row["edge_type"] == edge["edge_type"]
                                    and _normalised(edge["rid_types"]) <= _normalised(row["rid_types"] or []))
            status = "APPROVED" if (approved_already or auto_approve_edges()) else "PROPOSED"
            if not await self._upsert_edge(conn, self.own_rid, sender, edge["edge_type"], status, edge["rid_types"]):
                # Revoked by an operator or by the peer's own FORGET: only an operator re-opens it.
                logger.info("negotiation: refusing edge %s from %s: the edge is revoked", ev["rid"], sender)
                outbox.append((peer, [forget_event(ev["rid"])]))
                return 1
            logger.info("negotiation: edge %s from %s stored %s", ev["rid"], sender, status)
            if status == "APPROVED":
                outbox.append((peer, [bundle_event("UPDATE", edge_bundle(
                    self.own_rid, sender, edge["edge_type"], "APPROVED", edge["rid_types"]))]))
            return 1

        if edge["target"] == self.own_rid and edge["status"] == "APPROVED":
            # The sender approved a subscription we proposed.
            row = await self._edge_row(conn, sender, self.own_rid)
            meta = (row["metadata"] if row else None) or {}
            if isinstance(meta, str):
                meta = json.loads(meta or "{}")
            if not row or row["status"] != "PROPOSED" or meta.get("negotiation") != NEGOTIATION_BUNDLE:
                logger.info("negotiation: ignoring approval of edge %s we did not propose", ev["rid"])
                return 0
            granted = [t for t in edge["rid_types"] if t.strip().lower() in _normalised(row["rid_types"] or [])]
            if self.membrane_on() or self.defer_new_edges():
                # An approved subscription makes our poller fetch and ingest the peer's events, and
                # we proposed it to any first contact that offers types we want. So the peer's
                # approval alone does not open it: an operator approves it, as every new edge
                # (register 26). KOI_NET_DEFER_UNKNOWN_HANDSHAKE=false restores the stock behaviour.
                if not await self._peer_edge_update(
                        conn, row["id"],
                        "rid_types = $3, metadata = COALESCE(metadata, '{}'::jsonb) || '{\"peer_approved\": true}'::jsonb",
                        granted, expect_status="PROPOSED"):
                    logger.info("negotiation: approval of %s from %s not applied: the edge is no longer PROPOSED",
                                ev["rid"], sender)
                    return 0
                logger.info("negotiation: %s approved our subscription %s; waiting for an operator", sender, ev["rid"])
                return 1
            if not await self._peer_edge_update(conn, row["id"], "status = 'APPROVED', rid_types = $3", granted,
                                                expect_status="PROPOSED"):
                logger.info("negotiation: approval of %s from %s not applied: the edge is no longer PROPOSED",
                            ev["rid"], sender)
                return 0
            logger.info("negotiation: %s approved our subscription %s", sender, ev["rid"])
            return 1

        logger.info("negotiation: ignoring edge %s (%s, source %s) from %s", ev["rid"], edge["status"], edge["source"], sender)
        return 0
