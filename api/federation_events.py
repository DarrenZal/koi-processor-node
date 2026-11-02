"""Shared helper for emitting federation domain events from mutation endpoints.

Usage in routers:
    from api.federation_events import emit_domain_event
    await emit_domain_event("entity", "NEW", rid, payload)

For knowledge-domain emits that require idempotent subscriber behavior:
    event_id = uuid.uuid4()
    await emit_domain_event(
        "knowledge_episode", "NEW", rid, payload,
        payload_event_id=str(event_id),
    )
The caller-supplied event_id is BOTH passed to the queue (for queue-level
dedup via UNIQUE (source_node, event_id, target_node)) AND injected into
payload as `_federation_event_id` (for subscriber-level dedup via the
federation_applied_events table — see migration 100).

The event_queue is set during app startup by personal_ingest_api.py.
If federation is not enabled, calls are silently no-ops.

Feature flag KOI_FEDERATE_KNOWLEDGE gates the three knowledge-related
domains only ('knowledge_episode', 'knowledge_fact', 'document_entity_link').
Other domains (entity, claim, attestation, commitment, etc.) are not
affected by the flag. Default off — flip to "true" per node after
subscriber handlers are deployed.
"""

import hashlib
import logging
import os
from typing import Any, Dict, Optional
from uuid import uuid4

from api.federation_blocks import FederationBlocksUnavailable, rid_sha256

logger = logging.getLogger(__name__)

_event_queue = None

# Knowledge-domain emits gated by KOI_FEDERATE_KNOWLEDGE.
KNOWLEDGE_DOMAINS = frozenset({
    "knowledge_episode",
    "knowledge_fact",
    "document_entity_link",
})


def set_event_queue(eq):
    """Called once during app startup to wire the event queue."""
    global _event_queue
    _event_queue = eq


def _knowledge_federation_enabled() -> bool:
    """Read KOI_FEDERATE_KNOWLEDGE flag (re-read every call so env flips take
    effect without process restart; cheap — single os.environ lookup).
    """
    return os.getenv("KOI_FEDERATE_KNOWLEDGE", "false").lower() == "true"


async def emit_domain_event(
    domain: str,
    event_type: str,
    rid: str,
    payload: Dict[str, Any],
    target_node: Optional[str] = None,
    payload_event_id: Optional[str] = None,
):
    """Queue a domain federation event. No-op if federation is not enabled.

    payload_event_id (optional): caller-supplied UUID string. When provided,
      (a) used as the queue's event_id (queue dedups on UNIQUE
          (source_node, event_id, target_node)), and
      (b) injected into payload as `_federation_event_id` so subscriber-side
          idempotency tracking in federation_applied_events can dedup on the
          same key. Required for knowledge-domain emits that hit additive
          subscriber paths (e.g., document_entity_link mention_count).
    """
    if _event_queue is None:
        return

    # Feature-flag gate for knowledge domains (publish side).
    if domain in KNOWLEDGE_DOMAINS and not _knowledge_federation_enabled():
        return

    # Inject the federation event_id into payload contents so the subscriber
    # can read it back without depending on queue-level fields.
    effective_payload = payload
    if payload_event_id is not None:
        effective_payload = {**payload, "_federation_event_id": payload_event_id}

    try:
        await _event_queue.add(
            event_type=event_type,
            rid=rid,
            contents={
                "_koi_domain": domain,
                "payload": effective_payload,
            },
            target_node=target_node,
            event_id=payload_event_id,
        )
    except FederationBlocksUnavailable:
        # Fail closed: nothing can be recognised as blocked, so the record is named by hash prefix only.
        logger.error(f"federation_events.emit withheld domain={domain} record={rid_sha256(rid)[:12]}: federation_rid_blocks is missing")
    except Exception as e:
        logger.warning(f"federation_events.emit failed domain={domain} rid={rid}: {e}")


def doclink_rid(document_rid: str, entity_uri: str) -> str:
    """Deterministic RID for a document_entity_link federation event.

    Per plan Decision 1: sha256(document_rid + entity_uri), first 16 hex chars.
    Deterministic so the same (document, entity) pair maps to a stable RID
    across nodes — but note the subscriber (_apply_doclink) keys the upsert on
    the payload's (document_rid, entity_uri), not on this RID. The RID is for
    queue identity / audit.
    """
    h = hashlib.sha256(f"{document_rid}{entity_uri}".encode()).hexdigest()[:16]
    return f"orn:personal-koi.doclink:{h}"


def doclink_row_created(execute_status: str) -> bool:
    """True if an asyncpg `conn.execute()` status string indicates a row was
    actually inserted.

    Group B doclink sites use `INSERT ... ON CONFLICT DO NOTHING`, which returns
    "INSERT 0 1" when a row was created and "INSERT 0 0" when the conflict was
    hit (publisher state unchanged). Only the former should emit a federation
    event — emitting on a conflict would be a publisher-side double-count.
    """
    parts = (execute_status or "").split()
    return len(parts) >= 3 and parts[-1] == "1"


async def emit_doclink_event(
    document_rid: str,
    entity_uri: str,
    mention_delta: int,
    context: Optional[str] = None,
    created_at: Optional[str] = None,
):
    """Emit a `document_entity_link` NEW federation event.

    `mention_delta` is REQUIRED — the publisher-supplied count delta. It MUST
    NOT be inferred from a SELECT or a post-insert read of mention_count (that
    is the exact double-count bug the subscriber's idempotency design exists to
    prevent — see scripts/lint_mention_delta.py and _apply_doclink).

      - Group A sites (always-+1 upserts: fresh insert sets count to 1, conflict
        does +1) pass `mention_delta=1` unconditionally after a successful
        execute.
      - Group B sites (`ON CONFLICT DO NOTHING`) pass `mention_delta=1` ONLY
        when a row was actually created (see `doclink_row_created`), and skip
        this call entirely on a conflict hit.

    Builds the deterministic doclink RID, generates a fresh payload_event_id
    for subscriber-side idempotency, and delegates to `emit_domain_event` —
    which gates on KOI_FEDERATE_KNOWLEDGE and never raises.
    """
    payload: Dict[str, Any] = {
        "document_rid": document_rid,
        "entity_uri": entity_uri,
        "mention_delta": mention_delta,
    }
    if context is not None:
        payload["context"] = context
    if created_at is not None:
        payload["created_at"] = created_at

    await emit_domain_event(
        "document_entity_link",
        "NEW",
        doclink_rid(document_rid, entity_uri),
        payload,
        payload_event_id=str(uuid4()),
    )
