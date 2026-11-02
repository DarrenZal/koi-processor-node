"""Consumer-side handlers for domain federation events.

Each handler applies a federated domain event (entity, claim, attestation,
commitment, commitment_pool, task, intent) to the local database via UPSERT
semantics.

Knowledge domains (knowledge_episode, knowledge_fact, document_entity_link)
are gated by feature flag KOI_FEDERATE_KNOWLEDGE — handlers are imported and
dispatched only when the flag is on. See plan koi-graph-graceful-toucan.

Called from koi_poller._process_event() when contents contain a "_koi_domain" marker.

Note on `FederationDeferred`: handlers raise this to signal "do not confirm
this event; redeliver next poll cycle." Used by knowledge_fact when an event
references an episode_id not yet present locally. koi_poller's broad except
clause (lines 614-627) treats raises as "skip-without-confirm" — there is
no return-path equivalent.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

import asyncpg

from api.utils import parse_ts
from api.resolution_primitives import normalize_alias_list
from api import document_federation
from api.document_federation import DOCUMENT_DOMAIN  # re-exported for the poller

logger = logging.getLogger(__name__)


class FederationDeferred(Exception):
    """Raised by a handler to defer event confirmation. The poller will
    log and redeliver on the next poll cycle. Use sparingly — only for
    expected, recoverable conditions like missing-FK-parent races.
    """
    pass


# Knowledge-domain handlers are gated by KOI_FEDERATE_KNOWLEDGE flag.
KNOWLEDGE_DOMAINS = frozenset({
    "knowledge_episode",
    "knowledge_fact",
    "document_entity_link",
})


def _knowledge_federation_enabled() -> bool:
    """Re-read every call so env flips take effect without process restart."""
    return os.getenv("KOI_FEDERATE_KNOWLEDGE", "false").lower() == "true"


async def apply_domain_event(
    conn,
    domain: str,
    rid: str,
    event_type: str,
    payload: Dict[str, Any],
    source_node: str,
):
    """Dispatch to the appropriate domain handler."""
    # Feature-flag gate for knowledge domains (apply side).
    # If the flag is off, return cleanly — event will be confirmed (poller
    # treats clean return as "applied"). This intentionally drops knowledge
    # events when the flag is off, matching the publish-side gate. Pre-flag
    # operators should not flip the flag asymmetrically across the cluster.
    if domain in KNOWLEDGE_DOMAINS and not _knowledge_federation_enabled():
        logger.debug(
            f"Skipping {domain} event {rid} (KOI_FEDERATE_KNOWLEDGE=false)"
        )
        return

    handlers = {
        "entity": _apply_entity,
        "claim": _apply_claim,
        "attestation": _apply_attestation,
        "commitment": _apply_commitment,
        "commitment_pool": _apply_commitment_pool,
        "task": _apply_task,
        "intent": _apply_intent,
        # Knowledge-domain handlers wired in 2b/2c/2d:
        "knowledge_episode": _apply_knowledge_episode,
        "knowledge_fact": _apply_knowledge_fact,
        "document_entity_link": _apply_doclink,
    }
    # Document federation is registered only while KOI_FEDERATE_DOCUMENTS is on.
    # With the flag off the key is ABSENT, so a document event falls through to
    # the "Unknown _koi_domain" warning below — byte-identical to this node's
    # behaviour before the domain existed. That matters for a node that runs this
    # code without having opted in.
    if document_federation.document_federation_enabled():
        handlers[document_federation.DOCUMENT_DOMAIN] = _apply_document
    handler = handlers.get(domain)
    if not handler:
        logger.warning(f"Unknown _koi_domain '{domain}' in event {rid}")
        return

    if event_type == "FORGET":
        await _handle_forget(conn, domain, rid, payload)
        return

    await handler(conn, rid, event_type, payload, source_node)


async def _handle_forget(conn, domain: str, rid: str, payload: Dict[str, Any]):
    """Handle FORGET events by deleting the row from the domain table."""
    table_rid_map = {
        "entity": ("entity_registry", "fuseki_uri"),
        "claim": ("claims", "claim_rid"),
        "attestation": ("claim_attestations", "attestation_rid"),
        "commitment": ("commitments", "commitment_rid"),
        "commitment_pool": ("commitment_pools", "pool_rid"),
        "task": ("task_registry", "task_key"),
        "intent": ("intent_discovery_cache", "intent_rid"),
    }
    if domain == document_federation.DOCUMENT_DOMAIN:
        # A document spans two tables; the generic single-table delete below
        # cannot express it. Gated identically to the apply path.
        if not document_federation.document_federation_enabled():
            logger.warning(
                "domain.document.forget_skip rid=%s reason=KOI_FEDERATE_DOCUMENTS_disabled",
                rid,
            )
            return
        await _forget_document(conn, rid, payload)
        return

    table, col = table_rid_map.get(domain, (None, None))
    if not table:
        return
    rid_val = payload.get(col, rid)
    await conn.execute(f"DELETE FROM {table} WHERE {col} = $1", rid_val)
    logger.info(f"domain.forget domain={domain} rid={rid_val}")


async def _apply_entity(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT entity into entity_registry."""
    fuseki_uri = payload.get("fuseki_uri", rid)
    entity_text = payload.get("entity_text", "")
    entity_type = payload.get("entity_type", "")
    normalized_text = payload.get("normalized_text", entity_text.lower().strip())
    aliases = normalize_alias_list(payload.get("aliases", []))
    metadata = payload.get("metadata", {})

    # migration 111: resolution_tier records WHY a row was minted.
    # COALESCE on conflict, deliberately: never overwrite a non-NULL tier, or the
    # record of how the row was ORIGINALLY minted is erased — the one thing the
    # column exists to preserve. Filling a NULL is desirable: it labels a
    # pre-instrumentation row that federation later touched.
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, aliases, metadata,
                                     resolution_tier)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'federation')
        ON CONFLICT (fuseki_uri) DO UPDATE SET
            entity_text = EXCLUDED.entity_text,
            entity_type = EXCLUDED.entity_type,
            normalized_text = EXCLUDED.normalized_text,
            aliases = EXCLUDED.aliases,
            metadata = EXCLUDED.metadata,
            resolution_tier = COALESCE(entity_registry.resolution_tier, EXCLUDED.resolution_tier),
            updated_at = NOW()
    """, fuseki_uri, entity_text, entity_type, normalized_text,
        aliases or [], json.dumps(metadata) if isinstance(metadata, dict) else metadata)

    # Upsert relationships if included
    relationships = payload.get("relationships", [])
    for rel in relationships:
        subj = rel.get("subject_uri", "")
        pred = rel.get("predicate", "")
        obj = rel.get("object_uri", "")
        conf = rel.get("confidence", 1.0)
        src = rel.get("source", "federation")
        if subj and pred and obj:
            # Use a savepoint so FK violations don't abort the outer transaction
            # (asyncpg puts the PG transaction in error state on any failed SQL)
            try:
                await conn.execute("SAVEPOINT rel_insert")
                await conn.execute("""
                    INSERT INTO entity_relationships (subject_uri, predicate, object_uri, confidence, source)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (subject_uri, predicate, object_uri) DO UPDATE SET
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source,
                        updated_at = NOW()
                """, subj, pred, obj, conf, src)
                await conn.execute("RELEASE SAVEPOINT rel_insert")
            except Exception as e:
                await conn.execute("ROLLBACK TO SAVEPOINT rel_insert")
                # FK violations are expected if related entities haven't arrived yet
                logger.debug(f"domain.entity.rel_skip pred={pred} err={e}")

    logger.info(f"domain.entity.apply uri={fuseki_uri} type={entity_type} new_or_update={event_type}")


async def _apply_claim(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT claim into claims table."""
    claim_rid = payload.get("claim_rid", rid)

    await conn.execute("""
        INSERT INTO claims (
            claim_rid, entity_uri, claimant_uri, statement, claim_type,
            verification, source_document, ai_confidence, content_hash,
            supersedes_rid, metadata, created_by, operator_uri
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13)
        ON CONFLICT (claim_rid) DO UPDATE SET
            verification = EXCLUDED.verification,
            content_hash = EXCLUDED.content_hash,
            metadata = EXCLUDED.metadata,
            operator_uri = EXCLUDED.operator_uri,
            updated_at = NOW()
    """,
        claim_rid,
        payload.get("entity_uri"),
        payload.get("claimant_uri"),
        payload.get("statement"),
        payload.get("claim_type", "ecological"),
        payload.get("verification", "self_reported"),
        payload.get("source_document"),
        payload.get("ai_confidence"),
        payload.get("content_hash"),
        payload.get("supersedes_rid"),
        json.dumps(payload.get("metadata", {})),
        payload.get("created_by"),
        payload.get("operator_uri"),
    )

    # Append state transition if included (idempotent via pre-check)
    st = payload.get("state_transition")
    if st:
        await _append_state_log(
            conn, "claim_state_log", "claim_rid", claim_rid,
            st.get("from_state"), st.get("to_state"), st.get("actor"),
            st.get("reason"), st.get("metadata"), st.get("created_at"),
        )

    logger.info(f"domain.claim.apply rid={claim_rid}")


async def _apply_attestation(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT attestation into claim_attestations."""
    att_rid = payload.get("attestation_rid", rid)
    claim_rid = payload.get("claim_rid")
    reviewer_uri = payload.get("reviewer_uri")

    await conn.execute("""
        INSERT INTO claim_attestations (
            attestation_rid, claim_rid, reviewer_uri, verdict, rationale,
            evidence_uris, content_hash, metadata
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
        ON CONFLICT (claim_rid, reviewer_uri) DO UPDATE SET
            verdict = EXCLUDED.verdict,
            rationale = EXCLUDED.rationale,
            evidence_uris = EXCLUDED.evidence_uris,
            content_hash = EXCLUDED.content_hash,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """,
        att_rid, claim_rid, reviewer_uri,
        payload.get("verdict", "pending"),
        payload.get("rationale"),
        payload.get("evidence_uris", []),
        payload.get("content_hash"),
        json.dumps(payload.get("metadata", {})),
    )

    logger.info(f"domain.attestation.apply rid={att_rid} claim={claim_rid}")


async def _apply_commitment(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT commitment into commitments table."""
    commitment_rid = payload.get("commitment_rid", rid)

    await conn.execute("""
        INSERT INTO commitments (
            commitment_rid, pledger_uri, title, description, offer_type,
            quantity, unit, validity_start, validity_end, state,
            evidence_uri, metadata, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::commitment_state, $11, $12::jsonb, $13)
        ON CONFLICT (commitment_rid) DO UPDATE SET
            state = EXCLUDED.state,
            evidence_uri = EXCLUDED.evidence_uri,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """,
        commitment_rid,
        payload.get("pledger_uri"),
        payload.get("title"),
        payload.get("description"),
        payload.get("offer_type", "labor"),
        payload.get("quantity"),
        payload.get("unit"),
        parse_ts(payload.get("validity_start")),
        parse_ts(payload.get("validity_end")),
        payload.get("state", "PROPOSED"),
        payload.get("evidence_uri"),
        json.dumps(payload.get("metadata", {})),
        payload.get("created_by"),
    )

    # Append state log if included
    st = payload.get("state_transition")
    if st:
        await _append_state_log(
            conn, "commitment_state_log", "commitment_rid", commitment_rid,
            st.get("from_state"), st.get("to_state"), st.get("actor"),
            st.get("reason"), st.get("metadata"), st.get("created_at"),
        )

    logger.info(f"domain.commitment.apply rid={commitment_rid}")


async def _apply_commitment_pool(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT commitment pool. V1: pool creation only (no pledge membership)."""
    pool_rid = payload.get("pool_rid", rid)

    await conn.execute("""
        INSERT INTO commitment_pools (
            pool_rid, name, description, steward_uri, bioregion_uri,
            activation_threshold_pct, activation_threshold_count,
            demurrage_rate_monthly, state, metadata, created_by
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
        ON CONFLICT (pool_rid) DO UPDATE SET
            state = EXCLUDED.state,
            metadata = EXCLUDED.metadata,
            updated_at = NOW()
    """,
        pool_rid,
        payload.get("name"),
        payload.get("description"),
        payload.get("steward_uri"),
        payload.get("bioregion_uri"),
        payload.get("activation_threshold_pct"),
        payload.get("activation_threshold_count"),
        payload.get("demurrage_rate_monthly"),
        payload.get("state", "forming"),
        json.dumps(payload.get("metadata", {})),
        payload.get("created_by"),
    )

    logger.info(f"domain.pool.apply rid={pool_rid}")


async def _apply_task(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT task into task_registry."""
    task_key = payload.get("task_key", rid)

    await conn.execute("""
        INSERT INTO task_registry (
            task_key, uuid, title, status, priority,
            due_date, start_date, wait_until, context, effort,
            owner_uri, project_uri, collaborator_uris, blocked_by,
            source_note, source_type, vault_path, tags,
            validity_start, validity_end
        ) VALUES (
            $1, $2, $3,
            COALESCE($4, 'inbox'), COALESCE($5, 'medium'),
            $6::date, $7::date, $8::date, $9, $10,
            $11, $12, $13, $14,
            $15, COALESCE($16, 'meeting'), $17, $18,
            $19, $20
        )
        ON CONFLICT (task_key) DO UPDATE SET
            uuid = COALESCE(EXCLUDED.uuid, task_registry.uuid),
            title = EXCLUDED.title,
            status = CASE WHEN $4 IS NULL THEN task_registry.status ELSE $4 END,
            priority = CASE WHEN $5 IS NULL THEN task_registry.priority ELSE $5 END,
            due_date = COALESCE(EXCLUDED.due_date, task_registry.due_date),
            start_date = COALESCE(EXCLUDED.start_date, task_registry.start_date),
            wait_until = COALESCE(EXCLUDED.wait_until, task_registry.wait_until),
            context = COALESCE(EXCLUDED.context, task_registry.context),
            effort = COALESCE(EXCLUDED.effort, task_registry.effort),
            owner_uri = COALESCE(EXCLUDED.owner_uri, task_registry.owner_uri),
            project_uri = COALESCE(EXCLUDED.project_uri, task_registry.project_uri),
            collaborator_uris = CASE WHEN array_length(EXCLUDED.collaborator_uris, 1) > 0
                                    THEN EXCLUDED.collaborator_uris
                                    ELSE task_registry.collaborator_uris END,
            blocked_by = CASE WHEN array_length(EXCLUDED.blocked_by, 1) > 0
                              THEN EXCLUDED.blocked_by
                              ELSE task_registry.blocked_by END,
            source_note = COALESCE(EXCLUDED.source_note, task_registry.source_note),
            source_type = COALESCE(EXCLUDED.source_type, task_registry.source_type),
            vault_path = COALESCE(EXCLUDED.vault_path, task_registry.vault_path),
            tags = CASE WHEN array_length(EXCLUDED.tags, 1) > 0
                        THEN EXCLUDED.tags ELSE task_registry.tags END,
            validity_start = COALESCE(EXCLUDED.validity_start, task_registry.validity_start),
            validity_end   = COALESCE(EXCLUDED.validity_end,   task_registry.validity_end),
            updated_at = NOW()
    """,
        task_key,
        payload.get("uuid"),
        payload.get("title"),
        payload.get("status"),
        payload.get("priority"),
        payload.get("due_date"),
        payload.get("start_date"),
        payload.get("wait_until"),
        payload.get("context"),
        payload.get("effort"),
        payload.get("owner_uri"),
        payload.get("project_uri"),
        payload.get("collaborator_uris", []),
        payload.get("blocked_by", []),
        payload.get("source_note"),
        payload.get("source_type"),
        payload.get("vault_path"),
        payload.get("tags", []),
        parse_ts(payload.get("validity_start")),
        parse_ts(payload.get("validity_end")),
    )

    logger.info(f"domain.task.apply key={task_key}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _append_state_log(
    conn,
    table: str,
    rid_col: str,
    rid_val: str,
    from_state: Optional[str],
    to_state: Optional[str],
    actor: Optional[str],
    reason: Optional[str],
    metadata: Any = None,
    created_at: Optional[str] = None,
):
    """Append a state log entry, skipping duplicates by (rid, to_state, created_at)."""
    if not to_state:
        return

    # Parse created_at for dedup check
    ts = None
    if created_at:
        try:
            ts = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts = None

    # Dedup: skip if this exact transition already exists
    if ts:
        exists = await conn.fetchval(
            f"SELECT 1 FROM {table} WHERE {rid_col} = $1 AND to_state = $2 AND created_at = $3",
            rid_val, to_state, ts,
        )
        if exists:
            return

    meta_json = json.dumps(metadata) if isinstance(metadata, dict) else metadata

    if table == "commitment_state_log":
        # commitment_state_log uses commitment_state enum for from_state/to_state
        if ts:
            await conn.execute(f"""
                INSERT INTO {table} ({rid_col}, from_state, to_state, actor, reason, metadata, created_at)
                VALUES ($1, $2::commitment_state, $3::commitment_state, $4, $5, $6::jsonb, $7)
            """, rid_val, from_state, to_state, actor, reason, meta_json, ts)
        else:
            await conn.execute(f"""
                INSERT INTO {table} ({rid_col}, from_state, to_state, actor, reason, metadata)
                VALUES ($1, $2::commitment_state, $3::commitment_state, $4, $5, $6::jsonb)
            """, rid_val, from_state, to_state, actor, reason, meta_json)
    else:
        if ts:
            await conn.execute(f"""
                INSERT INTO {table} ({rid_col}, from_state, to_state, actor, reason, metadata, created_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """, rid_val, from_state, to_state, actor, reason, meta_json, ts)
        else:
            await conn.execute(f"""
                INSERT INTO {table} ({rid_col}, from_state, to_state, actor, reason, metadata)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            """, rid_val, from_state, to_state, actor, reason, meta_json)


# _parse_ts() lifted to api.utils.parse_ts so task_router / commitment_router /
# intent_router can share the same parser. Backward-compat alias preserved for
# any in-tree callers of the old private name.
_parse_ts = parse_ts


# ---------------------------------------------------------------------------
# Knowledge-domain handlers (KOI_FEDERATE_KNOWLEDGE-gated)
# ---------------------------------------------------------------------------

_UNDEFINED_COL_RE = re.compile(r'column "([^"]+)" of relation "[^"]+" does not exist')
_DRIFT_MAX_RETRIES = 5

# Embedding column discriminator: payload selects which column the embedding
# vector lands in. None / unknown values omit the column entirely.
_KNOWN_EMBEDDING_COLS = frozenset({"fact_embedding", "fact_embedding_3072"})


def _assert_embedding_format(payload: Mapping[str, Any], rid: str) -> None:
    """Gate 3 (Federation Phase 1 step 2e): fail loud on unknown embedding_format.

    v1 is `json_floats`-only. The subscriber's `_format_vector` does NOT branch
    on this key, so a future base64-emitting publisher would otherwise silently
    feed a non-vector literal into the `::vector` cast. Reject anything but
    `json_floats` here. The publisher does not set the key today, so the
    `.get(..., "json_floats")` default always passes — this is purely defensive.
    """
    fmt = payload.get("embedding_format", "json_floats")
    if fmt != "json_floats":
        raise ValueError(
            f"federation: unsupported embedding_format={fmt!r} on {rid} "
            f"(only 'json_floats' is supported in v1)"
        )


def _strip_conflict_assignments(clause: str, dropped_col: str) -> str:
    """Remove `dropped_col = ...` assignments from an ON CONFLICT DO UPDATE SET.

    Splits the SET list on TOP-LEVEL commas only. A naive split breaks on
    `group_id = COALESCE(EXCLUDED.group_id, knowledge_episodes.group_id)`, whose
    inner comma is not an assignment boundary -- and that exact shape is live at
    the knowledge_episodes call site.

    Also drops assignments whose right-hand side references the column (e.g.
    `x = COALESCE(EXCLUDED.dropped_col, ...)`), since those fail identically.

    Returns the clause unchanged when there is no SET list (e.g. DO NOTHING).
    """
    upper = clause.upper()
    idx = upper.find("DO UPDATE SET")
    if idx == -1:
        return clause
    head = clause[: idx + len("DO UPDATE SET")]
    body = clause[idx + len("DO UPDATE SET"):]

    parts, depth, cur = [], 0, ""
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur)

    pattern = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(dropped_col)}(?![A-Za-z0-9_])")
    kept = [p for p in parts if p.strip() and not pattern.search(p)]
    if not kept:
        # Every assignment referenced the dropped column; an empty SET is invalid
        # SQL, so degrade to DO NOTHING rather than emit a syntax error.
        conflict_target = head[: head.upper().find("DO UPDATE SET")].rstrip()
        return f"{conflict_target} DO NOTHING"
    return f"{head}{','.join(kept)}"


async def _insert_with_drift_retry(
    conn,
    table: str,
    columns_values: Mapping[str, Any],
    cast_map: Optional[Mapping[str, str]] = None,
    conflict_clause: str = "",
) -> None:
    """INSERT into `table` with schema-drift retry.

    On asyncpg.UndefinedColumnError, identify the offending column from the
    error message, drop it, and retry — up to 5 times. Each retry drops one
    additional column. Fails loud on the 6th.

    Each attempt is wrapped in a SAVEPOINT so a failed INSERT does not abort
    the surrounding transaction. Caller MUST already be inside a transaction
    (asyncpg requires this for SAVEPOINT).
    """
    cv = dict(columns_values)
    casts = dict(cast_map or {})
    clause = conflict_clause
    retries = 0
    while True:
        cols = list(cv.keys())
        if not cols:
            raise RuntimeError(f"federation drift retry on {table}: all columns dropped")
        vals = [cv[c] for c in cols]
        placeholders = [f"${i + 1}{casts.get(c, '')}" for i, c in enumerate(cols)]
        sql = (
            f"INSERT INTO {table} ({', '.join(cols)}) "
            f"VALUES ({', '.join(placeholders)}) "
            f"{clause}"
        )
        await conn.execute("SAVEPOINT drift_retry")
        try:
            await conn.execute(sql, *vals)
            await conn.execute("RELEASE SAVEPOINT drift_retry")
            return
        except asyncpg.exceptions.UndefinedColumnError as e:
            await conn.execute("ROLLBACK TO SAVEPOINT drift_retry")
            await conn.execute("RELEASE SAVEPOINT drift_retry")
            if retries >= _DRIFT_MAX_RETRIES:
                logger.error(
                    f"federation.drift.exceeded table={table} retries={retries} err={e}"
                )
                raise
            m = _UNDEFINED_COL_RE.search(str(e))
            col = m.group(1) if m else None
            if not col or col not in cv:
                logger.error(
                    f"federation.drift.unparseable table={table} err={e}"
                )
                raise
            retries += 1
            del cv[col]
            # The ON CONFLICT clause names columns too, and it is re-interpolated
            # verbatim on every retry. Dropping the column ONLY from the value
            # list leaves `col = EXCLUDED.col` in the clause, so the retry raises
            # the identical UndefinedColumnError -- and because `col` is no longer
            # in cv, the `col not in cv` branch above re-raises it as unparseable.
            # Net effect before 2026-08-22: drift-retry silently protected only
            # columns ABSENT from the conflict clause, which for both existing
            # callers is almost none. Verified with a scratch table: a missing
            # column named only in the INSERT recovered; the same column also
            # named in ON CONFLICT failed with "column excluded.<col> does not
            # exist".
            clause = _strip_conflict_assignments(clause, col)
            logger.warning(
                f"federation.drift.drop_col table={table} col={col} "
                f"retry={retries}/{_DRIFT_MAX_RETRIES}"
            )
        except Exception:
            await conn.execute("ROLLBACK TO SAVEPOINT drift_retry")
            await conn.execute("RELEASE SAVEPOINT drift_retry")
            raise


async def _apply_knowledge_episode(
    conn,
    rid: str,
    event_type: str,
    payload: Dict[str, Any],
    source_node: str,
):
    """Apply a federated knowledge_episode event (episode + bundled facts).

    Idempotency: ON CONFLICT (id) DO UPDATE — UUID is content-stable, so
    re-delivery cleanly upserts. Apply-first-record-on-success: the episode
    and facts apply via UPSERT, and only then is federation_applied_events
    recorded (audit, not safety — ON CONFLICT already provides safety).

    Originator metadata (`source_node_rid`, `group_id`, `created_at`) is
    preserved verbatim from the payload — the `source_node` function arg is
    used only for logging/audit.

    Trigger constraint: must NOT set `application_name` to anything starting
    with `deep-extract:layers_only:` (would trip
    `deep_extract_layers_only_guard()`). Default app_name is safe.
    """
    episode_id = payload.get("id")
    if not episode_id:
        logger.warning(f"domain.knowledge_episode.skip rid={rid} reason=missing_id")
        return

    _assert_embedding_format(payload, rid)

    event_id = payload.get("_federation_event_id")
    facts = payload.get("facts") or []

    async with conn.transaction():
        await conn.execute("SAVEPOINT ep_apply")
        try:
            await _insert_episode(conn, payload)

            facts_applied = 0
            for fact in facts:
                fact_id = fact.get("id")
                if not fact_id:
                    logger.warning(
                        f"domain.knowledge_episode.fact_skip episode={episode_id} "
                        f"reason=missing_id"
                    )
                    continue
                await conn.execute("SAVEPOINT fact_insert")
                try:
                    await _insert_fact(conn, episode_id, fact)
                    await conn.execute("RELEASE SAVEPOINT fact_insert")
                    facts_applied += 1
                except Exception as e:
                    await conn.execute("ROLLBACK TO SAVEPOINT fact_insert")
                    await conn.execute("RELEASE SAVEPOINT fact_insert")
                    logger.warning(
                        f"domain.knowledge_episode.fact_skip episode={episode_id} "
                        f"fact_id={fact_id} err={e}"
                    )

            if event_id:
                await conn.execute(
                    """
                    INSERT INTO federation_applied_events (domain, event_id, source_node)
                    VALUES ('knowledge_episode', $1::uuid, $2)
                    ON CONFLICT DO NOTHING
                    """,
                    event_id,
                    source_node,
                )

            await conn.execute("RELEASE SAVEPOINT ep_apply")
        except Exception:
            await conn.execute("ROLLBACK TO SAVEPOINT ep_apply")
            await conn.execute("RELEASE SAVEPOINT ep_apply")
            raise

    logger.info(
        f"domain.knowledge_episode.apply rid={rid} id={episode_id} "
        f"facts={facts_applied}/{len(facts)} source={source_node}"
    )


async def _insert_episode(conn, payload: Dict[str, Any]) -> None:
    """UPSERT knowledge_episodes row. Originator created_at preserved verbatim."""
    metadata = payload.get("metadata") or {}
    if not isinstance(metadata, str):
        metadata = json.dumps(metadata)

    cols_vals: Dict[str, Any] = {
        "id": payload.get("id"),
        "name": payload.get("name", ""),
        "content": payload.get("content"),
        "source_description": payload.get("source_description"),
        "source_document": payload.get("source_document"),
        "group_id": payload.get("group_id"),
        "valid_at": parse_ts(payload.get("valid_at")),
        "created_at": parse_ts(payload.get("created_at")),
        "metadata": metadata,
    }
    casts = {"id": "::uuid", "metadata": "::jsonb"}
    conflict = """
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            content = EXCLUDED.content,
            source_description = EXCLUDED.source_description,
            source_document = EXCLUDED.source_document,
            group_id = COALESCE(EXCLUDED.group_id, knowledge_episodes.group_id),
            valid_at = EXCLUDED.valid_at,
            metadata = EXCLUDED.metadata
    """
    await _insert_with_drift_retry(
        conn, "knowledge_episodes", cols_vals, casts, conflict,
    )


def _format_vector(value: Any) -> Optional[str]:
    """Render an embedding value as a Postgres vector literal.

    Accepts list/tuple of floats or a pre-formatted string. Returns None
    for None / empty list (caller omits the column).
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        return "[" + ",".join(repr(float(x)) for x in value) + "]"
    raise TypeError(f"unsupported embedding value type: {type(value).__name__}")


async def _insert_fact(conn, episode_id: str, fact: Dict[str, Any]) -> None:
    """UPSERT knowledge_facts row with embedding-column discriminator.

    Per plan Phase 1 step 8: each fact carries `embedding_column` ∈
    {"fact_embedding", "fact_embedding_3072", null} and `embedding_value`.
    Insert into the named column with ::vector cast (NOT ::halfvec — that
    would be a type-conversion error against the live vector column type).
    """
    cols_vals: Dict[str, Any] = {
        "id": fact.get("id"),
        "episode_id": episode_id,
        "subject_uri": fact.get("subject_uri", ""),
        "predicate": fact.get("predicate", ""),
        "object_uri": fact.get("object_uri"),
        "object_literal": fact.get("object_literal"),
        "fact_text": fact.get("fact_text", ""),
        "valid_from": parse_ts(fact.get("valid_from")),
        "valid_to": parse_ts(fact.get("valid_to")),
        "created_at": parse_ts(fact.get("created_at")),
        "group_id": fact.get("group_id"),
        "source_node_rid": fact.get("source_node_rid"),
        "turn_range_start": fact.get("turn_range_start"),
        "turn_range_end": fact.get("turn_range_end"),
    }
    casts: Dict[str, str] = {"id": "::uuid", "episode_id": "::uuid"}

    emb_col = fact.get("embedding_column")
    if emb_col in _KNOWN_EMBEDDING_COLS:
        emb_literal = _format_vector(fact.get("embedding_value"))
        if emb_literal is not None:
            cols_vals[emb_col] = emb_literal
            casts[emb_col] = "::vector"

    conflict = """
        ON CONFLICT (id) DO UPDATE SET
            valid_to = EXCLUDED.valid_to
    """
    await _insert_with_drift_retry(
        conn, "knowledge_facts", cols_vals, casts, conflict,
    )


async def _apply_knowledge_fact(
    conn,
    rid: str,
    event_type: str,
    payload: Dict[str, Any],
    source_node: str,
):
    """Apply a federated standalone knowledge_fact event (late-bound).

    Most facts arrive bundled inside a `knowledge_episode` event (handled by
    `_apply_knowledge_episode`). This path covers facts emitted independently
    of their parent episode — either oversized-bundle splits (plan step 8a)
    or any future late-bound emit site.

    Idempotency: ON CONFLICT (id) DO UPDATE SET valid_to = EXCLUDED.valid_to.
    Facts are content-stable once minted; only validity_interval is mutable
    per the plan's temporal-validity design. Apply-first-record-on-success:
    the fact INSERT runs first; only on success is
    federation_applied_events recorded. On FK miss the raise propagates
    before the idempotency row is written.

    FK-skew handling: if `episode_id` is not yet present locally, asyncpg
    raises ForeignKeyViolationError. The handler logs INFO and raises
    FederationDeferred. The poller's broad except (koi_poller.py:625-627)
    catches the exception and skips confirming the event, so it redelivers
    next poll cycle. Bounded by koi_net_events 72h TTL.

    Originator metadata (`source_node_rid`, `group_id`, `created_at`) is
    preserved verbatim from payload; `source_node` arg is for logging only.
    """
    fact_id = payload.get("id")
    if not fact_id:
        logger.warning(f"domain.knowledge_fact.skip rid={rid} reason=missing_id")
        return

    episode_id = payload.get("episode_id")
    if not episode_id:
        logger.warning(
            f"domain.knowledge_fact.skip rid={rid} reason=missing_episode_id"
        )
        return

    _assert_embedding_format(payload, rid)

    event_id = payload.get("_federation_event_id")

    async with conn.transaction():
        try:
            await _insert_fact(conn, episode_id, payload)
        except asyncpg.exceptions.ForeignKeyViolationError as e:
            # Identify FK miss on episode_id vs. some other constraint.
            err_str = str(e)
            if (
                "episode_id" in err_str
                or "knowledge_facts_episode_id_fkey" in err_str
            ):
                logger.info(
                    f"domain.knowledge_fact.defer rid={rid} fact_id={fact_id} "
                    f"episode_id={episode_id} reason=awaiting_episode_arrival"
                )
                raise FederationDeferred(
                    f"fact {rid} awaiting episode {episode_id}"
                )
            raise

        if event_id:
            await conn.execute(
                """
                INSERT INTO federation_applied_events (domain, event_id, source_node)
                VALUES ('knowledge_fact', $1::uuid, $2)
                ON CONFLICT DO NOTHING
                """,
                event_id,
                source_node,
            )

    logger.info(
        f"domain.knowledge_fact.apply rid={rid} id={fact_id} "
        f"episode_id={episode_id} source={source_node}"
    )


async def _apply_doclink(
    conn,
    rid: str,
    event_type: str,
    payload: Dict[str, Any],
    source_node: str,
):
    """Apply a federated document_entity_link event.

    UNLIKE episodes and facts, the doclink upsert is ADDITIVE and NOT
    idempotent: `mention_count = mention_count + delta`. Re-delivering the
    same event would double-increment. So this handler uses
    **check-first-apply** — the OPPOSITE ordering from 2b/2c:

      1. INSERT the idempotency row FIRST (`ON CONFLICT DO NOTHING RETURNING 1`).
         No row returned → event already applied → return cleanly (the txn
         commits the no-op lookup; poller confirms the event).
      2. Only on a fresh idempotency row do we run the additive doclink upsert.

    Both statements run inside a SINGLE `async with conn.transaction():` block.
    This is load-bearing: asyncpg auto-commits per-statement otherwise, so a
    failure in the doclink upsert would leave a stale idempotency row that
    permanently blocks legitimate retry. With the single-txn wrap, any raise
    inside the block rolls back BOTH statements — the event redelivers next
    poll with no idempotency row to block it, no double-count risk.

    `mention_count` is taken from `payload["mention_delta"]` — the
    publisher-supplied delta, NEVER inferred from a SELECT or post-insert
    total. Missing/null/zero delta is a publisher bug — log WARNING and
    fall back to delta=1 rather than failing the event.

    Originator metadata (`created_at`) is preserved verbatim from payload;
    `source_node` arg is for logging/audit only. Note: `document_entity_links.
    created_at` is `timestamp WITHOUT time zone`, so a tz-aware payload value
    is normalized to naive UTC before binding.

    Trigger constraint (same as 2b/2c): must NOT set `application_name` to a
    value starting with `deep-extract:layers_only:` (would trip
    `deep_extract_layers_only_guard()`). Default app_name is safe.
    """
    document_rid = payload.get("document_rid")
    entity_uri = payload.get("entity_uri")
    if not document_rid or not entity_uri:
        logger.warning(
            f"domain.document_entity_link.skip rid={rid} reason=missing_key "
            f"document_rid={document_rid!r} entity_uri={entity_uri!r}"
        )
        return

    event_id = payload.get("_federation_event_id")

    mention_delta = payload.get("mention_delta")
    if not mention_delta:
        logger.warning(
            f"domain.document_entity_link.mention_delta_fallback rid={rid} "
            f"value={mention_delta!r} — publisher bug; applying delta=1"
        )
        mention_delta = 1

    context = payload.get("context")
    created_at = parse_ts(payload.get("created_at"))
    if created_at is not None and created_at.tzinfo is not None:
        created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)

    async with conn.transaction():
        # Idempotency check FIRST — opposite of 2b/2c ordering.
        if event_id:
            applied = await conn.fetchval(
                """
                INSERT INTO federation_applied_events (domain, event_id, source_node)
                VALUES ('document_entity_link', $1::uuid, $2)
                ON CONFLICT DO NOTHING
                RETURNING 1
                """,
                event_id,
                source_node,
            )
            if applied is None:
                logger.info(
                    f"domain.document_entity_link.skip_duplicate rid={rid} "
                    f"event_id={event_id}"
                )
                return

        # Additive upsert — NOT idempotent; guarded by the check above.
        await conn.execute(
            """
            INSERT INTO document_entity_links
                (document_rid, entity_uri, mention_count, context, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (document_rid, entity_uri) DO UPDATE SET
                mention_count = document_entity_links.mention_count
                                + EXCLUDED.mention_count,
                context = COALESCE(EXCLUDED.context, document_entity_links.context)
            """,
            document_rid,
            entity_uri,
            mention_delta,
            context,
            created_at,
        )

    logger.info(
        f"domain.document_entity_link.apply rid={rid} document_rid={document_rid} "
        f"entity_uri={entity_uri} delta={mention_delta} source={source_node}"
    )


async def _apply_intent(conn, rid: str, event_type: str, payload: Dict[str, Any], source_node: str):
    """UPSERT remote intent discovery projection into intent_discovery_cache.

    Only discovery-level fields are stored — no publisher_contact,
    source_excerpt, priority, tags, or match criteria cross node boundaries.
    """
    intent_rid = payload.get("intent_rid", rid)

    await conn.execute("""
        INSERT INTO intent_discovery_cache (
            intent_rid, source_node, intent_type, status,
            landscape_group, visibility, asset_offered, asset_wanted,
            quantity
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (intent_rid) DO UPDATE SET
            status = EXCLUDED.status,
            visibility = EXCLUDED.visibility,
            asset_offered = EXCLUDED.asset_offered,
            asset_wanted = EXCLUDED.asset_wanted,
            quantity = EXCLUDED.quantity,
            updated_at = NOW()
    """,
        intent_rid,
        source_node or "unknown",
        payload.get("intent_type", "OFFER"),
        payload.get("status", "active"),
        payload.get("landscape_group", "unknown"),
        payload.get("visibility", "local"),
        payload.get("asset_offered"),
        payload.get("asset_wanted"),
        payload.get("quantity"),
    )


# ── document domain ─────────────────────────────────────────────────────────────
#
# The receive side of KOI-net document federation. Federation moves bytes; it
# does not answer questions — so a federated document only becomes searchable
# once it is chunked and embedded at 3072-dim into the two tables local
# retrieval actually reads. This handler is a thin adapter from a koi-net bundle
# onto the sink that already does that (`scripts/ingest_document.ingest_document_rag`).
#
# Gated by KOI_FEDERATE_DOCUMENTS (default OFF) — see api/document_federation.py.


def _load_document_sink():
    """Import the RAG sink, bootstrapping sys.path if `scripts` is not visible.

    The serving process normally runs uvicorn `api.personal_ingest_api:app` with
    the repo root as cwd, so the plain import resolves. The fallback exists because an ImportError raised
    inside a poll handler would leave the event unconfirmed forever with only a
    log line to show for it — the silent-stall shape this codebase keeps hitting.
    """
    try:
        from scripts.ingest_document import ingest_document_rag_conn  # noqa: PLC0415
    except ImportError:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from scripts.ingest_document import ingest_document_rag_conn  # noqa: PLC0415
    return ingest_document_rag_conn


def _document_embedder_and_chunker():
    """Build the 3072-dim embedder + chunker used by the document sink.

    Constructed explicitly rather than taken from the federation subsystem:
    `setup_koi_net` is called with no `embed_fn` (personal_ingest_api.py:1895),
    so koi-net has no embedding capability of its own. Reads the key at call
    time so a rotated key is picked up without a restart.
    """
    from api.embedding_provider import OpenAIEmbeddingProvider  # noqa: PLC0415
    from api.chunker import TextChunker  # noqa: PLC0415

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY not set — federated documents cannot be embedded. "
            "Refusing to land unembedded chunks, which are invisible to search "
            "and indistinguishable from a healthy ingest by row count."
        )
    embedder = OpenAIEmbeddingProvider(
        api_key=api_key,
        model=os.getenv("EMBEDDING_MODEL", "text-embedding-3-large"),
        dimension=int(os.getenv("EMBEDDING_DIMENSION", "3072")),
    )
    chunker = TextChunker(chunk_size=500, chunk_overlap=50)
    return embedder, chunker


def build_document_source_meta(rid: str, contents: Mapping[str, Any]) -> Dict[str, Any]:
    """Map a koi-net document bundle onto the sink's `source_meta` contract.

    Split out from the handler so the mapping — which is where the author,
    privacy and URL fields are actually decided — is testable without a database
    or an OpenAI key.
    """
    doc = contents.get("document") or {}
    meta = contents.get("metadata") or {}

    slug = document_federation.newsletter_slug(contents)
    # Raises UnmappedNewsletterSlug rather than defaulting. `doc.get("author")`
    # is deliberately never consulted: it is null in all 448 measured bundles.
    author = document_federation.resolve_author(slug)

    # NOT `doc.get("url") or meta.get("url")`: 112/448 bundles carry a
    # tracking-pixel URL in BOTH fields, 224 of those values decode to the
    # subscriber's email address. `canonical_url` returns None rather than
    # storing one — see its docstring for the measurement.
    url = document_federation.canonical_url(slug, contents)
    title = doc.get("title") or meta.get("title")

    is_private = meta.get("is_private")
    access_source = meta.get("access_source")

    return {
        "name": title,
        "slug": doc.get("id") or slug,
        "source_url": url,
        "author": author,
        "is_private": is_private,
        "access_source": access_source,
        "source_sensor": document_federation.FEDERATION_SOURCE_SENSOR,
        "retrieval_method": "koi-net/bundles.fetch",
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "group_id": os.getenv("KOI_FEDERATE_DOCUMENTS_GROUP", "personal"),
        "tier": "rag",
    }


async def _apply_document(
    conn,
    rid: str,
    event_type: str,
    payload: Dict[str, Any],
    source_node: str,
):
    """Land a federated document as locally-searchable RAG content.

    `payload` is the bundle's `contents` object: {document, metadata, processing}.

    Containment (AC10) is enforced HERE, not on the edge: `extract_rid_type()`
    returns None for `regen.newsletter:` RIDs, so the poll-side `rid_types`
    filter — which only skips when a type *is* extracted — cannot scope them. A
    disallowed RID is logged and dropped cleanly (it is confirmed, not
    redelivered: redelivering something we will always reject is a loop).

    Failures that a retry could fix — a missing API key, an embedding outage —
    raise, so the poller leaves the event unconfirmed and it redelivers.
    """
    if not document_federation.document_federation_enabled():
        logger.warning(
            "domain.document.skip rid=%s reason=KOI_FEDERATE_DOCUMENTS_disabled", rid
        )
        return

    if not document_federation.rid_allowed(rid):
        logger.warning(
            "domain.document.reject rid=%s source_node=%s reason=rid_not_in_allowlist "
            "allowlist=%s",
            rid, source_node, list(document_federation.rid_allowlist()),
        )
        return

    if not document_federation.is_document_payload(payload):
        logger.warning(
            "domain.document.skip rid=%s reason=no_document_body — refusing to "
            "write a parent row with no text (it would look ingested and return "
            "nothing)", rid,
        )
        return

    markdown = payload["document"]["content"]

    # Strip the subscriber's own address from the body before it is chunked,
    # embedded and indexed. This MODIFIES STORED CONTENT (~0.74% of characters
    # corpus-wide); the verbatim original stays in the Phase-1 snapshot and the
    # bundle's sha256_hash is computed over the untouched bundle, so provenance
    # is unaffected. See document_federation.redact_subscriber_pii.
    markdown, redactions = document_federation.redact_subscriber_pii(markdown)

    source_meta = build_document_source_meta(rid, payload)
    if redactions:
        # Recorded so a redacted document is identifiable later without
        # re-deriving it from the snapshot.
        source_meta["redactions_applied"] = redactions

    ingest_document_rag_conn = _load_document_sink()
    embedder, chunker = _document_embedder_and_chunker()

    # The whole landing is one transaction so that a partial embed leaves NO
    # trace. A chunk with a null embedding is invisible to retrieval but counts
    # as a healthy row under count(*) — the precedent is 3,464 entities (11.8%)
    # that sat unsearchable with null embeds. Raising AFTER the write committed
    # would both persist that half-visible document and redeliver it; raising
    # inside the transaction rolls it back, so the redelivery starts clean.
    async with conn.transaction():
        result = await ingest_document_rag_conn(
            conn,
            document_rid=rid,
            markdown=markdown,
            source_meta=source_meta,
            embedder=embedder,
            chunker=chunker,
        )
        if result.get("null_embeds"):
            raise FederationDeferred(
                f"{rid}: {result['null_embeds']}/{result['chunks_total']} chunks "
                f"failed to embed — rolled back, not confirming, will redeliver"
            )

    logger.info(
        "domain.document.apply rid=%s author=%s is_private=%s chunks=%d "
        "redactions=%d source_node=%s",
        rid, source_meta.get("author"), source_meta.get("is_private"),
        result.get("chunks_written", 0), redactions, source_node,
    )


async def _forget_document(conn, rid: str, payload: Dict[str, Any]):
    """Delete a federated document and its chunks.

    Not routed through `_handle_forget`'s generic table map because that map
    deletes from a single table by a single column, and a document spans
    koi_memories + koi_memory_chunks. The FK is ON DELETE CASCADE so the parent
    delete suffices, but the chunks are deleted explicitly so the count is
    observable and the behaviour does not depend on a schema property.
    """
    if not document_federation.rid_allowed(rid):
        logger.warning(
            "domain.document.forget_reject rid=%s reason=rid_not_in_allowlist", rid
        )
        return
    chunks = await conn.execute(
        "DELETE FROM koi_memory_chunks WHERE document_rid = $1", rid
    )
    await conn.execute("DELETE FROM koi_memories WHERE rid = $1", rid)
    logger.info("domain.document.forget rid=%s chunks=%s", rid, chunks)
