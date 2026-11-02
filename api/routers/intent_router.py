"""Intent registry endpoints (ingest, list, update, review, vocabulary, groups, stats).

Provides the MVIS (Minimum Viable Intent System) registry for the Cascadia pilot.
Intents are first-class KOI graph entities: each gets an entity_registry row and
a deterministic ORN (orn:koi-net.intent:<hash>).

Privacy model: three response projections (Discovery / Detail / Coordinator).
  - Discovery: categories only, no identities or process metadata
  - Detail: adds publisher_name, intent_key, process metadata (no contact/excerpt)
  - Coordinator: adds publisher_contact, source_excerpt (Slice 2 digest only)

Routes are prefix-relative — prefix "/intents" is applied at mount in personal_ingest_api.py.
"""

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.utils import validity_filter_clause
from api.optional_federation import optional_federation_attr


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class IntentIngestRequest(BaseModel):
    intentKey: str = Field(..., description="Client idempotency key")
    intentType: str = Field(..., description="OFFER | WANT | SWAP")
    publisherName: str
    publisherContact: Optional[str] = None
    landscapeGroup: str
    visibility: Optional[str] = "local"
    assetOffered: Optional[str] = None
    assetWanted: Optional[str] = None
    quantity: Optional[str] = None
    description: Optional[str] = None
    decayRate: Optional[str] = "normal"
    validFrom: Optional[str] = None  # ISO date YYYY-MM-DD -> DATE
    expiresAt: Optional[str] = None
    captureMethod: Optional[str] = "manual"
    sourceDocument: Optional[str] = None
    sourceExcerpt: Optional[str] = None
    enteredBy: Optional[str] = None
    aiConfidence: Optional[float] = None
    tags: Optional[List[str]] = []


class IntentDiscoveryResponse(BaseModel):
    """Discovery projection — public/regional. Categories only, not identities."""
    intent_rid: str
    intent_type: str
    status: str
    landscape_group: str
    visibility: str
    asset_offered: Optional[str] = None
    asset_wanted: Optional[str] = None
    quantity: Optional[str] = None
    valid_from: Optional[str] = None
    expires_at: Optional[str] = None


class IntentDetailResponse(IntentDiscoveryResponse):
    """Internal view — includes process metadata but NOT contact or excerpt."""
    intent_key: str
    publisher_name: str
    priority: float
    tags: List[str] = []
    created_at: Optional[str] = None
    publisher_uri: Optional[str] = None
    description: Optional[str] = None
    capture_method: str = "manual"
    reviewed_by: Optional[str] = None


class IntentCoordinatorResponse(IntentDetailResponse):
    """Coordinator digest only — includes contact and excerpt for introduction."""
    publisher_contact: Optional[str] = None
    source_excerpt: Optional[str] = None


class IntentPatchRequest(BaseModel):
    status: Optional[str] = None
    visibility: Optional[str] = None
    description: Optional[str] = None
    notes: Optional[str] = None
    tags: Optional[List[str]] = None
    validFrom: Optional[str] = None  # ISO date; explicit null clears
    expiresAt: Optional[str] = None  # ISO date; explicit null clears


class ReviewRequest(BaseModel):
    reviewedBy: str = Field(..., description="Who is promoting this draft to active")


class VocabularyRequest(BaseModel):
    assetKey: str
    displayName: str
    category: Optional[str] = None
    landscapeGroup: Optional[str] = None


class VocabularyResponse(BaseModel):
    id: int
    asset_key: str
    display_name: str
    category: Optional[str] = None
    landscape_group: Optional[str] = None
    created_at: Optional[str] = None


class GroupConfigRequest(BaseModel):
    groupKey: str
    displayName: str
    decayLambda: Optional[float] = 0.023
    coordinatorName: Optional[str] = None
    coordinatorContact: Optional[str] = None


class GroupConfigResponse(BaseModel):
    id: int
    group_key: str
    display_name: str
    decay_lambda: float
    coordinator_name: Optional[str] = None
    coordinator_contact: Optional[str] = None
    created_at: Optional[str] = None


class IntentStatsResponse(BaseModel):
    by_status: Dict[str, int]
    by_type: Dict[str, int]
    by_landscape_group: Dict[str, int]
    stale_count: int
    expiring_soon: int


# ---------------------------------------------------------------------------
# Slice 2: Matching + Proposals models
# ---------------------------------------------------------------------------

class MatchRequest(BaseModel):
    landscapeGroup: Optional[str] = Field(
        None, description="Restrict matching to this landscape group"
    )
    includeRegional: bool = Field(
        False, description="Include regional intents in cross-group matching"
    )


class ProposalResponse(BaseModel):
    proposal_rid: str
    offer_intent_rid: str
    want_intent_rid: str
    match_type: str
    status: str
    score: Optional[float] = None
    coordinator_notes: Optional[str] = None
    proposed_at: Optional[str] = None
    introduced_at: Optional[str] = None
    resolved_at: Optional[str] = None
    resolved_by: Optional[str] = None


class ProposalPatchRequest(BaseModel):
    status: str = Field(
        ..., description="New status: introduced, accepted, declined, expired"
    )
    coordinatorNotes: Optional[str] = None
    resolvedBy: Optional[str] = None


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

def create_router(pool) -> APIRouter:
    """Return an APIRouter for intent registry endpoints.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    """
    router = APIRouter(tags=["intents"])

    emit_domain_event = optional_federation_attr("api.federation_events", "emit_domain_event")

    async def _emit_intent_discovery(row_dict: Dict[str, Any]):
        """Emit federation event with discovery-projection fields only.

        Only emits for active/fulfilled/archived intents — drafts are local-only.
        """
        status = row_dict.get("status", "draft")
        if status not in ("active", "fulfilled", "archived"):
            return
        await emit_domain_event(
            "intent",
            "NEW" if status == "active" else "UPDATE",
            row_dict["intent_rid"],
            {
                "intent_rid": row_dict["intent_rid"],
                "intent_type": row_dict.get("intent_type"),
                "status": status,
                "landscape_group": row_dict.get("landscape_group"),
                "visibility": row_dict.get("visibility"),
                "asset_offered": row_dict.get("asset_offered"),
                "asset_wanted": row_dict.get("asset_wanted"),
                "quantity": row_dict.get("quantity"),
            },
        )

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    def _generate_intent_rid(intent_key: str) -> str:
        """Generate deterministic ORN from intent_key using blake2b."""
        h = hashlib.blake2b(intent_key.encode("utf-8"), digest_size=16)
        return f"orn:koi-net.intent:{h.hexdigest()}"

    def _parse_wikilink_path(wikilink: str) -> Optional[str]:
        """Extract vault path from a wikilink like [[Folder/Name|alias]].

        Returns the path portion (e.g. "People/David Fortson"), or None if
        the input is not a wikilink format.
        """
        m = re.match(r'^\[\[([^\]|]+)(?:\|[^\]]+)?\]\]$', wikilink.strip())
        return m.group(1) if m else None

    async def _resolve_entity(conn, raw: str, entity_type: str) -> Optional[str]:
        """Resolve a wikilink or plain name to a canonical_uri.

        Attempts:
        1. Wikilink vault path lookup in entity_rid_mappings (exact, case-insensitive)
        2. Plain text name lookup in entity_rid_mappings (case-insensitive)
        3. Normalized text lookup in entity_registry (case-insensitive)

        entity_type hint prevents cross-type collisions (e.g. a Project named
        "RiverlinkAI" won't resolve as a Person).
        """
        if not raw or not raw.strip():
            return None

        raw = raw.strip()
        vault_path = _parse_wikilink_path(raw)

        if vault_path:
            row = await conn.fetchrow(
                """
                SELECT canonical_uri FROM entity_rid_mappings
                WHERE LOWER(vault_path) = LOWER($1)
                  AND (entity_type = $2 OR entity_type IS NULL)
                LIMIT 1
                """,
                vault_path, entity_type
            )
            if row:
                return row["canonical_uri"]

        name = vault_path.split("/")[-1] if vault_path else raw
        row = await conn.fetchrow(
            """
            SELECT canonical_uri FROM entity_rid_mappings
            WHERE LOWER(name) = LOWER($1)
              AND (entity_type = $2 OR entity_type IS NULL)
            LIMIT 1
            """,
            name, entity_type
        )
        if row:
            return row["canonical_uri"]

        row = await conn.fetchrow(
            """
            SELECT fuseki_uri FROM entity_registry
            WHERE LOWER(normalized_text) = LOWER($1)
              AND (entity_type = $2 OR entity_type IS NULL)
            LIMIT 1
            """,
            name.lower().strip(), entity_type
        )
        if row:
            # Follow a merge: this is a RESOLUTION path (the uri is persisted as an
            # owner / subject), so excluding a tombstone would lose the binding entirely
            # while following it attaches to the entity the caller meant.
            from api.resolution_primitives import resolve_to_live_uri
            return await resolve_to_live_uri(conn, row["fuseki_uri"])

        return None

    def _row_to_dict(row) -> Dict[str, Any]:
        """Convert an asyncpg Record to a serialisable dict."""
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, (date, datetime)):
                d[k] = v.isoformat()
        return d

    def _parse_date(s: Optional[str]) -> Optional[date]:
        """Parse an ISO date string; return None on failure or None input."""
        if not s:
            return None
        try:
            return date.fromisoformat(s)
        except ValueError:
            return None

    def _build_entity_text(req: IntentIngestRequest) -> str:
        """Build entity_text for entity_registry from ingest request."""
        if req.description:
            return req.description[:200]
        parts = [f"Intent: {req.intentType}"]
        if req.assetOffered:
            parts.append(req.assetOffered)
        if req.assetWanted:
            parts.append(req.assetWanted)
        return " ".join(parts)[:200]

    def _to_discovery(row_dict: Dict[str, Any]) -> IntentDiscoveryResponse:
        """Project a row dict into a discovery response (minimal fields)."""
        return IntentDiscoveryResponse(
            intent_rid=row_dict["intent_rid"],
            intent_type=row_dict["intent_type"],
            status=row_dict["status"],
            landscape_group=row_dict["landscape_group"],
            visibility=row_dict["visibility"],
            asset_offered=row_dict.get("asset_offered"),
            asset_wanted=row_dict.get("asset_wanted"),
            quantity=row_dict.get("quantity"),
            valid_from=row_dict.get("valid_from"),
            expires_at=row_dict.get("expires_at"),
        )

    def _to_detail(row_dict: Dict[str, Any]) -> IntentDetailResponse:
        """Project a row dict into a detail response (internal view)."""
        return IntentDetailResponse(
            intent_rid=row_dict["intent_rid"],
            intent_type=row_dict["intent_type"],
            status=row_dict["status"],
            landscape_group=row_dict["landscape_group"],
            visibility=row_dict["visibility"],
            asset_offered=row_dict.get("asset_offered"),
            asset_wanted=row_dict.get("asset_wanted"),
            quantity=row_dict.get("quantity"),
            valid_from=row_dict.get("valid_from"),
            expires_at=row_dict.get("expires_at"),
            intent_key=row_dict["intent_key"],
            publisher_name=row_dict["publisher_name"],
            priority=row_dict.get("priority", 100.0),
            tags=row_dict.get("tags") or [],
            created_at=row_dict.get("created_at"),
            publisher_uri=row_dict.get("publisher_uri"),
            description=row_dict.get("description"),
            capture_method=row_dict.get("capture_method", "manual"),
            reviewed_by=row_dict.get("reviewed_by"),
        )

    async def _log_state_transition(
        conn, intent_rid: str, from_status: Optional[str],
        to_status: str, actor: Optional[str] = None,
        reason: Optional[str] = None,
    ):
        """Insert a row into intent_state_log."""
        await conn.execute(
            """
            INSERT INTO intent_state_log
                (intent_rid, from_status, to_status, actor, reason, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            """,
            intent_rid, from_status, to_status, actor, reason,
        )

    # -------------------------------------------------------------------
    # POST /ingest — upsert by intent_key
    # -------------------------------------------------------------------

    @router.post("/ingest", response_model=IntentDetailResponse)
    async def ingest_intent(req: IntentIngestRequest):
        """Upsert an intent by intentKey.

        Creates entity_registry row (Intent as first-class graph entity) and
        intent_registry row. Default status is 'draft'. Partial payloads are
        safe: existing values are preserved via COALESCE on conflict.
        """
        intent_rid = _generate_intent_rid(req.intentKey)
        entity_text = _build_entity_text(req)
        valid_from = _parse_date(req.validFrom)
        expires_at = _parse_date(req.expiresAt)

        async with pool.acquire() as conn:
            # Resolve publisher to entity URI
            publisher_uri = await _resolve_entity(
                conn, req.publisherName, "Person"
            ) if req.publisherName else None

            # Ensure entity_registry row exists for this intent.
            #
            # PROVENANCE IS LOAD-BEARING HERE, not decoration. This INSERT bypasses
            # store_new_entity, so before 2026-08-22 it named no `source` and no
            # `metadata`: every row it wrote inherited the column default
            # source='personal-vault' and was indistinguishable from a vault entity.
            # The fuseki_uri is a bare blake2b hash and the entity_text is built from
            # the description ("Test firewood offer", "Intent: OFFER"), so there was
            # nothing to match on either — see the comment on tests/test_intent_registry.py
            # intent_rid(), which had to recompute the hash per key to clean up after itself.
            # 300 fixture rows accumulated in the live graph, and because the every-300s
            # embedding-repair job is type-agnostic it embedded all of them, after which
            # they ranked #1 in /knowledge/unified-search for need- and offer-shaped
            # queries. Stamping source + intent_key makes the population identifiable by
            # a plain WHERE clause instead of a hash recomputation.
            #
            # Deliberately NOT routed through store_new_entity: that would add a
            # synchronous embedding round-trip to this request (+369ms median, 17s
            # observed worst case) inside `async with pool.acquire()`, and buy nothing,
            # because backfill_null_embeddings.py already covers the entity surface
            # every 300s. The columns below are what store_new_entity would have set
            # that actually matter for attribution.
            #
            # resolution_tier='manual' closes the migration-111 diagnostic on this path.
            # That instrumentation is how this leak was found: every instrumented create
            # path stamps a tier, so a population that is entirely NULL identifies an
            # uninstrumented direct-INSERT. Leaving this path NULL after fixing it would
            # hand the next investigator the same false signal. 'manual' (not
            # 'tier3_created') because an intent is keyed by its RID, so two intents
            # sharing a description are not a minted duplicate and must not be counted
            # as tier3_created_ambiguous.
            #
            # merged_into is deliberately untouched on conflict: reviving a tombstoned
            # row here would silently undo an operator's disposition.
            await conn.execute(
                """
                INSERT INTO entity_registry
                    (entity_type, entity_text, normalized_text, fuseki_uri,
                     source, first_seen_rid, metadata, resolution_tier, created_at)
                VALUES ('Intent', $1, LOWER($1), $2,
                        'intent-registry', $2, $3::jsonb, 'manual', NOW())
                ON CONFLICT (fuseki_uri) DO UPDATE SET
                    entity_text = EXCLUDED.entity_text,
                    normalized_text = EXCLUDED.normalized_text,
                    source = EXCLUDED.source,
                    metadata = COALESCE(entity_registry.metadata, '{}'::jsonb)
                               || EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                entity_text, intent_rid,
                json.dumps({
                    "intent_key": req.intentKey,
                    "intent_type": req.intentType,
                    "landscape_group": req.landscapeGroup,
                }),
            )

            # Upsert intent_registry row
            row = await conn.fetchrow(
                """
                INSERT INTO intent_registry (
                    intent_rid, intent_key, entity_uri,
                    intent_type, status,
                    publisher_name, publisher_contact, publisher_uri,
                    landscape_group, visibility,
                    asset_offered, asset_wanted, quantity, description,
                    priority, decay_rate, last_refreshed_at, valid_from, expires_at,
                    capture_method, source_document, source_excerpt,
                    entered_by, reviewed_by, ai_confidence,
                    tags, created_at, updated_at
                ) VALUES (
                    $1, $2, $1,
                    $3, COALESCE($4, 'draft'),
                    $5, $6, $7,
                    $8, COALESCE($9, 'local'),
                    $10, $11, $12, $13,
                    100.0, COALESCE($14, 'normal'), NOW(), $15, $16,
                    COALESCE($17, 'manual'), $18, $19,
                    $20, NULL, $21,
                    $22, NOW(), NOW()
                )
                ON CONFLICT (intent_key) DO UPDATE SET
                    intent_type     = EXCLUDED.intent_type,
                    status          = CASE WHEN $4 IS NULL
                                        THEN intent_registry.status
                                        ELSE $4 END,
                    publisher_name  = EXCLUDED.publisher_name,
                    publisher_contact = COALESCE(EXCLUDED.publisher_contact,
                                                 intent_registry.publisher_contact),
                    publisher_uri   = COALESCE(EXCLUDED.publisher_uri,
                                               intent_registry.publisher_uri),
                    landscape_group = EXCLUDED.landscape_group,
                    visibility      = COALESCE(EXCLUDED.visibility,
                                               intent_registry.visibility),
                    asset_offered   = COALESCE(EXCLUDED.asset_offered,
                                               intent_registry.asset_offered),
                    asset_wanted    = COALESCE(EXCLUDED.asset_wanted,
                                               intent_registry.asset_wanted),
                    quantity        = COALESCE(EXCLUDED.quantity,
                                              intent_registry.quantity),
                    description     = COALESCE(EXCLUDED.description,
                                               intent_registry.description),
                    decay_rate      = COALESCE(EXCLUDED.decay_rate,
                                               intent_registry.decay_rate),
                    valid_from      = COALESCE(EXCLUDED.valid_from,
                                               intent_registry.valid_from),
                    expires_at      = COALESCE(EXCLUDED.expires_at,
                                               intent_registry.expires_at),
                    capture_method  = COALESCE(EXCLUDED.capture_method,
                                               intent_registry.capture_method),
                    source_document = COALESCE(EXCLUDED.source_document,
                                               intent_registry.source_document),
                    source_excerpt  = COALESCE(EXCLUDED.source_excerpt,
                                               intent_registry.source_excerpt),
                    entered_by      = COALESCE(EXCLUDED.entered_by,
                                               intent_registry.entered_by),
                    ai_confidence   = COALESCE(EXCLUDED.ai_confidence,
                                               intent_registry.ai_confidence),
                    tags            = CASE WHEN array_length(EXCLUDED.tags, 1) > 0
                                        THEN EXCLUDED.tags
                                        ELSE intent_registry.tags END,
                    updated_at      = NOW()
                RETURNING *,
                    (xmax = 0) AS was_inserted
                """,
                intent_rid, req.intentKey,
                req.intentType, None,  # status always None for COALESCE default
                req.publisherName, req.publisherContact, publisher_uri,
                req.landscapeGroup, req.visibility,
                req.assetOffered, req.assetWanted, req.quantity, req.description,
                req.decayRate, valid_from, expires_at,
                req.captureMethod, req.sourceDocument, req.sourceExcerpt,
                req.enteredBy, req.aiConfidence,
                req.tags or [],
            )

            # Log state transition
            was_inserted = row["was_inserted"]
            if was_inserted:
                await _log_state_transition(
                    conn, intent_rid, None, row["status"],
                    actor=req.enteredBy or "system",
                    reason="ingest_created",
                )

        row_dict = _row_to_dict(row)
        # Emit federation update if upserting an already-active intent.
        # New drafts are not federated (handled by _emit_intent_discovery guard).
        # But if an active intent is updated via ingest, peers need the new data.
        if not was_inserted:
            await _emit_intent_discovery(row_dict)
        return _to_detail(row_dict)

    # -------------------------------------------------------------------
    # GET / — public discovery (active intents only by default)
    # -------------------------------------------------------------------

    @router.get("/", response_model=List[IntentDiscoveryResponse])
    async def list_intents_discovery(
        status: Optional[str] = Query(
            None,
            description="Comma-separated statuses; default shows only active",
        ),
        landscape_group: Optional[str] = Query(None),
        intent_type: Optional[str] = Query(None),
        asset_offered: Optional[str] = Query(None),
        asset_wanted: Optional[str] = Query(None),
        t_now: Optional[str] = Query(
            None,
            description=(
                "ISO date YYYY-MM-DD. When set, filters out intents whose "
                "[valid_from, expires_at] window does not contain t_now. "
                "Default-off, opt-in."
            ),
        ),
        include_out_of_window: bool = Query(
            False, description="Bypass validity filter when t_now is set."
        ),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """Public discovery endpoint. Returns only active intents by default.

        Discovery projection: categories and asset types only. No publisher
        names, tags, priority, or process metadata.
        """
        async with pool.acquire() as conn:
            conditions: List[str] = []
            params: List[Any] = []

            def add(clause: str, val: Any):
                params.append(val)
                conditions.append(clause.replace("?", f"${len(params)}"))

            if status:
                status_list = [s.strip() for s in status.split(",")]
                params.append(status_list)
                conditions.append(f"status = ANY(${len(params)})")
            else:
                conditions.append("status = 'active'")

            if landscape_group:
                add("landscape_group = ?", landscape_group)
            if intent_type:
                add("intent_type = ?", intent_type)
            if asset_offered:
                add("asset_offered = ?", asset_offered)
            if asset_wanted:
                add("asset_wanted = ?", asset_wanted)

            # Opt-in validity filter on intent_registry (DATE columns).
            t_now_date: Optional[date] = None
            if t_now:
                try:
                    t_now_date = date.fromisoformat(t_now)
                except ValueError:
                    t_now_date = None
            validity_sql, validity_binds = validity_filter_clause(
                t_now=t_now_date,
                include_out_of_window=include_out_of_window,
                start_col="valid_from",
                end_col="expires_at",
                value_cast="::date",
                next_param_index=len(params) + 1,
            )
            params.extend(validity_binds)

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            if validity_sql:
                where = (where or "WHERE 1=1") + validity_sql

            params += [limit, offset]
            query = f"""
                SELECT * FROM intent_registry
                {where}
                ORDER BY priority DESC, created_at DESC
                LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """
            rows = await conn.fetch(query, *params)

        return [_to_discovery(_row_to_dict(r)) for r in rows]

    # -------------------------------------------------------------------
    # GET /detail — internal list (all statuses)
    # -------------------------------------------------------------------

    @router.get("/detail", response_model=List[IntentDetailResponse])
    async def list_intents_detail(
        status: Optional[str] = Query(None, description="Comma-separated statuses"),
        landscape_group: Optional[str] = Query(None),
        intent_type: Optional[str] = Query(None),
        capture_method: Optional[str] = Query(None),
        t_now: Optional[str] = Query(
            None,
            description=(
                "ISO date YYYY-MM-DD. When set, filters out intents whose "
                "[valid_from, expires_at] window does not contain t_now. "
                "Default-off, opt-in."
            ),
        ),
        include_out_of_window: bool = Query(
            False, description="Bypass validity filter when t_now is set."
        ),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """Internal list endpoint. All statuses including draft.

        Detail projection: includes publisher_name, intent_key, process metadata
        but NOT publisher_contact or source_excerpt.
        """
        async with pool.acquire() as conn:
            conditions: List[str] = []
            params: List[Any] = []

            def add(clause: str, val: Any):
                params.append(val)
                conditions.append(clause.replace("?", f"${len(params)}"))

            if status:
                status_list = [s.strip() for s in status.split(",")]
                params.append(status_list)
                conditions.append(f"status = ANY(${len(params)})")

            if landscape_group:
                add("landscape_group = ?", landscape_group)
            if intent_type:
                add("intent_type = ?", intent_type)
            if capture_method:
                add("capture_method = ?", capture_method)

            # Same opt-in validity filter as list_intents_discovery.
            t_now_date: Optional[date] = None
            if t_now:
                try:
                    t_now_date = date.fromisoformat(t_now)
                except ValueError:
                    t_now_date = None
            validity_sql, validity_binds = validity_filter_clause(
                t_now=t_now_date,
                include_out_of_window=include_out_of_window,
                start_col="valid_from",
                end_col="expires_at",
                value_cast="::date",
                next_param_index=len(params) + 1,
            )
            params.extend(validity_binds)

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            if validity_sql:
                where = (where or "WHERE 1=1") + validity_sql

            params += [limit, offset]
            query = f"""
                SELECT * FROM intent_registry
                {where}
                ORDER BY
                    CASE status
                        WHEN 'draft' THEN 1
                        WHEN 'active' THEN 2
                        WHEN 'stale' THEN 3
                        WHEN 'fulfilled' THEN 4
                        WHEN 'archived' THEN 5
                        ELSE 6
                    END,
                    priority DESC,
                    created_at DESC
                LIMIT ${len(params) - 1} OFFSET ${len(params)}
            """
            rows = await conn.fetch(query, *params)

        return [_to_detail(_row_to_dict(r)) for r in rows]

    # -------------------------------------------------------------------
    # GET /detail/{intent_key} — single intent by key
    # -------------------------------------------------------------------

    @router.get("/detail/{intent_key}", response_model=IntentDetailResponse)
    async def get_intent_detail(intent_key: str):
        """Get a single intent by key. Used for draft inspection and CRUD verification."""
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM intent_registry WHERE intent_key = $1",
                intent_key,
            )
        if not row:
            raise HTTPException(status_code=404, detail=f"Intent not found: {intent_key}")
        return _to_detail(_row_to_dict(row))

    # -------------------------------------------------------------------
    # PATCH /{intent_key} — partial update
    # -------------------------------------------------------------------

    @router.patch("/{intent_key}", response_model=IntentDetailResponse)
    async def patch_intent(intent_key: str, req: IntentPatchRequest):
        """Partial update. State log entry on status change.

        Only provided fields are updated; absent fields are preserved.

        Draft → active MUST go through POST /{key}/review (the human-in-the-loop
        membrane). This endpoint rejects that transition to prevent bypass.
        """
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT * FROM intent_registry WHERE intent_key = $1", intent_key
            )
            if not existing:
                raise HTTPException(
                    status_code=404, detail=f"Intent not found: {intent_key}"
                )

            old_status = existing["status"]

            # Guard: draft → active must go through /review, not PATCH
            if req.status == "active" and old_status == "draft":
                raise HTTPException(
                    status_code=409,
                    detail="Draft → active transition requires POST /{key}/review. "
                           "Use the review endpoint to promote drafts.",
                )

            now = datetime.now(timezone.utc).replace(tzinfo=None)

            updates: Dict[str, Any] = {"updated_at": now}

            if req.status is not None:
                updates["status"] = req.status
            if req.visibility is not None:
                updates["visibility"] = req.visibility
            if req.description is not None:
                updates["description"] = req.description
            if req.notes is not None:
                updates["notes"] = req.notes
            if req.tags is not None:
                updates["tags"] = req.tags

            # Validity columns: explicit-null clears; valid string sets; absent skips.
            explicitly_set = req.model_fields_set
            for field, col in (("validFrom", "valid_from"), ("expiresAt", "expires_at")):
                if field in explicitly_set:
                    raw = getattr(req, field)
                    if raw is None:
                        updates[col] = None
                    else:
                        parsed = _parse_date(raw)
                        if parsed is not None:
                            updates[col] = parsed

            # Auto-timestamps based on status transition
            new_status = req.status if req.status is not None else old_status
            if new_status == "fulfilled" and old_status != "fulfilled":
                updates["fulfilled_at"] = now
            if new_status == "archived" and old_status != "archived":
                updates["archived_at"] = now

            # Build SET clause
            set_clauses: List[str] = []
            vals: List[Any] = []
            for col, val in updates.items():
                vals.append(val)
                set_clauses.append(f"{col} = ${len(vals)}")

            vals.append(intent_key)
            row = await conn.fetchrow(
                f"""UPDATE intent_registry
                    SET {', '.join(set_clauses)}
                    WHERE intent_key = ${len(vals)}
                    RETURNING *""",
                *vals,
            )

            # Log state transition if status changed
            if req.status is not None and req.status != old_status:
                await _log_state_transition(
                    conn,
                    existing["intent_rid"],
                    old_status,
                    req.status,
                    actor="api",
                    reason="patch",
                )

        row_dict = _row_to_dict(row)
        # Only federate if the intent was previously known to peers (was active).
        # Draft intents that get archived/stale via PATCH were never federated,
        # so emitting would leak their existence.
        if old_status in ("active", "fulfilled"):
            await _emit_intent_discovery(row_dict)
        return _to_detail(row_dict)

    # -------------------------------------------------------------------
    # POST /{intent_key}/refresh — reset priority, update last_refreshed_at
    # -------------------------------------------------------------------

    @router.post("/{intent_key}/refresh", response_model=IntentDetailResponse)
    async def refresh_intent(intent_key: str):
        """Reset priority to 100 and update last_refreshed_at.

        Used when a publisher confirms their intent is still valid.
        """
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE intent_registry
                SET priority = 100.0,
                    last_refreshed_at = NOW(),
                    updated_at = NOW()
                WHERE intent_key = $1
                RETURNING *
                """,
                intent_key,
            )
        if not row:
            raise HTTPException(
                status_code=404, detail=f"Intent not found: {intent_key}"
            )
        return _to_detail(_row_to_dict(row))

    # -------------------------------------------------------------------
    # POST /{intent_key}/review — promote draft to active
    # -------------------------------------------------------------------

    @router.post("/{intent_key}/review", response_model=IntentDetailResponse)
    async def review_intent(intent_key: str, req: ReviewRequest):
        """Promote a draft intent to active. Sets reviewed_by.

        Rejects if the intent is not in 'draft' status — the draft-to-active
        transition is the human-in-the-loop membrane.
        """
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT * FROM intent_registry WHERE intent_key = $1", intent_key
            )
            if not existing:
                raise HTTPException(
                    status_code=404, detail=f"Intent not found: {intent_key}"
                )
            if existing["status"] != "draft":
                raise HTTPException(
                    status_code=409,
                    detail=f"Intent is '{existing['status']}', not 'draft'. "
                           f"Only draft intents can be reviewed.",
                )

            row = await conn.fetchrow(
                """
                UPDATE intent_registry
                SET status = 'active',
                    reviewed_by = $1,
                    updated_at = NOW()
                WHERE intent_key = $2
                RETURNING *
                """,
                req.reviewedBy, intent_key,
            )

            await _log_state_transition(
                conn,
                existing["intent_rid"],
                "draft",
                "active",
                actor=req.reviewedBy,
                reason="review_approved",
            )

        row_dict = _row_to_dict(row)
        await _emit_intent_discovery(row_dict)
        return _to_detail(row_dict)

    # -------------------------------------------------------------------
    # GET /stats — aggregate counts
    # -------------------------------------------------------------------

    @router.get("/stats", response_model=IntentStatsResponse)
    async def get_stats():
        """Return aggregate intent counts by status, type, and landscape group.

        Also returns stale count and expiring-soon count (expires within 7 days).
        """
        async with pool.acquire() as conn:
            today = date.today()

            by_status_rows = await conn.fetch(
                "SELECT status, COUNT(*) AS cnt FROM intent_registry GROUP BY status"
            )
            by_status = {r["status"]: r["cnt"] for r in by_status_rows}

            by_type_rows = await conn.fetch(
                "SELECT intent_type, COUNT(*) AS cnt FROM intent_registry GROUP BY intent_type"
            )
            by_type = {r["intent_type"]: r["cnt"] for r in by_type_rows}

            by_group_rows = await conn.fetch(
                "SELECT landscape_group, COUNT(*) AS cnt FROM intent_registry GROUP BY landscape_group"
            )
            by_landscape_group = {r["landscape_group"]: r["cnt"] for r in by_group_rows}

            stale_count = await conn.fetchval(
                "SELECT COUNT(*) FROM intent_registry WHERE status = 'stale'"
            )

            expiring_soon = await conn.fetchval(
                """
                SELECT COUNT(*) FROM intent_registry
                WHERE expires_at IS NOT NULL
                  AND expires_at BETWEEN $1 AND $1 + INTERVAL '7 days'
                  AND status NOT IN ('fulfilled', 'archived')
                """,
                today,
            )

        return IntentStatsResponse(
            by_status=by_status,
            by_type=by_type,
            by_landscape_group=by_landscape_group,
            stale_count=stale_count or 0,
            expiring_soon=expiring_soon or 0,
        )

    # -------------------------------------------------------------------
    # GET /vocabulary — list asset vocabulary
    # -------------------------------------------------------------------

    @router.get("/vocabulary", response_model=List[VocabularyResponse])
    async def list_vocabulary(
        category: Optional[str] = Query(None),
        landscape_group: Optional[str] = Query(None),
    ):
        """List controlled asset vocabulary. Filter by category or landscape group."""
        async with pool.acquire() as conn:
            conditions: List[str] = []
            params: List[Any] = []

            def add(clause: str, val: Any):
                params.append(val)
                conditions.append(clause.replace("?", f"${len(params)}"))

            if category:
                add("category = ?", category)
            if landscape_group:
                # Include global terms (NULL landscape_group) plus group-specific
                params.append(landscape_group)
                conditions.append(
                    f"(landscape_group = ${len(params)} OR landscape_group IS NULL)"
                )

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            rows = await conn.fetch(
                f"""
                SELECT * FROM intent_asset_vocabulary
                {where}
                ORDER BY category NULLS LAST, display_name
                """,
                *params,
            )

        return [VocabularyResponse(**_row_to_dict(r)) for r in rows]

    # -------------------------------------------------------------------
    # POST /vocabulary — add to controlled vocabulary
    # -------------------------------------------------------------------

    @router.post("/vocabulary", response_model=VocabularyResponse)
    async def add_vocabulary(req: VocabularyRequest):
        """Add an asset to the controlled vocabulary.

        Upserts by asset_key — display_name and category are updated on conflict.
        """
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO intent_asset_vocabulary
                    (asset_key, display_name, category, landscape_group, created_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (asset_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    category = COALESCE(EXCLUDED.category,
                                        intent_asset_vocabulary.category),
                    landscape_group = COALESCE(EXCLUDED.landscape_group,
                                               intent_asset_vocabulary.landscape_group)
                RETURNING *
                """,
                req.assetKey, req.displayName, req.category, req.landscapeGroup,
            )

        return VocabularyResponse(**_row_to_dict(row))

    # -------------------------------------------------------------------
    # GET /groups — list landscape group configs
    # -------------------------------------------------------------------

    @router.get("/groups", response_model=List[GroupConfigResponse])
    async def list_groups():
        """List all landscape group configurations."""
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM landscape_group_config ORDER BY display_name"
            )
        return [GroupConfigResponse(**_row_to_dict(r)) for r in rows]

    # -------------------------------------------------------------------
    # POST /groups — create/update landscape group config
    # -------------------------------------------------------------------

    @router.post("/groups", response_model=GroupConfigResponse)
    async def upsert_group(req: GroupConfigRequest):
        """Create or update a landscape group configuration.

        Upserts by group_key. Coordinator info and decay_lambda are updated
        on conflict.
        """
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO landscape_group_config
                    (group_key, display_name, decay_lambda,
                     coordinator_name, coordinator_contact, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (group_key) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    decay_lambda = COALESCE(EXCLUDED.decay_lambda,
                                            landscape_group_config.decay_lambda),
                    coordinator_name = COALESCE(EXCLUDED.coordinator_name,
                                                landscape_group_config.coordinator_name),
                    coordinator_contact = COALESCE(EXCLUDED.coordinator_contact,
                                                   landscape_group_config.coordinator_contact)
                RETURNING *
                """,
                req.groupKey, req.displayName, req.decayLambda,
                req.coordinatorName, req.coordinatorContact,
            )

        return GroupConfigResponse(**_row_to_dict(row))

    # ===================================================================
    # Slice 2: Matching + Proposals + Coordinator Digest
    # ===================================================================

    def _generate_proposal_rid(offer_rid: str, want_rid: str) -> str:
        """Generate deterministic proposal ORN from offer + want RIDs."""
        payload = f"{offer_rid}+{want_rid}"
        h = hashlib.blake2b(payload.encode("utf-8"), digest_size=16)
        return f"orn:koi-net.match:{h.hexdigest()}"

    def _proposal_row_to_response(row) -> ProposalResponse:
        d = _row_to_dict(row)
        return ProposalResponse(**{k: d[k] for k in ProposalResponse.model_fields if k in d})

    # -------------------------------------------------------------------
    # POST /match — run matching algorithm
    # -------------------------------------------------------------------

    @router.post("/match", response_model=List[ProposalResponse])
    async def run_matching(req: MatchRequest):
        """Run matching algorithm over active intents.

        O(n^2) over active intents, exact match on asset_key.
        Respects visibility: local intents only match within same
        landscape_group; regional intents can match cross-group.
        SWAP intents are treated as both OFFER and WANT simultaneously.
        """
        async with pool.acquire() as conn:
            # Fetch active intents, optionally filtered by landscape group.
            # Phase 1 scope expansion: matcher MUST NOT propose against expired
            # or not-yet-valid intents. Native CURRENT_DATE predicate (not via
            # the helper) — matcher has no t_now param; "now" is server-side.
            conditions = [
                "status = 'active'",
                "(valid_from IS NULL OR valid_from <= CURRENT_DATE)",
                "(expires_at IS NULL OR expires_at >= CURRENT_DATE)",
            ]
            params: List[Any] = []

            if req.landscapeGroup and not req.includeRegional:
                params.append(req.landscapeGroup)
                conditions.append(f"landscape_group = ${len(params)}")
            elif req.landscapeGroup and req.includeRegional:
                # Include intents from this group + all regional intents
                params.append(req.landscapeGroup)
                conditions.append(
                    f"(landscape_group = ${len(params)} OR visibility = 'regional')"
                )

            where = "WHERE " + " AND ".join(conditions)
            rows = await conn.fetch(
                f"SELECT * FROM intent_registry {where}", *params
            )

            intents = [dict(r) for r in rows]

            # Build offer-side and want-side lists.
            # OFFER intents contribute to offer_side.
            # WANT intents contribute to want_side.
            # SWAP intents contribute to BOTH sides.
            offer_side = []  # (intent_dict, offered_asset)
            want_side = []   # (intent_dict, wanted_asset)

            for i in intents:
                itype = i["intent_type"]
                if itype == "OFFER":
                    if i.get("asset_offered"):
                        offer_side.append((i, i["asset_offered"]))
                elif itype == "WANT":
                    if i.get("asset_wanted"):
                        want_side.append((i, i["asset_wanted"]))
                elif itype == "SWAP":
                    if i.get("asset_offered"):
                        offer_side.append((i, i["asset_offered"]))
                    if i.get("asset_wanted"):
                        want_side.append((i, i["asset_wanted"]))

            created_proposals = []
            seen_bilateral_pairs = set()  # Track SWAP bilateral matches

            for offer_intent, offered_asset in offer_side:
                for want_intent, wanted_asset in want_side:
                    # Skip self-match
                    if offer_intent["intent_rid"] == want_intent["intent_rid"]:
                        continue

                    # Check exact asset match: offer's asset == want's desired asset
                    if offered_asset != wanted_asset:
                        continue

                    # Visibility check: local intents only match within same group
                    offer_vis = offer_intent.get("visibility", "local")
                    want_vis = want_intent.get("visibility", "local")
                    offer_group = offer_intent["landscape_group"]
                    want_group = want_intent["landscape_group"]
                    same_group = (offer_group == want_group)

                    if not same_group:
                        # Cross-group match: at least one must be regional
                        if offer_vis == "local" and want_vis == "local":
                            continue

                    match_type = "local" if same_group else "cross_landscape"

                    # Determine canonical RID ordering
                    o_rid = offer_intent["intent_rid"]
                    w_rid = want_intent["intent_rid"]

                    # SWAP bilateral detection: both are SWAP and each offers
                    # what the other wants
                    if (offer_intent["intent_type"] == "SWAP"
                            and want_intent["intent_type"] == "SWAP"):
                        # Check if this is a bilateral match (both directions)
                        # Use canonical ordering: lower RID as offer_intent_rid
                        pair_key = tuple(sorted([o_rid, w_rid]))
                        if pair_key in seen_bilateral_pairs:
                            continue
                        seen_bilateral_pairs.add(pair_key)
                        # Canonical: lower RID as offer
                        o_rid, w_rid = pair_key

                    # Compute score: min priority of the pair
                    score = min(
                        offer_intent.get("priority", 100.0),
                        want_intent.get("priority", 100.0),
                    )

                    proposal_rid = _generate_proposal_rid(o_rid, w_rid)

                    # INSERT with ON CONFLICT for dedup
                    row = await conn.fetchrow(
                        """
                        INSERT INTO intent_match_proposals
                            (proposal_rid, offer_intent_rid, want_intent_rid,
                             match_type, status, score, proposed_at)
                        VALUES ($1, $2, $3, $4, 'candidate', $5, NOW())
                        ON CONFLICT (offer_intent_rid, want_intent_rid)
                            WHERE status IN ('candidate', 'introduced')
                        DO NOTHING
                        RETURNING *
                        """,
                        proposal_rid, o_rid, w_rid, match_type, score,
                    )

                    if row:
                        created_proposals.append(_proposal_row_to_response(row))

        return created_proposals

    # -------------------------------------------------------------------
    # PATCH /proposals/{proposal_rid} — update proposal status
    # -------------------------------------------------------------------

    VALID_TRANSITIONS = {
        "candidate": {"introduced", "declined", "expired"},
        "introduced": {"accepted", "declined", "expired"},
    }

    @router.patch("/proposals/{proposal_rid}", response_model=ProposalResponse)
    async def patch_proposal(proposal_rid: str, req: ProposalPatchRequest):
        """Update a match proposal's status.

        Valid transitions:
          candidate -> introduced, declined, expired
          introduced -> accepted, declined, expired

        On 'accepted': both intents move to 'fulfilled' with state log entries.
        """
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT * FROM intent_match_proposals WHERE proposal_rid = $1",
                proposal_rid,
            )
            if not existing:
                raise HTTPException(
                    status_code=404,
                    detail=f"Proposal not found: {proposal_rid}",
                )

            old_status = existing["status"]
            new_status = req.status

            allowed = VALID_TRANSITIONS.get(old_status)
            if not allowed or new_status not in allowed:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Invalid transition: {old_status} -> {new_status}. "
                        f"Allowed from '{old_status}': {sorted(allowed) if allowed else 'none (terminal state)'}"
                    ),
                )

            now = datetime.now(timezone.utc).replace(tzinfo=None)

            # Build update fields
            updates: Dict[str, Any] = {"status": new_status}
            if req.coordinatorNotes is not None:
                updates["coordinator_notes"] = req.coordinatorNotes
            if new_status == "introduced":
                updates["introduced_at"] = now
            if new_status in ("accepted", "declined", "expired"):
                updates["resolved_at"] = now
                if req.resolvedBy:
                    updates["resolved_by"] = req.resolvedBy

            set_clauses: List[str] = []
            vals: List[Any] = []
            for col, val in updates.items():
                vals.append(val)
                set_clauses.append(f"{col} = ${len(vals)}")

            vals.append(proposal_rid)
            row = await conn.fetchrow(
                f"""UPDATE intent_match_proposals
                    SET {', '.join(set_clauses)}
                    WHERE proposal_rid = ${len(vals)}
                    RETURNING *""",
                *vals,
            )

            # On accepted: fulfill both intents and emit federation updates
            fulfilled_intents = []
            if new_status == "accepted":
                for rid_col in ("offer_intent_rid", "want_intent_rid"):
                    intent_rid = existing[rid_col]

                    # Get current intent status
                    intent_row = await conn.fetchrow(
                        "SELECT * FROM intent_registry WHERE intent_rid = $1",
                        intent_rid,
                    )
                    if intent_row and intent_row["status"] != "fulfilled":
                        old_intent_status = intent_row["status"]
                        updated_intent = await conn.fetchrow(
                            """UPDATE intent_registry
                               SET status = 'fulfilled',
                                   fulfilled_at = $1,
                                   updated_at = $1
                               WHERE intent_rid = $2
                               RETURNING *""",
                            now, intent_rid,
                        )
                        await _log_state_transition(
                            conn, intent_rid, old_intent_status, "fulfilled",
                            actor=req.resolvedBy or "coordinator",
                            reason=f"match_accepted:{proposal_rid}",
                        )
                        if updated_intent:
                            fulfilled_intents.append(_row_to_dict(updated_intent))

        # Emit fulfilled-state federation updates for both intents
        # (outside the connection context manager to avoid holding the conn)
        for intent_dict in fulfilled_intents:
            await _emit_intent_discovery(intent_dict)

        return _proposal_row_to_response(row)

    # -------------------------------------------------------------------
    # GET /proposals — list proposals with filters
    # -------------------------------------------------------------------

    @router.get("/proposals", response_model=List[ProposalResponse])
    async def list_proposals(
        status: Optional[str] = Query(None, description="Filter by proposal status"),
        landscape_group: Optional[str] = Query(
            None,
            description="Filter by landscape group (joins to intent_registry)",
        ),
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
    ):
        """List match proposals with optional filters."""
        async with pool.acquire() as conn:
            conditions: List[str] = []
            params: List[Any] = []

            def add(clause: str, val: Any):
                params.append(val)
                conditions.append(clause.replace("?", f"${len(params)}"))

            if status:
                add("p.status = ?", status)
            if landscape_group:
                add(
                    "(o.landscape_group = ? OR w.landscape_group = ?)",
                    landscape_group,
                )
                # Need to add the parameter again for the second placeholder
                params.append(landscape_group)
                # Fix the condition to use correct param index
                conditions[-1] = (
                    f"(o.landscape_group = ${len(params) - 1}"
                    f" OR w.landscape_group = ${len(params)})"
                )

            where = "WHERE " + " AND ".join(conditions) if conditions else ""

            params += [limit, offset]
            rows = await conn.fetch(
                f"""
                SELECT p.*
                FROM intent_match_proposals p
                JOIN intent_registry o ON o.intent_rid = p.offer_intent_rid
                JOIN intent_registry w ON w.intent_rid = p.want_intent_rid
                {where}
                ORDER BY p.score DESC NULLS LAST, p.proposed_at DESC
                LIMIT ${len(params) - 1} OFFSET ${len(params)}
                """,
                *params,
            )

        return [_proposal_row_to_response(r) for r in rows]

    # -------------------------------------------------------------------
    # GET /digest/{landscape_group} — coordinator digest
    # -------------------------------------------------------------------

    @router.get("/digest/{landscape_group}")
    async def coordinator_digest(landscape_group: str):
        """Generate a coordinator digest for a landscape group.

        Returns markdown text with sections:
        1. Local Matches — candidate proposals where both intents are in this group
        2. Cross-Landscape Opportunities — candidate proposals where one intent is
           in this group and the other is regional
        3. Unmet Needs — active WANT intents with no matching proposals
        4. Stale Intents — active intents with priority < 20

        This is the ONLY endpoint that uses IntentCoordinatorResponse
        (includes publisher_contact and source_excerpt).
        """
        async with pool.acquire() as conn:
            # 1. Local matches: both intents in this landscape group
            local_matches = await conn.fetch(
                """
                SELECT p.*,
                    o.publisher_name AS offer_publisher, o.publisher_contact AS offer_contact,
                    o.asset_offered AS offer_asset, o.description AS offer_desc,
                    o.intent_type AS offer_type, o.source_excerpt AS offer_excerpt,
                    w.publisher_name AS want_publisher, w.publisher_contact AS want_contact,
                    w.asset_wanted AS want_asset, w.description AS want_desc,
                    w.intent_type AS want_type, w.source_excerpt AS want_excerpt
                FROM intent_match_proposals p
                JOIN intent_registry o ON o.intent_rid = p.offer_intent_rid
                JOIN intent_registry w ON w.intent_rid = p.want_intent_rid
                WHERE p.status = 'candidate'
                  AND p.match_type = 'local'
                  AND o.landscape_group = $1
                  AND w.landscape_group = $1
                ORDER BY p.score DESC NULLS LAST
                """,
                landscape_group,
            )

            # 2. Cross-landscape opportunities: one intent in this group,
            #    other is regional or from different group
            cross_matches = await conn.fetch(
                """
                SELECT p.*,
                    o.publisher_name AS offer_publisher, o.publisher_contact AS offer_contact,
                    o.asset_offered AS offer_asset, o.description AS offer_desc,
                    o.intent_type AS offer_type, o.landscape_group AS offer_group,
                    o.source_excerpt AS offer_excerpt,
                    w.publisher_name AS want_publisher, w.publisher_contact AS want_contact,
                    w.asset_wanted AS want_asset, w.description AS want_desc,
                    w.intent_type AS want_type, w.landscape_group AS want_group,
                    w.source_excerpt AS want_excerpt
                FROM intent_match_proposals p
                JOIN intent_registry o ON o.intent_rid = p.offer_intent_rid
                JOIN intent_registry w ON w.intent_rid = p.want_intent_rid
                WHERE p.status = 'candidate'
                  AND p.match_type = 'cross_landscape'
                  AND (o.landscape_group = $1 OR w.landscape_group = $1)
                ORDER BY p.score DESC NULLS LAST
                """,
                landscape_group,
            )

            # 3. Unmet needs: active WANT intents with no candidate/introduced proposals
            unmet_wants = await conn.fetch(
                """
                SELECT ir.*
                FROM intent_registry ir
                WHERE ir.status = 'active'
                  AND ir.intent_type = 'WANT'
                  AND ir.landscape_group = $1
                  AND NOT EXISTS (
                      SELECT 1 FROM intent_match_proposals p
                      WHERE p.want_intent_rid = ir.intent_rid
                        AND p.status IN ('candidate', 'introduced')
                  )
                ORDER BY ir.priority DESC
                """,
                landscape_group,
            )

            # 4. Stale intents: active intents with priority < 20
            stale_intents = await conn.fetch(
                """
                SELECT * FROM intent_registry
                WHERE status = 'active'
                  AND landscape_group = $1
                  AND priority < 20
                ORDER BY priority ASC
                """,
                landscape_group,
            )

        # Build markdown digest
        lines = [f"# Coordinator Digest: {landscape_group}", ""]

        # Section 1: Local Matches
        lines.append("## Local Matches")
        lines.append("")
        if not local_matches:
            lines.append("*No local matches at this time.*")
        else:
            for idx, m in enumerate(local_matches, 1):
                offer_asset = m["offer_asset"] or "unknown"
                want_asset = m["want_asset"] or "unknown"
                lines.append(
                    f"{idx}. **{m['offer_publisher']}** "
                    f"({m['offer_type']}: {offer_asset}) "
                    f"<> **{m['want_publisher']}** "
                    f"({m['want_type']}: {want_asset})"
                )
                if m["offer_desc"] or m["want_desc"]:
                    offer_d = m["offer_desc"] or "(no description)"
                    want_d = m["want_desc"] or "(no description)"
                    lines.append(
                        f"   {m['offer_publisher']} has \"{offer_d}\". "
                        f"{m['want_publisher']} needs \"{want_d}\"."
                    )
                contacts = []
                if m["offer_contact"]:
                    contacts.append(m["offer_contact"])
                if m["want_contact"]:
                    contacts.append(m["want_contact"])
                if contacts:
                    lines.append(f"   Contact: {' / '.join(contacts)}")
                lines.append(
                    "   -> Coordinator action: Would you like to introduce them?"
                )
                lines.append("")
        lines.append("")

        # Section 2: Cross-Landscape Opportunities
        lines.append("## Cross-Landscape Opportunities")
        lines.append("")
        if not cross_matches:
            lines.append("*No cross-landscape opportunities at this time.*")
        else:
            for idx, m in enumerate(cross_matches, 1):
                offer_asset = m["offer_asset"] or "unknown"
                want_asset = m["want_asset"] or "unknown"
                lines.append(
                    f"{idx}. **{m['offer_publisher']}** "
                    f"({m['offer_type']}: {offer_asset}, {m['offer_group']}) "
                    f"<> **{m['want_publisher']}** "
                    f"({m['want_type']}: {want_asset}, {m['want_group']})"
                )
                if m["offer_desc"] or m["want_desc"]:
                    offer_d = m["offer_desc"] or "(no description)"
                    want_d = m["want_desc"] or "(no description)"
                    lines.append(
                        f"   {m['offer_publisher']} has \"{offer_d}\". "
                        f"{m['want_publisher']} needs \"{want_d}\"."
                    )
                contacts = []
                if m["offer_contact"]:
                    contacts.append(m["offer_contact"])
                if m["want_contact"]:
                    contacts.append(m["want_contact"])
                if contacts:
                    lines.append(f"   Contact: {' / '.join(contacts)}")
                lines.append(
                    "   -> Coordinator action: Would you like to introduce them?"
                )
                lines.append("")
        lines.append("")

        # Section 3: Unmet Needs
        lines.append("## Unmet Needs")
        lines.append("")
        if not unmet_wants:
            lines.append("*All WANT intents have matching proposals.*")
        else:
            for idx, w in enumerate(unmet_wants, 1):
                d = dict(w)
                asset = d.get("asset_wanted") or "unknown"
                desc = d.get("description") or ""
                name = d["publisher_name"]
                lines.append(f"{idx}. **{name}** wants: {asset}")
                if desc:
                    lines.append(f"   \"{desc}\"")
                contact = d.get("publisher_contact")
                if contact:
                    lines.append(f"   Contact: {contact}")
                lines.append("")
        lines.append("")

        # Section 4: Stale Intents
        lines.append("## Stale Intents")
        lines.append("")
        if not stale_intents:
            lines.append("*No stale intents.*")
        else:
            for idx, s in enumerate(stale_intents, 1):
                d = dict(s)
                itype = d["intent_type"]
                asset = d.get("asset_offered") or d.get("asset_wanted") or "unknown"
                name = d["publisher_name"]
                priority = d.get("priority", 0)
                lines.append(
                    f"{idx}. **{name}** ({itype}: {asset}) "
                    f"— priority {priority:.1f}"
                )
                lines.append(
                    "   -> Action needed: Contact to refresh, modify, or archive"
                )
                lines.append("")

        return {"landscape_group": landscape_group, "digest": "\n".join(lines)}

    return router
