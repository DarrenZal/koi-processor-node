"""Knowledge graph endpoints — episodes and temporal facts.

Provides storage and retrieval for knowledge episodes (grouping unit)
and facts (searchable natural-language statements with entity references,
temporal validity, and pgvector embeddings).

Routes are prefix-relative — prefix "/knowledge" is applied at mount
in personal_ingest_api.py.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import time

import asyncpg
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine, Dict, List, Literal, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from api.secret_guard import check_fact as check_credential_fact

# Pack 2.2 (2026-04-28): per-request fallback-fired observability.
# `was_fallback_fired()` reports whether FallbackChainEmbeddingProvider
# fell through to the secondary on the most recent embed_query call in
# the current async context. Used to surface `degraded_embedding: true`
# in unified-search responses when reads succeeded but on a degraded path.
from api.embedding_provider import (
    reset_fallback_fired,
    was_fallback_fired,
)
from api.auth_deps import make_service_token_auth
from api.session_exclusions import session_exclusion_filter

logger = logging.getLogger(__name__)


def _unified_search_surface_timeout_s() -> float:
    raw = os.environ.get("KOI_UNIFIED_SEARCH_SURFACE_TIMEOUT_S", "8.0")
    try:
        return max(0.1, float(raw))
    except (TypeError, ValueError):
        return 8.0


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; using default %.1f", name, raw, default)
        return default


UNIFIED_SEARCH_SESSIONS_TIMEOUT_SECONDS = _float_env(
    "UNIFIED_SEARCH_SESSIONS_TIMEOUT_SECONDS",
    8.0,
)

# The complete set of surface names /knowledge/unified-search accepts via
# `include=`. Keep in sync with every `if "<name>" in surfaces:` branch in
# unified_search() below -- entities/facts/sessions/docs/wiki/memories run
# inside the pooled-connection block, vault runs as a subprocess after it.
UNIFIED_SEARCH_VALID_SURFACES: frozenset = frozenset({
    "entities",
    "facts",
    "sessions",
    "docs",
    "wiki",
    "vault",
    "memories",
})


# ---------------------------------------------------------------------------
# B1 (2026-04-30): Predicate-aware default supersession policy.
#
# Refines yesterday's per-request `expire_existing` flag into a per-predicate
# policy. Three buckets:
#   - SUPERSEDE_PREDICATES: single-valued; new fact with different object_uri
#     auto-retires the old (sets valid_to=NOW). Use for canonical replacement
#     relationships.
#   - COEXIST_PREDICATES: multi-valued; new facts always insert; old facts
#     never touched. Parallel attributions are valid.
#   - Unknown predicates: fall through to per-request `expire_existing` flag
#     (yesterday's mechanism). Default behavior unchanged for predicates we
#     haven't classified yet.
#
# Wave B B2 (2026-05-03): operator-extensible via env vars at module-load time.
#   SUPERSEDE_PREDICATES env var (comma-separated) — defaults to the four
#     canonical names below if unset/empty.
#   COEXIST_PREDICATES env var (comma-separated) — defaults to the four
#     canonical names below if unset/empty.
# Whitespace + empty-token entries are stripped; values normalized to
# UPPER_CASE per existing predicate_upper convention. Setting the env to a
# non-empty value REPLACES the default set entirely (does not extend); operators
# wanting to add a single predicate must include the canonical four in their
# override. Example:
#   SUPERSEDE_PREDICATES="SUPERSEDES,REPLACES,INVALIDATES,DEPRECATES,EXTENDS"
#
# All matches case-insensitive; values stored UPPER_CASE per existing
# convention (predicate_upper).
# ---------------------------------------------------------------------------

_DEFAULT_SUPERSEDE_PREDICATES: frozenset[str] = frozenset({
    "SUPERSEDES",
    "REPLACES",
    "INVALIDATES",
    "DEPRECATES",
})

_DEFAULT_COEXIST_PREDICATES: frozenset[str] = frozenset({
    "AUTHORED_WITHIN",
    "MENTIONS",
    "RELATES_TO",
    "DEPENDS_ON",
})


def _parse_predicate_env(env_name: str, default: frozenset[str]) -> frozenset[str]:
    """Parse a comma-separated predicate list from env. Returns default if
    unset, empty, or all-whitespace. Tokens stripped + uppercased; empty
    tokens (e.g. trailing comma) discarded.
    """
    raw = os.environ.get(env_name, "")
    tokens = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tokens:
        return default
    return frozenset(tokens)


SUPERSEDE_PREDICATES: frozenset[str] = _parse_predicate_env(
    "SUPERSEDE_PREDICATES", _DEFAULT_SUPERSEDE_PREDICATES,
)
COEXIST_PREDICATES: frozenset[str] = _parse_predicate_env(
    "COEXIST_PREDICATES", _DEFAULT_COEXIST_PREDICATES,
)

# Log the effective policy at module-load so operators can confirm overrides
# took effect (or the defaults applied as expected).
if SUPERSEDE_PREDICATES != _DEFAULT_SUPERSEDE_PREDICATES \
        or COEXIST_PREDICATES != _DEFAULT_COEXIST_PREDICATES:
    logger.info(
        "Predicate policy overrides active. SUPERSEDE=%s COEXIST=%s",
        sorted(SUPERSEDE_PREDICATES), sorted(COEXIST_PREDICATES),
    )
else:
    logger.info(
        "Predicate policy: defaults. SUPERSEDE=%s COEXIST=%s",
        sorted(SUPERSEDE_PREDICATES), sorted(COEXIST_PREDICATES),
    )


def _resolve_supersession_policy(predicate_upper: str, request_flag: bool) -> bool:
    """Return True if same-(subj,pred) different-object should retire old fact.

    Order: predicate-bucket match wins over request flag; unknown falls through.
    """
    if predicate_upper in SUPERSEDE_PREDICATES:
        return True
    if predicate_upper in COEXIST_PREDICATES:
        return False
    return request_flag


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

# Documented mapping from the extraction schema's confidence enum to
# knowledge_facts.confidence (REAL). See migration 115. .get() on an absent
# or None key returns None, which binds to NULL -- the correct behavior for
# a caller that omits confidence entirely.
_CONFIDENCE_TO_REAL = {"high": 1.0, "medium": 0.6, "low": 0.3}


class FactInput(BaseModel):
    # extra='forbid': the deep-extraction schema REQUIRES a `confidence` field
    # (enum high/medium/low) on every fact it emits. Before this field existed
    # here, Pydantic's default extra='ignore' silently dropped it on every
    # write -- verified by execution (model_dump had no confidence key). A
    # future field this model doesn't yet know about should fail loud (422),
    # not vanish the same way. Migration 115 (2026-08-31) adds the matching
    # knowledge_facts.confidence column.
    model_config = ConfigDict(extra="forbid")

    subject: str = Field(..., description="Entity name for the subject")
    # ── Pinned endpoints (issue #62) ────────────────────────────────────────
    # When supplied, the entity is NOT resolved: this URI is used verbatim after a
    # liveness + type check. It is the difference between "the resolver will
    # probably pick the right row" and "no resolver runs at all".
    #
    # A bulk importer knows its complete typed endpoint set before it writes any
    # fact, so it can pin every endpoint up front. Without pinning, an entity
    # CREATED while writing fact 45 becomes a fuzzy candidate for a DIFFERENT
    # endpoint at fact 47, and members of one payload collapse into each other in
    # a way that depends on fact order. Measured on the audited Buehler (2024)
    # import: 6 wrong self-collapses, 11 fact endpoints left pointing at the wrong
    # entity, with the write reporting success.
    #
    # An unresolvable pin is a 422, never a fallback to name resolution — falling
    # back would reintroduce exactly the nondeterminism the pin exists to remove,
    # and would do it silently.
    subject_uri: Optional[str] = Field(
        None,
        description="Pre-resolved canonical URI for the subject. When set, skips entity "
                    "resolution entirely and binds this URI directly. Must name a live "
                    "(not merged, not revoked) entity whose type matches subject_type when "
                    "that is also supplied. A missing/tombstoned/type-mismatched URI is a "
                    "422 for the whole request — never a silent fallback to name resolution."
    )
    subject_type: Optional[str] = Field(
        None,
        description="Optional type hint for the subject (Person, Organization, Place, "
                    "Project, Concept, etc.). Used only when the entity must be CREATED "
                    "(does not exist in registry). When the entity is resolved against "
                    "an existing registry row, the existing type is preserved. Defaults "
                    "to 'Concept' when omitted. Use this for facts whose subject is "
                    "clearly typed (e.g. a Person predicate like SIBLING_OF) so newly "
                    "created entities get the right type."
    )
    predicate: str = Field(..., description="Relationship type. Stored exactly as sent (no case-folding as of 2026-08-31).")
    object: Optional[str] = Field(None, description="Entity name for the object (if entity)")
    object_uri: Optional[str] = Field(
        None,
        description="Pre-resolved canonical URI for the object — parallels subject_uri. "
                    "Ignored when `object` is absent (a literal-object fact has no object "
                    "entity to pin)."
    )
    object_type: Optional[str] = Field(
        None,
        description="Optional type hint for the object (parallels subject_type — see above)."
    )
    object_literal: Optional[str] = Field(None, description="Free text value (if not entity)")
    fact_text: str = Field(..., description="Natural language sentence")
    valid_from: Optional[str] = Field(None, description="ISO datetime when fact became true")
    valid_to: Optional[str] = Field(None, description="ISO datetime when fact stopped being true")
    confidence: Optional[Literal["high", "medium", "low"]] = Field(
        None,
        description="Extraction confidence. Mapped to knowledge_facts.confidence "
                    "as high=1.0, medium=0.6, low=0.3; omitted -> NULL."
    )

    @model_validator(mode="after")
    def _reject_credential_facts(self):
        """Refuse credential-shaped facts at the boundary, before any write.

        The extraction pipeline once minted HAS_PASSWORD / HAS_TOKEN / SSH_USERNAME
        facts that stayed current behind an API without authentication.
        A 422 here is the loud failure; the alternative is a silent one that
        nothing notices until someone reads the table.

        Calibrated against live data (8/9 real credential facts caught, 1 false
        positive in 1,233 legitimate literals) -- see api/secret_guard.py for
        the measurements and for the two rules that reading the code alone
        would have gotten wrong.
        """
        flagged, reason = check_credential_fact(self.predicate, self.object_literal)
        if flagged:
            # The reason deliberately never echoes the value.
            raise ValueError(f"refusing credential-shaped fact: {reason}")
        return self


class EpisodeCreateRequest(BaseModel):
    name: str = Field(..., description="Episode title")
    content: Optional[str] = None
    source_description: Optional[str] = None
    source_document: Optional[str] = None
    group_id: str = "personal"
    valid_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    facts: List[FactInput] = Field(default_factory=list)
    create_entities: bool = Field(True, description="Create missing entities in entity_registry")
    # Task A 2026-04-29: parallel-attribution dedup-shape fix.
    # When false (default): cosine>0.95 skip requires (predicate, object_uri)
    # to ALSO match — parallel attributions like AUTHORED_WITHIN with
    # different session UUIDs coexist instead of getting silently dropped.
    # When true: legacy single-valued-predicate behavior — same (subject,
    # predicate) with different object_uri SUPERSEDES the existing fact
    # (sets valid_to=NOW()). Use for SUPERSEDES / REPLACES / INVALIDATES
    # predicates and similar single-valued relationships.
    expire_existing: bool = Field(
        False,
        description="If true, same-(subject,predicate) different-object writes "
        "supersede existing facts (legacy behavior). If false (default), "
        "parallel attributions coexist."
    )
    # Idempotency: when supplied and already recorded, the endpoint returns the
    # stored response (idempotent_replay=true) without writing again. The stored
    # response is committed in the SAME transaction as the writes, so a rolled-
    # back request never leaves a stale idempotency entry. Requires migration
    # 106 (ingest_idempotency).
    request_id: Optional[str] = Field(
        None, description="Optional idempotency key for safe client retries.")


class FactRecord(BaseModel):
    id: str
    episode_id: Optional[str] = None
    episode_name: Optional[str] = None
    subject_uri: str
    subject_name: Optional[str] = None
    predicate: str
    object_uri: Optional[str] = None
    object_name: Optional[str] = None
    object_literal: Optional[str] = None
    fact_text: str
    valid_from: Optional[str] = None
    valid_to: Optional[str] = None
    created_at: Optional[str] = None
    similarity: Optional[float] = None


class TypeMismatch(BaseModel):
    """Type-hint divergence between caller and resolved entity.

    Surfaced in EpisodeCreateResponse.type_mismatches when a fact provides
    subject_type/object_type but the entity resolved to an existing registry
    row with a different entity_type. Resolution preserves the existing type
    (per the contract — type_hint is create-time-only), but callers may want
    to know about the mismatch so they can flag it as a Guard-class issue
    (e.g. "this name exists as a Concept but we believe it's a Person").
    """
    name: str
    role: str  # "subject" | "object"
    requested_type: str  # caller's type_hint
    resolved_type: str   # existing registry row's entity_type
    resolved_uri: str


class EpisodeCreateResponse(BaseModel):
    episode_id: str
    episode_reused: bool = False
    facts_created: int
    facts_skipped: int = 0
    facts_superseded: int = 0
    entities_resolved: int
    entities_created: int
    # Wave A A2 (2026-05-01): null-embed observability. Count of facts in
    # this request that were written with `fact_embedding_3072=NULL`
    # (typically because the embedding provider was degraded). NULL-embedded
    # facts BYPASS future cosine dedup, so this count surfaces silent-fail
    # episodes immediately at write-time.
    facts_null_embed: int = 0
    # Entities created in this request with NO type hint from the caller, so typed
    # "Concept" by default. The type is hashed into the URI and cannot be corrected
    # in place, so a sustained non-zero here means the extractor is emitting facts
    # whose subject/object are absent from its own entities[] list.
    entities_typed_by_default: int = 0
    # Issue #62: fact endpoints bound from a caller-supplied `subject_uri`/`object_uri`
    # instead of being resolved by name. A bulk importer that froze its endpoint map
    # should see this equal the number of entity-valued endpoints in the request; any
    # shortfall is endpoints that still went through the order-dependent resolver, and
    # is the difference between "we pinned the payload" and "we meant to".
    endpoints_pinned: int = 0
    # Issue #62 / review M5: the ids of the knowledge_facts rows THIS request
    # inserted (dedup-skipped facts are not listed — they already exist under the
    # same pinned URIs). Episodes are shared across documents by
    # (source_document, group_id), so an importer verifying "every persisted
    # endpoint is in my frozen map" must scope to these rows, not to the episode,
    # or it fails against a sibling document's facts after its own have committed.
    fact_ids: List[str] = Field(default_factory=list)
    # Type-hint divergence list — empty unless caller provided subject_type
    # or object_type that conflicted with an existing entity. See TypeMismatch.
    type_mismatches: List[TypeMismatch] = Field(default_factory=list)
    # True only when this response is a replay of a prior request with the same
    # request_id (no new writes were performed). See EpisodeCreateRequest.request_id.
    idempotent_replay: bool = False


class EpisodeRecord(BaseModel):
    id: str
    name: str
    content: Optional[str] = None
    source_description: Optional[str] = None
    source_document: Optional[str] = None
    group_id: Optional[str] = None
    valid_at: Optional[str] = None
    created_at: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    fact_count: Optional[int] = None


class RecallWalkRequest(BaseModel):
    query: str = Field(..., description="Natural-language query")
    shape: str = Field(
        "semantic",
        description="One of: semantic | temporal | relationship",
    )
    limit: int = Field(5, ge=1, le=20)
    group_id: Optional[str] = Field(
        None,
        description="Filter walk to a specific knowledge_episodes.group_id (e.g. koi_canon_v1)",
    )
    max_hops: int = Field(3, ge=1, le=5, description="Cap on CTE walk depth")
    # Wave 3 C2 (2026-04-30): null-answer-shape detection. When set, the walk
    # restricts to facts whose `predicate IN (...)` and emits an explicit
    # null_answer block when 0 such facts surface. Use for supersession-shape
    # queries (e.g. "Has ADR-X been superseded?" with predicate_filter=["SUPERSEDED_BY"]).
    predicate_filter: Optional[List[str]] = Field(
        None,
        description="If set, walk only emits facts with predicate IN (this list). "
        "When the filtered walk returns 0 rows, the response carries a structured "
        "null_answer block instead of falling back to the unfiltered walk.",
    )
    subject_uri: Optional[str] = Field(
        None,
        description="If set, anchor the null_answer block's subject. Used together with "
        "predicate_filter to assert 'no edge of this type exists from this subject'.",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dt(val: Optional[str]) -> Optional[datetime]:
    """Parse an ISO datetime string, returning None on failure."""
    if not val:
        return None
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _fact_embedding_discriminator(
    *,
    fact_embedding_3072: Optional[List[float]] = None,
    fact_embedding: Optional[List[float]] = None,
) -> tuple:
    """Return ``(embedding_column, embedding_value)`` for a federated fact payload.

    Federation Phase 1 step 2e — publisher-side embedding-column discriminator.
    Prefers the 3072-dim column (`fact_embedding_3072`, live primary as of
    migration 096) over the legacy 1024-dim `fact_embedding`. Returns
    ``(None, None)`` when neither vector is populated — the subscriber omits the
    column entirely in that case. ``embedding_value`` is a plain list of floats
    (JSON-serializable; subscriber's `_format_vector` renders it).

    The `/knowledge/episodes` endpoint only ever writes `fact_embedding_3072`,
    so the 1024-dim branch is unexercised there today; it exists so the
    discriminator is correct for any future caller.
    """
    if fact_embedding_3072:
        return "fact_embedding_3072", list(fact_embedding_3072)
    if fact_embedding:
        return "fact_embedding", list(fact_embedding)
    return None, None


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert an asyncpg Record to a serializable dict."""
    from datetime import date, datetime as dt_type
    d = dict(row)
    for k, v in d.items():
        if isinstance(v, (date, dt_type)):
            d[k] = v.isoformat()
        elif isinstance(v, UUID):
            d[k] = str(v)
    return d


# ---------------------------------------------------------------------------
# Router factory
# ---------------------------------------------------------------------------

EmbedFn = Callable[[str], Coroutine[Any, Any, Optional[List[float]]]]
# Batch embed: many texts -> parallel list of vectors (or None on provider failure).
BatchEmbedFn = Callable[
    [List[str]], Coroutine[Any, Any, Optional[List[Optional[List[float]]]]]
]


def _parse_jsonb(value) -> Dict:
    """Safely parse a JSONB column value — handles both dict and string returns from asyncpg."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


# ---------------------------------------------------------------------------
# Source-link surfacing (Piece C / G3): make every retrieval row citable.
# ---------------------------------------------------------------------------

_HTTP_URL_RE = re.compile(r"https?://\S+")
# Bare arXiv id (post-2007 scheme): 2005.12798 or 2605.15778v1.
_ARXIV_ID_RE = re.compile(r"\b(\d{4}\.\d{4,5})(v\d+)?\b")


def derive_source_url(
    source_node_rid: Optional[str],
    source_document: Optional[str],
    metadata: Optional[Dict[str, Any]],
    url_map: Optional[Dict[str, str]],
) -> Optional[str]:
    """Pure helper — derive a citable URL for a fact/row. NEVER fabricates; does
    NO DB I/O (the caller preloads `url_map` in one batch query).

    Precedence:
      1. ONLY if `source_node_rid` is a `document:` rid → `url_map.get(it)`
         (authoritative `document_ingestion_log.source_url`, PK 1-row/rid).
         Non-document rids (session/episode/entity) SKIP this step entirely —
         they are never looked up in url_map.
      2. else the first `http(s)://` URL found in `metadata`
         (prefers an explicit `source_url` key).
      3. else a bare arXiv id in `source_document`/`metadata`
         → `https://arxiv.org/abs/<id>`.
      4. else None.
    """
    # 1. document-rid authoritative lookup (only for document: rids).
    if isinstance(source_node_rid, str) and source_node_rid.startswith("document:"):
        if url_map:
            u = url_map.get(source_node_rid)
            if u:
                return u

    # 2. http(s):// in metadata (explicit source_url key first, then any value).
    meta = metadata if isinstance(metadata, dict) else _parse_jsonb(metadata)
    str_values: List[str] = []
    if isinstance(meta, dict):
        explicit = meta.get("source_url")
        if isinstance(explicit, str) and _HTTP_URL_RE.match(explicit):
            return explicit
        for v in meta.values():
            if isinstance(v, str):
                str_values.append(v)
    blob = " ".join(str_values)
    m = _HTTP_URL_RE.search(blob)
    if m:
        return m.group(0).rstrip(").,;>\"'")

    # 3. bare arXiv id in source_document / metadata.
    hay_parts = [p for p in ([source_document] + str_values) if isinstance(p, str)]
    am = _ARXIV_ID_RE.search(" ".join(hay_parts))
    if am:
        return f"https://arxiv.org/abs/{am.group(1)}{am.group(2) or ''}"

    # 4. underivable — never fabricate.
    return None


def _quartz_url(entity_type: Optional[str], name: Optional[str]) -> Optional[str]:
    """Lazily reuse personal_ingest_api.quartz_url (deferred import avoids the
    knowledge_router <-> personal_ingest_api circular import at module load).
    Returns None when QUARTZ_BASE_URL is unset or the type has no Quartz path."""
    if not entity_type or not name:
        return None
    try:
        from api.personal_ingest_api import quartz_url as _q
        return _q(entity_type, name)
    except Exception:  # noqa: BLE001 — a quartz miss must never break retrieval
        return None


async def _build_source_url_map(conn, source_node_rids) -> Dict[str, str]:
    """Batch-load {document_rid: source_url} for the `document:` rids in a result
    set, in ONE query (avoids N+1 / hidden globals in `derive_source_url`)."""
    doc_rids = sorted({
        r for r in (source_node_rids or [])
        if isinstance(r, str) and r.startswith("document:")
    })
    if not doc_rids:
        return {}
    rows = await conn.fetch(
        "SELECT document_rid, source_url FROM document_ingestion_log "
        "WHERE document_rid = ANY($1::text[])",
        doc_rids,
    )
    return {r["document_rid"]: r["source_url"] for r in rows if r["source_url"]}


# ---------------------------------------------------------------------------
# Discourse-move search (Piece A / G1): make session_discourse_moves queryable.
# READ-ONLY — every function below issues only SELECTs (conn.fetch).
# ---------------------------------------------------------------------------

def _normalize_move_types(move_type: Optional[List[str]]) -> List[str]:
    """Normalize repeated and/or comma-joined ``move_type`` params into a deduped,
    order-preserving list. Both ``?move_type=a&move_type=b`` (FastAPI-native
    repeated) and ``?move_type=a,b`` (convenience comma form) → ``['a','b']``;
    empty / None / all-blank → ``[]`` (no move_type filter)."""
    if not move_type:
        return []
    out: List[str] = []
    seen: set = set()
    for raw in move_type:
        if raw is None:
            continue
        for tok in str(raw).split(","):
            t = tok.strip()
            if t and t not in seen:
                seen.add(t)
                out.append(t)
    return out


async def _build_source_title_map(conn, source_rids) -> Dict[str, str]:
    """Batch-load ``{document_rid: title}`` for the ``document:`` rids in a result
    set, in ONE query (mirrors ``_build_source_url_map``; used for ``source.title``)."""
    doc_rids = sorted({
        r for r in (source_rids or [])
        if isinstance(r, str) and r.startswith("document:")
    })
    if not doc_rids:
        return {}
    rows = await conn.fetch(
        "SELECT document_rid, title FROM document_ingestion_log "
        "WHERE document_rid = ANY($1::text[])",
        doc_rids,
    )
    return {r["document_rid"]: r["title"] for r in rows if r["title"]}


async def _discourse_search(
    conn,
    *,
    query: Optional[str] = None,
    move_types: Optional[List[str]] = None,
    source_rid: Optional[str] = None,
    status: Optional[str] = None,
    source_type: str = "document",
    limit: int = 20,
    scope: Any,
) -> Dict[str, Any]:
    """Read-only executor for ``GET /knowledge/discourse-search``. Issues ONLY
    SELECTs (``conn.fetch``).

    Lexical full-text match over ``title``+``detail`` (``to_tsvector`` /
    ``plainto_tsquery``) when ``query`` is non-blank, ordered by ``ts_rank`` then
    recency; when ``query`` is blank, skips the FTS predicate and orders by
    ``created_at DESC`` (most-recent first). Each returned move is enriched with
    its **source** (``{rid, title, source_url}`` via ``document_ingestion_log`` +
    the reused ``derive_source_url`` — ``source_url`` is ``null``, never
    fabricated, for non-document rids) and its **one-hop ``resolves`` parent**
    (``{id, move_type, title}`` | ``null``). The source + parent enrichments are
    each ONE batch query (no N+1)."""
    move_types = move_types or []
    q = (query or "").strip()

    conditions = ["source_type = $1"]
    params: List[Any] = [source_type]
    idx = 2

    # Session moves of excluded sessions stay hidden, and this reader fails closed (503)
    # when the exclusion list is missing or session readers are disabled (migration 136).
    session_excl = None
    if source_type == "session":
        session_excl = await session_exclusion_filter(conn, "session_discourse_moves.session_id")

    use_fts = bool(q)
    if use_fts:
        # query is ALWAYS bound at $2 (added immediately after source_type=$1),
        # so the ts_rank ORDER BY below can reference $2 directly.
        conditions.append(
            "to_tsvector('english', coalesce(title,'')||' '||coalesce(detail,'')) "
            f"@@ plainto_tsquery('english', ${idx})"
        )
        params.append(q)
        idx += 1
    if move_types:
        conditions.append(f"move_type = ANY(${idx}::text[])")
        params.append(move_types)
        idx += 1
    if source_rid:
        conditions.append(f"source_rid = ${idx}")
        params.append(source_rid)
        idx += 1
    if status:
        conditions.append(f"status = ${idx}")
        params.append(status)
        idx += 1

    # Caller-scope visibility. ``scope`` is required and never
    # defaults: a caller that forgot it must fail, not silently read as personal.
    # A session search (whose exclusion list session_exclusion_filter verified
    # above, 503 otherwise) uses move_visible_sql: the session policy plus the
    # document-source rules. Any other search returns no session rows, so it
    # uses the source predicate alone and never references session_exclusions,
    # exactly as before this change. Personal reduces to excluding held corpora
    # (0 of 14,471 document moves at the 2026-10-29 audit).
    from api.read_scope import ReadScope, move_visible_sql, source_visible_sql
    if not isinstance(scope, ReadScope):
        raise TypeError("_discourse_search requires a ReadScope")
    _scope = scope

    def _visibility(start: int):
        if source_type == "session":
            return move_visible_sql(_scope, "session_discourse_moves", start)
        return source_visible_sql(_scope, "session_discourse_moves.source_rid", start)

    _vis_sql, _vis_params = _visibility(idx)
    conditions.append(_vis_sql)
    params.extend(_vis_params)

    where = " AND ".join(conditions)
    if use_fts:
        order_by = (
            "ts_rank(to_tsvector('english', coalesce(title,'')||' '||coalesce(detail,'')), "
            "plainto_tsquery('english', $2)) DESC, created_at DESC"
        )
    else:
        order_by = "created_at DESC"

    params.append(limit)
    sql = (
        "SELECT id, move_type, title, detail, status, resolves_move_id, source_rid "
        f"FROM session_discourse_moves WHERE {where} "
        f"ORDER BY {order_by} LIMIT ${len(params)}"
    )
    rows = await conn.fetch(sql, *params)

    # Batch-enrich source: {rid: source_url} (reuse helper) + {rid: title}.
    result_rids = [r["source_rid"] for r in rows]
    url_map = await _build_source_url_map(conn, result_rids)
    title_map = await _build_source_title_map(conn, result_rids)

    # Batch-resolve the one-hop `resolves` parents in ONE query (no N+1).
    parent_ids = sorted(
        {r["resolves_move_id"] for r in rows if r["resolves_move_id"] is not None},
        key=str,
    )
    parent_map: Dict[Any, Dict[str, Any]] = {}
    if parent_ids:
        # A restricted parent must not surface through an allowed child: the
        # parent lookup applies the caller-scope visibility. $1
        # is parent_ids, so scope params bind from $2. A document move never
        # shows a session parent, whatever the scope; a session search may show
        # a non-excluded session parent (personal only, via move_visible_sql).
        _pvis_sql, _pvis_params = _visibility(2)
        _parent_kind = "" if source_type == "session" else " AND source_type <> 'session'"
        prows = await conn.fetch(
            "SELECT id, move_type, title FROM session_discourse_moves "
            f"WHERE id = ANY($1::uuid[]) AND {_pvis_sql}{_parent_kind}",
            parent_ids, *_pvis_params,
        )
        for pr in prows:
            parent_map[pr["id"]] = {
                "id": str(pr["id"]),
                "move_type": pr["move_type"],
                "title": pr["title"],
            }

    moves: List[Dict[str, Any]] = []
    for r in rows:
        rid = r["source_rid"]
        moves.append({
            "id": str(r["id"]),
            "move_type": r["move_type"],
            "title": r["title"],
            "detail": r["detail"],
            "status": r["status"],
            "source": {
                "rid": rid,
                "title": title_map.get(rid) or rid,
                # derive_source_url never fabricates: document rid w/ url_map hit
                # → URL; everything else → None.
                "source_url": derive_source_url(rid, None, None, url_map),
            },
            # LEFT-join semantics: None resolves_move_id OR orphan parent → null.
            "resolves": parent_map.get(r["resolves_move_id"]),
        })

    return {"moves": moves, "count": len(moves), "query_mode": "lexical"}


class FactRetractRequest(BaseModel):
    reason: Optional[str] = Field(
        None,
        description="Optional human-readable reason for the retraction. Recorded "
                    "in the server log for audit; not persisted on the fact row "
                    "(knowledge_facts has no retraction_reason column).",
    )


class FactRetractResponse(BaseModel):
    fact_id: str
    retracted: bool          # True if THIS call set valid_to
    already_retracted: bool  # True if the fact was already expired before this call
    valid_to: Optional[str] = None
    subject_uri: Optional[str] = None
    predicate: Optional[str] = None
    object_uri: Optional[str] = None
    reason: Optional[str] = None


def create_router(
    pool,
    generate_embedding: Optional[EmbedFn] = None,
    *,
    generate_query_embedding: Optional[EmbedFn] = None,
    generate_document_embedding: Optional[EmbedFn] = None,
    generate_batch_embedding: Optional[BatchEmbedFn] = None,
) -> APIRouter:
    """Return an APIRouter for knowledge graph endpoints.

    Parameters
    ----------
    pool : asyncpg.Pool
        Database connection pool.
    generate_embedding : callable, optional
        DEPRECATED fallback: text -> Optional[List[float]].
    generate_query_embedding : callable, optional
        QUERY mode embedding (with instruction prefix).
    generate_document_embedding : callable, optional
        DOCUMENT mode embedding (no instruction prefix).
    generate_batch_embedding : callable, optional
        DOCUMENT mode batch embedding: List[str] -> List[Optional[vector]].
        Used by POST /episodes to prefetch all fact/entity embeddings in one
        provider round-trip BEFORE acquiring a DB connection.
    """
    # Resolve to explicit query/document or fall back to unified
    _query_embed = generate_query_embedding or generate_embedding
    _doc_embed = generate_document_embedding or generate_embedding
    _batch_embed = generate_batch_embedding
    router = APIRouter(tags=["knowledge"])

    # Tranche-1 read auth: each route validates its credential
    # and binds a ReadScope that its SQL applies. Built here, unconditionally, so
    # the routes cannot be mounted without it.
    from api.read_auth import make_read_scope_dependency
    from api.read_scope import ReadScope, chunk_visible_sql, entity_visible_sql
    _discourse_scope = make_read_scope_dependency("discourse-search", lambda: pool)
    _unified_scope = make_read_scope_dependency("unified-search", lambda: pool)

    # Federation Phase 1 step 2e: knowledge_episode emit. emit_domain_event is
    # internally gated by KOI_FEDERATE_KNOWLEDGE — a no-op when the flag is off,
    # so the call site below is unconditional (no caller-side double-gate).
    from api.federation_events import emit_domain_event

    # Service-token gate for mutating endpoints (retract). Accepts the
    # KOI_CLAIMS_SERVICE_TOKEN service token OR a valid session token; see
    # api/auth_deps.py. Mirrors claims_router's `require_auth` construction.
    require_service_auth = make_service_token_auth(pool)

    def _facts_surface_available(request: Request) -> bool:
        return bool(getattr(request.app.state, "facts_surface_available", True))

    def _facts_surface_headers(request: Request) -> Dict[str, str]:
        return {
            "X-Facts-Surface": (
                "available" if _facts_surface_available(request) else "unavailable"
            )
        }

    # -------------------------------------------------------------------
    # POST /episodes — create episode with facts
    # -------------------------------------------------------------------
    @router.post("/episodes", response_model=EpisodeCreateResponse, status_code=201)
    async def create_episode(
        request: Request,
        body: EpisodeCreateRequest,
        _identity: str = Depends(require_service_auth),
    ):
        if not _facts_surface_available(request):
            raise HTTPException(
                status_code=503,
                detail={"error": "facts surface not configured on this node"},
            )

        valid_at = _dt(body.valid_at)
        metadata = body.metadata or {}

        entities_resolved = 0
        entities_created = 0
        seen_uris: dict = {}  # cache name->uri within this request

        episode_reused = False
        import json as json_mod

        # Idempotency: replay a prior identical request without re-writing.
        # (Sequential retries — the common case — hit here. A rare concurrent
        # duplicate loses the race on the in-transaction INSERT below and its
        # transaction rolls back; the client's retry then replays here.)
        if body.request_id:
            async with pool.acquire() as conn:
                prior = await conn.fetchval(
                    "SELECT response FROM ingest_idempotency WHERE request_id = $1",
                    body.request_id)
            if prior is not None:
                data = prior if isinstance(prior, dict) else json_mod.loads(prior)
                data["idempotent_replay"] = True
                logger.info("idempotent replay for request_id=%s", body.request_id)
                return EpisodeCreateResponse(**data)

        # P4: prefetch every fact/entity embedding in ONE provider round-trip
        # BEFORE acquiring a DB connection, so no embedding network call happens
        # while a connection is held. The per-text path (_cached_doc_embed) then
        # serves from this cache; entities discovered mid-request that weren't in
        # the batch fall back to a single embed call. Keyed by normalized text so
        # the fact-text, raw-name, and resolver's normalized-name lookups all hit.
        from api.personal_ingest_api import normalize_entity_text
        embed_cache: Dict[str, Optional[List[float]]] = {}
        if _batch_embed and body.facts:
            _raw_texts: List[str] = []
            for _f in body.facts:
                if _f.fact_text:
                    _raw_texts.append(_f.fact_text)
                if _f.subject:
                    _raw_texts.append(_f.subject)
                if _f.object:
                    _raw_texts.append(_f.object)
            _norm_to_raw: Dict[str, str] = {}
            for _t in _raw_texts:
                _norm_to_raw.setdefault(normalize_entity_text(_t), _t)
            _unique_norms = list(_norm_to_raw.keys())
            if _unique_norms:
                _batch = await _batch_embed([_norm_to_raw[n] for n in _unique_norms])
                if _batch and len(_batch) == len(_unique_norms):
                    embed_cache = dict(zip(_unique_norms, _batch))

        async def _cached_doc_embed(text):
            if not text:
                return None
            key = normalize_entity_text(text)
            if key in embed_cache:
                return embed_cache[key]
            val = await _doc_embed(text) if _doc_embed else None
            embed_cache[key] = val
            return val

        embed_fn = _cached_doc_embed if (_doc_embed or _batch_embed) else None

        async with pool.acquire() as conn:
            # P4: single transaction — every write below commits together or not
            # at all. On an already-in-transaction connection (tests) this opens a
            # SAVEPOINT. Federation emit stays AFTER commit (see 2e note below).
            async with conn.transaction():
                # 1. Check for existing episode by (source_document, group_id) (dedup).
                # Wave 2 B3 (2026-04-30): scope dedup to group_id so multi-group
                # writes with the same source_document don't accidentally collapse
                # into a single episode. Today (2026-04-30) all production episodes
                # are in `koi_canon_v1` so 0 collisions exist; B3 hardens forward.
                episode_id = None
                existing_ep = None
                if body.source_document:
                    existing_ep = await conn.fetchrow("""
                        SELECT id, name, content, source_description,
                               source_document, group_id, valid_at, created_at,
                               metadata
                        FROM knowledge_episodes
                        WHERE source_document = $1 AND group_id = $2
                        LIMIT 1
                    """, body.source_document, body.group_id)
                    if existing_ep:
                        episode_id = existing_ep["id"]

                if episode_id:
                    episode_reused = True
                    logger.info(
                        f"Reusing existing episode {episode_id} "
                        f"for source_document: {body.source_document}")
                    # Federation 2e: bundled-emit episode payload — current DB row.
                    episode_payload = {
                        "id": str(existing_ep["id"]),
                        "name": existing_ep["name"],
                        "content": existing_ep["content"],
                        "source_description": existing_ep["source_description"],
                        "source_document": existing_ep["source_document"],
                        "group_id": existing_ep["group_id"],
                        "valid_at": existing_ep["valid_at"].isoformat()
                            if existing_ep["valid_at"] else None,
                        "created_at": existing_ep["created_at"].isoformat()
                            if existing_ep["created_at"] else None,
                        "metadata": existing_ep["metadata"],
                    }
                else:
                    new_ep = await conn.fetchrow("""
                        INSERT INTO knowledge_episodes
                            (name, content, source_description, source_document,
                             group_id, valid_at, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                        RETURNING id, created_at
                    """, body.name, body.content, body.source_description,
                        body.source_document, body.group_id, valid_at,
                        json_mod.dumps(metadata))
                    episode_id = new_ep["id"]
                    # Federation 2e: bundled-emit episode payload — request values.
                    episode_payload = {
                        "id": str(episode_id),
                        "name": body.name,
                        "content": body.content,
                        "source_description": body.source_description,
                        "source_document": body.source_document,
                        "group_id": body.group_id,
                        "valid_at": valid_at.isoformat() if valid_at else None,
                        "created_at": new_ep["created_at"].isoformat()
                            if new_ep["created_at"] else None,
                        "metadata": metadata,
                    }

                # 2. Process each fact
                facts_created = 0
                facts_skipped = 0
                facts_superseded = 0
                facts_null_embed = 0  # Wave A A2: silent-fail surface
                # Same shape as facts_null_embed: a write that succeeded while
                # quietly guessing. An entity minted with no type hint got "Concept"
                # by default, and that guess is hashed into its URI permanently.
                entities_typed_by_default = 0
                emit_facts = []  # Federation 2e: per-fact bundled-emit payloads
                # Collect type mismatches across all facts in this request
                type_mismatches: List[TypeMismatch] = []

                # Issue #62: how many endpoints in this request were bound from a
                # caller-pinned URI rather than resolved. A bulk import that means to
                # be order-independent should see this equal its endpoint count; any
                # shortfall names endpoints that still went through the resolver.
                endpoints_pinned = 0

                for fact in body.facts:
                    # Resolve subject — or bind it verbatim when the caller pinned it.
                    if fact.subject_uri:
                        subject_uri, is_new, subj_resolved_type = await _bind_pinned_uri(
                            conn, fact.subject_uri, fact.subject, "subject",
                            fact.subject_type)
                        endpoints_pinned += 1
                    else:
                        subject_uri, is_new, subj_resolved_type = await _resolve_or_create(
                            conn, fact.subject, body.create_entities,
                            embed_fn, seen_uris,
                            type_hint=fact.subject_type)
                    if not subject_uri:
                        logger.warning(f"Could not resolve subject: {fact.subject}")
                        continue
                    entities_resolved += 1
                    if is_new:
                        entities_created += 1
                        if not fact.subject_type:
                            entities_typed_by_default += 1
                    # Flag type mismatch (caller provided hint, resolved type differs,
                    # and this wasn't a cache hit which returns None for resolved_type)
                    if (fact.subject_type and subj_resolved_type
                            and fact.subject_type != subj_resolved_type):
                        type_mismatches.append(TypeMismatch(
                            name=fact.subject, role="subject",
                            requested_type=fact.subject_type,
                            resolved_type=subj_resolved_type,
                            resolved_uri=subject_uri,
                        ))

                    # Resolve object (if entity name provided)
                    object_uri = None
                    if fact.object:
                        if fact.object_uri:
                            object_uri, obj_new, obj_resolved_type = await _bind_pinned_uri(
                                conn, fact.object_uri, fact.object, "object",
                                fact.object_type)
                            endpoints_pinned += 1
                        else:
                            object_uri, obj_new, obj_resolved_type = await _resolve_or_create(
                                conn, fact.object, body.create_entities,
                                embed_fn, seen_uris,
                                type_hint=fact.object_type)
                        if object_uri:
                            entities_resolved += 1
                            if obj_new:
                                entities_created += 1
                                if not fact.object_type:
                                    entities_typed_by_default += 1
                            if (fact.object_type and obj_resolved_type
                                    and fact.object_type != obj_resolved_type):
                                type_mismatches.append(TypeMismatch(
                                    name=fact.object, role="object",
                                    requested_type=fact.object_type,
                                    resolved_type=obj_resolved_type,
                                    resolved_uri=object_uri,
                                ))

                    # Generate fact embedding (served from the prefetch cache)
                    fact_embedding = None
                    if embed_fn:
                        fact_embedding = await embed_fn(fact.fact_text)

                    # --- Dedup + invalidation ---
                    # Reads from fact_embedding_3072 (post-migration 096); halfvec
                    # cast required because pgvector full-precision indexes cap at
                    # 2000 dims (see migration 097).
                    if fact_embedding:
                        existing = await conn.fetch("""
                            SELECT id, fact_text, predicate, object_uri,
                                   1 - (fact_embedding_3072::halfvec(3072)
                                        <=> $1::halfvec(3072)) AS similarity
                            FROM knowledge_facts
                            WHERE subject_uri = $2 AND valid_to IS NULL
                              AND fact_embedding_3072 IS NOT NULL
                            ORDER BY fact_embedding_3072::halfvec(3072)
                                     <=> $1::halfvec(3072)
                            LIMIT 5
                        """, str(fact_embedding), subject_uri)

                        # Check for near-duplicate (similarity > 0.95).
                        # Task A 2026-04-29: parallel-attribution dedup-shape fix.
                        # The skip condition now also requires (predicate, object_uri)
                        # to match — otherwise parallel attributions like
                        # AUTHORED_WITHIN with different session UUIDs (which produce
                        # near-identical fact_text differing only by the embedded
                        # UUID) would be silently dropped on cosine>0.95.
                        # Re-writes of the EXACT same fact still skip correctly
                        # because (subject, predicate, object_uri, fact_text)
                        # all match.
                        predicate_upper = fact.predicate.upper()
                        skip = False
                        for row in existing:
                            sim = float(row['similarity'])
                            if sim > 0.95 \
                                    and row['predicate'] == predicate_upper \
                                    and row['object_uri'] == object_uri:
                                logger.info(
                                    f"Skipped duplicate fact: {fact.fact_text} "
                                    f"(similarity: {sim:.3f} with fact {row['id']}; "
                                    f"same predicate+object)")
                                facts_skipped += 1
                                skip = True
                                break

                        if skip:
                            continue

                        # Invalidation: same subject + same predicate + different object → retire old.
                        # Task A 2026-04-29: opt-in via request-level
                        # `expire_existing=true`. Default false → parallel
                        # attributions coexist.
                        # Wave 2 B1 2026-04-30: predicate-aware default. SUPERSEDE
                        # bucket auto-retires; COEXIST bucket auto-coexists; unknown
                        # falls through to request flag (yesterday's behavior).
                        should_supersede = _resolve_supersession_policy(
                            predicate_upper, body.expire_existing
                        )
                        if should_supersede:
                            for row in existing:
                                sim = float(row['similarity'])
                                if (row['predicate'] == predicate_upper
                                        and row['object_uri'] != object_uri
                                        and sim > 0.5):
                                    await conn.execute("""
                                        UPDATE knowledge_facts
                                        SET valid_to = NOW()
                                        WHERE id = $1
                                    """, row['id'])
                                    logger.info(
                                        f"Superseded fact {row['id']}: "
                                        f"{row['fact_text']} → {fact.fact_text}")
                                    facts_superseded += 1

                    # Wave A A2 (2026-05-01): silent-fail surface for NULL-embed
                    # writes. When fact_embedding is None (provider degraded /
                    # quota / auth fail), the fact still writes (don't lose data)
                    # but we (a) log structured WARNING with the predicate +
                    # subject identifying the affected fact, (b) bump a process-
                    # level counter on app.state for /health to surface, and
                    # (c) bump per-response facts_null_embed so the caller sees
                    # the silent-fail count immediately.
                    if fact_embedding is None:
                        facts_null_embed += 1
                        logger.warning(
                            "null_embed_fact_write subject=%s predicate=%s "
                            "group_id=%s episode_id=%s — provider degraded; "
                            "fact written without fact_embedding_3072 and will "
                            "BYPASS future cosine dedup until re-embedded",
                            subject_uri,
                            fact.predicate,
                            body.group_id,
                            episode_id,
                        )
                        try:
                            request.app.state.null_embed_fact_counter = (
                                getattr(request.app.state, "null_embed_fact_counter", 0) + 1
                            )
                        except Exception:
                            pass

                    fact_row = await conn.fetchrow("""
                        INSERT INTO knowledge_facts
                            (episode_id, subject_uri, predicate, object_uri,
                             object_literal, fact_text, fact_embedding_3072,
                             valid_from, valid_to, group_id, predicate_raw, confidence)
                        VALUES ($1, $2, $3, $4, $5, $6, $7::vector, $8, $9, $10, $11, $12)
                        RETURNING id, created_at, source_node_rid
                    """, episode_id, subject_uri, fact.predicate,
                        object_uri, fact.object_literal, fact.fact_text,
                        str(fact_embedding) if fact_embedding else None,
                        _dt(fact.valid_from), _dt(fact.valid_to), body.group_id,
                        fact.predicate, _CONFIDENCE_TO_REAL.get(fact.confidence))
                    facts_created += 1

                    # Federation 2e: accumulate per-fact bundled-emit payload.
                    # Shape pinned by the subscriber contract (_insert_fact in
                    # domain_event_handlers.py). turn_range_* are None — this
                    # endpoint is not the deep-extraction path.
                    emb_col, emb_val = _fact_embedding_discriminator(
                        fact_embedding_3072=fact_embedding
                    )
                    emit_facts.append({
                        "id": str(fact_row["id"]),
                        "subject_uri": subject_uri,
                        "predicate": fact.predicate,
                        "object_uri": object_uri,
                        "object_literal": fact.object_literal,
                        "fact_text": fact.fact_text,
                        "valid_from": fact.valid_from,
                        "valid_to": fact.valid_to,
                        "created_at": fact_row["created_at"].isoformat()
                            if fact_row["created_at"] else None,
                        "group_id": body.group_id,
                        "source_node_rid": fact_row["source_node_rid"],
                        "turn_range_start": None,
                        "turn_range_end": None,
                        "embedding_column": emb_col,
                        "embedding_value": emb_val,
                    })

                # Build the response inside the transaction so the idempotency
                # ledger stores exactly what the caller receives.
                response = EpisodeCreateResponse(
                    episode_id=str(episode_id),
                    episode_reused=episode_reused,
                    facts_created=facts_created,
                    facts_skipped=facts_skipped,
                    facts_superseded=facts_superseded,
                    entities_resolved=entities_resolved,
                    entities_created=entities_created,
                    facts_null_embed=facts_null_embed,
                    entities_typed_by_default=entities_typed_by_default,
                    endpoints_pinned=endpoints_pinned,
                    fact_ids=[f["id"] for f in emit_facts],
                    type_mismatches=type_mismatches,
                )

                # Idempotency: record the response in the SAME transaction. A
                # plain INSERT (not ON CONFLICT DO NOTHING) so a concurrent
                # duplicate loses here and its whole transaction rolls back
                # rather than committing duplicate writes.
                if body.request_id:
                    await conn.execute(
                        "INSERT INTO ingest_idempotency (request_id, endpoint, response) "
                        "VALUES ($1, $2, $3::jsonb)",
                        body.request_id, "/knowledge/episodes",
                        json_mod.dumps(response.model_dump()))

        # Federation 2e: emit the bundled knowledge_episode event AFTER the
        # transaction commits (the `async with` blocks above have exited).
        # Emitting inside the transaction would risk queueing an event for a row
        # a later rollback removed. emit_domain_event is internally gated by
        # KOI_FEDERATE_KNOWLEDGE and never raises, so a flag-off run and an emit
        # failure are both perfect no-ops here.
        bundled_payload = {**episode_payload, "facts": emit_facts}
        await emit_domain_event(
            "knowledge_episode",
            "UPDATE" if episode_reused else "NEW",
            f"orn:personal-koi.knowledge-episode:{episode_id}",
            bundled_payload,
            payload_event_id=str(uuid4()),
        )

        return response

    async def _bind_pinned_uri(
        conn, uri: str, name: str, role: str, type_hint: Optional[str],
    ) -> tuple[str, bool, Optional[str]]:
        """Bind a caller-supplied canonical URI verbatim. No resolution runs.

        Issue #62. The contract is deliberately unforgiving in both directions:

        - It NEVER falls back to name resolution. A pin that cannot be honoured is
          a 422 for the whole request. Falling back would mean the caller asked for
          a determinate binding, silently got a probabilistic one, and got a 201
          saying it worked — which is the precise failure shape the pin removes.
        - It does NOT follow `merged_into`. Tombstone-following is right for a
          NAME (a name matching a merged row is evidence the name belongs to the
          survivor), but a caller that pinned a URI is asserting an identity, not
          offering a hint. If that identity has been merged away since the caller
          froze its map, the map is stale and the caller must re-freeze — silently
          redirecting would make the persisted graph disagree with the map the
          caller is about to verify against, and the verification would then fail
          with a confusing error far from its cause.

        Returns the same (uri, is_new, resolved_type) shape as _resolve_or_create
        so the caller's bookkeeping is unchanged.
        """
        from api.entity_schema import canonicalize_entity_type

        row = await conn.fetchrow(
            "SELECT fuseki_uri, entity_type, entity_text, merged_into, revoked_at "
            "FROM entity_registry WHERE fuseki_uri = $1",
            uri,
        )
        if row is None:
            raise HTTPException(status_code=422, detail={
                "error": "pinned_entity_not_found",
                "role": role, "name": name, "pinned_uri": uri,
                "hint": "The pinned URI does not exist in entity_registry. Pre-register the "
                        "endpoint before writing facts that reference it.",
            })
        if row["merged_into"] is not None:
            raise HTTPException(status_code=422, detail={
                "error": "pinned_entity_merged_away",
                "role": role, "name": name, "pinned_uri": uri,
                "merged_into": row["merged_into"],
                "hint": "The pinned identity was merged after the caller froze its endpoint "
                        "map. Re-run the preflight and freeze against the current graph; this "
                        "endpoint is NOT silently redirected to the survivor.",
            })
        if row["revoked_at"] is not None:
            raise HTTPException(status_code=422, detail={
                "error": "pinned_entity_revoked",
                "role": role, "name": name, "pinned_uri": uri,
                "revoked_at": row["revoked_at"].isoformat(),
            })
        if type_hint:
            want = canonicalize_entity_type(type_hint)
            if want and row["entity_type"] != want:
                raise HTTPException(status_code=422, detail={
                    "error": "pinned_entity_type_mismatch",
                    "role": role, "name": name, "pinned_uri": uri,
                    "requested_type": want, "actual_type": row["entity_type"],
                    "hint": "A pinned URI whose type disagrees with the declared type is a "
                            "contradiction in the caller's own map, not a preference to "
                            "reconcile.",
                })
        return uri, False, row["entity_type"]

    async def _resolve_or_create(
        conn, name: str, create_if_missing: bool,
        embed_fn: Optional[EmbedFn],
        seen: dict,
        type_hint: Optional[str] = None,
    ) -> tuple[Optional[str], bool, Optional[str]]:
        """Resolve entity name → (uri, is_new, resolved_type). Uses per-request cache.

        `type_hint` is consulted first for same-type exact matches, and at
        create-time for new entities. Existing cross-type registry rows are still
        preserved and surfaced as mismatches, except Concept hints may bypass
        cross-type exact matches so scientific terms don't collapse into
        homonymous organizations/products.

        Returns the resolved entity's `entity_type` so the caller can detect
        `type_hint != resolved_type` and surface it as a Guard-class issue.
        For Tier-3 creates, resolved_type == (type_hint or "Concept").
        For cache hits (seen[]), resolved_type is None — the caller already saw
        the type on the first resolution of that name; subsequent hits reuse it.
        """
        from api.personal_ingest_api import (
            normalize_entity_text, generate_entity_uri
        )
        from api.entity_schema import canonicalize_entity_type
        from api.resolution_primitives import resolve_to_live_uri

        # Canonicalize the caller's type hint (strip schema:/bkc:, resolve
        # plural/case) so type-gated exact/alias/fuzzy queries and Tier-3
        # creates below use the canonical schema type_key.
        if type_hint:
            type_hint = canonicalize_entity_type(type_hint)

        normalized = normalize_entity_text(name)
        same_name_seen = False

        # Per-request cache hit — type already surfaced on first lookup of this
        # name in this request, so we return None for resolved_type here.
        if normalized in seen:
            return seen[normalized], False, None

        async def _accept(row):
            """Take a matched row, but never hand back a tombstone.

            `/entities/merge` keeps the loser's row, name and aliases, so all four tiers
            below can match one. Skipping it would be worse than returning it: the lookup
            would miss, fall through to create_new, and mint a THIRD row for a name that
            already has a canonical home. A tombstone match is positive evidence that this
            name belongs to the survivor, so follow it.

            This is a WRITE path — the URI returned here is stored on knowledge_facts — and
            53 fact rows (21 subject, 32 object) are already bound to tombstoned URIs
            because it did not. Those are damage on disk, not just a degraded response.

            Re-reads entity_type when the URI moves: the tombstone's type is not the
            survivor's. Pol.is was merged schema:SoftwareApplication -> Concept, so
            returning the matched row's type would report the type of a dead row.
            """
            uri = await resolve_to_live_uri(conn, row["fuseki_uri"])
            etype = row["entity_type"]
            if uri != row["fuseki_uri"]:
                live = await conn.fetchrow(
                    "SELECT entity_type FROM entity_registry WHERE fuseki_uri = $1", uri)
                if live:
                    etype = live["entity_type"]
                logger.info("knowledge-add followed tombstone %s -> %s for %r",
                            row["fuseki_uri"], uri, name)
            seen[normalized] = uri
            return uri, False, etype

        # Tier 1: exact match on normalized_text. When the caller supplies a type
        # hint, prefer same-type exact matches before considering cross-type
        # rows; this avoids resolving paper-local concepts such as "discord" to
        # unrelated organizations with the same normalized name.
        if type_hint:
            row = await conn.fetchrow("""
                SELECT fuseki_uri, entity_type FROM entity_registry
                WHERE normalized_text = $1 AND entity_type = $2
                LIMIT 1
            """, normalized, type_hint)
            if row:
                return await _accept(row)

        row = await conn.fetchrow("""
            SELECT fuseki_uri, entity_type FROM entity_registry
            WHERE normalized_text = $1
            LIMIT 1
        """, normalized)
        if row:
            if type_hint == "Concept" and row["entity_type"] != "Concept":
                # This is the intentional Concept-homonym bypass. If it reaches
                # create, migration 111 must classify the new row as ambiguous
                # rather than pretending no same-name entity existed.
                same_name_seen = True
                row = None
            else:
                return await _accept(row)

        # Tier 1b: case-insensitive alias match
        if type_hint:
            row = await conn.fetchrow("""
                SELECT fuseki_uri, entity_type FROM entity_registry
                WHERE entity_type = $2
                  AND $1 = ANY(SELECT LOWER(unnest(aliases)))
                LIMIT 1
            """, normalized, type_hint)
            if row:
                return await _accept(row)

        row = await conn.fetchrow("""
            SELECT fuseki_uri, entity_type FROM entity_registry
            WHERE $1 = ANY(SELECT LOWER(unnest(aliases)))
            LIMIT 1
        """, normalized)
        if row:
            if type_hint == "Concept" and row["entity_type"] != "Concept":
                row = None
            else:
                return await _accept(row)

        # Tier 2: fuzzy + semantic match against same-type entities.
        # 2026-05-19 root-cause fix: previously knowledge-add only ran Tier 1
        # exact + Tier 1.1 alias, then jumped straight to Tier 3 create_new.
        # This bypassed the multi-tier resolver and created sibling rows for
        # name variants ("Calla Amberly" alongside "Calla Amberly Hartwell";
        # "Riverlink AI" alongside "RiverlinkAI"). Always run multi-tier
        # before considering create_new — same type-filter as the resolver, so
        # we still won't bleed across types unintentionally.
        if type_hint:
            try:
                from api.resolution_primitives import resolve_entity_multi_tier
                mode = "semantic" if embed_fn else "fuzzy"
                resolved_uri, confidence, _rel = await resolve_entity_multi_tier(
                    conn,
                    name,
                    type_hint,
                    mode=mode,
                    embed_fn=embed_fn,
                    resolution_caller="knowledge_router.add_knowledge",
                )
                if resolved_uri and confidence >= 0.85:
                    # Fetch resolved_type for return contract
                    rtype_row = await conn.fetchrow(
                        "SELECT entity_type FROM entity_registry WHERE fuseki_uri = $1",
                        resolved_uri,
                    )
                    rtype = rtype_row["entity_type"] if rtype_row else type_hint
                    seen[normalized] = resolved_uri
                    logger.info(
                        f"knowledge-add multi-tier resolved {name!r} -> {resolved_uri} "
                        f"(confidence={confidence:.2f}, mode={mode}); skipped create_new"
                    )
                    return resolved_uri, False, rtype
            except Exception as e:
                # Resolver failure must not block ingestion — fall through to
                # legacy Tier-3 create_new path. Log loudly so the failure is
                # visible in the receipt logs.
                logger.warning(
                    f"knowledge-add multi-tier resolver failed for {name!r}: {e}; "
                    f"falling back to create_new"
                )

        if not create_if_missing:
            return None, False, None

        # Tier 3: create new entity. Use type_hint when provided; default to
        # "Concept" only when caller has no idea (matches legacy behavior).
        #
        # The default is recorded, not just applied. entity_type is hashed into the
        # URI by generate_entity_uri and prefixed onto it, so a guess cannot be
        # corrected in place afterwards — updating the column would leave the URI
        # permanently wrong. What CAN be recovered is whether it was a guess, and
        # until 2026-08-13 nothing distinguished "the extractor said Concept" from
        # "nobody said anything", which made the guess rate unmeasurable by
        # construction. Concept is also the largest bucket (8,603 of 13,625
        # knowledge-add rows), so a wrong guess disappears into the crowd.
        typed_by_default = type_hint is None
        entity_type = type_hint if type_hint else "Concept"

        # Nothing validated entity_type on the way in, so the registry accumulated 25
        # non-canonical variants across 561 rows before anyone looked: namespace
        # duplicates of canonical types (schema:Person, bkc:Concept, schema:Place),
        # unregistered-but-consistent types (Document 240, Event 150,
        # SoftwareApplication 100), and lowercase one-offs (paper, gist, article).
        # Each variant partitions the registry -- a name stored as `Place` cannot be
        # found by a Tier-1 lookup for `Location`, so it silently duplicates instead.
        # Warn rather than reject: a caller with a genuinely new type should not have
        # its write fail, but the drift must not be silent. A recurring name here is
        # a prompt to add the type to DEFAULT_SCHEMAS or to fix the caller.
        try:
            from api.entity_schema import DEFAULT_SCHEMAS
            if entity_type not in DEFAULT_SCHEMAS:
                logger.warning(
                    f"entity_type {entity_type!r} is not in DEFAULT_SCHEMAS "
                    f"(creating {name!r} anyway) — add it to the schema or fix the caller"
                )
        except Exception:
            pass  # never let a schema-introspection failure block a write
        new_uri = generate_entity_uri(name, entity_type)

        embedding = None
        if embed_fn:
            embedding = await embed_fn(name)

        # Writes to embedding_3072 (post-migration 089); legacy embedding
        # column (1024) retained for rollback only — do not write to it.
        insert_tag = await conn.execute("""
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, normalized_text, entity_type,
                 source, embedding_3072, metadata, resolution_tier)
            VALUES ($1, $2, $3, $4, $5, $6::vector, $7::jsonb, $8)
            ON CONFLICT (fuseki_uri) DO NOTHING
        """, new_uri, name, normalized, entity_type,
            'knowledge-add', str(embedding) if embedding else None,
            '{"type_source": "default"}' if typed_by_default else '{}',
            'tier3_created_ambiguous' if same_name_seen else 'tier3_created')

        # asyncpg's status tag is "INSERT 0 1" on a real insert, "INSERT 0 0"
        # when ON CONFLICT DO NOTHING no-op'd. Never trusted before this fix —
        # a 0-row result was still reported is_new=True, live-log-proven twice
        # on the identical URI (same deterministic name+type hash reached this
        # INSERT a second time, most likely via a same-name concurrent race).
        # That silently rebinds the fact to whatever row actually holds this
        # URI without checking its real type, and skips the caller's
        # type_mismatches check (:897-898 above), which only compares against
        # the type THIS call requested — never the type the existing row
        # actually has when a conflict is hidden.
        try:
            rows_affected = int(insert_tag.split()[-1])
        except (ValueError, IndexError):
            rows_affected = 1  # unexpected tag shape — don't misreport a real insert as a conflict

        if rows_affected == 0:
            # The row holding this URI may be a tombstone: the deterministic URI of a
            # merged-away entity is still taken, and every tier above skips it because
            # they match live rows only. Follow it like _accept does, or the fact is
            # bound to a dead URI (Phase 1b, 2026-10-12).
            live_uri = await resolve_to_live_uri(conn, new_uri)
            existing = await conn.fetchrow(
                "SELECT entity_type FROM entity_registry WHERE fuseki_uri = $1", live_uri
            )
            actual_type = existing["entity_type"] if existing else entity_type
            seen[normalized] = live_uri
            logger.warning(
                f"Tier-3 create for {name!r} conflicted on {new_uri} (already exists"
                f"{'' if live_uri == new_uri else f', merged into {live_uri}'} as "
                f"{actual_type!r}); reporting is_new=False instead of a silent re-create"
            )
            return live_uri, False, actual_type

        seen[normalized] = new_uri
        logger.info(f"Created new entity: {name} -> {new_uri}")
        return new_uri, True, entity_type

    # -------------------------------------------------------------------
    # GET /facts/search — semantic search over facts
    # -------------------------------------------------------------------
    @router.get("/facts/search")
    async def search_facts(
        request: Request,
        query: str = Query(..., description="Search query"),
        limit: int = Query(10, ge=1, le=100),
        group_id: Optional[str] = Query(None),
        include_expired: bool = Query(False),
    ):
        if not _facts_surface_available(request):
            return JSONResponse(
                content={"facts": [], "count": 0},
                headers=_facts_surface_headers(request),
            )

        if not _query_embed:
            raise HTTPException(
                status_code=503,
                detail="Embedding provider not configured")

        query_embedding = await _query_embed(query)
        if not query_embedding:
            raise HTTPException(
                status_code=500,
                detail="Failed to generate query embedding")

        async with pool.acquire() as conn:
            validity_filter = "" if include_expired else "AND f.valid_to IS NULL"
            group_filter = "AND f.group_id = $3" if group_id else ""

            params: list = [str(query_embedding), limit]
            if group_id:
                params.append(group_id)

            rows = await conn.fetch(f"""
                SELECT f.id, f.episode_id, e.name AS episode_name,
                       f.subject_uri, f.predicate, f.object_uri,
                       f.object_literal, f.fact_text,
                       f.valid_from, f.valid_to, f.created_at,
                       f.source_node_rid,
                       e.source_document, e.metadata AS ep_metadata,
                       1 - (f.fact_embedding_3072::halfvec(3072)
                            <=> $1::halfvec(3072)) AS similarity
                FROM knowledge_facts f
                LEFT JOIN knowledge_episodes e ON e.id = f.episode_id
                WHERE f.fact_embedding_3072 IS NOT NULL
                  {validity_filter}
                  {group_filter}
                ORDER BY f.fact_embedding_3072::halfvec(3072)
                         <=> $1::halfvec(3072)
                LIMIT $2
            """, *params)

            # Piece C: batch-load source_url for document-sourced facts (one query).
            url_map = await _build_source_url_map(
                conn, [r.get("source_node_rid") for r in rows])

            results = []
            for row in rows:
                d = _row_to_dict(row)
                ep_meta = _parse_jsonb(d.pop("ep_metadata", None))
                # Source-link surfacing: always-present source_document + source_url.
                d['source_document'] = row.get('source_document')
                d['source_url'] = derive_source_url(
                    row.get('source_node_rid'), row.get('source_document'), ep_meta, url_map)
                # Resolve entity names for display
                d['subject_name'] = await _get_entity_name(conn, d.get('subject_uri'))
                d['object_name'] = await _get_entity_name(conn, d.get('object_uri'))
                results.append(d)

            return {"facts": results, "count": len(results)}

    # -------------------------------------------------------------------
    # GET /episodes — list/search episodes
    # -------------------------------------------------------------------
    @router.get("/episodes")
    async def list_episodes(
        request: Request,
        source_document: Optional[str] = Query(None),
        query: Optional[str] = Query(None),
        group_id: Optional[str] = Query(None),
        created_after: Optional[str] = Query(None, description="ISO datetime — only return episodes created after this timestamp"),
        limit: int = Query(20, ge=1, le=100),
    ):
        if not _facts_surface_available(request):
            return JSONResponse(
                content={"episodes": [], "count": 0},
                headers=_facts_surface_headers(request),
            )

        async with pool.acquire() as conn:
            conditions = []
            params: list = []
            idx = 1

            if source_document:
                conditions.append(f"e.source_document ILIKE ${idx}")
                params.append(f"%{source_document}%")
                idx += 1

            if query:
                conditions.append(f"(e.name ILIKE ${idx} OR e.content ILIKE ${idx})")
                params.append(f"%{query}%")
                idx += 1

            if group_id:
                conditions.append(f"e.group_id = ${idx}")
                params.append(group_id)
                idx += 1

            if created_after:
                ca_dt = _dt(created_after)
                if ca_dt:
                    conditions.append(f"e.created_at > ${idx}")
                    params.append(ca_dt)
                    idx += 1

            where = "WHERE " + " AND ".join(conditions) if conditions else ""
            params.append(limit)

            rows = await conn.fetch(f"""
                SELECT e.*, COUNT(f.id) AS fact_count
                FROM knowledge_episodes e
                LEFT JOIN knowledge_facts f ON f.episode_id = e.id
                {where}
                GROUP BY e.id
                ORDER BY e.created_at DESC
                LIMIT ${idx}
            """, *params)

            return {"episodes": [_row_to_dict(r) for r in rows],
                    "count": len(rows)}

    # -------------------------------------------------------------------
    # GET /entity/{uri}/facts — all facts for an entity
    # -------------------------------------------------------------------
    @router.get("/entity/{uri:path}/facts")
    async def entity_facts(
        request: Request,
        uri: str,
        include_expired: bool = Query(False),
        limit: int = Query(50, ge=1, le=200),
    ):
        if not _facts_surface_available(request):
            return JSONResponse(
                content={
                    "entity_uri": uri,
                    "entity_name": None,
                    "facts": [],
                    "count": 0,
                },
                headers=_facts_surface_headers(request),
            )

        async with pool.acquire() as conn:
            validity_filter = "" if include_expired else "AND f.valid_to IS NULL"

            rows = await conn.fetch(f"""
                SELECT f.id, f.episode_id, e.name AS episode_name,
                       f.subject_uri, f.predicate, f.object_uri,
                       f.object_literal, f.fact_text,
                       f.valid_from, f.valid_to, f.created_at
                FROM knowledge_facts f
                LEFT JOIN knowledge_episodes e ON e.id = f.episode_id
                WHERE (f.subject_uri = $1 OR f.object_uri = $1)
                  {validity_filter}
                ORDER BY f.valid_from DESC NULLS LAST
                LIMIT $2
            """, uri, limit)

            results = []
            for row in rows:
                d = _row_to_dict(row)
                d['subject_name'] = await _get_entity_name(conn, d.get('subject_uri'))
                d['object_name'] = await _get_entity_name(conn, d.get('object_uri'))
                results.append(d)

            entity_name = await _get_entity_name(conn, uri)
            return {"entity_uri": uri, "entity_name": entity_name,
                    "facts": results, "count": len(results)}

    # -------------------------------------------------------------------
    # GET /unified-search — RRF fusion over entities, facts, sessions, docs
    # -------------------------------------------------------------------
    @router.get("/unified-search")
    async def unified_search(
        request: Request,
        response: Response,
        scope: ReadScope = Depends(_unified_scope),
        query: str = Query(..., description="Search query"),
        limit: int = Query(10, ge=1, le=50),
        include: str = Query(
            "entities,facts,sessions,wiki,vault,memories",
            description="Comma-separated surfaces to query: entities,facts,sessions,docs,wiki,vault,memories. 'memories' = koi_memory_chunks for email/substack/calendar/other sensors (everything not wiki- or repo-doc-scoped)."),
        doc_kind: Optional[str] = Query(None, description="Filter docs by doc_kind (e.g. architecture, spec, operations)"),
        status: Optional[str] = Query(None, description="Filter docs by status (e.g. active, draft)"),
        is_governed: Optional[bool] = Query(None, description="Filter docs by governed flag (has doc_id)"),
        repo: Optional[str] = Query(None, description="Filter docs by repo name (e.g. darren-workflow)"),
        rerank: str = Query(
            "rrf",
            description="Reranker: 'rrf' (default; pure RRF score) or 'mmr' (Maximal Marginal Relevance — diversity-aware)",
        ),
        mmr_lambda: float = Query(
            0.5,
            ge=0.0,
            le=1.0,
            description="MMR λ parameter [0,1]. 1.0 = pure relevance (≈RRF), 0.0 = pure diversity. Default 0.5.",
        ),
    ):
        # Tier-2 instrumentation: per-route latency_ms (Step 6).
        _t_route_start = time.monotonic()

        surfaces = [s.strip() for s in include.split(",") if s.strip()]
        invalid_surfaces = sorted(set(surfaces) - UNIFIED_SEARCH_VALID_SURFACES)
        if invalid_surfaces:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "unknown surface(s) in 'include'",
                    "invalid": invalid_surfaces,
                    "valid_surfaces": sorted(UNIFIED_SEARCH_VALID_SURFACES),
                },
            )
        # Caller scope. Sessions and the vault have no source
        # privacy metadata and no grant in tranche 1, so they are personal-only:
        # a non-personal caller asking for them is narrowed, never widened.
        surfaces_withheld: List[str] = []
        if not scope.is_personal:
            surfaces_withheld = [s for s in surfaces if s in ("sessions", "vault")]
            surfaces = [s for s in surfaces if s not in ("sessions", "vault")]

        def _src_vis(col: str, start: int):
            # TRUE (no params) for personal, so the operator's ranked query is today's.
            return chunk_visible_sql(scope, col, start)

        def _ent_vis(start: int, alias: str = "er"):
            if scope.is_personal:
                return "TRUE", []
            return entity_visible_sql(scope, alias, start)

        k = 60  # RRF constant
        facts_surface_available = _facts_surface_available(request)
        response.headers.update(_facts_surface_headers(request))

        # ── Attempt embedding; degrade gracefully on any failure ──────
        degraded = False
        degraded_reason: Optional[str] = None
        query_embedding: Optional[List[float]] = None

        # Pack 2.2: reset the fallback-fired marker at request entry so a
        # prior request's flag can never bleed into this one.
        reset_fallback_fired()

        if not _query_embed:
            degraded = True
            degraded_reason = "embedding_unavailable"
            logger.warning(
                "unified-search degraded: no embedding provider configured")
        else:
            try:
                query_embedding = await _query_embed(query)
                if not query_embedding:
                    degraded = True
                    degraded_reason = "embedding_failed"
                    logger.warning(
                        "unified-search degraded: embedding returned None")
            except Exception as exc:
                degraded = True
                degraded_reason = "embedding_failed"
                logger.warning(
                    "unified-search degraded: embedding raised %s", exc)

        # Pack 2.2: capture the fallback-fired marker immediately after
        # the embedding call. True iff FallbackChainEmbeddingProvider
        # fell through to its secondary provider on a query path
        # (read succeeded but on degraded quality). Distinct from
        # `degraded` above which signals embedding-unavailable / None.
        embedding_fallback_fired = was_fallback_fired()

        all_results: list[dict] = []
        facts_results: list[dict] = []
        surface_errors: Dict[str, str] = {}

        async def _bounded_surface(
            surface: str,
            coro: Coroutine[Any, Any, Any],
            timeout_seconds: float,
        ) -> Any:
            try:
                return await asyncio.wait_for(coro, timeout=timeout_seconds)
            except asyncio.TimeoutError:
                surface_errors[surface] = "timeout"
                logger.warning(
                    "unified-search %s surface timed out after %.1fs",
                    surface,
                    timeout_seconds,
                )
                return None
            except Exception as exc:
                surface_errors[surface] = type(exc).__name__
                logger.warning(
                    "unified-search %s surface failed: %s",
                    surface,
                    exc,
                )
                return None

        async with pool.acquire() as conn:
            if degraded:
                # ── Text-first fallback (no vectors) ─────────────────
                words = [w for w in query.lower().split() if len(w) >= 3]
                if words:
                    # Entities: ILIKE on normalized_text (prefer shorter names)
                    if "entities" in surfaces:
                        conditions = " OR ".join(
                            f"normalized_text ILIKE ${i + 1}"
                            for i in range(len(words)))
                        e_params: list = [f"%{w}%" for w in words]
                        e_vis, e_vis_params = _ent_vis(len(e_params) + 1)
                        e_params += e_vis_params + [20]
                        rows = await conn.fetch(f"""
                            SELECT er.fuseki_uri, er.entity_text, er.entity_type,
                                   erm.vault_path
                            FROM entity_registry er
                            LEFT JOIN LATERAL (
                                SELECT vault_path FROM entity_rid_mappings
                                WHERE canonical_uri = er.fuseki_uri
                                ORDER BY last_synced DESC NULLS LAST LIMIT 1
                            ) erm ON true
                            WHERE ({conditions}) AND NOT er.node_private
                              AND er.merged_into IS NULL
                              AND {e_vis}
                            ORDER BY LENGTH(er.entity_text)
                            LIMIT ${len(e_params)}
                        """, *e_params)
                        for rank, row in enumerate(rows):
                            all_results.append({
                                "text": row["entity_text"],
                                "score": 1.0 / (k + rank + 1),
                                "source": "entity",
                                "type": row["entity_type"],
                                "uri": row["fuseki_uri"],
                                # Piece C: openable entity locators (vault + quartz).
                                "vault_path": row.get("vault_path"),
                                "quartz_url": _quartz_url(row["entity_type"], row["entity_text"]),
                                "metadata": {"match_mode": "text"},
                            })

                    # Facts: ILIKE on fact_text (offset to rank below entities)
                    if "facts" in surfaces and facts_surface_available:
                        conditions = " OR ".join(
                            f"fact_text ILIKE ${i + 1}"
                            for i in range(len(words)))
                        f_params: list = [f"%{w}%" for w in words]
                        f_vis, f_vis_params = _src_vis("f.source_node_rid", len(f_params) + 1)
                        f_params += f_vis_params + [20]
                        rows = await conn.fetch(f"""
                            SELECT f.id, f.subject_uri, f.predicate,
                                   f.object_uri, f.fact_text,
                                   f.source_node_rid,
                                   e.name AS episode_name,
                                   e.source_document, e.metadata AS ep_metadata
                            FROM knowledge_facts f
                            LEFT JOIN knowledge_episodes e
                                   ON f.episode_id = e.id
                            WHERE ({conditions}) AND f.valid_to IS NULL
                              AND {f_vis}
                            ORDER BY f.created_at DESC
                            LIMIT ${len(f_params)}
                        """, *f_params)
                        f_url_map = await _build_source_url_map(
                            conn, [r.get("source_node_rid") for r in rows])
                        for rank, row in enumerate(rows):
                            fact_result = {
                                "text": row["fact_text"],
                                "score": 1.0 / (k + 20 + rank + 1),
                                "source": "fact",
                                "episode": row["episode_name"],
                                # Piece C: citable + openable.
                                "source_document": row.get("source_document"),
                                "source_url": derive_source_url(
                                    row.get("source_node_rid"), row.get("source_document"),
                                    _parse_jsonb(row.get("ep_metadata")), f_url_map),
                                "metadata": {
                                    "subject": row["subject_uri"],
                                    "predicate": row["predicate"],
                                    "object": row["object_uri"],
                                    "match_mode": "text",
                                },
                            }
                            facts_results.append(fact_result)
                            all_results.append(fact_result)
                    else:
                        facts_results = []

                    # Sessions: ILIKE on chunk_text (lowest priority)
                    if "sessions" in surfaces:
                        async def _fetch_text_session_rows():
                            async with pool.acquire() as session_conn:
                                table_exists = await session_conn.fetchval("""
                                    SELECT EXISTS (
                                        SELECT FROM information_schema.tables
                                        WHERE table_name = 'session_chunks'
                                    )
                                """)
                                if not table_exists:
                                    return []

                                conditions = " OR ".join(
                                    f"chunk_text ILIKE ${i + 1}"
                                    for i in range(len(words)))
                                s_params: list = [f"%{w}%" for w in words] + [20]
                                return await session_conn.fetch(f"""
                                    SELECT sc.id, sc.session_id, sc.chunk_text
                                    FROM session_chunks sc
                                    WHERE ({conditions})
                                    ORDER BY sc.created_at DESC
                                    LIMIT ${len(words) + 1}
                                """, *s_params)

                        rows = await _bounded_surface(
                            "sessions",
                            _fetch_text_session_rows(),
                            UNIFIED_SEARCH_SESSIONS_TIMEOUT_SECONDS,
                        ) or []
                        if rows:
                            for rank, row in enumerate(rows):
                                all_results.append({
                                    "text": row["chunk_text"][:500],
                                    "score": 1.0 / (k + 40 + rank + 1),
                                    "source": "session",
                                    "session_id": row["session_id"],
                                    "metadata": {"match_mode": "text"},
                                })

                    # Docs: ILIKE on chunk text from doc-scanner (lowest priority after sessions)
                    if "docs" in surfaces:
                        conditions = " OR ".join(
                            f"(mc.content->>'text') ILIKE ${i + 1}"
                            for i in range(len(words)))
                        d_filter = ""
                        d_params: list = [f"%{w}%" for w in words]
                        if doc_kind:
                            d_params.append(doc_kind)
                            d_filter += f" AND mc.metadata->>'doc_kind' = ${len(d_params)}"
                        if status:
                            d_params.append(status)
                            d_filter += f" AND mc.metadata->>'status' = ${len(d_params)}"
                        if is_governed is not None:
                            d_params.append(str(is_governed).lower())
                            d_filter += f" AND mc.metadata->>'is_governed' = ${len(d_params)}"
                        if repo:
                            d_params.append(repo)
                            d_filter += f" AND mc.metadata->>'repo' = ${len(d_params)}"
                        d_vis, d_vis_params = _src_vis("mc.document_rid", len(d_params) + 1)
                        d_params += d_vis_params
                        d_params.append(20)
                        rows = await conn.fetch(f"""
                            SELECT mc.chunk_rid, mc.content->>'text' AS chunk_text,
                                   mc.metadata
                            FROM koi_memory_chunks mc
                            WHERE ({conditions})
                              AND mc.metadata->>'repo' IS NOT NULL
                              AND {d_vis}
                              {d_filter}
                            ORDER BY mc.created_at DESC
                            LIMIT ${len(d_params)}
                        """, *d_params)
                        for rank, row in enumerate(rows):
                            meta = _parse_jsonb(row["metadata"])
                            meta["match_mode"] = "text"
                            all_results.append({
                                "text": row["chunk_text"][:500],
                                "score": 1.0 / (k + 60 + rank + 1),
                                "source": "doc",
                                "doc_id": meta.get("doc_id"),
                                "doc_kind": meta.get("doc_kind"),
                                "repo": meta.get("repo"),
                                # Piece C: citable doc surface.
                                "title": meta.get("title"),
                                "source_url": derive_source_url(None, None, meta, None),
                                "metadata": meta,
                            })

            else:
                # ── Normal semantic RRF mode ──────────────────────────
                emb_str = str(query_embedding)

                # Raise ivfflat.probes so recently-inserted chunks in
                # non-nearest centroids are still considered. Default probes=1
                # misses new rows until the index is rebuilt. Session-level
                # SET persists on the pooled connection, which is fine — we
                # always want higher recall on retrieval paths.
                await conn.execute("SET ivfflat.probes = 10")

                # Entities (vector similarity, exclude private)
                # OpenAI text-embedding-3-large @ 3072-dim via halfvec HNSW index.
                # Rollback: see migrations 089/090 + config/personal.env.
                facts_results = []
                if "entities" in surfaces:
                    e_vis, e_vis_params = _ent_vis(2)
                    rows = await conn.fetch(f"""
                        SELECT er.fuseki_uri, er.entity_text, er.entity_type,
                               erm.vault_path,
                               er.metadata -> 'closure' AS closure,
                               1 - (er.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score
                        FROM entity_registry er
                        LEFT JOIN LATERAL (
                            SELECT vault_path FROM entity_rid_mappings
                            WHERE canonical_uri = er.fuseki_uri
                            ORDER BY last_synced DESC NULLS LAST LIMIT 1
                        ) erm ON true
                        WHERE er.embedding_3072 IS NOT NULL AND NOT er.node_private
                          AND er.merged_into IS NULL
                          AND {e_vis}
                        ORDER BY er.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                        LIMIT 20
                    """, emb_str, *e_vis_params)
                    for rank, row in enumerate(rows):
                        meta = {"vector_score": float(row["score"])}
                        # An entity's CLOSURE — its adjudicated standing — must ride
                        # every retrieval result, or the closure operation is invisible
                        # to exactly the readers it exists for. A REFUTED/SCOPED claim
                        # that still retrieves as live is how a killed experiment gets
                        # re-proposed (see migration 107 / admin_router closure block).
                        raw_closure = row["closure"]
                        if raw_closure:
                            closure = (json.loads(raw_closure)
                                       if isinstance(raw_closure, str) else raw_closure)
                            meta["closure"] = closure
                            meta["is_closed"] = closure.get("status") != "OPEN"
                        all_results.append({
                            "text": row["entity_text"],
                            "score": 1.0 / (k + rank + 1),
                            "source": "entity",
                            "type": row["entity_type"],
                            "uri": row["fuseki_uri"],
                            # Piece C: openable entity locators (vault + quartz).
                            "vault_path": row.get("vault_path"),
                            "quartz_url": _quartz_url(row["entity_type"], row["entity_text"]),
                            "metadata": meta,
                        })

                # Facts (vector similarity, joined with episodes).
                # Reads from fact_embedding_3072 (post-migration 096); halfvec
                # cast required because pgvector full-precision indexes cap at
                # 2000 dims (see migration 097). Old 1024-dim rows backfilled
                # NULL — they're filtered by the IS NOT NULL guard.
                if "facts" in surfaces and facts_surface_available:
                    f_vis, f_vis_params = _src_vis("f.source_node_rid", 2)
                    # The subject entity's closure rides along only when that
                    # entity is itself visible to the caller (TRUE for personal).
                    se_vis, se_vis_params = _ent_vis(2 + len(f_vis_params), "se")
                    try:
                        rows = await conn.fetch(f"""
                            SELECT f.id, f.subject_uri, f.predicate, f.object_uri,
                                   f.fact_text, f.source_node_rid, e.name AS episode_name,
                                   e.source_document, e.metadata AS ep_metadata,
                                   se.metadata -> 'closure' AS subject_closure,
                                   1 - (f.fact_embedding_3072::halfvec(3072)
                                        <=> $1::halfvec(3072)) AS score
                            FROM knowledge_facts f
                            LEFT JOIN knowledge_episodes e ON f.episode_id = e.id
                            LEFT JOIN entity_registry se ON se.fuseki_uri = f.subject_uri
                                                         AND {se_vis}
                            WHERE f.valid_to IS NULL
                              AND f.fact_embedding_3072 IS NOT NULL
                              AND {f_vis}
                            ORDER BY f.fact_embedding_3072::halfvec(3072)
                                     <=> $1::halfvec(3072)
                            LIMIT 20
                        """, emb_str, *f_vis_params, *se_vis_params)
                    except asyncpg.exceptions.DataError as e:
                        surface_errors["facts"] = "data_error"
                        logger.warning("facts surface vector query skipped: %s", e)
                        rows = []
                    f_url_map = await _build_source_url_map(
                        conn, [r.get("source_node_rid") for r in rows])
                    for rank, row in enumerate(rows):
                        fact_meta = {
                            "subject": row["subject_uri"],
                            "predicate": row["predicate"],
                            "object": row["object_uri"],
                            "vector_score": float(row["score"]),
                        }
                        # Carry the SUBJECT's adjudicated standing onto the fact.
                        #
                        # This is the load-bearing one. Facts are what actually
                        # retrieve — roughly half of all Claim entities have no
                        # embedding_3072 at all, so they never surface on the entity
                        # surface, and an agent asking "what's the frontier here?" gets
                        # FACTS. If a fact about a REFUTED claim retrieves clean, the
                        # closure operation is decorative. On 2026-07-14 exactly that
                        # happened: a fact asserting a killed hypothesis was "the
                        # cleanest novelty bet" retrieved as live, and the dead
                        # experiment was re-proposed.
                        raw_sc = row["subject_closure"]
                        if raw_sc:
                            sc = json.loads(raw_sc) if isinstance(raw_sc, str) else raw_sc
                            fact_meta["subject_closure"] = sc
                            fact_meta["subject_is_closed"] = sc.get("status") != "OPEN"
                        fact_result = {
                            "text": row["fact_text"],
                            "score": 1.0 / (k + rank + 1),
                            "source": "fact",
                            "episode": row["episode_name"],
                            # Piece C: citable + openable.
                            "source_document": row.get("source_document"),
                            "source_url": derive_source_url(
                                row.get("source_node_rid"), row.get("source_document"),
                                _parse_jsonb(row.get("ep_metadata")), f_url_map),
                            "metadata": fact_meta,
                        }
                        facts_results.append(fact_result)
                        all_results.append(fact_result)

                # Sessions (HYBRID retrieval: pgvector dense + tsvector lexical
                # fused by RRF inside SQL).
                #
                # P2 hybrid refactor (plan session-recall-tier-1-expanded
                # 2026-04-28). Two ranked legs:
                #   (a) pgvector cosine over embedding_3072 (OpenAI 3072-dim,
                #       text-embedding-3-large; HNSW halfvec(3072) index)
                #   (b) tsvector ts_rank_cd over chunk_tsv (GENERATED STORED
                #       to_tsvector('english', chunk_text); GIN index)
                #
                # Reciprocal Rank Fusion combines per-leg ranks at k=60 (Octo
                # Pattern B6). Each leg fetches top 100 chunks; FULL OUTER JOIN
                # on chunk id yields the union; rrf_score sums the two
                # contributions. Caller sees a single ranked list of top 20
                # chunks, with internal vector_score + lex_score for diagnostics.
                #
                # Lexical query: convert user query to OR-disjunctive
                # websearch_to_tsquery to maximize partial-match recall on
                # natural-language queries (e.g. "When did F2 transition...");
                # AND-conjunctive plainto_tsquery is too restrictive for the
                # benchmark's recall-shape.
                if "sessions" in surfaces:
                    async def _fetch_semantic_session_rows():
                        async with pool.acquire() as session_conn:
                            await session_conn.execute("SET ivfflat.probes = 10")
                            table_exists = await session_conn.fetchval("""
                                SELECT EXISTS (
                                    SELECT FROM information_schema.tables
                                    WHERE table_name = 'session_chunks'
                                )
                            """)
                            if not table_exists:
                                return []

                            # Build OR-disjunctive tsquery string from user query.
                            # Hyphens preserved (websearch handles ADR-0080 etc).
                            # Stopwords filtered to reduce 0-recall on natural-
                            # language phrasings.
                            _stopwords = {
                                "the", "what", "when", "where", "how", "why",
                                "which", "who", "is", "are", "was", "were", "be",
                                "been", "being", "do", "did", "does", "done",
                                "can", "could", "should", "would", "may", "might",
                                "must", "shall", "will", "or", "and", "but",
                                "not", "of", "to", "for", "on", "at", "in", "by",
                                "with", "from", "as", "into", "that", "this",
                                "these", "those", "there", "here", "then", "than",
                                "such", "also", "very", "more", "most", "just",
                                "only", "over", "under", "have", "has", "had",
                            }
                            _toks = re.findall(r"[A-Za-z0-9_-]{2,}", query.lower())
                            _toks = [t for t in _toks if t not in _stopwords]
                            ts_query_str = " OR ".join(_toks) if _toks else query

                            return await session_conn.fetch("""
                                WITH vec_ranked AS (
                                    SELECT id, session_id, chunk_text,
                                           ROW_NUMBER() OVER (
                                               ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                                           ) AS rnk,
                                           1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS vec_score
                                    FROM session_chunks
                                    WHERE embedding_3072 IS NOT NULL
                                    ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                                    LIMIT 100
                                ),
                                lex_ranked AS (
                                    SELECT sc.id, sc.session_id, sc.chunk_text,
                                           ROW_NUMBER() OVER (
                                               ORDER BY ts_rank_cd(sc.chunk_tsv, q) DESC
                                           ) AS rnk,
                                           ts_rank_cd(sc.chunk_tsv, q) AS lex_score
                                    FROM session_chunks sc,
                                         websearch_to_tsquery('english', $2) q
                                    WHERE sc.chunk_tsv @@ q
                                    ORDER BY ts_rank_cd(sc.chunk_tsv, q) DESC
                                    LIMIT 100
                                ),
                                fused AS (
                                    -- Unweighted RRF (1.0 / (k + rank) per leg).
                                    -- Tuning attempt (2026-04-28): tried vector-leg 1.5×
                                    -- to recover MRR drop on q01/q03 hits; result was
                                    -- recall regression (0.318 → 0.200) because higher
                                    -- vector weight displaced lex-rescued sessions
                                    -- (q01's 585633a5 found via vec_rank=20 + lex_rank=8).
                                    -- Reverted to unweighted; B2 0.318 is the optimal
                                    -- balance for this corpus.
                                    SELECT
                                        COALESCE(v.id, l.id)            AS id,
                                        COALESCE(v.session_id, l.session_id) AS session_id,
                                        COALESCE(v.chunk_text, l.chunk_text) AS chunk_text,
                                        v.vec_score                      AS vec_score,
                                        l.lex_score                      AS lex_score,
                                        v.rnk                            AS vec_rank,
                                        l.rnk                            AS lex_rank,
                                        COALESCE(1.0 / (60 + v.rnk), 0)
                                        + COALESCE(1.0 / (60 + l.rnk), 0) AS rrf_score
                                    FROM vec_ranked v
                                    FULL OUTER JOIN lex_ranked l USING (id)
                                )
                                SELECT id, session_id, chunk_text,
                                       vec_score, lex_score, vec_rank, lex_rank, rrf_score
                                FROM fused
                                ORDER BY rrf_score DESC
                                LIMIT 20
                            """, emb_str, ts_query_str)

                    rows = await _bounded_surface(
                        "sessions",
                        _fetch_semantic_session_rows(),
                        UNIFIED_SEARCH_SESSIONS_TIMEOUT_SECONDS,
                    ) or []
                    for rank, row in enumerate(rows):
                        all_results.append({
                            "text": (row["chunk_text"] or "")[:500],
                            "score": 1.0 / (k + rank + 1),
                            "source": "session",
                            "session_id": row["session_id"],
                            "metadata": {
                                "vec_score": float(row["vec_score"]) if row["vec_score"] is not None else None,
                                "lex_score": float(row["lex_score"]) if row["lex_score"] is not None else None,
                                "vec_rank": int(row["vec_rank"]) if row["vec_rank"] is not None else None,
                                "lex_rank": int(row["lex_rank"]) if row["lex_rank"] is not None else None,
                                "rrf_score": float(row["rrf_score"]),
                            },
                        })

                # Docs (vector similarity on koi_memory_chunks from doc-scanner)
                if "docs" in surfaces:
                    d_filter = ""
                    d_params_vec: list = [emb_str]
                    if doc_kind:
                        d_params_vec.append(doc_kind)
                        d_filter += f" AND mc.metadata->>'doc_kind' = ${len(d_params_vec)}"
                    if status:
                        d_params_vec.append(status)
                        d_filter += f" AND mc.metadata->>'status' = ${len(d_params_vec)}"
                    if is_governed is not None:
                        d_params_vec.append(str(is_governed).lower())
                        d_filter += f" AND mc.metadata->>'is_governed' = ${len(d_params_vec)}"
                    if repo:
                        d_params_vec.append(repo)
                        d_filter += f" AND mc.metadata->>'repo' = ${len(d_params_vec)}"
                    d_vis, d_vis_params = _src_vis("mc.document_rid", len(d_params_vec) + 1)
                    d_params_vec += d_vis_params
                    # hnsw.iterative_scan is `relaxed_order` (set deliberately by
                    # migrations/109_pgvector_cost_model.sql, where it is
                    # load-bearing for FILTERED ANN correctness — do not turn it
                    # off). It returns rows only APPROXIMATELY sorted, and this
                    # query previously trusted that order. Each row's fused score
                    # is 1.0/(k+rank+1) and the list is then truncated, so a
                    # mis-ranked chunk falls off the end rather than merely
                    # ranking lower. The outer ORDER BY costs one sort of 20 rows.
                    # Measured on a frozen 15-query set over this surface's own
                    # corpus: 18 inversions across 9/15 queries -> 0, no change
                    # of membership, kept top-10 similarity improved on 5
                    # queries and degraded on 0.
                    rows = await conn.fetch(f"""
                        WITH hits AS (
                            SELECT mc.chunk_rid,
                                   mc.content->>'text' AS chunk_text,
                                   mc.metadata,
                                   1 - (mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score
                            FROM koi_memory_chunks mc
                            WHERE mc.embedding_3072 IS NOT NULL
                              AND mc.metadata->>'repo' IS NOT NULL
                              AND {d_vis}
                              {d_filter}
                            ORDER BY mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                            LIMIT 20
                        )
                        SELECT * FROM hits ORDER BY score DESC
                    """, *d_params_vec)
                    for rank, row in enumerate(rows):
                        meta = _parse_jsonb(row["metadata"])
                        meta["vector_score"] = float(row["score"])
                        all_results.append({
                            "text": row["chunk_text"][:500],
                            "score": 1.0 / (k + rank + 1),
                            "source": "doc",
                            "doc_id": meta.get("doc_id"),
                            "doc_kind": meta.get("doc_kind"),
                            "repo": meta.get("repo"),
                            # Piece C: citable doc surface.
                            "title": meta.get("title"),
                            "source_url": derive_source_url(None, None, meta, None),
                            "metadata": meta,
                        })

                # Wiki (vector similarity on koi_memory_chunks from mediawiki-sensor)
                if "wiki" in surfaces:
                    # hnsw.iterative_scan is `relaxed_order` (set deliberately by
                    # migrations/109_pgvector_cost_model.sql, where it is
                    # load-bearing for FILTERED ANN correctness — do not turn it
                    # off). It returns rows only APPROXIMATELY sorted, and this
                    # query previously trusted that order. Each row's fused score
                    # is 1.0/(k+rank+1) and the list is then truncated, so a
                    # mis-ranked chunk falls off the end rather than merely
                    # ranking lower. The outer ORDER BY costs one sort of 20 rows.
                    # Measured on a frozen 15-query set over the 99,034-chunk
                    # P2P Foundation corpus: 7 inversions across 4/15 queries
                    # -> 0, no change of membership, kept top-10 similarity
                    # unchanged (those reorderings sat below the cutoff).
                    w_vis, w_vis_params = _src_vis("mc.document_rid", 2)
                    rows = await conn.fetch(f"""
                        WITH hits AS (
                            SELECT mc.chunk_rid,
                                   mc.document_rid,
                                   mc.content->>'text' AS chunk_text,
                                   mc.content->>'title' AS title,
                                   mc.content->>'wiki_url' AS wiki_url,
                                   mc.content->>'section_title' AS section_title,
                                   1 - (mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score
                            FROM koi_memory_chunks mc
                            WHERE mc.embedding_3072 IS NOT NULL
                              AND mc.document_rid LIKE 'mediawiki:%'
                              AND {w_vis}
                            ORDER BY mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                            LIMIT 20
                        )
                        SELECT * FROM hits ORDER BY score DESC
                    """, emb_str, *w_vis_params)
                    for rank, row in enumerate(rows):
                        all_results.append({
                            "text": (row["chunk_text"] or "")[:500],
                            "score": 1.0 / (k + rank + 1),
                            "source": "wiki",
                            "title": row["title"],
                            "section_title": row["section_title"],
                            "wiki_url": row["wiki_url"],
                            "document_rid": row["document_rid"],
                            "chunk_rid": row["chunk_rid"],
                            "metadata": {"vector_score": float(row["score"])},
                        })

                # Memories (vector similarity on koi_memory_chunks for every
                # source NOT covered by the mediawiki-scoped 'wiki' surface or
                # the repo-scoped 'docs' surface — i.e. email/orn, substack,
                # calendar/ics, and any future sensor. These chunks have
                # embedding_3072 populated by the chunk-embedder but had no read
                # path before (fix 2026-06-01: retrieval-chunk-surfaces).
                if "memories" in surfaces:
                    # ANN + LIMIT inside the CTE, THEN join the parent for the
                    # 20 surviving rows — one indexed lookup each on
                    # koi_memories_rid_key. Verified by EXPLAIN that the HNSW
                    # index is still used; measured at +5.5ms p50 on a frozen
                    # 15-query set. The alternative was copying title/author/
                    # source_url onto 61k chunk rows, which is how the existing
                    # metadata got stale in the first place.
                    #
                    # LEFT, not INNER: the FK makes a missing parent impossible
                    # today, but an inner join would make result-set membership
                    # depend on that staying true forever.
                    #
                    # The outer ORDER BY is NOT redundant. hnsw.iterative_scan is
                    # set to `relaxed_order`, so the index returns rows only
                    # APPROXIMATELY sorted and the previous query trusted that
                    # order: measured 15 out-of-order adjacent pairs across
                    # 10 of 15 queries. Since each row's fused score is
                    # 1.0/(k+rank+1), a wrong order is a wrong RRF weight.
                    m_vis, m_vis_params = _src_vis("mc.document_rid", 2)
                    rows = await conn.fetch(f"""
                        WITH hits AS (
                            SELECT mc.chunk_rid,
                                   mc.document_rid,
                                   mc.content->>'text' AS chunk_text,
                                   mc.content->>'title' AS title,
                                   mc.metadata,
                                   1 - (mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score
                            FROM koi_memory_chunks mc
                            WHERE mc.embedding_3072 IS NOT NULL
                              AND mc.document_rid NOT LIKE 'mediawiki:%'
                              AND mc.document_rid NOT LIKE 'doc-scanner:%'
                              AND {m_vis}
                            ORDER BY mc.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                            LIMIT 20
                        )
                        SELECT h.chunk_rid, h.document_rid, h.chunk_text,
                               h.metadata, h.score,
                               COALESCE(h.title, m.content->>'title') AS title,
                               m.metadata->>'author'     AS parent_author,
                               m.metadata->>'source_url' AS parent_source_url
                        FROM hits h
                        LEFT JOIN koi_memories m ON m.rid = h.document_rid
                        ORDER BY h.score DESC
                    """, emb_str, *m_vis_params)
                    for rank, row in enumerate(rows):
                        drid = row["document_rid"] or ""
                        if drid.startswith("orn:gmail"):
                            _src = "email"
                        elif drid.startswith("substack-corpus") or drid.startswith("orn:substack"):
                            _src = "substack"
                        elif drid.startswith("orn:ics") or "ics-event" in drid:
                            _src = "calendar"
                        else:
                            _src = "memory"
                        # This surface reads koi_memory_chunks WITHOUT joining
                        # koi_memories, so anything not on the chunk row cannot
                        # reach a caller. It previously returned only
                        # {vector_score} and dropped mc.metadata entirely, which
                        # is why `title` was null on EVERY result it has ever
                        # served (measured: 0 of 61,730 chunks carry a
                        # chunk-level content title; the writers put it in
                        # metadata). Mirrors the `docs` surface above, which
                        # already returns the whole mc.metadata blob.
                        meta = _parse_jsonb(row["metadata"])
                        meta["vector_score"] = float(row["score"])
                        all_results.append({
                            "text": (row["chunk_text"] or "")[:500],
                            "score": 1.0 / (k + rank + 1),
                            "source": _src,
                            # chunk metadata first (it is the more specific
                            # record), parent row as the fallback that actually
                            # has the data for the 61k legacy chunks.
                            "title": row["title"],
                            "author": meta.get("author") or row["parent_author"],
                            "source_url": (meta.get("source_url")
                                           or row["parent_source_url"]),
                            "document_rid": drid,
                            "chunk_rid": row["chunk_rid"],
                            "metadata": meta,
                        })

        # Vault BM25 (pageindex — Mac only, graceful skip if venv not present)
        if "vault" in surfaces:
            _venv_py = os.path.expanduser(
                "~/.claude/local/darren-workflow/pageindex/venv/bin/python3")
            _script = os.path.expanduser(
                "~/projects/darren-workflow/scripts/pageindex.py")
            if os.path.exists(_venv_py) and os.path.exists(_script):
                try:
                    proc = await asyncio.create_subprocess_exec(
                        _venv_py, _script, "query", query,
                        "--json", "--limit", "10",
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                    )
                    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10.0)
                    if stdout:
                        pi_data = json.loads(stdout)
                        k_vault = 60
                        for rank, hit in enumerate(pi_data.get("results", [])):
                            all_results.append({
                                "text": hit.get("snippet") or hit.get("title", ""),
                                "score": 1.0 / (k_vault + rank + 1),
                                "source": "vault",
                                "title": hit.get("title"),
                                "path": hit.get("path"),
                                "folder": hit.get("folder"),
                                "metadata": {
                                    "bm25_score": hit.get("score"),
                                    "match_mode": "bm25",
                                },
                            })
                except Exception as _e:
                    surface_errors["vault"] = type(_e).__name__
                    logger.warning("unified-search vault surface failed: %s", _e)

        # Sort by RRF score descending; truncate to a 2× candidate pool when
        # MMR is requested, otherwise straight to limit.
        all_results.sort(key=lambda x: x["score"], reverse=True)
        # Candidate count BEFORE the limit cut -- the only way to distinguish
        # "we returned everything we found" from "we found more than `limit`
        # and cut it off" for the `truncated` three-state field below.
        _candidate_count_before_limit = len(all_results)

        rerank_mode = (rerank or "rrf").lower()
        rerank_applied = "rrf"
        if rerank_mode == "mmr" and not degraded and len(all_results) > 1:
            # ── B1 MMR pass (Phase 8 capability parity, 2026-04-29) ──────
            # Maximal Marginal Relevance: post-RRF diversity-aware reranking.
            # candidates = top 2*limit (or all if smaller) → embed each
            # result's text → iteratively select highest-scoring candidate
            # by MMR formula:
            #   mmr_score(c) = λ·rrf_score(c) - (1-λ)·max_cosine(c, S)
            # where S is the already-selected set.
            # Embedding via _doc_embed (same provider as fact/entity writes).
            cand_pool = all_results[: max(limit * 2, 20)]
            try:
                # Wave 3 C1 (2026-04-30): MMR latency optimization —
                # asyncio.gather parallelizes the candidate embedding calls
                # so the network round-trips overlap instead of serializing.
                # B2: prompt_type="rerank" tags the metric line; best-effort
                # kwarg pass-through with TypeError fallback for older shims.
                async def _embed_one(text: str) -> Optional[List[float]]:
                    if not text or _doc_embed is None:
                        return None
                    try:
                        return await _doc_embed(text[:2000], prompt_type="rerank")
                    except TypeError:
                        return await _doc_embed(text[:2000])

                _texts = [(c.get("text") or "").strip() for c in cand_pool]
                cand_embeddings: list[Optional[List[float]]] = await asyncio.gather(
                    *(_embed_one(t) for t in _texts)
                )
                # Iterative MMR selection.
                import math as _math
                def _cos(a, b):
                    if not a or not b:
                        return 0.0
                    s = sum(x * y for x, y in zip(a, b))
                    na = _math.sqrt(sum(x * x for x in a))
                    nb = _math.sqrt(sum(y * y for y in b))
                    if na == 0 or nb == 0:
                        return 0.0
                    return s / (na * nb)
                selected_idx: list[int] = []
                remaining_idx: set[int] = set(range(len(cand_pool)))
                # Normalize RRF scores into [0,1] for the relevance term so
                # the (1-λ) diversity-penalty is on a comparable scale.
                _rrf_scores = [c["score"] for c in cand_pool]
                _rrf_max = max(_rrf_scores) if _rrf_scores else 1.0
                while remaining_idx and len(selected_idx) < limit:
                    best_i = None
                    best_mmr = float("-inf")
                    for i in remaining_idx:
                        rel = (cand_pool[i]["score"] / _rrf_max) if _rrf_max > 0 else 0.0
                        max_sim = 0.0
                        emb_i = cand_embeddings[i]
                        for j in selected_idx:
                            sim = _cos(emb_i, cand_embeddings[j])
                            if sim > max_sim:
                                max_sim = sim
                        mmr = mmr_lambda * rel - (1 - mmr_lambda) * max_sim
                        if mmr > best_mmr:
                            best_mmr = mmr
                            best_i = i
                    if best_i is None:
                        break
                    selected_idx.append(best_i)
                    remaining_idx.remove(best_i)
                    # Annotate result with MMR diagnostics.
                    cand_pool[best_i].setdefault("metadata", {})
                    cand_pool[best_i]["metadata"]["mmr_score"] = best_mmr
                    cand_pool[best_i]["metadata"]["mmr_rank"] = len(selected_idx)
                all_results = [cand_pool[i] for i in selected_idx]
                rerank_applied = "mmr"
            except Exception as _mmr_err:
                logger.warning("MMR rerank failed; falling back to RRF: %s", _mmr_err)
                all_results = all_results[:limit]
                rerank_applied = "rrf_fallback_after_mmr_error"
        else:
            all_results = all_results[:limit]

        # Tier-2: latency_ms field on response (Step 6 instrumentation).
        _latency_ms = round((time.monotonic() - _t_route_start) * 1000, 1)

        # A surface the caller explicitly asked for but that this node can't
        # serve (capability-gated, not a runtime error) is the same kind of
        # gap as a timeout or exception -- fold it into surface_errors so it
        # drives `truncated` below instead of silently vanishing from the
        # results with only an HTTP header (X-Facts-Surface) as a trace.
        if "facts" in surfaces and not facts_surface_available:
            surface_errors.setdefault("facts", "surface_unavailable")

        # Three-state completeness signal -- previously indistinguishable:
        #   "complete"       -- every requested surface ran and nothing was cut.
        #   "truncated"      -- all requested surfaces ran, but there were more
        #                       candidates than `limit`; this response is a
        #                       top-N slice, not the full match set.
        #   "partial_source" -- at least one requested surface (see
        #                       surface_errors) errored, timed out, or was
        #                       unavailable and contributed zero results -- the
        #                       response may be missing a whole surface,
        #                       independent of whether `limit` was also hit.
        if surface_errors:
            truncated = "partial_source"
        elif _candidate_count_before_limit > limit:
            truncated = "truncated"
        else:
            truncated = "complete"

        response: dict = {
            "results": all_results,
            "facts": facts_results,
            "query": query,
            "surfaces_queried": surfaces,
            "total_results": len(all_results),
            "embedding_available": not degraded,
            "latency_ms": _latency_ms,
            "rerank_applied": rerank_applied,
            "mmr_lambda": mmr_lambda if rerank_applied == "mmr" else None,
            "truncated": truncated,
        }
        if degraded:
            response["degraded"] = True
            response["degraded_reason"] = degraded_reason
        if surface_errors:
            response["surface_errors"] = surface_errors
        if surfaces_withheld:
            response["surfaces_withheld"] = surfaces_withheld
        # Pack 2.2: surface fallback-fired-but-recovered case at request
        # scope. Distinct from `degraded` (embedding unavailable / None);
        # this signals "read succeeded on the secondary provider, quality
        # is degraded for this response only." Field is omitted (not False)
        # in the non-degraded path to keep response shape stable.
        if embedding_fallback_fired:
            response["degraded_embedding"] = True
        return response

    # -------------------------------------------------------------------
    # POST /recall-walk — shape-routed recall over knowledge_facts via
    # PostgreSQL recursive CTE. Replaces the FalkorDB-Cypher walk in
    # python/graphiti_recall.py per Phase 3 of plan
    # `koi-graph-consolidation-retire-graphiti.md`.
    #
    # Walk: start from a unified-search seed → episode → fact-graph traversal
    # over AUTHORED_WITHIN + RELATES_TO predicates → return surfaced subjects,
    # objects, and session UUIDs (extracted from object_uri or fact_text).
    #
    # Shape flags:
    #   - "semantic"     filters out expired edges (valid_to IS NOT NULL).
    #   - "temporal"     same as semantic (currently-valid only) but the
    #                    response surfaces valid_from explicitly per fact.
    #   - "relationship" includes expired edges with `expired: true` marker.
    # -------------------------------------------------------------------
    @router.post("/recall-walk")
    async def recall_walk(request: Request, body: RecallWalkRequest):
        if not _facts_surface_available(request):
            raise HTTPException(
                status_code=503,
                detail={"error": "facts surface not configured on this node"},
            )
        if not _query_embed:
            raise HTTPException(
                status_code=503,
                detail="Embedding provider not configured",
            )

        t_total_start = time.monotonic()
        shape = body.shape.lower()
        if shape not in ("semantic", "temporal", "relationship"):
            shape = "semantic"

        # Phase 1: query embedding for episode seed lookup.
        t_query_start = time.monotonic()
        query_embedding = await _query_embed(body.query)
        if not query_embedding:
            raise HTTPException(
                status_code=500, detail="Failed to generate query embedding"
            )
        emb_str = str(query_embedding)
        latency_query_ms = round((time.monotonic() - t_query_start) * 1000, 1)

        # Phase 2: walk in SQL.
        t_walk_start = time.monotonic()
        results: list[dict] = []
        walk_path: list[dict] = []
        seed_episode_ids: list[str] = []
        session_uuid_re = re.compile(
            r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
            re.IGNORECASE,
        )

        async with pool.acquire() as conn:
            # 2a. Seed episodes via vector similarity over fact_embedding_3072
            #     (find episodes whose facts semantically match the query).
            #     Then also try matching against episode names/content via
            #     fact text join. Group_id filter is optional.
            group_filter = "AND f.group_id = $2" if body.group_id else ""
            seed_params: list = [emb_str]
            if body.group_id:
                seed_params.append(body.group_id)
            try:
                seed_rows = await conn.fetch(
                    f"""
                    SELECT DISTINCT f.episode_id,
                           MIN(f.fact_embedding_3072::halfvec(3072)
                               <=> $1::halfvec(3072)) AS dist
                    FROM knowledge_facts f
                    WHERE f.episode_id IS NOT NULL
                      AND f.fact_embedding_3072 IS NOT NULL
                      AND f.valid_to IS NULL
                      {group_filter}
                    GROUP BY f.episode_id
                    ORDER BY dist
                    LIMIT 12
                    """,
                    *seed_params,
                )
            except asyncpg.exceptions.DataError as e:
                logger.warning("recall-walk seed query skipped: %s", e)
                seed_rows = []

            seed_episode_ids = [str(r["episode_id"]) for r in seed_rows]
            walk_path.append(
                {"phase": "seed", "n_episodes": len(seed_episode_ids)}
            )

            if not seed_episode_ids:
                latency_walk_ms = round((time.monotonic() - t_walk_start) * 1000, 1)
                return {
                    "results": [],
                    "walk_path": walk_path,
                    "routing": {
                        "shape_resolved": shape,
                        "shape_source": "endpoint",
                        "legs_queried": ["koi"],
                    },
                    "latency_ms": {
                        "total": round((time.monotonic() - t_total_start) * 1000, 1),
                        "query_phase": latency_query_ms,
                        "walk_phase": latency_walk_ms,
                    },
                }

            # 2b. Recursive CTE walk: start from seed episodes, traverse
            #     fact graph by subject_uri/object_uri up to max_hops.
            #     Wave 3 C2: when body.predicate_filter is set, restrict the
            #     final SELECT (not the walk traversal) to those predicates.
            #     The walk itself still uses default walk_predicates so the
            #     graph traversal isn't artificially constrained — only the
            #     emitted result set.
            include_expired = shape == "relationship"
            validity_filter = "" if include_expired else "AND f.valid_to IS NULL"
            walk_predicates = ("AUTHORED_WITHIN", "RELATES_TO", "RELATED_TO")
            # Result-emit predicate set: filter if requested, else default walk set.
            emit_predicates = (
                [p.upper() for p in body.predicate_filter]
                if body.predicate_filter
                else list(walk_predicates)
            )
            walk_rows = await conn.fetch(
                f"""
                WITH RECURSIVE
                seed AS (
                    SELECT DISTINCT subject_uri AS uri
                    FROM knowledge_facts
                    WHERE episode_id = ANY($1::uuid[])
                ),
                walk(uri, depth) AS (
                    SELECT uri, 0 FROM seed
                    UNION
                    SELECT
                        CASE WHEN f.subject_uri = w.uri
                             THEN COALESCE(f.object_uri, f.object_literal)
                             ELSE f.subject_uri END,
                        w.depth + 1
                    FROM walk w
                    JOIN knowledge_facts f
                      ON (f.subject_uri = w.uri OR f.object_uri = w.uri)
                    WHERE w.depth < $2
                      AND f.predicate = ANY($3::text[])
                      {validity_filter}
                )
                SELECT
                    f.id, f.episode_id, f.subject_uri, f.predicate,
                    f.object_uri, f.object_literal, f.fact_text,
                    f.valid_from, f.valid_to, f.created_at,
                    e.name AS episode_name, e.source_document, e.metadata AS ep_metadata
                FROM knowledge_facts f
                LEFT JOIN knowledge_episodes e ON e.id = f.episode_id
                WHERE (f.subject_uri IN (SELECT uri FROM walk WHERE uri IS NOT NULL)
                       OR f.object_uri IN (SELECT uri FROM walk WHERE uri IS NOT NULL))
                  AND f.predicate = ANY($5::text[])
                  {validity_filter}
                ORDER BY f.created_at DESC
                LIMIT $4
                """,
                seed_episode_ids,
                body.max_hops,
                list(walk_predicates),
                max(body.limit * 4, 20),
                emit_predicates,
            )

            walk_path.append(
                {
                    "phase": "walk",
                    "max_hops": body.max_hops,
                    "n_facts": len(walk_rows),
                    "predicate_filter": body.predicate_filter,
                }
            )

            # 2c. Project facts → result items with session-uuid extraction.
            seen_session_ids: set[str] = set()
            for row in walk_rows:
                fact_text = row["fact_text"] or ""
                # Extract session UUID from object_uri (Session entity path)
                # or fact_text (legacy graphiti-style "session <uuid>" text).
                object_uri = row["object_uri"] or ""
                literal = row["object_literal"] or ""
                # Direct UUID in object_uri or literal:
                for candidate in (object_uri, literal, fact_text):
                    for m in session_uuid_re.finditer(candidate):
                        sid = m.group(0).lower()
                        if sid not in seen_session_ids:
                            seen_session_ids.add(sid)

                ep_metadata = _parse_jsonb(row["ep_metadata"])
                expired = bool(row["valid_to"])
                results.append(
                    {
                        "id": str(row["id"]),
                        "score": 1.0,  # walk hits are rank-stable; uniform score
                        "leg": "koi",
                        "content": fact_text[:500],
                        "metadata": {
                            "source": "fact",
                            "subject_uri": row["subject_uri"],
                            "predicate": row["predicate"],
                            "object_uri": row["object_uri"],
                            "object_literal": row["object_literal"],
                            "valid_from": (
                                row["valid_from"].isoformat()
                                if row["valid_from"] else None
                            ),
                            "valid_to": (
                                row["valid_to"].isoformat()
                                if row["valid_to"] else None
                            ),
                            "expired": expired,
                            "episode_id": str(row["episode_id"]) if row["episode_id"] else None,
                            "episode_name": row["episode_name"],
                            "source_document": row["source_document"],
                            "batch_id": ep_metadata.get("batch_id"),
                            "doc_kind": ep_metadata.get("doc_kind"),
                            "repo": ep_metadata.get("repo"),
                        },
                    }
                )
                if len(results) >= body.limit:
                    break

            # 2d. Add a synthetic "session" item per surfaced session UUID
            #     for parity with graphiti_recall.py output shape (the recall
            #     MCP tool emits one item per session_id when present).
            for sid in list(seen_session_ids)[: body.limit]:
                results.append(
                    {
                        "id": sid,
                        "score": 1.0,
                        "leg": "koi",
                        "content": f"claude-code session {sid}",
                        "metadata": {
                            "source": "session",
                            "session_id": sid,
                        },
                    }
                )

        latency_walk_ms = round((time.monotonic() - t_walk_start) * 1000, 1)
        # Wave 3 C2: emit a structured null_answer block when predicate_filter
        # was set AND the filtered walk returned 0 facts. Lets callers
        # distinguish "no edge of this type exists" from "there might be edges
        # we just didn't return any." Subject anchor optional but recommended
        # for assertion shape.
        null_answer: Optional[dict] = None
        if body.predicate_filter and len(walk_rows) == 0:
            null_answer = {
                "predicate": body.predicate_filter,
                "subject": body.subject_uri,
                "asserted": "no edge found",
                "scope": {
                    "group_id": body.group_id,
                    "shape": shape,
                    "max_hops": body.max_hops,
                    "seed_episodes": len(seed_episode_ids),
                },
            }
        resp = {
            "results": results[: body.limit + len(seen_session_ids)],
            "walk_path": walk_path,
            "session_ids": list(seen_session_ids)[: body.limit],
            "routing": {
                "shape_resolved": shape,
                "shape_source": "endpoint",
                "legs_queried": ["koi"],
            },
            "latency_ms": {
                "total": round((time.monotonic() - t_total_start) * 1000, 1),
                "query_phase": latency_query_ms,
                "walk_phase": latency_walk_ms,
            },
        }
        if null_answer is not None:
            resp["null_answer"] = null_answer
        return resp

    # -------------------------------------------------------------------
    # POST /facts/{fact_id}/retract — soft-expire a fact (set valid_to=NOW)
    # -------------------------------------------------------------------
    # Service-token gated. The sanctioned, reversible verb for retiring a
    # wrong fact: sets `valid_to = NOW()` (the same UPDATE the predicate-
    # supersession path uses at create_episode) rather than hard-DELETEing the
    # row, so the retraction is auditable and recoverable (clear valid_to to
    # undo). Unlike the supersede path it needs neither a replacement fact nor
    # a SUPERSEDE-class predicate, so it can retire facts that path structurally
    # cannot — e.g. a null-object AUTHORED fact.
    @router.post("/facts/{fact_id}/retract", response_model=FactRetractResponse)
    async def retract_fact(
        fact_id: str,
        body: Optional[FactRetractRequest] = None,
        _identity: str = Depends(require_service_auth),
    ):
        try:
            fid = UUID(fact_id)
        except (ValueError, AttributeError, TypeError):
            raise HTTPException(
                status_code=422,
                detail=f"fact_id is not a valid UUID: {fact_id!r}",
            )

        reason = body.reason if body else None

        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT id, valid_to, subject_uri, predicate, object_uri, fact_text
                FROM knowledge_facts
                WHERE id = $1
            """, fid)

            if row is None:
                raise HTTPException(status_code=404, detail=f"Fact not found: {fact_id}")

            # Idempotent: already expired → 200 no-op, report current valid_to.
            if row["valid_to"] is not None:
                logger.info(
                    "retract_fact no-op (already expired) fact=%s valid_to=%s by=%s",
                    fact_id, row["valid_to"], _identity,
                )
                return FactRetractResponse(
                    fact_id=fact_id, retracted=False, already_retracted=True,
                    valid_to=row["valid_to"].isoformat(),
                    subject_uri=row["subject_uri"], predicate=row["predicate"],
                    object_uri=row["object_uri"], reason=reason,
                )

            updated = await conn.fetchrow("""
                UPDATE knowledge_facts
                SET valid_to = NOW()
                WHERE id = $1 AND valid_to IS NULL
                RETURNING valid_to
            """, fid)

            # A concurrent retract could have set valid_to between SELECT and
            # UPDATE; treat the empty RETURNING as already-done (still 200).
            if updated is None:
                refetched = await conn.fetchval(
                    "SELECT valid_to FROM knowledge_facts WHERE id = $1", fid)
                return FactRetractResponse(
                    fact_id=fact_id, retracted=False, already_retracted=True,
                    valid_to=refetched.isoformat() if refetched else None,
                    subject_uri=row["subject_uri"], predicate=row["predicate"],
                    object_uri=row["object_uri"], reason=reason,
                )

            logger.info(
                "retract_fact fact=%s subject=%s predicate=%s object=%s "
                "valid_to=%s reason=%r by=%s text=%r",
                fact_id, row["subject_uri"], row["predicate"], row["object_uri"],
                updated["valid_to"], reason, _identity, row["fact_text"],
            )
            return FactRetractResponse(
                fact_id=fact_id, retracted=True, already_retracted=False,
                valid_to=updated["valid_to"].isoformat(),
                subject_uri=row["subject_uri"], predicate=row["predicate"],
                object_uri=row["object_uri"], reason=reason,
            )

    # -------------------------------------------------------------------
    # GET /discourse-search — lexical search over scientific discourse moves
    # (Piece A / G1). READ-ONLY. Reuses derive_source_url / _build_source_url_map.
    # -------------------------------------------------------------------
    @router.get("/discourse-search")
    async def discourse_search(
        scope: ReadScope = Depends(_discourse_scope),
        query: Optional[str] = Query(
            None,
            description="Lexical full-text query over move title+detail "
                        "(plainto_tsquery/english). Omit to browse most-recent moves.",
        ),
        move_type: Optional[List[str]] = Query(
            None,
            description="Filter by discourse move_type (claim/evidence/thesis/"
                        "counterpoint/premise/implication/definition/open_question). "
                        "Repeatable (?move_type=claim&move_type=evidence) and/or "
                        "comma-joined (?move_type=claim,evidence).",
        ),
        document_rid: Optional[str] = Query(
            None,
            description="Friendly alias for source_rid — a document:<sha> rid; "
                        "returns only that document's moves.",
        ),
        source_rid: Optional[str] = Query(
            None,
            description="Filter by source_rid (document:<sha>). HTTP 400 if it "
                        "differs from a supplied document_rid.",
        ),
        status: Optional[str] = Query(
            None,
            description="Filter by move status (e.g. asserted, contested, open, "
                        "deferred, supported, speculative).",
        ),
        source_type: str = Query(
            "document",
            description="Provenance class; defaults to 'document' (v1 scope).",
        ),
        limit: int = Query(20, ge=1, le=100),
    ):
        # document_rid is a friendly alias for source_rid (document moves' source_rid
        # IS the document:<sha>). Reconcile: equal → fine; differ → HTTP 400.
        eff_source_rid = source_rid
        if document_rid is not None:
            if source_rid is not None and source_rid != document_rid:
                raise HTTPException(
                    status_code=400,
                    detail="document_rid and source_rid conflict",
                )
            eff_source_rid = document_rid

        move_types = _normalize_move_types(move_type)

        # ``scope`` comes from the read-auth dependency: bound
        # to the validated credential, never to a request parameter.
        async with pool.acquire() as conn:
            return await _discourse_search(
                conn,
                query=query,
                move_types=move_types,
                source_rid=eff_source_rid,
                status=status,
                source_type=source_type,
                limit=limit,
                scope=scope,
            )

    # -------------------------------------------------------------------
    # Shared helper
    # -------------------------------------------------------------------
    async def _get_entity_name(conn, uri: Optional[str]) -> Optional[str]:
        if not uri:
            return None
        return await conn.fetchval(
            "SELECT entity_text FROM entity_registry WHERE fuseki_uri = $1",
            uri)

    return router
