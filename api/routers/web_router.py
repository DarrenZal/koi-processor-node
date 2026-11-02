"""Web curation endpoints (preview, evaluate, process, ingest, submissions, monitor).

Wraps the web_fetcher and llm_enricher modules to provide a REST API for
web content curation.  Only included when caps.web_sensor is True.
"""

import logging
import os
import time
from collections import defaultdict, deque
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from api.federation_events import doclink_row_created
from api.llm_enricher import LLM_BACKEND
from api import ontology_registry
from api.entity_schema import type_to_folder
from api.vault_parser import FIELD_TO_PREDICATE
from api.vault_note_utils import sanitize_filename, vault_slug, build_frontmatter, vault_note_path

logger = logging.getLogger(__name__)


# -- Request / Response models -----------------------------------------------

class WebPreviewRequest(BaseModel):
    url: str = Field(..., description="URL to fetch and preview")
    submitted_by: Optional[str] = None
    submitted_via: str = "api"


class WebPreviewResponse(BaseModel):
    url: str
    rid: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    published_date: Optional[str] = None
    word_count: int = 0
    content_hash: Optional[str] = None
    matching_entities: List[Dict[str, Any]] = []
    is_duplicate: bool = False
    error: Optional[str] = None


class WebEvaluateRequest(BaseModel):
    url: str = Field(..., description="URL of content to evaluate")
    content: Optional[str] = Field(None, description="Pre-fetched content (skips re-fetch)")
    criteria: Optional[str] = None


class WebEvaluateResponse(BaseModel):
    url: str
    relevance_score: float = Field(0.0, ge=0.0, le=1.0)
    summary: str = ""
    suggested_entities: List[Dict[str, Any]] = []
    rationale: str = ""


class WebProcessRequest(BaseModel):
    url: str
    hint_entities: List[str] = Field(default_factory=list)
    auto_ingest: bool = Field(False, description="If true, ingest after processing")


class WebProcessResponse(BaseModel):
    url: str
    status: str
    preview: Optional[WebPreviewResponse] = None
    evaluation: Optional[WebEvaluateResponse] = None
    entities: List[Dict[str, Any]] = []
    relationships: List[Dict[str, Any]] = []
    quality_stats: Optional[Dict[str, Any]] = None
    ingestion_stats: Optional[Dict[str, Any]] = None
    model_used: Optional[str] = None
    error: Optional[str] = None


class WebIngestEntity(BaseModel):
    name: str
    type: str
    context: Optional[str] = None
    confidence: Optional[float] = None
    description: Optional[str] = None


class WebIngestRelationship(BaseModel):
    subject: str
    predicate: str
    object: str


class WebIngestRequest(BaseModel):
    url: str
    entities: List[WebIngestEntity] = Field(default_factory=list)
    relationships: List[WebIngestRelationship] = Field(default_factory=list)


class WebIngestResponse(BaseModel):
    url: str
    status: str
    entities_resolved: int = 0
    entities_created: int = 0
    relationships_created: int = 0
    vault_notes_created: int = 0
    quality_stats: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class WebMonitorAddRequest(BaseModel):
    url: str
    title: str = ""


class WebMonitorRemoveRequest(BaseModel):
    url: str


# -- Agentic crawl (Phase 1: synchronous, inert by default) ------------------

class CrawlBudgetIn(BaseModel):
    max_pages: Optional[int] = None
    max_vision_calls: Optional[int] = None
    max_seconds: Optional[int] = None
    max_usd: Optional[float] = None


class CrawlAgenticRequest(BaseModel):
    url: str
    goal: Optional[str] = None
    budget: Optional[CrawlBudgetIn] = None
    # Explicitly not accepted: submitted_by (server-derived from auth token).


class CrawlAgenticEnqueuedResponse(BaseModel):
    job_id: int
    deduped: bool = False


class CrawlJobStatusResponse(BaseModel):
    job_id: int
    status: str
    start_url: str
    submitted_by: str
    progress: Dict[str, Any] = Field(default_factory=dict)
    cost_usd: float = 0.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[str] = None
    heartbeat_at: Optional[str] = None
    finished_at: Optional[str] = None


class EntityEditFields(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Optional[str] = None
    description: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ProposalOverrides(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dropped_entity_indices: List[int] = Field(default_factory=list)
    entity_edits: Dict[int, EntityEditFields] = Field(default_factory=dict)
    dropped_relationship_indices: List[int] = Field(default_factory=list)


class ExtraRelationshipIn(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_: Union[int, str] = Field(..., alias="from")
    predicate: str = "related_to"
    to: Union[int, str]


class CrawlCommitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_overrides: ProposalOverrides = Field(default_factory=ProposalOverrides)
    extra_relationships: List[ExtraRelationshipIn] = Field(default_factory=list)


class ParseRelateClauseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    instruction: str


class StoredProposalEntity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int
    name: str
    type: str
    description: Optional[str] = None
    source_url: Optional[str] = None
    source_image: Optional[str] = None
    confidence: float = 1.0
    requires_review: bool = False
    metadata: Dict[str, Any] = Field(default_factory=dict)
    existing_rid: Optional[str] = None


class StoredProposalRelationship(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subject_index: int
    predicate: str
    object_index: int


class StoredCrawlProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")

    proposal_version: str
    ontology_version: str
    start_url: str
    root_entity_index: int = 0
    entities: List[StoredProposalEntity] = Field(default_factory=list)
    relationships: List[StoredProposalRelationship] = Field(default_factory=list)
    recommended_next_crawls: List[str] = Field(default_factory=list)
    stats: Dict[str, Any] = Field(default_factory=dict)


PER_PARSE_RELATE_HOURLY_CAP = 60
PER_USER_CONCURRENT_CAP = 2
PER_USER_DAILY_CAP = 10
_PARSE_RELATE_CALLS: dict[str, deque[float]] = defaultdict(deque)


# -- Vault note creation for web ingest --------------------------------------

_VAULT_ROOT = os.path.expanduser(
    os.environ.get('VAULT_PATH') or os.environ.get('OBSIDIAN_VAULT_PATH', '')
)

# Direction-aware reverse mapping: (predicate, role) -> [(field, type_hint), ...]
_PREDICATE_ROLE_TO_FIELDS: Dict[Tuple[str, str], List[Tuple[str, Optional[str]]]] = {}
for _field, (_pred, _direction, _hint) in FIELD_TO_PREDICATE.items():
    _role = 'subject' if _direction == 'outgoing' else 'object'
    _PREDICATE_ROLE_TO_FIELDS.setdefault((_pred, _role), []).append((_field, _hint))


def _find_field_for_relationship(
    predicate: str, is_subject: bool, target_type: Optional[str] = None
) -> Optional[str]:
    """Find frontmatter field via strict (predicate, role) mapping only."""
    role = 'subject' if is_subject else 'object'
    candidates = _PREDICATE_ROLE_TO_FIELDS.get((predicate, role), [])
    for field, hint in candidates:
        if hint is None or target_type is None or hint == target_type:
            return field
    return None


def write_vault_note(
    entity_name: str,
    entity_type: str,
    entity_uri: str,
    source_url: str,
    context: Optional[str],
    description: Optional[str],
    relationships: List[WebIngestRelationship],
    all_entities: List[dict],
) -> Optional[Tuple[str, str, bool]]:
    """Write a vault .md note for a newly-ingested entity.

    Returns (vault_rel, vault_rid, note_created) or None if vault not configured.
    """
    if not _VAULT_ROOT or not os.path.isdir(_VAULT_ROOT):
        return None

    folder = type_to_folder(entity_type)
    safe_name = sanitize_filename(entity_name)
    if safe_name is None:
        logger.warning(f"Entity name sanitizes to empty, skipping vault note: {entity_name!r}")
        return None

    note_path = vault_note_path(_VAULT_ROOT, folder, safe_name)
    if note_path is None:
        logger.warning(f"Path traversal rejected for entity: {entity_name}")
        return None

    vault_rel = os.path.relpath(note_path, _VAULT_ROOT)
    vault_rid = f"{folder.lower()}/{vault_slug(entity_name)}"

    if os.path.exists(note_path):
        return vault_rel, vault_rid, False

    # Build entity name lookup for wikilink generation
    entity_name_to_info: Dict[str, dict] = {}
    for ent in all_entities:
        ent_name = ent.get("name", "")
        ent_type = ent.get("type", "Concept")
        ent_folder = type_to_folder(ent_type)
        entity_name_to_info[ent_name.lower()] = {
            "name": ent_name, "type": ent_type, "folder": ent_folder
        }

    # Build relationship wikilinks grouped by frontmatter field
    rel_fields: Dict[str, List[str]] = {}
    for rel in relationships:
        is_subject = rel.subject.lower() == entity_name.lower()
        is_object = rel.object.lower() == entity_name.lower()
        if not is_subject and not is_object:
            continue

        target_name = rel.object if is_subject else rel.subject
        target_info = entity_name_to_info.get(target_name.lower())
        target_type = target_info["type"] if target_info else None
        target_folder = target_info["folder"] if target_info else None

        field = _find_field_for_relationship(rel.predicate, is_subject, target_type)
        if not field or not target_folder:
            continue

        target_display = target_info["name"] if target_info else target_name
        wikilink = f"[[{target_folder}/{target_display}]]"
        rel_fields.setdefault(field, [])
        if wikilink not in rel_fields[field]:
            rel_fields[field].append(wikilink)

    # Build frontmatter
    schema_type = f"bkc:{entity_type}"
    fm: Dict[str, Any] = {"@type": schema_type, "name": entity_name}

    body_text = description or context or ""
    if body_text:
        fm["description"] = body_text

    fm["url"] = source_url
    fm["uri"] = entity_uri
    fm["source"] = "web_ingest"
    fm["dateAccessed"] = str(date.today())

    # Add relationship fields
    for field, links in sorted(rel_fields.items()):
        fm[field] = links

    # Build note content
    content = build_frontmatter(fm)
    content += f"\n# {entity_name}\n"
    if body_text:
        content += f"\n{body_text}\n"
    content += f"\n## Source\n\n"
    # Extract domain for display
    from urllib.parse import urlparse
    parsed = urlparse(source_url)
    domain = parsed.netloc or source_url
    content += f"- Ingested from: [{domain}]({source_url})\n"

    os.makedirs(os.path.dirname(note_path), exist_ok=True)
    with open(note_path, 'w', encoding='utf-8') as f:
        f.write(content)

    logger.info(f"Created vault note: {vault_rel}")
    return vault_rel, vault_rid, True


# -- Router factory ----------------------------------------------------------

_web_sensor_instances: Dict[int, Any] = {}  # keyed by pool id to allow lazy init


def _parse_json_maybe(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (dict, list)):
        return raw
    try:
        import json
        return json.loads(raw)
    except (ValueError, TypeError):
        return None


def _load_stored_proposal(raw: Any) -> StoredCrawlProposal:
    data = _parse_json_maybe(raw)
    if not isinstance(data, dict):
        raise HTTPException(status_code=409, detail={"error": "crawl job has no stored proposal"})
    try:
        proposal = StoredCrawlProposal(**data)
    except Exception as exc:
        raise HTTPException(status_code=422, detail={"error": f"stored proposal invalid: {exc}"})
    if proposal.proposal_version != "v1":
        raise HTTPException(
            status_code=422,
            detail={"error": f"unknown proposal version '{proposal.proposal_version}'"},
        )
    if proposal.root_entity_index < 0 or proposal.root_entity_index >= len(proposal.entities):
        raise HTTPException(status_code=422, detail={"error": "stored proposal has invalid root_entity_index"})
    for entity in proposal.entities:
        if entity.type not in ontology_registry.ALLOWED_ENTITY_TYPES:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": f"Unknown entity type '{entity.type}'. Valid types: {sorted(ontology_registry.ALLOWED_ENTITY_TYPES)}"
                },
            )
    for rel in proposal.relationships:
        if rel.predicate not in ontology_registry.ALLOWED_PREDICATES:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": f"Unknown predicate '{rel.predicate}'. Valid predicates: {sorted(ontology_registry.ALLOWED_PREDICATES)}"
                },
            )
    return proposal


def _copy_entity_with_edits(entity: StoredProposalEntity, edits: Optional[EntityEditFields]) -> StoredProposalEntity:
    if edits is None:
        return entity.model_copy(deep=True)
    data = entity.model_dump()
    if edits.name is not None:
        data["name"] = edits.name
    if edits.description is not None:
        data["description"] = edits.description
    if edits.metadata is not None:
        data["metadata"] = edits.metadata
    return StoredProposalEntity(**data)


async def _resolve_extra_label_candidates(
    conn,
    label: str,
    type_hint: Optional[str] = None,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    from api.personal_ingest_api import normalize_alias, normalize_entity_text, quartz_url

    normalized = normalize_entity_text(label)
    normalized_alias = normalize_alias(label)
    if type_hint:
        rows = await conn.fetch(
            """
            SELECT fuseki_uri, entity_text, entity_type, aliases, created_at,
                   CASE WHEN normalized_text = $1 THEN 1.0
                        WHEN $2 = ANY(aliases) THEN 0.98
                        WHEN normalized_text ILIKE $3 THEN 0.90
                        ELSE 0.70 END AS similarity
            FROM entity_registry
            WHERE entity_type = $4
              AND NOT COALESCE(node_private, false)
              AND (
                    normalized_text = $1 OR
                    $2 = ANY(aliases) OR
                    normalized_text ILIKE $3
              )
            ORDER BY similarity DESC, created_at DESC NULLS LAST
            LIMIT $5
            """,
            normalized,
            normalized_alias,
            f"%{normalized}%",
            type_hint,
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT fuseki_uri, entity_text, entity_type, aliases, created_at,
                   CASE WHEN normalized_text = $1 THEN 1.0
                        WHEN $2 = ANY(aliases) THEN 0.98
                        WHEN normalized_text ILIKE $3 THEN 0.90
                        ELSE 0.70 END AS similarity
            FROM entity_registry
            WHERE NOT COALESCE(node_private, false)
              AND (
                    normalized_text = $1 OR
                    $2 = ANY(aliases) OR
                    normalized_text ILIKE $3
              )
            ORDER BY similarity DESC, created_at DESC NULLS LAST
            LIMIT $4
            """,
            normalized,
            normalized_alias,
            f"%{normalized}%",
            limit,
        )
    return [
        {
            "uri": row["fuseki_uri"],
            "name": row["entity_text"],
            "type": row["entity_type"],
            "confidence": float(row["similarity"] or 0.0),
            "quartz_url": quartz_url(row["entity_type"], row["entity_text"]),
        }
        for row in rows
    ]


async def _resolve_extra_endpoint_ref(
    conn,
    ref: Union[int, str],
    proposal: StoredCrawlProposal,
    committed_index_to_rid: Dict[int, str],
    dropped_indices: set[int],
    errored_indices: set[int],
) -> tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    if isinstance(ref, int):
        if ref < 0 or ref >= len(proposal.entities):
            raise HTTPException(status_code=422, detail={"error": f"entity index {ref} out of range"})
        if ref in dropped_indices:
            return None, None, "entity dropped in this attempt"
        if ref in errored_indices:
            return None, None, "entity failed earlier in this attempt"
        rid = committed_index_to_rid.get(ref)
        if rid:
            return rid, None, None
        return None, None, "entity not committed in this attempt"

    ref_str = ref.strip()
    if ref_str.startswith("orn:"):
        return ref_str, None, None
    candidates = await _resolve_extra_label_candidates(conn, ref_str)
    if len(candidates) == 1 and candidates[0]["confidence"] >= 0.9:
        return candidates[0]["uri"], None, None
    return None, {"label": ref_str, "candidates": candidates}, None


def _check_parse_relate_rate_limit(identity: str) -> None:
    now = time.time()
    window = _PARSE_RELATE_CALLS[identity]
    cutoff = now - 3600
    while window and window[0] <= cutoff:
        window.popleft()
    if len(window) >= PER_PARSE_RELATE_HOURLY_CAP:
        raise HTTPException(
            status_code=429,
            detail={"error": f"parse_relate rate limit ({PER_PARSE_RELATE_HOURLY_CAP}/hour) exceeded"},
        )
    window.append(now)


def _count_inserted(status: str) -> int:
    """Parse the affected-row count from an asyncpg command tag.

    asyncpg's ``conn.execute`` returns e.g. ``'INSERT 0 1'`` / ``'INSERT 0 0'``
    (oid, rows). The row count is the last token — 0 when ON CONFLICT DO NOTHING
    suppressed the insert. Mirrors the ``_count()`` idiom in admin_router.py.
    """
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


def _rel_attr(rel: Any, name: str) -> Optional[str]:
    """Read subject/predicate/object from either a dict or a pydantic model.

    /web/ingest passes WebIngestRelationship models; /web/process passes plain
    dicts extracted from the LLM. Both flow through _store_relationships.
    """
    if isinstance(rel, dict):
        return rel.get(name)
    return getattr(rel, name, None)


async def _store_relationships(conn, relationships, source: str,
                              doc_rid: Optional[str] = None) -> int:
    """Insert entity relationships, returning the count actually inserted.

    Matches subject/object against ``entity_registry.normalized_text`` using
    ``normalize_entity_text()`` (hyphen/underscore-aware) rather than a raw
    ``lower(trim())`` — the latter missed hyphenated/underscored names because
    ``normalized_text`` stores them with those separators collapsed to spaces.

    Counts only rows that were really inserted (parses the asyncpg command tag,
    so ON CONFLICT DO NOTHING no longer inflates the total) and skips self-loops
    (subject == object after normalization would violate the table's
    ``CHECK (subject_uri != object_uri)`` constraint anyway).
    """
    from api.personal_ingest_api import normalize_entity_text

    created = 0
    for rel in (relationships or []):
        subject = _rel_attr(rel, "subject")
        obj = _rel_attr(rel, "object")
        predicate = _rel_attr(rel, "predicate")
        if not subject or not obj or not predicate:
            continue
        subj_norm = normalize_entity_text(subject)
        obj_norm = normalize_entity_text(obj)
        if not subj_norm or not obj_norm or subj_norm == obj_norm:
            continue  # skip empties and self-loops
        try:
            status = await conn.execute(
                """
                INSERT INTO entity_relationships (subject_uri, predicate, object_uri, source)
                SELECT s.fuseki_uri, $3, o.fuseki_uri, $4
                FROM entity_registry s, entity_registry o
                WHERE s.normalized_text = $1
                  AND o.normalized_text = $2
                ON CONFLICT DO NOTHING
                """,
                subj_norm, obj_norm, predicate, source,
            )
            created += _count_inserted(status)
        except Exception as e:
            logger.warning(f"Failed to create relationship {rel} (doc_rid={doc_rid}): {e}")
    return created


def create_router(pool, caps):
    """Return an APIRouter for web sensor endpoints."""
    from api import crawl_auth

    crawl_auth.reload_identity_config()
    api_router = APIRouter()
    router = APIRouter(prefix="/web", tags=["web"])
    tools_router = APIRouter(prefix="/tools", tags=["tools"])

    @router.post("/preview", response_model=WebPreviewResponse)
    async def web_preview(body: WebPreviewRequest):
        """Fetch a URL and return metadata plus entity matches."""
        t0 = time.monotonic()
        from api.web_fetcher import fetch_and_preview

        try:
            preview = await fetch_and_preview(body.url, db_pool=pool)
        except Exception as e:
            logger.error(f"Preview failed for {body.url}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        if preview.fetch_error:
            return WebPreviewResponse(
                url=body.url, error=preview.fetch_error
            )

        # Check for duplicate in web_submissions
        is_duplicate = False
        async with pool.acquire() as conn:
            existing = await conn.fetchrow(
                "SELECT id, status FROM web_submissions WHERE url = $1 LIMIT 1",
                body.url,
            )
            if existing:
                is_duplicate = True

            # Record the submission
            await conn.execute("""
                INSERT INTO web_submissions (url, rid, domain, submitted_by, submitted_via,
                    status, title, description, content_hash, word_count,
                    matching_entities, fetched_at, content_text)
                VALUES ($1, $2, $3, $4, $5, 'previewed', $6, $7, $8, $9, $10::jsonb, NOW(), $11)
                ON CONFLICT (url) DO UPDATE SET
                    status = CASE WHEN web_submissions.status = 'pending' THEN 'previewed' ELSE web_submissions.status END,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    content_hash = EXCLUDED.content_hash,
                    word_count = EXCLUDED.word_count,
                    matching_entities = EXCLUDED.matching_entities,
                    fetched_at = NOW(),
                    content_text = EXCLUDED.content_text
            """,
                body.url,
                preview.rid,
                preview.domain,
                body.submitted_by,
                body.submitted_via,
                preview.title,
                preview.description,
                preview.content_hash,
                preview.word_count,
                _serialize_matching_entities(preview.matching_entities),
                preview.content_text[:50000] if preview.content_text else None,
            )

        # Create CAT receipt for web fetch
        async with pool.acquire() as conn:
            try:
                from api.cat_receipts import create_receipt
                await create_receipt(
                    conn,
                    transformation_type="web_fetch",
                    input_rid=body.url,
                    output_rid=preview.rid or body.url,
                    processor_name="web_fetcher",
                    source_sensor=body.submitted_via or "api",
                    content_hash=preview.content_hash,
                    metadata={"title": preview.title, "word_count": preview.word_count},
                )
            except Exception as e:
                logger.warning(f"CAT receipt creation failed (non-fatal): {e}")

        elapsed = (time.monotonic() - t0) * 1000
        logger.info(f"web.preview url={body.url} status=ok elapsed_ms={elapsed:.0f}")

        return WebPreviewResponse(
            url=body.url,
            rid=preview.rid,
            title=preview.title,
            description=preview.description,
            author=preview.metadata.author if preview.metadata else None,
            published_date=preview.metadata.published_date if preview.metadata else None,
            word_count=preview.word_count,
            content_hash=preview.content_hash,
            matching_entities=[_entity_to_dict(e) for e in (preview.matching_entities or [])],
            is_duplicate=is_duplicate,
        )

    @router.post("/evaluate", response_model=WebEvaluateResponse)
    async def web_evaluate(body: WebEvaluateRequest):
        """Evaluate content relevance using LLM enrichment."""
        if not caps.llm_enrichment:
            raise HTTPException(status_code=501, detail="LLM enrichment not enabled")

        from api.llm_enricher import extract_from_content
        from api.web_fetcher import fetch_and_preview

        # Get content — either from request or by fetching
        content = body.content
        title = ""
        if not content:
            preview = await fetch_and_preview(body.url, db_pool=pool)
            if preview.fetch_error:
                raise HTTPException(status_code=502, detail=f"Fetch failed: {preview.fetch_error}")
            content = preview.content_text
            title = preview.title or ""

        # Get existing entities for matching context
        existing_entities = []
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT entity_text AS name, entity_type AS type FROM entity_registry WHERE NOT node_private LIMIT 200"
            )
            existing_entities = [dict(r) for r in rows]

        result = await extract_from_content(content, title, body.url, existing_entities)

        # Compute relevance from extraction quality
        entity_count = len(result.entities)
        has_summary = bool(result.summary)
        relevance = min(1.0, (entity_count * 0.15) + (0.3 if has_summary else 0.0))

        suggested = [
            {"name": e.name, "type": e.type, "confidence": e.confidence}
            for e in result.entities
        ]

        # Update web_submissions with evaluation
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE web_submissions SET
                    status = 'evaluated',
                    relevance_score = $2,
                    relevance_reasoning = $3,
                    evaluated_at = NOW()
                WHERE url = $1
            """, body.url, relevance, result.summary)

        return WebEvaluateResponse(
            url=body.url,
            relevance_score=relevance,
            summary=result.summary,
            suggested_entities=suggested,
            rationale=f"Found {entity_count} entities, {len(result.relationships)} relationships",
        )

    @router.post("/process", response_model=WebProcessResponse)
    async def web_process(body: WebProcessRequest):
        """Full pipeline: fetch, preview, extract entities, optionally ingest."""
        t0 = time.monotonic()
        from api.web_fetcher import fetch_and_preview
        from api.llm_enricher import extract_from_content
        from api.quality_gates import filter_entities, get_accepted_entities

        # Step 1: Preview
        try:
            preview = await fetch_and_preview(body.url, db_pool=pool)
        except Exception as e:
            return WebProcessResponse(url=body.url, status="error", error=str(e))

        if preview.fetch_error:
            return WebProcessResponse(url=body.url, status="error", error=preview.fetch_error)

        preview_resp = WebPreviewResponse(
            url=body.url,
            rid=preview.rid,
            title=preview.title,
            description=preview.description,
            word_count=preview.word_count,
            content_hash=preview.content_hash,
            matching_entities=[_entity_to_dict(e) for e in (preview.matching_entities or [])],
        )

        # Record submission
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO web_submissions (url, rid, domain, status, title, description,
                    content_hash, word_count, matching_entities, fetched_at, content_text)
                VALUES ($1, $2, $3, 'previewed', $4, $5, $6, $7, $8::jsonb, NOW(), $9)
                ON CONFLICT (url) DO UPDATE SET
                    status = 'previewed',
                    title = EXCLUDED.title,
                    content_hash = EXCLUDED.content_hash,
                    word_count = EXCLUDED.word_count,
                    matching_entities = EXCLUDED.matching_entities,
                    fetched_at = NOW(),
                    content_text = EXCLUDED.content_text
            """,
                body.url, preview.rid, preview.domain,
                preview.title, preview.description,
                preview.content_hash, preview.word_count,
                _serialize_matching_entities(preview.matching_entities),
                preview.content_text[:50000] if preview.content_text else None,
            )

        # Step 2: LLM extraction
        eval_resp = None
        entities_raw = []
        relationships_raw = []
        model_used = None

        if caps.llm_enrichment and preview.content_text:
            existing_entities = []
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT entity_text AS name, entity_type AS type FROM entity_registry WHERE NOT node_private LIMIT 200"
                )
                existing_entities = [dict(r) for r in rows]

            extraction = await extract_from_content(
                preview.content_text, preview.title or "", body.url, existing_entities
            )
            model_used = extraction.model_used

            entities_raw = [
                {"name": e.name, "type": e.type, "confidence": e.confidence, "context": getattr(e, "context", "")}
                for e in extraction.entities
            ]
            relationships_raw = [
                {"subject": r.subject, "predicate": r.predicate, "object": r.object}
                for r in extraction.relationships
            ]

            relevance = min(1.0, len(extraction.entities) * 0.15 + (0.3 if extraction.summary else 0.0))
            eval_resp = WebEvaluateResponse(
                url=body.url,
                relevance_score=relevance,
                summary=extraction.summary,
                suggested_entities=[
                    {"name": e.name, "type": e.type, "confidence": e.confidence}
                    for e in extraction.entities
                ],
                rationale=f"Extracted {len(extraction.entities)} entities, {len(extraction.relationships)} relationships",
            )

            # Update submission status
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE web_submissions SET status = 'evaluated', relevance_score = $2,
                        relevance_reasoning = $3, evaluated_at = NOW()
                    WHERE url = $1
                """, body.url, relevance, extraction.summary)

                # CAT receipt for LLM extraction
                try:
                    from api.cat_receipts import create_receipt
                    # Find parent (web_fetch) receipt
                    parent = await conn.fetchrow(
                        "SELECT receipt_id FROM koi_transformation_receipts WHERE input_rid = $1 AND transformation_type = 'web_fetch' ORDER BY created_at DESC LIMIT 1",
                        body.url,
                    )
                    await create_receipt(
                        conn,
                        transformation_type="llm_extraction",
                        input_rid=preview.rid or body.url,
                        output_rid=f"{preview.rid or body.url}:extraction",
                        processor_name=extraction.model_used or LLM_BACKEND,
                        source_sensor="api",
                        parent_receipt_id=parent["receipt_id"] if parent else None,
                        metadata={
                            "entities_extracted": len(extraction.entities),
                            "relationships_extracted": len(extraction.relationships),
                            "relevance_score": relevance,
                        },
                    )
                except Exception as e:
                    logger.warning(f"CAT receipt creation failed (non-fatal): {e}")

        # Step 3: Quality gates on extracted entities
        quality_stats = None
        if entities_raw:
            async with pool.acquire() as conn:
                source_relevance = eval_resp.relevance_score if eval_resp else None
                report = await filter_entities(entities_raw, conn=conn, source_relevance_score=source_relevance)
                entities_raw = get_accepted_entities(report)
                quality_stats = {
                    "total_input": report.total_input,
                    "accepted": report.accepted,
                    "rejected": report.rejected,
                    "rejected_by_stage": report.rejected_by_stage,
                }

        # Step 4: Auto-ingest if requested
        entities_created = 0
        entities_resolved = 0
        new_relationships = 0
        if body.auto_ingest and entities_raw:
            async with pool.acquire() as conn:
                from api.personal_ingest_api import resolve_entity, store_new_entity, ExtractedEntity
                for ent in entities_raw:
                    extracted = ExtractedEntity(
                        name=ent["name"],
                        type=ent.get("type", "Concept"),
                        confidence=ent.get("confidence") if ent.get("confidence") is not None else 0.9,
                    )
                    canonical, is_new = await resolve_entity(
                        conn,
                        extracted,
                        resolution_caller="web_router.process",
                    )
                    entities_resolved += 1
                    if is_new:
                        await store_new_entity(conn, extracted, canonical, preview.rid or body.url, source="web_process")
                        entities_created += 1

                # Store extracted relationships now that both endpoints' entities
                # exist in entity_registry for the normalized-text match to hit.
                new_relationships = await _store_relationships(
                    conn, relationships_raw, source="web_process",
                    doc_rid=(preview.rid or body.url),
                )

                await conn.execute("""
                    UPDATE web_submissions SET status = 'ingested', ingested_at = NOW()
                    WHERE url = $1
                """, body.url)

        elapsed = (time.monotonic() - t0) * 1000
        logger.info(f"web.process url={body.url} entities={len(entities_raw)} elapsed_ms={elapsed:.0f}")

        return WebProcessResponse(
            url=body.url,
            status="processed",
            preview=preview_resp,
            evaluation=eval_resp,
            entities=entities_raw,
            relationships=relationships_raw,
            quality_stats=quality_stats,
            ingestion_stats={
                "new_entities": entities_created,
                "resolved_entities": entities_resolved,
                "new_relationships": new_relationships,
            } if body.auto_ingest else None,
            model_used=model_used,
        )

    @router.post("/ingest", response_model=WebIngestResponse)
    async def web_ingest(body: WebIngestRequest):
        """Ingest entities from a previously-processed URL into the knowledge graph."""
        t0 = time.monotonic()
        from api.personal_ingest_api import resolve_entity, store_new_entity, ExtractedEntity
        from api.quality_gates import filter_entities, get_accepted_entities

        # Convert request entities to dicts for quality gates
        entities_raw = [
            {"name": e.name, "type": e.type, "context": e.context,
             "confidence": e.confidence, "description": e.description}
            for e in body.entities
        ]

        entities_created = 0
        entities_resolved = 0
        relationships_created = 0
        quality_stats = None
        # (document_rid, entity_uri, context) for doclink federation emits.
        # Group B site (ON CONFLICT DO NOTHING) — only rows actually inserted
        # are recorded; emitted AFTER the connection block commits (2e rule).
        doclink_emits = []

        async with pool.acquire() as conn:
            # Quality gates on ingest path (skip confidence for agent-curated entities)
            if entities_raw:
                report = await filter_entities(entities_raw, conn=conn, skip_confidence=True)
                entities_raw = get_accepted_entities(report)
                quality_stats = {
                    "total_input": report.total_input,
                    "accepted": report.accepted,
                    "rejected": report.rejected,
                    "rejected_by_stage": report.rejected_by_stage,
                }

            # Look up submission RID once (used for linking)
            submission = await conn.fetchrow(
                "SELECT rid FROM web_submissions WHERE url = $1 LIMIT 1",
                body.url,
            )

            # Resolve entities
            new_entities_batch = []
            for ent in entities_raw:
                extracted = ExtractedEntity(
                    name=ent["name"],
                    type=ent.get("type", "Concept"),
                    confidence=ent.get("confidence") if ent.get("confidence") is not None else 0.9,
                )
                canonical, is_new = await resolve_entity(
                    conn,
                    extracted,
                    resolution_caller="web_router.ingest",
                )
                entities_resolved += 1
                if is_new:
                    doc_rid = submission["rid"] if submission else body.url
                    await store_new_entity(conn, extracted, canonical, doc_rid, source="web_ingest")
                    entities_created += 1
                    new_entities_batch.append({
                        "name": canonical.name,
                        "type": extracted.type or "Concept",
                        "uri": canonical.uri,
                        "context": ent.get("context"),
                        "description": ent.get("description"),
                    })

                # Link document to entity
                if submission and canonical.uri:
                    _dl_ctx = ent.get("context", "web_ingest")
                    _dl_status = await conn.execute("""
                        INSERT INTO document_entity_links (document_rid, entity_uri, context)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (document_rid, entity_uri) DO NOTHING
                    """, submission["rid"], canonical.uri, _dl_ctx)
                    if doclink_row_created(_dl_status):
                        doclink_emits.append(
                            (submission["rid"], canonical.uri, _dl_ctx)
                        )

            # Create relationships (normalized matching; real-insert count; self-loops skipped)
            relationships_created = await _store_relationships(
                conn, body.relationships, source="web_ingest",
                doc_rid=(submission["rid"] if submission else body.url),
            )

            # Create vault notes + RID mappings for new entities
            vault_notes_created = 0
            for new_ent in new_entities_batch:
                result = None
                try:
                    result = write_vault_note(
                        entity_name=new_ent["name"],
                        entity_type=new_ent["type"],
                        entity_uri=new_ent["uri"],
                        source_url=body.url,
                        context=new_ent.get("context"),
                        description=new_ent.get("description"),
                        relationships=body.relationships,
                        all_entities=entities_raw,
                    )
                    if result and result[2]:
                        vault_notes_created += 1
                except Exception as e:
                    logger.warning(f"Vault note creation failed for {new_ent['name']} (non-fatal): {e}")

                if result:
                    vault_rel, vault_rid, _created = result
                    tag = await conn.execute("""
                        INSERT INTO entity_rid_mappings (
                            vault_rid, vault_path, canonical_uri, entity_type,
                            name, sync_status, last_synced
                        ) VALUES ($1, $2, $3, $4, $5, 'linked', NOW())
                        ON CONFLICT (vault_rid) DO UPDATE SET
                            vault_path = EXCLUDED.vault_path,
                            canonical_uri = EXCLUDED.canonical_uri,
                            entity_type = EXCLUDED.entity_type,
                            name = EXCLUDED.name,
                            sync_status = 'linked',
                            last_synced = NOW()
                        WHERE entity_rid_mappings.canonical_uri = EXCLUDED.canonical_uri
                    """, vault_rid, vault_rel, new_ent["uri"],
                        new_ent["type"], new_ent["name"])
                    if tag.endswith(" 0"):
                        logger.warning(
                            f"vault_rid collision: {vault_rid} already maps to a different "
                            f"canonical_uri, skipping remap to {new_ent['uri']}"
                        )

            # Update submission status
            await conn.execute("""
                UPDATE web_submissions SET
                    status = 'ingested',
                    ingested_entities = $2::jsonb,
                    ingested_at = NOW()
                WHERE url = $1
            """, body.url, _json_dumps([
                {"name": e["name"], "type": e.get("type")} for e in entities_raw
            ]))

        # Emit doclink federation events post-commit (after the connection
        # block above exits). Group B site → already filtered to inserted rows.
        from api.federation_events import emit_doclink_event
        for document_rid, entity_uri, ctx in doclink_emits:
            await emit_doclink_event(document_rid, entity_uri, 1, context=ctx)

        # CAT receipt for entity resolution
        async with pool.acquire() as conn:
            try:
                from api.cat_receipts import create_receipt
                parent = await conn.fetchrow(
                    "SELECT receipt_id FROM koi_transformation_receipts WHERE input_rid = $1 ORDER BY created_at DESC LIMIT 1",
                    body.url,
                )
                submission = await conn.fetchrow(
                    "SELECT rid FROM web_submissions WHERE url = $1 LIMIT 1", body.url,
                )
                await create_receipt(
                    conn,
                    transformation_type="entity_resolution",
                    input_rid=submission["rid"] if submission else body.url,
                    output_rid=f"{body.url}:ingested",
                    processor_name="koi_entity_resolver",
                    source_sensor="api",
                    parent_receipt_id=parent["receipt_id"] if parent else None,
                    metadata={
                        "entities_resolved": entities_resolved,
                        "entities_created": entities_created,
                        "relationships_created": relationships_created,
                    },
                )
            except Exception as e:
                logger.warning(f"CAT receipt creation failed (non-fatal): {e}")

        elapsed = (time.monotonic() - t0) * 1000
        logger.info(f"web.ingest url={body.url} resolved={entities_resolved} created={entities_created} elapsed_ms={elapsed:.0f}")

        return WebIngestResponse(
            url=body.url,
            status="ingested",
            entities_resolved=entities_resolved,
            entities_created=entities_created,
            relationships_created=relationships_created,
            vault_notes_created=vault_notes_created,
            quality_stats=quality_stats,
        )

    @router.get("/submissions")
    async def web_submissions(
        status: Optional[str] = Query(None),
        limit: int = Query(50, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ):
        """List web content submissions and their processing status."""
        async with pool.acquire() as conn:
            if status:
                rows = await conn.fetch("""
                    SELECT url, rid, status, title, relevance_score, word_count,
                           submitted_by, submitted_via, created_at, fetched_at, evaluated_at, ingested_at
                    FROM web_submissions WHERE status = $1
                    ORDER BY created_at DESC LIMIT $2 OFFSET $3
                """, status, limit, offset)
            else:
                rows = await conn.fetch("""
                    SELECT url, rid, status, title, relevance_score, word_count,
                           submitted_by, submitted_via, created_at, fetched_at, evaluated_at, ingested_at
                    FROM web_submissions
                    ORDER BY created_at DESC LIMIT $1 OFFSET $2
                """, limit, offset)

        return [dict(r) for r in rows]

    @router.get("/monitor")
    async def web_monitor_status():
        """Return web sensor health and activity metrics."""
        pool_key = id(pool)
        if pool_key in _web_sensor_instances:
            status = await _web_sensor_instances[pool_key].get_status()
            return status
        return {
            "enabled": caps.web_sensor,
            "urls_monitored": 0,
            "urls_processed": 0,
            "last_scan_at": None,
        }

    @router.post("/monitor/add")
    async def web_monitor_add(body: WebMonitorAddRequest):
        """Add a URL to the monitoring list."""
        from api.web_sensor import WebSensor

        pool_key = id(pool)
        if pool_key not in _web_sensor_instances:
            _web_sensor_instances[pool_key] = WebSensor(pool)

        result = await _web_sensor_instances[pool_key].add_url(body.url, body.title)

        # CAT receipt for sensor registration
        async with pool.acquire() as conn:
            try:
                from api.cat_receipts import create_receipt
                from api.web_fetcher import generate_web_rid
                url_rid = generate_web_rid(body.url)
                await create_receipt(
                    conn,
                    transformation_type="sensor_registration",
                    input_rid=body.url,
                    output_rid=f"{url_rid}:monitor",
                    processor_name="web_sensor",
                    source_sensor="api",
                    metadata={"title": body.title, "action": "add"},
                )
            except Exception as e:
                logger.warning(f"CAT receipt creation failed (non-fatal): {e}")

        return result

    @router.post("/monitor/remove")
    async def web_monitor_remove(body: WebMonitorRemoveRequest):
        """Remove a URL from the monitoring list."""
        pool_key = id(pool)
        if pool_key not in _web_sensor_instances:
            raise HTTPException(status_code=404, detail="No web sensor active")

        result = await _web_sensor_instances[pool_key].remove_url(body.url)
        return result

    @router.get("/monitor/status")
    async def web_monitor_detailed_status():
        """Detailed monitoring status (alias for /monitor)."""
        return await web_monitor_status()

    @router.get("/health")
    async def web_health():
        """Web pipeline health: submission counts, error rates, monitoring stats."""
        async with pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '24 hours') AS submissions_24h,
                    COUNT(*) FILTER (WHERE status = 'error' AND created_at > NOW() - INTERVAL '24 hours') AS errors_24h,
                    COUNT(*) FILTER (WHERE status = 'monitoring') AS monitored,
                    COUNT(*) FILTER (WHERE status = 'previewed' AND evaluated_at IS NULL) AS pending_eval
                FROM web_submissions
            """)
        return {
            "status": "ok" if (stats["errors_24h"] or 0) < 5 else "degraded",
            "submissions_24h": stats["submissions_24h"],
            "errors_24h": stats["errors_24h"],
            "monitored_urls": stats["monitored"],
            "pending_evaluations": stats["pending_eval"],
        }

    @router.get("/provenance/{url:path}")
    async def web_provenance(url: str):
        """Get full CAT receipt chain for a URL."""
        from api.cat_receipts import get_receipts_for_url

        async with pool.acquire() as conn:
            # Collect all identifiers for this URL
            search_rids = {url}

            submission = await conn.fetchrow(
                "SELECT rid FROM web_submissions WHERE url = $1 LIMIT 1", url,
            )
            if submission:
                search_rids.add(submission["rid"])

            # Also look for derived RIDs (e.g., url:extraction, url:ingested)
            all_receipts = {}
            for rid in search_rids:
                for receipt in await get_receipts_for_url(conn, rid):
                    all_receipts[receipt.receipt_id] = receipt
                # Also search for derived output RIDs
                derived_rows = await conn.fetch(
                    "SELECT DISTINCT receipt_id, transformation_type, input_rid, output_rid, "
                    "parent_receipt_id, processor_name, source_sensor, metadata, content_hash, created_at "
                    "FROM koi_transformation_receipts WHERE input_rid LIKE $1 OR output_rid LIKE $1",
                    f"{rid}%",
                )
                for row in derived_rows:
                    import json as _json
                    r_id = row["receipt_id"]
                    if r_id not in all_receipts:
                        from api.cat_receipts import CATReceipt
                        all_receipts[r_id] = CATReceipt(
                            receipt_id=r_id,
                            transformation_type=row["transformation_type"],
                            input_rid=row["input_rid"],
                            output_rid=row["output_rid"],
                            parent_receipt_id=row["parent_receipt_id"],
                            processor_name=row["processor_name"],
                            source_sensor=row["source_sensor"],
                            metadata=_json.loads(row["metadata"]) if isinstance(row["metadata"], str) else (row["metadata"] or {}),
                            content_hash=row["content_hash"],
                            created_at=row["created_at"],
                        )

            receipts = sorted(all_receipts.values(), key=lambda r: r.created_at or datetime.min)

        if not receipts:
            raise HTTPException(status_code=404, detail=f"No provenance records for {url}")

        return {
            "url": url,
            "receipt_count": len(receipts),
            "receipts": [
                {
                    "receipt_id": r.receipt_id,
                    "transformation_type": r.transformation_type,
                    "input_rid": r.input_rid,
                    "output_rid": r.output_rid,
                    "parent_receipt_id": r.parent_receipt_id,
                    "processor_name": r.processor_name,
                    "source_sensor": r.source_sensor,
                    "metadata": r.metadata,
                    "content_hash": r.content_hash,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in receipts
            ],
        }

    @router.post("/crawl-agentic", response_model=CrawlAgenticEnqueuedResponse)
    async def web_crawl_agentic(
        body: CrawlAgenticRequest,
        request: Request,
    ):
        """Enqueue an agentic crawl; background worker drives it.

        Ships INERT: returns 503 unless ``AGENTIC_CRAWL_ENABLED=true`` and at
        least one ``CRAWL_TOKEN_*`` is configured. All cap checks and the
        INSERT run inside a single transaction holding
        ``pg_advisory_xact_lock(hashtext(submitted_by))``. The partial unique
        index ``uniq_inflight_per_user_url`` is the belt-and-suspenders
        second line — on ``unique_violation`` we return the existing
        in-flight ``job_id`` with ``deduped=true``.
        """
        from api import crawl_auth
        from api.agentic_crawler import CrawlBudget
        from api.crawl_canonicalize import StartUrlError, canonicalize_start_url
        from api.web_fetcher import URLValidationError, URLValidator

        try:
            auth = crawl_auth.authenticate_request(
                authorization_header=request.headers.get("authorization"),
                identity_claim_header=request.headers.get("x-identity-claim"),
                body_submitted_by=None,
            )
        except crawl_auth.CrawlAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail={"error": exc.message})

        # 1. Canonicalize start_url (strict, local; no DNS). SSRF gate is
        # separate (below) and uses URLValidator which blocks private ranges.
        try:
            canonical_url = canonicalize_start_url(body.url)
        except StartUrlError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)})
        try:
            canonical_url = URLValidator().validate(canonical_url)
        except URLValidationError as exc:
            raise HTTPException(status_code=400, detail={"error": str(exc)})

        # 2. Validate budget against system ceilings; build snapshot.
        budget = CrawlBudget()
        if body.budget:
            if body.budget.max_pages is not None:
                budget.max_pages = body.budget.max_pages
            if body.budget.max_vision_calls is not None:
                budget.max_vision_calls = body.budget.max_vision_calls
            if body.budget.max_seconds is not None:
                budget.max_seconds = body.budget.max_seconds
            if body.budget.max_usd is not None:
                budget.max_usd = body.budget.max_usd
        try:
            budget.clamp_to_system_ceilings()
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": f"budget exceeds system ceiling: {exc}"},
            )
        budget_snapshot = budget.as_snapshot()

        # 3. Atomic enqueue: cap checks + dedup + INSERT serialized by
        # pg_advisory_xact_lock on submitted_by. Unique-violation on
        # uniq_inflight_per_user_url collapses races past the lock.
        import asyncpg
        from api import ontology_registry

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        auth.submitted_by,
                    )
                    # URL dedup (per-user): return existing in-flight job.
                    existing = await conn.fetchval(
                        """
                        SELECT id FROM web_crawl_jobs
                         WHERE submitted_by=$1 AND start_url=$2
                           AND status IN ('queued','running')
                         LIMIT 1
                        """,
                        auth.submitted_by,
                        canonical_url,
                    )
                    if existing is not None:
                        return CrawlAgenticEnqueuedResponse(job_id=int(existing), deduped=True)
                    # Per-user concurrency cap.
                    concurrent = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM web_crawl_jobs
                         WHERE submitted_by=$1 AND status IN ('queued','running')
                        """,
                        auth.submitted_by,
                    )
                    if concurrent >= PER_USER_CONCURRENT_CAP:
                        raise HTTPException(
                            status_code=429,
                            detail={
                                "error": f"concurrent crawl limit ({PER_USER_CONCURRENT_CAP}) reached"
                            },
                        )
                    # Per-day cap (counts terminal-state rows too).
                    daily = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM web_crawl_jobs
                         WHERE submitted_by=$1 AND created_at > now() - interval '24 hours'
                        """,
                        auth.submitted_by,
                    )
                    if daily >= PER_USER_DAILY_CAP:
                        raise HTTPException(
                            status_code=429,
                            detail={
                                "error": f"daily crawl limit ({PER_USER_DAILY_CAP}) reached"
                            },
                        )
                    # Insert.
                    try:
                        row = await conn.fetchrow(
                            """
                            INSERT INTO web_crawl_jobs
                                (start_url, goal, submitted_by, status,
                                 proposal_version, ontology_version, budget_json)
                            VALUES ($1, $2, $3, 'queued', 'v1', $4, $5::jsonb)
                            RETURNING id
                            """,
                            canonical_url,
                            body.goal,
                            auth.submitted_by,
                            ontology_registry.ONTOLOGY_VERSION,
                            _json_dumps(budget_snapshot),
                        )
                    except asyncpg.exceptions.UniqueViolationError:
                        # Race past the advisory lock — return the in-flight one.
                        existing = await conn.fetchval(
                            """
                            SELECT id FROM web_crawl_jobs
                             WHERE submitted_by=$1 AND start_url=$2
                               AND status IN ('queued','running')
                             LIMIT 1
                            """,
                            auth.submitted_by,
                            canonical_url,
                        )
                        if existing is None:
                            raise HTTPException(
                                status_code=500,
                                detail={"error": "unique violation with no in-flight row"},
                            )
                        return CrawlAgenticEnqueuedResponse(job_id=int(existing), deduped=True)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("enqueue failed: %s", exc)
            raise HTTPException(status_code=500, detail={"error": f"enqueue failed: {exc}"})

        logger.info(
            "web.crawl-agentic enqueued job_id=%d url=%s submitted_by=%s",
            row["id"], canonical_url, auth.submitted_by,
        )
        return CrawlAgenticEnqueuedResponse(job_id=int(row["id"]), deduped=False)

    @router.get("/crawl-jobs/{job_id}", response_model=CrawlJobStatusResponse)
    async def web_crawl_job_status(job_id: int, request: Request):
        """Poll status of a crawl job. Ownership-checked."""
        from api import crawl_auth

        try:
            auth = crawl_auth.authenticate_request(
                authorization_header=request.headers.get("authorization"),
                identity_claim_header=request.headers.get("x-identity-claim"),
                body_submitted_by=None,
            )
        except crawl_auth.CrawlAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail={"error": exc.message})

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, start_url, submitted_by, status,
                       progress_json, result_json, cost_usd, error,
                       started_at, heartbeat_at, finished_at
                  FROM web_crawl_jobs WHERE id=$1
                """,
                job_id,
            )
        if row is None:
            raise HTTPException(status_code=404, detail={"error": "job not found"})
        if row["submitted_by"] != auth.submitted_by:
            raise HTTPException(status_code=403, detail={"error": "ownership mismatch"})

        def _parse_json(raw: Any) -> Any:
            if raw is None:
                return None
            if isinstance(raw, (dict, list)):
                return raw
            try:
                import json
                return json.loads(raw)
            except (ValueError, TypeError):
                return None

        return CrawlJobStatusResponse(
            job_id=int(row["id"]),
            status=row["status"],
            start_url=row["start_url"],
            submitted_by=row["submitted_by"],
            progress=_parse_json(row["progress_json"]) or {},
            cost_usd=float(row["cost_usd"] or 0.0),
            result=_parse_json(row["result_json"]),
            error=row["error"],
            started_at=row["started_at"].isoformat() if row["started_at"] else None,
            heartbeat_at=row["heartbeat_at"].isoformat() if row["heartbeat_at"] else None,
            finished_at=row["finished_at"].isoformat() if row["finished_at"] else None,
        )

    @router.post("/crawl-jobs/{job_id}/commit")
    async def web_crawl_job_commit(job_id: int, body: CrawlCommitRequest, request: Request):
        from api import crawl_auth
        from api.personal_ingest_api import ExtractedEntity, resolve_entity, store_new_entity

        try:
            auth = crawl_auth.authenticate_request(
                authorization_header=request.headers.get("authorization"),
                identity_claim_header=request.headers.get("x-identity-claim"),
                body_submitted_by=None,
            )
        except crawl_auth.CrawlAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail={"error": exc.message})

        t0 = time.monotonic()
        overrides = body.proposal_overrides or ProposalOverrides()
        dropped_entity_indices = set(overrides.dropped_entity_indices or [])
        dropped_relationship_indices = set(overrides.dropped_relationship_indices or [])

        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT id, start_url, submitted_by, status, result_json, commit_history
                      FROM web_crawl_jobs
                     WHERE id=$1
                     FOR UPDATE
                    """,
                    job_id,
                )
                if row is None:
                    raise HTTPException(status_code=404, detail={"error": "job not found"})
                if row["submitted_by"] != auth.submitted_by:
                    raise HTTPException(status_code=403, detail={"error": "ownership mismatch"})
                if row["status"] == "committed":
                    raise HTTPException(status_code=409, detail={"error": "job already committed"})
                if row["status"] not in ("done", "partially_committed"):
                    raise HTTPException(
                        status_code=409,
                        detail={"error": f"job status '{row['status']}' cannot be committed"},
                    )

                proposal = _load_stored_proposal(row["result_json"])
                entity_count = len(proposal.entities)
                for idx in dropped_entity_indices:
                    if idx < 0 or idx >= entity_count:
                        raise HTTPException(status_code=422, detail={"error": f"dropped_entity_indices contains out-of-range index {idx}"})
                for idx in dropped_relationship_indices:
                    if idx < 0 or idx >= len(proposal.relationships):
                        raise HTTPException(status_code=422, detail={"error": f"dropped_relationship_indices contains out-of-range index {idx}"})
                for idx in overrides.entity_edits.keys():
                    if idx < 0 or idx >= entity_count:
                        raise HTTPException(status_code=422, detail={"error": f"entity_edits contains out-of-range index {idx}"})

                raw_history = _parse_json_maybe(row["commit_history"]) or []
                if not isinstance(raw_history, list):
                    raw_history = []
                prior_committed_map: Dict[int, str] = {}
                for entry in raw_history:
                    committed_map = (entry or {}).get("committed_index_to_rid") or {}
                    if isinstance(committed_map, dict):
                        for key, value in committed_map.items():
                            try:
                                prior_committed_map[int(key)] = value
                            except (TypeError, ValueError):
                                continue

                committed_results: List[Dict[str, Any]] = []
                skipped_results: List[Dict[str, Any]] = []
                error_results: List[Dict[str, Any]] = []
                unresolved_extra_relationships: List[Dict[str, Any]] = []
                attempt_committed_map: Dict[int, str] = {}
                current_entity_rids: Dict[int, str] = dict(prior_committed_map)
                errored_indices: set[int] = set()
                extra_relationships_created = 0

                entities_by_index = {
                    entity.index: _copy_entity_with_edits(entity, overrides.entity_edits.get(entity.index))
                    for entity in proposal.entities
                }
                renamed_indices = {
                    idx for idx, edit in overrides.entity_edits.items()
                    if edit.name is not None
                }
                root_index = proposal.root_entity_index

                await conn.execute("SAVEPOINT inner_tx")
                root_failed = False

                ordered_indices = [root_index] + [i for i in range(entity_count) if i != root_index]
                for idx in ordered_indices:
                    entity = entities_by_index[idx]
                    if idx in prior_committed_map:
                        skipped_results.append({"name": entity.name, "reason": "already committed"})
                        continue
                    if idx in dropped_entity_indices:
                        skipped_results.append({"name": entity.name, "reason": "dropped by override"})
                        continue

                    await conn.execute(f"SAVEPOINT ent_{idx}")
                    try:
                        if entity.existing_rid:
                            canonical_uri = entity.existing_rid
                            was_new = False
                        else:
                            extracted = ExtractedEntity(
                                name=entity.name,
                                type=entity.type,
                                confidence=entity.confidence,
                                context=entity.description,
                            )
                            canonical, is_new = await resolve_entity(
                                conn,
                                extracted,
                                context=None,
                                skip_fuzzy=idx in renamed_indices,
                                resolution_caller="web_router.batch_ingest",
                            )
                            canonical_uri = canonical.uri
                            was_new = bool(is_new)
                            if is_new:
                                await store_new_entity(
                                    conn,
                                    extracted,
                                    canonical,
                                    document_rid=entity.source_url or proposal.start_url,
                                    source="web-crawl",
                                )

                        await conn.execute(
                            """
                            UPDATE entity_registry
                               SET metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
                                   updated_at = now()
                             WHERE fuseki_uri = $1
                            """,
                            canonical_uri,
                            _json_dumps(
                                {
                                    "crawl_job_id": job_id,
                                    "crawl_start_url": proposal.start_url,
                                    "source_url": entity.source_url,
                                    "source_image": entity.source_image,
                                    "confidence": entity.confidence,
                                }
                            ),
                        )

                        await conn.execute(f"RELEASE SAVEPOINT ent_{idx}")
                        attempt_committed_map[idx] = canonical_uri
                        current_entity_rids[idx] = canonical_uri
                        committed_results.append(
                            {
                                "rid": canonical_uri,
                                "name": entity.name,
                                "type": entity.type,
                                "was_new": was_new,
                            }
                        )
                    except Exception as exc:
                        await conn.execute(f"ROLLBACK TO SAVEPOINT ent_{idx}")
                        errored_indices.add(idx)
                        error_results.append({"index": idx, "name": entity.name, "error": str(exc)})
                        if idx == root_index:
                            await conn.execute("ROLLBACK TO SAVEPOINT inner_tx")
                            attempt_committed_map = {}
                            current_entity_rids = dict(prior_committed_map)
                            committed_results = []
                            skipped_results = [
                                {"name": entities_by_index[i].name, "reason": "root entity failed"}
                                for i in range(entity_count)
                                if i != root_index and i not in prior_committed_map
                            ]
                            root_failed = True
                            break

                if not root_failed:
                    for rel_idx, rel in enumerate(proposal.relationships):
                        if rel_idx in dropped_relationship_indices:
                            continue
                        if rel.predicate not in ontology_registry.ALLOWED_PREDICATES:
                            error_results.append({"name": f"relationship:{rel_idx}", "error": f"predicate '{rel.predicate}' retired or invalid"})
                            continue
                        if rel.subject_index in dropped_entity_indices or rel.object_index in dropped_entity_indices:
                            continue
                        if rel.subject_index in errored_indices or rel.object_index in errored_indices:
                            continue
                        subj_uri = current_entity_rids.get(rel.subject_index)
                        obj_uri = current_entity_rids.get(rel.object_index)
                        if not subj_uri or not obj_uri:
                            continue
                        savepoint_name = f"rel_{rel_idx}"
                        await conn.execute(f"SAVEPOINT {savepoint_name}")
                        try:
                            await conn.execute(
                                """
                                INSERT INTO entity_relationships
                                    (subject_uri, predicate, object_uri, source, source_rid)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
                                """,
                                subj_uri,
                                rel.predicate,
                                obj_uri,
                                "web-crawl",
                                proposal.start_url,
                            )
                            await conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                        except Exception as exc:
                            await conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                            error_results.append({"name": f"relationship:{rel_idx}", "error": str(exc)})

                    for extra_idx, extra in enumerate(body.extra_relationships):
                        if extra.predicate not in ontology_registry.ALLOWED_PREDICATES:
                            raise HTTPException(
                                status_code=422,
                                detail={"error": f"predicate '{extra.predicate}' is not allowed"},
                            )
                        from_uri, from_unresolved, from_skip = await _resolve_extra_endpoint_ref(
                            conn,
                            extra.from_,
                            proposal,
                            current_entity_rids,
                            dropped_entity_indices,
                            errored_indices,
                        )
                        to_uri, to_unresolved, to_skip = await _resolve_extra_endpoint_ref(
                            conn,
                            extra.to,
                            proposal,
                            current_entity_rids,
                            dropped_entity_indices,
                            errored_indices,
                        )
                        if from_unresolved:
                            unresolved_extra_relationships.append(from_unresolved)
                            continue
                        if to_unresolved:
                            unresolved_extra_relationships.append(to_unresolved)
                            continue
                        if from_skip or to_skip:
                            skipped_results.append(
                                {
                                    "name": f"extra:{extra.predicate}",
                                    "reason": from_skip or to_skip,
                                }
                            )
                            continue
                        if not from_uri or not to_uri:
                            continue
                        savepoint_name = f"extra_rel_{extra_idx}"
                        await conn.execute(f"SAVEPOINT {savepoint_name}")
                        try:
                            await conn.execute(
                                """
                                INSERT INTO entity_relationships
                                    (subject_uri, predicate, object_uri, source, source_rid)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
                                """,
                                from_uri,
                                extra.predicate,
                                to_uri,
                                "web-crawl-extra",
                                proposal.start_url,
                            )
                            await conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                            extra_relationships_created += 1
                        except Exception as exc:
                            await conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                            error_results.append(
                                {
                                    "name": f"extra_relationship:{extra_idx}",
                                    "error": str(exc),
                                }
                            )

                    await conn.execute("RELEASE SAVEPOINT inner_tx")

                merged_committed = dict(prior_committed_map)
                merged_committed.update(attempt_committed_map)
                all_indices = set(range(entity_count))
                final_status = (
                    "committed"
                    if set(merged_committed.keys()).union(dropped_entity_indices) >= all_indices
                    else "partially_committed"
                )

                history_entry = {
                    "attempted_at": datetime.utcnow().isoformat() + "Z",
                    "committed_index_to_rid": {str(k): v for k, v in sorted(attempt_committed_map.items())},
                    "dropped_entity_indices": sorted(dropped_entity_indices),
                    "dropped_relationship_indices": sorted(dropped_relationship_indices),
                    "committed": committed_results,
                    "skipped": skipped_results,
                    "errors": error_results,
                    "unresolved_extra_relationships": unresolved_extra_relationships,
                    "extra_relationships_created": extra_relationships_created,
                }

                new_history = list(raw_history)
                new_history.append(history_entry)
                await conn.execute(
                    """
                    UPDATE web_crawl_jobs
                       SET status=$2,
                           commit_history=$3::jsonb
                     WHERE id=$1
                    """,
                    job_id,
                    final_status,
                    _json_dumps(new_history),
                )

        return {
            "committed": committed_results,
            "skipped": skipped_results,
            "errors": error_results,
            "unresolved_extra_relationships": unresolved_extra_relationships,
            "extra_relationships_created": extra_relationships_created,
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "status": final_status,
        }

    @tools_router.post("/parse-relate-clause")
    async def parse_relate_clause_endpoint(body: ParseRelateClauseRequest, request: Request):
        from api import crawl_auth
        from api.tools.parse_relate_clause import parse_relate_clause

        raw_body = await request.body()
        if len(raw_body) > 2048:
            raise HTTPException(status_code=413, detail={"error": "request body exceeds 2KB"})
        try:
            auth = crawl_auth.authenticate_request(
                authorization_header=request.headers.get("authorization"),
                identity_claim_header=request.headers.get("x-identity-claim"),
                body_submitted_by=None,
            )
        except crawl_auth.CrawlAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail={"error": exc.message})

        _check_parse_relate_rate_limit(auth.submitted_by)
        try:
            parsed = await parse_relate_clause(body.instruction)
        except Exception as exc:
            logger.exception("parse-relate-clause failed: %s", exc)
            raise HTTPException(status_code=500, detail={"error": f"parse relate failed: {exc}"})
        return {"targets": parsed["targets"]}

    api_router.include_router(router)
    api_router.include_router(tools_router)
    return api_router


# -- Helpers -----------------------------------------------------------------

def _entity_to_dict(entity) -> Dict[str, Any]:
    """Convert a MatchingEntity to a dict."""
    if isinstance(entity, dict):
        return entity
    return {
        "name": getattr(entity, "name", getattr(entity, "entity_text", "")),
        "type": getattr(entity, "type", getattr(entity, "entity_type", "")),
        "uri": getattr(entity, "uri", getattr(entity, "fuseki_uri", "")),
        "similarity": getattr(entity, "similarity", None),
    }


def _serialize_matching_entities(entities) -> str:
    """Serialize matching entities to JSON string."""
    import json
    if not entities:
        return "[]"
    return json.dumps([_entity_to_dict(e) for e in entities])


def _json_dumps(obj) -> str:
    """JSON serialize helper."""
    import json
    return json.dumps(obj)
