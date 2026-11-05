#!/usr/bin/env python3
"""
Personal KOI Ingest API

FastAPI server for ingesting pre-extracted entities from Claude Code.
Runs on port 8351 as part of the personal KOI-net.

This endpoint accepts entities already extracted by Claude (no LLM cost)
and performs:
1. Entity deduplication against the personal knowledge base
2. Canonical URI assignment
3. Storage in PostgreSQL with pgvector embeddings
4. Returns resolved entities with URIs for vault linking

Entity Resolution Tiers:
- Tier 1: Exact match (normalized text, B-Tree index)
- Tier 1.x: Fuzzy string match (Jaro-Winkler similarity)
- Tier 2: Semantic match (BGE embeddings + pgvector HNSW)
- Tier 3: Create new entity with deterministic URI
"""

import json as json_module_global
import importlib.util
import os
import sys
import re
import asyncio
import asyncpg
import hashlib
import httpx
import unicodedata
from datetime import datetime, timezone
from typing import List, Dict, Any, Literal, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator, model_validator
import logging
import time
import uuid
from pathlib import Path as _VaultPath
from metaphone import doublemetaphone

# --- Vault filesystem awareness (phantom-path guard) ------------------------
# entity_rid_mappings.vault_path is stored at register time and historically
# returned without verifying the note still exists on disk. Deleting /
# relocating / never-creating a note leaves an orphaned row that resolves to a
# dangling wikilink (~113 orphans observed 2026-06-02). These helpers let the
# vault-path-returning endpoints check existence before emitting a wikilink.
_VAULT_ROOT = _VaultPath(os.path.expanduser(
    os.environ.get("OBSIDIAN_VAULT_PATH", "~/Documents/Notes")
))


def _vault_note_exists(rel_path: Optional[str]) -> bool:
    """True if a vault note exists for `rel_path` (with or without .md).

    Best-effort: if the configured vault root is not present on this host
    (e.g. a headless deploy that doesn't mount the vault), returns True so we
    never strip legitimate links just because the FS isn't visible here.
    """
    if not rel_path:
        return False
    if not _VAULT_ROOT.is_dir():
        return True  # vault not mounted here — don't second-guess stored paths
    rel = rel_path[:-3] if rel_path.endswith(".md") else rel_path
    return (_VAULT_ROOT / f"{rel}.md").exists() or (_VAULT_ROOT / rel).exists()


def _pick_best_mapping(rows, expected_folder: Optional[str] = None) -> Optional[str]:
    """Choose one vault_path among entity_rid_mappings rows sharing a canonical.

    Several rows can share one canonical_uri because /register-entity upserts on
    ``ON CONFLICT (vault_rid)``: registering a second note for an entity that
    already has one inserts a NEW row instead of repointing the old one. Renames
    and duplicate-note consolidation both produce this.

    The old query was a bare ``LIMIT 1`` with no ORDER BY, so Postgres returned
    an arbitrary (in practice oldest) row. Observed 2026-07-16: one person's
    canonical entity had mappings to two notes, a duplicate that had been
    deleted and then restored by sync, and the live note. The stale loser won
    on age, so resolution and --propagate both wrote to the wrong note.

    Preference order, highest first:
      1. the note actually exists on disk (a mapping to a missing file is dead);
      2. the note sits in the folder for the entity's OWN type — Regenerate
         Cascadia is an Organization, so Organizations/ beats Projects/. Without
         this, same-name cross-type pairs (Regenerate Cascadia, PKOLS, KOI,
         Kelpwood, Silas Marchbank) tie on name and fall through to recency, which
         is arbitrary and silently flips the answer;
      3. the mapping's recorded name matches the canonical entity's name
         ("Mabel Patel" beats "Mabs" for canonical "Mabel Patel");
      4. most recently synced — the caller orders rows accordingly, and max()
         keeps the first maximal element, so recency is the natural tiebreak.

    (2) and (3) are independent: Mabel Patel's duplicates are both in People/ and are
    separated by name; Regenerate Cascadia's both match on name and are
    separated by folder.

    Note (1) is fail-safe: when the vault isn't mounted _vault_note_exists
    returns True for every row, collapsing this to the later signals rather than
    discarding all candidates. expected_folder=None likewise just skips (2).
    """
    if not rows:
        return None

    def comparable(value: Optional[str]) -> Optional[str]:
        # normalize_entity_text collapses runs of spaces with a single
        # non-idempotent .replace('  ', ' '), so 'a   b' survives as 'a  b'.
        # Collapse here rather than changing that shared normalizer, whose
        # semantics the whole resolution stack depends on.
        if not value:
            return None
        return " ".join(normalize_entity_text(value).split())

    def rank(row):
        path = row["vault_path"] or ""
        exists = _vault_note_exists(path)
        folder_match = bool(
            expected_folder and path.startswith(f"{expected_folder}/")
        )
        canonical_name = comparable(row.get("canonical_name"))
        mapping_name = comparable(row.get("name"))
        name_match = bool(
            canonical_name and mapping_name and mapping_name == canonical_name
        )
        return (exists, folder_match, name_match)

    return max(rows, key=rank)["vault_path"]


# Import CAT receipt chain
from api.cat_receipts import create_receipt, generate_receipt_id, get_receipt_chain

# Import vault relationship parser
from api.vault_parser import (
    sync_vault_relationships,
    resolve_pending_relationships,
    get_entity_relationships,
    check_relationship_exists,
    SYMMETRIC_PREDICATES,
)

# Import schema loader
from api.entity_schema import (
    get_entity_schemas,
    get_schema_for_type,
    get_schema_version,
    reload_entity_schemas,
    get_first_significant_token,
    get_phonetic_enabled_types,
    canonicalize_entity_type,
    EntityTypeConfig,
)

# Shared resolver name guards (single source of truth in resolution_primitives).
# Wired into Tier 1.5 (Person), Tier 2a and Tier 2b — both of the latter via
# passes_semantic_match_guard_with_policy / passes_token_overlap_strict.
#
# NOT via passes_semantic_match_guard: that convenience wrapper hardcodes the LEGACY
# token policy and no production path calls it (only tests do). The comment here used to
# say Tier 2b went through it, which stopped being true and would have made the legacy
# default look sanctioned. tests/test_resolver_shadow_replay.py pins that no production
# module calls it.
from api.resolution_primitives import (
    GENERIC_NAME_TOKENS,
    extract_leading_meeting_date,
    passes_person_name_guard,
    passes_distinctive_token_check,
    passes_semantic_match_guard_with_policy,
    passes_token_overlap_legacy,
    passes_token_overlap_strict,
    resolve_to_live_uri,
)
from api.resolver_shadow import emitter_status, start_attempt, shutdown_emitter
from api.resolver_decisions_log import log_decision as _log_resolver_decision
from api.session_exclusions import session_exclusion_filter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from api.optional_federation import (  # noqa: E402
    federation_unavailable as _federation_unavailable,
    optional_federation_attr as _optional_federation_attr,
)

# FastAPI app
app = FastAPI(
    title="Personal KOI Ingest API",
    version="1.0.0",
    description="Ingests pre-extracted entities from Claude Code into personal knowledge base"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# C14: KOI write-attribution log middleware. Added after CORS so it is the
# OUTERMOST layer (add_middleware prepends, so the last-added runs first): it
# buffers and replays the request body and captures the response status. It
# records one koi_write_log row per request that carries X-KOI-Client or arrives
# from the tunnel address (KOI_RECORDED_CLIENT_IP), and nothing else. The route map is
# built from the regen-prod commit this branch records (RQ41); db_pool is read
# late (it is created in startup).
_koi_route_map = None
try:
    from api.koi_write_log import KoiWriteLogMiddleware, load_route_map
    _koi_routes_tsv = os.path.join(os.path.dirname(__file__), "koi-routes.tsv")
    _koi_route_map = load_route_map(_koi_routes_tsv)
    app.add_middleware(
        KoiWriteLogMiddleware,
        pool_provider=lambda: db_pool,
        route_map=_koi_route_map,
    )
    logging.getLogger(__name__).info(
        f"KOI write-log middleware mounted ({len(_koi_route_map)} routes, "
        f"regen-prod {_koi_route_map.commit})"
    )
except Exception as e:
    logging.getLogger(__name__).warning(f"KOI write-log middleware not mounted: {e}")
    _koi_route_map = None

# Log-only auth observation. Added LAST, so it is the outermost
# layer (user_middleware = [AuthObservation, KoiWriteLog, CORS]): it sees every
# request, including ones C14 records, and logs the final status. It denies
# nothing by construction; tranche-1 enforcement is the per-route read-auth
# dependency (api/read_auth.py). Not wrapped in try/except: if the module cannot
# be imported the app must not start silently without it.
from api.auth_observability import AuthObservationMiddleware  # noqa: E402
app.add_middleware(AuthObservationMiddleware)

# Serve static files (demo portal)
from fastapi.staticfiles import StaticFiles
_static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

@app.get("/demo")
async def demo_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse("/static/demo.html")

# Load capabilities registry
try:
    from api.capabilities import Capabilities
    _caps = Capabilities.from_env()
    logging.getLogger(__name__).info(f"Capabilities loaded (profile={_caps.deployment_profile})")
except ImportError:
    _caps = None
    logging.getLogger(__name__).info("Capabilities registry not available, using legacy startup")

# Mount KOI-net federation router (if enabled)
if os.getenv('KOI_NET_ENABLED', 'false').lower() in ('true', '1', 'yes'):
    try:
        from api.koi_net_router import koi_net_router
        app.include_router(koi_net_router, prefix="/koi-net")
        logging.getLogger(__name__).info("KOI-net federation router mounted")
    except ImportError as e:
        logging.getLogger(__name__).warning(f"KOI-net federation not available: {e}")

# Configuration
DB_URL = os.getenv('POSTGRES_URL', 'postgresql://koi:@localhost:5432/personal_koi')
KOI_MODE = os.getenv('KOI_MODE', 'personal')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')  # kept for /chat LLM endpoint
ENABLE_SEMANTIC_MATCHING = os.getenv('ENABLE_SEMANTIC_MATCHING', 'true').lower() == 'true'
KOI_NET_ENABLED = os.getenv('KOI_NET_ENABLED', 'false').lower() in ('true', '1', 'yes')
TERMINUSDB_ENABLED = os.getenv('TERMINUSDB_ENABLED', 'false').lower() in ('true', '1', 'yes')
QUARTZ_BASE_URL = os.getenv('QUARTZ_BASE_URL', '').rstrip('/')

# Quartz URL generation
QUARTZ_TYPE_PATHS = {
    "Person": "People",
    "Organization": "Organizations",
    "Project": "Projects",
    "Location": "Locations",
    "Concept": "Concepts",
    "Meeting": "Meetings",
    "Practice": "Practices",
    "Pattern": "Patterns",
    "CaseStudy": "CaseStudies",
    "Bioregion": "Bioregions",
    "Protocol": "Protocols",
    "Playbook": "Playbooks",
    "Question": "Questions",
    "Claim": "Claims",
    "Evidence": "Evidence",
    "Commitment": "Commitments",
    "CommitmentPool": "CommitmentPools",
    "CommitmentAction": "CommitmentActions",
    "Source": "Sources",
    "WorkItem": "WorkItems",
    "Milestone": "Milestones",
    "Initiative": "Initiatives",
    "Decision": "Decisions",
    "Risk": "Risks",
    "Metric": "Metrics",
    "Outcome": "Outcomes",
}

def quartz_slug(name: str) -> str:
    """Convert entity name to Quartz-compatible URL slug."""
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = s.strip()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^\w-]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")

def quartz_url(entity_type: str, name: str) -> Optional[str]:
    """Build Quartz URL. Returns None for unknown entity types or if base URL not set."""
    if not QUARTZ_BASE_URL:
        return None
    path = QUARTZ_TYPE_PATHS.get(entity_type)
    if path is None:
        return None
    return f"{QUARTZ_BASE_URL}/{path}/{quartz_slug(name)}"

# DEPRECATED: These are now loaded from vault schemas via entity_schema.py
# Kept as fallback comments for reference
# SEMANTIC_THRESHOLDS = loaded from schema.semantic_threshold
# SIMILARITY_THRESHOLDS = loaded from schema.similarity_threshold

# Global connection pool
db_pool: Optional[asyncpg.Pool] = None
# Least-privilege pool for POST /sql; None => /sql fails closed.
sql_ro_pool: Optional[asyncpg.Pool] = None

# Fixed mapping for knowledge_search source filter — prevents SQL injection.
# Unknown values are rejected with HTTP 400 before any SQL is constructed.
SOURCE_TO_FILTER = {
    'email': (
        "AND m.source_sensor IN ('email-sensor', 'ics-event')"
        " AND (m.metadata->>'status' IS NULL OR m.metadata->>'status' != 'cancelled')"
    ),
    'vault': "AND m.source_sensor = 'obsidian-sensor'",
    'substack': "AND m.source_sensor IN ('substack-corpus-backfill', 'substack-sensor')",
}

from api.embedding_provider import EmbeddingProvider, create_embedding_provider
embedding_provider: Optional[EmbeddingProvider] = None

from api.chat_provider import (
    ChatProvider,
    create_classifier_provider,
    create_chat_provider,
    create_expansion_provider,
)
classifier_provider: Optional[ChatProvider] = None
chat_answer_provider: Optional[ChatProvider] = None
expansion_provider: Optional[ChatProvider] = None
terminusdb_adapter: Optional[Any] = None  # TerminusDBAdapter instance (lazy init)


# =============================================================================
# Pydantic Models
# =============================================================================

class ExtractedEntity(BaseModel):
    """Entity extracted by Claude Code"""
    name: str
    type: str  # Person, Organization, Location, Project, Concept
    mentions: List[str] = []
    confidence: float = 0.9
    context: Optional[str] = None
    # Per-entity context for resolution (merged with global context)
    associated_people: Optional[List[str]] = None
    associated_organizations: Optional[List[str]] = None

    @field_validator("type")
    @classmethod
    def _canonicalize_type(cls, v: str) -> str:
        # Strip schema:/bkc: prefixes and resolve to the canonical schema
        # type_key (case/plural tolerant). Empty string passes through.
        return canonicalize_entity_type(v) if v else v


class ExtractedRelationship(BaseModel):
    """Relationship between entities"""
    subject: str
    predicate: str
    object: str
    confidence: float = 0.9


class ResolutionContext(BaseModel):
    """Context for entity resolution disambiguation"""
    associated_people: Optional[List[str]] = None
    project: Optional[str] = None           # Meeting project name for relationship matching
    organizations: Optional[List[str]] = None  # Mentioned organizations for relationship matching
    topics: Optional[List[str]] = None      # Topics for future use
    associated_orgs: Optional[List[str]] = None  # Deprecated: use organizations instead
    source_text: Optional[str] = None  # Reserved for future use


class IngestRequest(BaseModel):
    """Request to ingest extracted entities"""
    document_rid: str  # e.g., "vault:notes/salish-sea-herring"
    content: Optional[str] = None
    entities: List[ExtractedEntity]
    relationships: List[ExtractedRelationship] = []
    source: str = "obsidian-vault"
    context: Optional[ResolutionContext] = None  # For contextual entity resolution
    parent_receipt_id: Optional[str] = None  # Chain to a parent CAT receipt
    # Optional idempotency key for safe client retries. See ingest_idempotency
    # (migration 106). NOTE: /ingest stores the response AFTER its post-commit
    # receipt step (store-after-commit, not single-transaction) because the CAT
    # receipt is created outside the main write transaction — see the endpoint.
    request_id: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _log_dropped_fields(cls, data: Any) -> Any:
        """Make pydantic's silent drop of unknown request keys visible.

        The default extra='ignore' discards keys this model does not declare.
        The claude-sessions sensor has sent `link_existing_only` and
        `replace_existing` on every call, and /ingest has never implemented
        either, so sessions created entities and never cleared stale links while
        the sensor believed otherwise. extra='forbid' would 422 every sensor call,
        and declaring the fields would change the /ingest OpenAPI schema (plan
        AC7), so this only logs. Implementing either field is a separate change.
        """
        if isinstance(data, dict):
            dropped = sorted(k for k in data if k not in cls.model_fields)
            if dropped:
                logger.warning(
                    "/ingest ignored unknown request field(s) %s (document_rid=%r, "
                    "source=%r); they are not implemented",
                    dropped, data.get("document_rid"), data.get("source"))
        return data


class CanonicalEntity(BaseModel):
    """Resolved canonical entity"""
    name: str
    uri: str
    type: str
    is_new: bool
    merged_with: Optional[str] = None  # If deduplicated
    confidence: float = 1.0
    # Vault-path annotation (P2, 2026-07-13). Populated for resolved
    # (is_new=false) entities on /ingest. ANNOTATE-ONLY — never gates or
    # demotes resolution. Null when there is no entity_rid_mappings row.
    vault_path: Optional[str] = None
    vault_note_exists: Optional[bool] = None
    vault_folder: Optional[str] = None


class IngestStats(BaseModel):
    """Stats from ingest operation"""
    entities_processed: int
    new_entities: int
    resolved_entities: int
    relationships_processed: int
    failed_entities: int
    errors: Optional[List[Dict[str, str]]] = None


class IngestResponse(BaseModel):
    """Response from ingest endpoint"""
    success: bool
    canonical_entities: List[CanonicalEntity]
    receipt_id: str
    receipt_rid: Optional[str] = None  # Deprecated alias — use receipt_id
    receipt_persisted: bool = True  # False if receipt DB write failed (receipt_id still valid format but not in DB)
    stats: IngestStats
    # True only when this response is a replay of a prior request with the same
    # request_id (no new writes were performed). See IngestRequest.request_id.
    idempotent_replay: bool = False


class RegisterEntityRequest(BaseModel):
    """Request to register a vault entity"""
    # vault_rid / vault_path are optional to support alias-only updates to an
    # existing entity (e.g. process-note alias auto-learning POSTs only
    # {name, entity_type, frontmatter.aliases}). When omitted, the handler
    # backfills them from the existing entity_rid_mappings row so the RID
    # mapping is updated in place rather than 422-rejected.
    vault_rid: Optional[str] = None  # e.g., "orn:obsidian.entity:Notes/Person/corin-ashgrove"
    vault_path: Optional[str] = None  # e.g., "People/Corin Ashgrove.md"
    entity_type: str  # Person, Organization, etc.
    name: str
    properties: Dict[str, Any] = {}
    frontmatter: Optional[Dict[str, Any]] = None  # YAML frontmatter for relationship extraction
    content_hash: Optional[str] = None
    publication_scope: Optional[str] = "local_graph"  # "local_graph" | "federated"
    visibility_scope: Optional[str] = "public"  # "public" | "node_private"
    # When true, suppress the Tier 1.1b cross-type dedup guard so a same-name
    # entity of THIS type can register even if a live entity of a DIFFERENT type
    # already carries the name. Use to intentionally create a legitimately
    # distinct same-named entity (the advisory cross_type_warning still fires).
    force_type: bool = False
    # Opt-in exact-name-only resolution for controlled bulk registration.
    # Default false preserves the full alias/context/fuzzy/semantic resolver for
    # every existing caller. The Meeting attendee backfill sets this true so
    # distinct sessions held on the same date cannot fuzzy-collapse.
    exact_only: bool = False

    @field_validator("entity_type")
    @classmethod
    def _canonicalize_entity_type(cls, v: str) -> str:
        return canonicalize_entity_type(v) if v else v


class RegisterEntityResponse(BaseModel):
    """Response from register-entity endpoint"""
    success: bool
    canonical_uri: str
    is_new: bool
    vault_rid: str
    merged_with: Optional[str] = None
    collision_warning: Optional[str] = None
    cross_type_warning: Optional[str] = None
    koi_rid: Optional[str] = None
    # Non-null means the entity registered but its frontmatter relationships did NOT.
    #
    # This failure used to be invisible AND much larger than its name suggests: the
    # handler caught every exception from sync_vault_relationships and logged one
    # unattributable line, but catching it did not un-abort the transaction. The whole
    # handler shares one transaction, so the closing COMMIT became a silent ROLLBACK and
    # the endpoint returned success=True having discarded the registration itself. A
    # savepoint now contains that failure, which is what makes success=True honest here:
    # the entity really is registered and only the relationships are missing.
    relationship_sync_error: Optional[str] = None


class VaultEntityMapping(BaseModel):
    """Mapping between vault RID and canonical entity"""
    vault_rid: str
    vault_path: str
    canonical_uri: str
    entity_type: str
    name: str
    sync_status: str  # linked, local_only, pending_sync, conflict
    content_hash: str
    last_synced: str


class ResolveRequest(BaseModel):
    """Request to resolve an entity with optional context"""
    label: str
    type_hint: Optional[str] = None
    limit: int = 5
    context: Optional[ResolutionContext] = None
    # Wave 2 B2 (2026-04-30): resolver-persistence asymmetry fix.
    # When false (default; backwards-compatible): resolver may return
    # is_new=true with a freshly-computed URI but does NOT persist the
    # row to entity_registry. Caller must direct-INSERT if persistence
    # is desired. When true: on is_new, the resolver also calls
    # store_new_entity() so the row is committed before the response
    # returns. Use this when the URI is going to be referenced
    # immediately (e.g. as subject_uri in /knowledge/episodes facts).
    persist: bool = False


# Graph traversal response models

class GraphNode(BaseModel):
    uri: str
    name: Optional[str] = None
    entity_type: Optional[str] = None
    depth: int = 0


class GraphEdge(BaseModel):
    source: str
    target: str
    predicate: str
    confidence: float = 1.0


class NeighborhoodResponse(BaseModel):
    root: str
    max_depth: int
    direction: str
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    node_count: int
    total_nodes_discovered: int
    edge_count: int
    total_edges_discovered: int
    truncated: bool


class PathStep(BaseModel):
    from_uri: str
    from_name: Optional[str] = None
    predicate: str
    direction: str
    to_uri: str
    to_name: Optional[str] = None


class ShortestPathResponse(BaseModel):
    source: str
    target: str
    found: bool
    path_length: Optional[int] = None
    direction: str
    steps: List[PathStep]
    nodes: List[GraphNode]


# =============================================================================
# Embedding Service (provider-agnostic, explicit query/document split)
# =============================================================================

async def generate_query_embedding(text: str, prompt_type: str = "query") -> Optional[List[float]]:
    """Generate QUERY embedding (with instruction prefix for retrieval).

    Use for: /chat retrieval, session search, fact search, entity resolution,
    semantic classifier, unified search — any read/search path.

    Phase 8 B2: prompt_type defaults to 'query'; callers may override (e.g.
    'rerank' for MMR diversity-pass embeddings).
    """
    if not embedding_provider or not ENABLE_SEMANTIC_MATCHING:
        return None
    normalized = normalize_entity_text(text)
    return await embedding_provider.embed_or_none(
        normalized, is_query=True, prompt_type=prompt_type
    )


async def generate_document_embedding(text: str, prompt_type: str = "extraction") -> Optional[List[float]]:
    """Generate DOCUMENT embedding (no instruction prefix for storage).

    Use for: entity creation, fact storage, ingest, chunk re-embedding —
    any write/persist path.

    Phase 8 B2: prompt_type defaults to 'extraction'; callers may override
    (e.g. 'dedup' for fact-write near-duplicate checks).
    """
    if not embedding_provider or not ENABLE_SEMANTIC_MATCHING:
        return None
    normalized = normalize_entity_text(text)
    return await embedding_provider.embed_or_none(
        normalized, is_query=False, prompt_type=prompt_type
    )


async def generate_document_embeddings_batch(
    texts: List[str], prompt_type: str = "extraction"
) -> Optional[List[Optional[List[float]]]]:
    """Batch DOCUMENT embeddings — one provider round-trip for many texts.

    Mirrors generate_document_embedding's normalization (normalize_entity_text)
    and DOCUMENT mode so batch-computed vectors are byte-for-byte the same as
    the per-text path — callers can safely prefetch a batch and fall back to
    the single-text function for cache misses. Returns None when embeddings are
    disabled or the provider fails (caller falls back to per-text embeds).
    """
    if not embedding_provider or not ENABLE_SEMANTIC_MATCHING:
        return None
    normalized = [normalize_entity_text(t or "") for t in texts]
    return await embedding_provider.embed_batch_or_none(
        normalized, prompt_type=prompt_type
    )


# Backward-compatible alias — calls QUERY mode (most existing callers are reads).
# TODO: Remove once all callers are migrated to explicit query/document.
async def generate_embedding(text: str) -> Optional[List[float]]:
    """DEPRECATED: Use generate_query_embedding or generate_document_embedding."""
    return await generate_query_embedding(text)


# =============================================================================
# Entity Resolution
# =============================================================================

# Compatibility name retained for existing Meeting identity tests and callers.
# The implementation now lives with the strict policy it protects.
_extract_leading_date = extract_leading_meeting_date


def normalize_entity_text(text: str) -> str:
    """Normalize entity text for comparison"""
    return (
        text.lower()
        .strip()
        .replace('_', ' ')
        .replace('-', ' ')
        .replace('  ', ' ')
        .lstrip('@')
    )


def normalize_alias(alias: Any) -> str:
    """
    Strip [[...]], lowercase, trim for alias matching.

    Handles wikilinks like [[People/Name|Display]] → name
    """
    alias = str(alias)  # Guard against non-string values
    alias = re.sub(r'\[\[([^\]|]+)(\|[^\]]+)?\]\]', r'\1', alias)  # Strip wikilinks
    # Extract just the name part if it's a path
    if '/' in alias:
        alias = alias.rsplit('/', 1)[-1]
    alias = alias.lower().strip()
    return alias


def get_phonetic_code(text: str) -> Optional[str]:
    """
    Get Double Metaphone code for first token of text.

    Uses first token only to handle cases like "Maybell" vs "Mabel Sedgewick"
    where full-name comparison would fail but first-token matches.
    """
    if not text:
        return None
    first_token = text.split()[0]
    codes = doublemetaphone(first_token)
    return codes[0] if codes[0] else codes[1]  # Primary or secondary


def phonetic_codes_match(code1: Optional[str], code2: Optional[str]) -> bool:
    """Check if two phonetic codes match (both must be non-empty)."""
    return bool(code1 and code2 and code1 == code2)


def jaro_winkler_similarity(s1: str, s2: str) -> float:
    """Calculate Jaro-Winkler similarity between two strings"""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    len1, len2 = len(s1), len(s2)
    match_distance = max(len1, len2) // 2 - 1
    if match_distance < 0:
        match_distance = 0

    s1_matches = [False] * len1
    s2_matches = [False] * len2
    matches = 0
    transpositions = 0

    # Find matches
    for i in range(len1):
        start = max(0, i - match_distance)
        end = min(i + match_distance + 1, len2)

        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    # Count transpositions
    k = 0
    for i in range(len1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len1 +
        matches / len2 +
        (matches - transpositions / 2) / matches
    ) / 3

    # Winkler adjustment (common prefix)
    prefix_len = 0
    for i in range(min(4, min(len1, len2))):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * 0.1 * (1 - jaro)


# DEPRECATED: Similarity thresholds now loaded from schema
# See entity_schema.py get_schema_for_type() for schema-driven thresholds

# Token overlap constants (not type-specific, just thresholds)
MIN_TOKEN_OVERLAP_RATIO = 0.5  # At least 50% of shorter entity's tokens must match
MIN_TOKEN_OVERLAP_COUNT = 2    # At least 2 tokens must match (for 2+ token entities)


def compute_token_overlap(text1: str, text2: str) -> Tuple[float, int]:
    """
    Compute token (word) overlap between two texts.

    Returns: (overlap_ratio, overlap_count)
    - overlap_ratio: proportion of shorter text's tokens found in longer text
    - overlap_count: number of matching tokens
    """
    tokens1 = set(text1.lower().split())
    tokens2 = set(text2.lower().split())

    # Find intersection
    overlap = tokens1 & tokens2
    overlap_count = len(overlap)

    # Compute ratio based on shorter text
    shorter_len = min(len(tokens1), len(tokens2))
    if shorter_len == 0:
        return 0.0, 0

    overlap_ratio = overlap_count / shorter_len
    return overlap_ratio, overlap_count


# =============================================================================
# Relationship-Aware Context Relevance
# =============================================================================

class RelevanceSignal(Enum):
    """Signal from relationship-based context relevance check."""
    POSITIVE = "positive"      # Has relevant relationship
    NEGATIVE = "negative"      # Candidate HAS relationships, but NONE are relevant
    UNKNOWN = "unknown"        # Candidate has no relationships (data incomplete)


@dataclass
class RelevanceResult:
    """Result of context relevance check."""
    signal: RelevanceSignal
    score: float
    details: str


# Predicates that connect people to projects (not affiliated_with which is person→org)
PROJECT_RELEVANCE_PREDICATES = ('involves_person', 'founded', 'has_founder', 'attended')


async def resolve_entity_to_uri(
    conn: asyncpg.Connection,
    entity_name: str,
    entity_type: Optional[str] = None
) -> Optional[str]:
    """
    Resolve an entity name to its canonical URI.

    Args:
        conn: Database connection
        entity_name: Entity name to resolve
        entity_type: Optional type filter

    Returns:
        Canonical URI or None if not found
    """
    normalized = normalize_entity_text(entity_name)
    if entity_type:
        return await conn.fetchval("""
            SELECT fuseki_uri FROM entity_registry
            WHERE normalized_text = $1 AND entity_type = $2
            AND merged_into IS NULL
            LIMIT 1
        """, normalized, entity_type)
    else:
        return await conn.fetchval("""
            SELECT fuseki_uri FROM entity_registry
            WHERE normalized_text = $1
            AND merged_into IS NULL
            LIMIT 1
        """, normalized)


async def check_context_relevance(
    conn: asyncpg.Connection,
    candidate_uri: str,
    context: ResolutionContext
) -> RelevanceResult:
    """
    Check if candidate has relationships relevant to the resolution context.

    Returns:
    - POSITIVE: Candidate is connected to project/orgs (boost score)
    - NEGATIVE: Candidate has relationships, but none are relevant (penalize)
    - UNKNOWN: Candidate has no relationships (no penalty - data incomplete)
    """
    # First, check if candidate has ANY relationships
    has_any_relationships = await conn.fetchval("""
        SELECT EXISTS(
            SELECT 1 FROM entity_relationships
            WHERE subject_uri = $1 OR object_uri = $1
        )
    """, candidate_uri)

    if not has_any_relationships:
        # No relationships = data incomplete, don't penalize
        return RelevanceResult(RelevanceSignal.UNKNOWN, 0.0, "no relationships in DB")

    # Check connection to meeting's project
    if context.project:
        project_uri = await resolve_entity_to_uri(conn, context.project, 'Project')
        if project_uri:
            connected = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM entity_relationships
                    WHERE ((subject_uri = $1 AND object_uri = $2)
                           OR (subject_uri = $2 AND object_uri = $1))
                    AND predicate = ANY($3)
                )
            """, candidate_uri, project_uri, list(PROJECT_RELEVANCE_PREDICATES))
            if connected:
                return RelevanceResult(RelevanceSignal.POSITIVE, 0.3, f"connected to project")

    # Check connection to mentioned organizations
    # Based on actual data format (verified from DB):
    #   - affiliated_with: Person (subj) → Org (obj)
    #   - has_founder: Person (subj) → Org (obj) - from org's founders: field (parser uses 'incoming' direction)
    #   - founded: Person (subj) → Org (obj) - from person's founder: field
    #   - involves_person: Org/Project (subj) → Person (obj)
    orgs = context.organizations or context.associated_orgs or []
    if orgs:
        for org_name in orgs:
            org_uri = await resolve_entity_to_uri(conn, org_name, 'Organization')
            if org_uri:
                connected = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM entity_relationships
                        WHERE (
                            -- person→org predicates (person is subject)
                            (subject_uri = $1 AND predicate IN ('affiliated_with', 'founded', 'has_founder') AND object_uri = $2)
                            -- org→person predicates (person is object)
                            OR (subject_uri = $2 AND predicate = 'involves_person' AND object_uri = $1)
                        )
                    )
                """, candidate_uri, org_uri)
                if connected:
                    return RelevanceResult(RelevanceSignal.POSITIVE, 0.2, f"affiliated with {org_name}")

    # Try 2-hop path for person → org → project chains
    # Path: Person -[has_founder]→ Org -[has_project]→ Project
    # Or: Project -[involves_person]→ Person (direct link to project)
    if context.project:
        project_uri = await resolve_entity_to_uri(conn, context.project, 'Project')
        if project_uri:
            # Check direct involves_person link first
            direct_project = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM entity_relationships
                    WHERE subject_uri = $2 AND predicate = 'involves_person' AND object_uri = $1
                )
            """, candidate_uri, project_uri)

            if direct_project:
                return RelevanceResult(
                    signal=RelevanceSignal.POSITIVE,
                    score=0.25,
                    details=f"member of project {context.project}"
                )

            # 2-hop: Person -[affiliation/founded/has_founder]→ Org -[has_project]→ Project
            # All person→org predicates: person is subject, org is object
            two_hop = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT 1 FROM entity_relationships er1
                    JOIN entity_relationships er2 ON er1.object_uri = er2.subject_uri
                    WHERE er1.subject_uri = $1
                      AND er1.predicate IN ('affiliated_with', 'founded', 'has_founder')
                      AND er2.predicate = 'has_project'
                      AND er2.object_uri = $2
                )
            """, candidate_uri, project_uri)

            if two_hop:
                return RelevanceResult(
                    signal=RelevanceSignal.POSITIVE,
                    score=0.1,
                    details=f"2-hop path via org to {context.project}"
                )

    # Candidate HAS relationships but NONE match context = negative signal
    return RelevanceResult(RelevanceSignal.NEGATIVE, -0.15, "has relationships, none relevant")


async def check_fallback_relevance(
    conn: asyncpg.Connection,
    candidate_uri: str,
    context: ResolutionContext
) -> float:
    """
    Fallback: Use document_entity_links when relationships are sparse.

    If candidate appears in same documents as context entities, that's a weak positive signal.
    """
    if not context.associated_people:
        return 0.0

    # Check if candidate co-occurs with associated people in documents
    people_uris = []
    for person in context.associated_people:
        uri = await resolve_entity_to_uri(conn, person, 'Person')
        if uri:
            people_uris.append(uri)

    if not people_uris:
        return 0.0

    # Count shared documents
    shared_docs = await conn.fetchval("""
        SELECT COUNT(DISTINCT d1.document_rid)
        FROM document_entity_links d1
        JOIN document_entity_links d2 ON d1.document_rid = d2.document_rid
        WHERE d1.entity_uri = $1
        AND d2.entity_uri = ANY($2)
    """, candidate_uri, people_uris)

    if shared_docs and shared_docs > 0:
        return min(shared_docs * 0.05, 0.15)  # Cap at 0.15

    return 0.0


async def lookup_entity(
    conn: asyncpg.Connection,
    name: str,
    entity_type: Optional[str] = None,
) -> Optional[str]:
    """Read-only entity lookup. Returns the canonical URI (fuseki_uri) if the
    (name, type) pair matches an existing entity via exact or alias match; None
    otherwise.

    Used by the agentic crawler to populate ``proposal.entities[].existing_rid``
    at crawl time so the curator sees what will be matched vs. created. No rows
    are written — this is strictly a SELECT.
    """
    normalized = normalize_entity_text(name)

    if entity_type:
        exact = await conn.fetchrow(
            """
            SELECT fuseki_uri
            FROM entity_registry
            WHERE normalized_text = $1 AND entity_type = $2
            AND merged_into IS NULL
            LIMIT 1
            """,
            normalized,
            entity_type,
        )
    else:
        exact = await conn.fetchrow(
            """
            SELECT fuseki_uri
            FROM entity_registry
            WHERE normalized_text = $1
            AND merged_into IS NULL
            LIMIT 1
            """,
            normalized,
        )
    if exact:
        return exact["fuseki_uri"]

    normalized_alias = normalize_alias(name)
    if entity_type:
        alias = await conn.fetchrow(
            """
            SELECT fuseki_uri
            FROM entity_registry
            WHERE entity_type = $1 AND $2 = ANY(aliases)
            AND merged_into IS NULL
            LIMIT 1
            """,
            entity_type,
            normalized_alias,
        )
    else:
        alias = await conn.fetchrow(
            """
            SELECT fuseki_uri
            FROM entity_registry
            WHERE $1 = ANY(aliases)
            AND merged_into IS NULL
            LIMIT 1
            """,
            normalized_alias,
        )
    return alias["fuseki_uri"] if alias else None


async def is_vetoed_match(
    conn: asyncpg.Connection, query_normalized: str, candidate_uri: str
) -> Optional[str]:
    """Return the veto reason if resolving this query to this candidate is forbidden.

    entity_non_match records that two REGISTRY ROWS are different real things.
    Resolution compares a NAME to a candidate row, so the two need bridging.

    THE OBVIOUS BRIDGE DOES NOT WORK, and it fails silently. Synthesising the
    query's URI with generate_entity_uri(name, type) and looking that up misses
    28% of the register: measured 2026-09-02, 21 of 75 vetoed URIs do not
    reproduce from their current (name, type) -- retyped since creation, renamed,
    or in the `org:` namespace entirely. Among the missed were Salt Spring
    Digital Ecologies and Raven Trust, two of the five operator overrides, i.e.
    exactly the rows that exist in no corpus and cannot be re-derived.

    So the bridge is the REGISTRY, not a hash: is there a LIVE row carrying the
    query's normalized name that is vetoed against this candidate? That is the
    real question, it uses stored rows rather than a reconstruction, and it
    needs no URI to be reproducible.
    """
    # entity_non_match arrives with migration 117. A deployment or scratch schema
    # without it must not break RESOLUTION -- but it must not silently lose the
    # veto either, which is the fail-open pattern this whole guard exists to
    # oppose. So: tolerate the absence, and say so LOUDLY, once per process.
    # Probed PER CALL, not cached in a module global. A global would be a
    # process-wide fact about a per-CONNECTION property: tests (and any
    # multi-database process) hold connections to different databases, so a
    # True cached from one is simply wrong for the next -- which is exactly how
    # this broke test_entity_retype on first attempt.
    #
    # The cost is one extra round trip, and it is small in practice because
    # resolve_entity RETURNS at the first accept: this runs once or twice per
    # resolution, not once per tier.
    # Unqualified on purpose: to_regclass('public.x') resolves in public, while
    # the query below resolves through search_path. A caller using an isolated
    # schema (the retype tests do) would get "present" from the probe and then
    # UndefinedTableError from the query -- the probe must ask the same question
    # the query does.
    if not await conn.fetchval("SELECT to_regclass('entity_non_match') IS NOT NULL"):
        logger.warning(
            "entity_non_match is ABSENT (migration 117 not applied to this database) -- "
            "the operator-adjudicated do-not-merge vetoes are NOT enforced here. "
            "Resolution continues without them.")
        return None

    row = await conn.fetchrow(
        """
        SELECT n.reason, n.asserted_by, e.fuseki_uri AS query_uri
        FROM entity_registry e
        JOIN entity_non_match n
          ON (n.uri_lo = e.fuseki_uri AND n.uri_hi = $2)
          OR (n.uri_hi = e.fuseki_uri AND n.uri_lo = $2)
        WHERE e.normalized_text = $1
          AND e.merged_into IS NULL
          AND e.fuseki_uri <> $2
        LIMIT 1
        """,
        query_normalized, candidate_uri,
    )
    if row is None:
        return None
    return f"{row['reason']} [asserted_by: {row['asserted_by']}]"


class GeneratedUriHeldError(Exception):
    """A force_type create whose deterministic URI is held by an entity of another type.

    force_type exists to create a same-named entity of THIS type, but the URI is a
    pure hash of (type, name), so when it belongs to a tombstone merged into a
    different-type survivor no create of this type is possible. Returning the
    survivor would silently break that contract, so register-entity reports a 409.
    """

    def __init__(self, entity: ExtractedEntity, new_uri: str, holder: "CanonicalEntity"):
        self.new_uri = new_uri
        self.holder = holder
        super().__init__(
            f"'{entity.name}' cannot be registered as {entity.type}: its URI {new_uri} "
            f"was merged into {holder.uri} ({holder.type}). force_type cannot create "
            f"a second {entity.type} with this name. Unmerge {new_uri}, or register "
            f"without force_type to link to {holder.uri}."
        )


async def _existing_holder_of_generated_uri(
    conn: asyncpg.Connection, entity: ExtractedEntity, new_uri: str,
    skip_cross_type: bool = False,
) -> Optional[CanonicalEntity]:
    """The live entity that already holds a create's deterministic URI, or None.

    generate_entity_uri is a pure hash of (type, normalized name), so a merged-away
    row keeps its URI forever. Every tier above matches live rows only, so a name
    whose own URI is a tombstone falls through to create, store_new_entity's
    ON CONFLICT (fuseki_uri) DO NOTHING no-ops, and /ingest then linked the document
    to the tombstone and emitted a NEW event for it (81 links repointed by SQL on
    2026-10-12; "KOI" as Concept while concept-koi was merged away). The collision is
    evidence the name belongs to the survivor, so return the survivor, not a create.

    With skip_cross_type (register-entity force_type) a survivor of another type
    raises GeneratedUriHeldError instead of being returned.
    """
    row = await conn.fetchrow(
        "SELECT entity_text, entity_type, merged_into FROM entity_registry WHERE fuseki_uri = $1",
        new_uri,
    )
    if row is None:
        return None
    uri = new_uri
    if row["merged_into"]:
        uri = await resolve_to_live_uri(conn, new_uri)
        row = await conn.fetchrow(
            "SELECT entity_text, entity_type, merged_into FROM entity_registry WHERE fuseki_uri = $1",
            uri,
        )
        if row is None or row["merged_into"]:
            # Broken chain or cycle: resolve_to_live_uri stopped short. Still not a
            # create (the URI is taken); report what it reached and say so.
            logger.warning("generated URI %s is a tombstone whose chain ends at %s, "
                           "which is not a live row", new_uri, uri)
    name = row["entity_text"] if row else entity.name
    holder = CanonicalEntity(
        name=name,
        uri=uri,
        type=(row["entity_type"] if row else None) or entity.type,
        is_new=False,
        merged_with=entity.name if name != entity.name else None,
        confidence=1.0,
    )
    if skip_cross_type and holder.type != entity.type:
        raise GeneratedUriHeldError(entity, new_uri, holder)
    return holder


async def _fetchrow_hnsw(conn: asyncpg.Connection, sql: str, *args,
                         statement_timeout_ms: Optional[int] = None):
    """Run one filtered HNSW query with pgvector iterative scan, leaking no settings.

    A filtered HNSW scan without hnsw.iterative_scan stops after ef_search (40)
    candidates and returns zero rows when none of them pass the WHERE filters
    (migration 109 measured this). personal_koi sets relaxed_order per database,
    but personal_koi_test and a fresh node do not, so the query must not depend on
    that. SET LOCAL lasts until the end of the enclosing transaction, and inside a
    savepoint it survives RELEASE, so the read runs in its own transaction or
    savepoint which is then rolled back: that undoes the SET in every caller
    context (bare pooled connection, /ingest's per-entity savepoint, a caller's
    transaction). The statement is read-only, so the rollback discards nothing.

    plan_cache_mode = force_custom_plan: asyncpg caches the prepared statement per
    pooled connection, and after 5 executions PostgreSQL may switch to a generic
    plan that cannot see the entity_type value. That plan is always the HNSW scan
    with `entity_type = $2` as a post-filter, so a rare type walks up to
    max_scan_tuples (~18k) index tuples: 8-43 s under load on personal_koi, where
    the custom plan picks a btree, bitmap or seq scan plus exact sort in
    milliseconds. Custom planning per call costs a few ms and keeps that choice.

    statement_timeout_ms, when given, caps this one statement server-side (SET LOCAL,
    undone by the same rollback).
    """
    tx = conn.transaction()
    await tx.start()
    try:
        await conn.execute("SET LOCAL hnsw.iterative_scan = relaxed_order")
        await conn.execute("SET LOCAL plan_cache_mode = force_custom_plan")
        if statement_timeout_ms is not None:
            await conn.execute(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")
        return await conn.fetchrow(sql, *args)
    finally:
        await tx.rollback()


# Tier 2b semantic match: the single most similar live row (of the type, or of any
# type) whose cosine similarity exceeds the threshold, or none. Reads embedding_3072
# (post-2026-04-23 OpenAI 3072-dim migration); the halfvec cast is required because
# pgvector full-precision vector ANN caps at 2000 dims.
#
# Only the inner ORDER BY <raw distance> LIMIT can use
# idx_entity_registry_embedding_3072_hnsw. The previous form ordered by the computed
# similarity with the threshold in WHERE, which the planner cannot serve from the
# index: it sorted every live row of the type (2-58 s under load, the whole table
# without a type) and /ingest of an unmatched name hit the pool's 60 s
# command_timeout. iterative_scan relaxed_order returns rows only approximately
# sorted, so the outer ORDER BY re-sorts the candidates (the knowledge_router
# convention), and 10 candidates rather than 1 leave room for that mis-ranking.
# Recall against the exact form: tests/test_tier2b_semantic_hnsw.py docstring.
_TIER2B_SQL_TYPED = """
    WITH hits AS (
        SELECT id, fuseki_uri, entity_text, entity_type,
               1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
        FROM entity_registry
        WHERE embedding_3072 IS NOT NULL
          AND entity_type = $2
          AND merged_into IS NULL
        ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
        LIMIT 10
    )
    SELECT * FROM hits
    WHERE similarity > $3
    ORDER BY similarity DESC
    LIMIT 1
"""
_TIER2B_SQL_ANY_TYPE = """
    WITH hits AS (
        SELECT id, fuseki_uri, entity_text, entity_type,
               1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
        FROM entity_registry
        WHERE embedding_3072 IS NOT NULL
          AND merged_into IS NULL
        ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
        LIMIT 10
    )
    SELECT * FROM hits
    WHERE similarity > $2
    ORDER BY similarity DESC
    LIMIT 1
"""

# The exact typed form: identical to _TIER2B_SQL_TYPED except the inner ORDER BY is
# the computed similarity, which the HNSW opclass cannot serve, so the planner uses a
# btree, bitmap or seq scan of the type plus an exact sort. resolve_entity picks it
# for types with at most _TIER2B_EXACT_MAX_ROWS live embedded rows.
#
# Why (personal_koi, 2026-10-12, read-only, load average 15-23): the HNSW form
# post-filters entity_type, so for a mid-size type a query far from it walks
# thousands of index tuples before it collects 10 rows of the type: CaseStudy (374
# rows) 5.8 s, Incident (414) 3.1 s, Claim (2582) 1.6 s with a wrong nearest row,
# Component 1.4 s, Location 1.2 s. The exact form was faster than that worst case for
# every type up to Claim; HNSW was faster from Organization (4490) up.
_TIER2B_SQL_TYPED_EXACT = """
    WITH hits AS (
        SELECT id, fuseki_uri, entity_text, entity_type,
               1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
        FROM entity_registry
        WHERE embedding_3072 IS NOT NULL
          AND entity_type = $2
          AND merged_into IS NULL
        ORDER BY similarity DESC
        LIMIT 10
    )
    SELECT * FROM hits
    WHERE similarity > $3
    ORDER BY similarity DESC
    LIMIT 1
"""
# Measured exact/HNSW crossover lies between 2,582 (Claim) and 4,490 (Organization) rows.
_TIER2B_EXACT_MAX_ROWS = 4000
_TIER2B_COUNT_TTL_S = 600.0
_TIER2B_COUNT_RETRY_S = 60.0
_TIER2B_COUNT_TIMEOUT_MS = 5000
# Heap-only GROUP BY (it never reads the TOASTed vectors): 155 ms at load average 23.
# One row, so it can run through _fetchrow_hnsw and its savepoint. It counts the
# refreshing caller's own uncommitted rows too, which is immaterial at this threshold.
_TIER2B_TYPE_COUNTS_SQL = """
    SELECT array_agg(entity_type) AS types, array_agg(n) AS counts
    FROM (
        SELECT entity_type, count(*) AS n
        FROM entity_registry
        WHERE embedding_3072 IS NOT NULL
          AND merged_into IS NULL
          AND entity_type IS NOT NULL
        GROUP BY entity_type
    ) s
"""
_tier2b_type_counts: Dict[str, int] = {}
_tier2b_counts_expire_at = 0.0
_tier2b_counts_failing = False
_tier2b_counts_lock = asyncio.Lock()


async def _tier2b_live_count(conn: asyncpg.Connection, entity_type: str) -> int:
    """Cached count of live embedded entity_registry rows of entity_type.

    Refreshed at most every _TIER2B_COUNT_TTL_S, by one caller at a time: while a
    refresh is in flight other callers use the current counts rather than wait. A type
    the cache does not know, and every type before the first successful refresh,
    count as 0, which selects the exact form: always correct, at worst slower. A
    failed refresh keeps the last good counts (types grow by tens of rows a day, and
    the count query fails mostly when the database is overloaded, which is when
    sending Concept and Person to the exact sort would cost most), is logged once per
    failing streak, and is retried after _TIER2B_COUNT_RETRY_S. It runs through
    _fetchrow_hnsw with its own statement timeout, so a failure is rolled back in its
    own savepoint and leaves the caller's transaction usable.
    """
    global _tier2b_type_counts, _tier2b_counts_expire_at, _tier2b_counts_failing
    if time.monotonic() >= _tier2b_counts_expire_at and not _tier2b_counts_lock.locked():
        async with _tier2b_counts_lock:
            try:
                row = await _fetchrow_hnsw(conn, _TIER2B_TYPE_COUNTS_SQL,
                                           statement_timeout_ms=_TIER2B_COUNT_TIMEOUT_MS)
            except Exception as e:
                _tier2b_counts_expire_at = time.monotonic() + _TIER2B_COUNT_RETRY_S
                if not _tier2b_counts_failing:
                    logger.warning("Tier 2b type-count refresh failed; keeping the last "
                                   "counts (%d types) until it succeeds: %s",
                                   len(_tier2b_type_counts), e)
                _tier2b_counts_failing = True
            else:
                _tier2b_type_counts = dict(zip(row["types"] or [], row["counts"] or []))
                _tier2b_counts_expire_at = time.monotonic() + _TIER2B_COUNT_TTL_S
                if _tier2b_counts_failing:
                    logger.info("Tier 2b type-count refresh recovered")
                _tier2b_counts_failing = False
    return _tier2b_type_counts.get(entity_type, 0)


async def resolve_entity(
    conn: asyncpg.Connection,
    entity: ExtractedEntity,
    context: Optional[ResolutionContext] = None,
    skip_fuzzy: bool = False,
    skip_cross_type: bool = False,
    resolution_caller: str = "personal_ingest_api.resolve_entity",
) -> Tuple[CanonicalEntity, bool]:
    """
    Resolve an entity against the knowledge base.

    Resolution Tiers:
    - Tier 1: Exact match (normalized text)
    - Tier 1.5: Contextual co-occurrence match (all entity types with phonetic boost for Person)
    - Tier 2a: Fuzzy match (Jaro-Winkler with token overlap check)
    - Tier 2b: Semantic match (OpenAI embeddings + pgvector)
    - Tier 3: Create new entity

    Args:
        conn: Database connection
        entity: The entity to resolve
        context: Optional disambiguation context (associated_people)
        skip_fuzzy: When true, use Tier 1 exact only; on exact miss, create a
            new entity instead of consulting alias/contextual/fuzzy/semantic tiers.
        skip_cross_type: When true, suppress ONLY the Tier 1.1b type-agnostic
            cross-type dedup fallback (see register-entity ``force_type``). All
            other tiers run normally.
        resolution_caller: Stable entrypoint label for behavior-neutral shadow
            measurement. It has no effect on the returned resolution.

    Returns: (CanonicalEntity, is_new)
    """
    # Durable (unsampled) accept/reject/create log for this call -- see
    # api/resolver_decisions_log.py. Correlates every row logged below.
    resolver_attempt_id = uuid.uuid4().hex
    normalized = normalize_entity_text(entity.name)

    # Get schema-driven config for this entity type
    schema = get_schema_for_type(entity.type)
    threshold = schema.similarity_threshold

    # Tier 1: Exact match (normalized text)
    if entity.type:
        exact_match = await conn.fetchrow("""
            SELECT id, fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE normalized_text = $1
            AND entity_type = $2
            AND merged_into IS NULL
            LIMIT 1
        """, normalized, entity.type)
    else:
        exact_match = await conn.fetchrow("""
            SELECT id, fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE normalized_text = $1
            AND merged_into IS NULL
            LIMIT 1
        """, normalized)

    if exact_match:
        _veto = await is_vetoed_match(conn, normalized, exact_match['fuseki_uri'])
        if _veto:
            logger.info("Tier 1 exact match VETOED: '%s' -/-> '%s' (%s)",
                        entity.name, exact_match['entity_text'], _veto[:140])
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier1_exact", decision="rejected",
                candidate_uri=exact_match['fuseki_uri'],
                candidate_text=exact_match['entity_text'],
                score=1.0, reason=f"entity_non_match veto: {_veto[:160]}",
            )
            exact_match = None

    if exact_match:
        _log_resolver_decision(
            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
            tier="tier1_exact", decision="accepted",
            candidate_uri=exact_match['fuseki_uri'], candidate_text=exact_match['entity_text'],
            score=1.0, reason="normalized_text exact match",
        )
        return CanonicalEntity(
            name=exact_match['entity_text'],
            uri=exact_match['fuseki_uri'],
            type=exact_match['entity_type'] or entity.type,
            is_new=False,
            merged_with=entity.name if exact_match['entity_text'] != entity.name else None,
            confidence=1.0
        ), False

    if skip_fuzzy:
        logger.info("Exact-only resolution miss for '%s' (%s); creating new entity", entity.name, entity.type)
        new_uri = generate_entity_uri(entity.name, entity.type)
        holder = await _existing_holder_of_generated_uri(
            conn, entity, new_uri, skip_cross_type=skip_cross_type)
        if holder:
            logger.info("Exact-only create for '%s' collides with %s; using %s",
                        entity.name, new_uri, holder.uri)
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier3_create", decision="accepted",
                candidate_uri=holder.uri, candidate_text=holder.name,
                score=1.0, reason=f"generated URI already held ({new_uri})",
            )
            return holder, False
        _log_resolver_decision(
            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
            tier="tier3_create", decision="created",
            candidate_uri=new_uri, reason="skip_fuzzy exact-only miss",
        )
        return CanonicalEntity(
            name=entity.name,
            uri=new_uri,
            type=entity.type,
            is_new=True,
            confidence=entity.confidence,
        ), True

    # Tier 1.1: Alias match (check if input matches any registered alias)
    # Uses normalized name to search against TEXT[] aliases column
    normalized_name = normalize_alias(entity.name)

    if entity.type:
        alias_match = await conn.fetchrow("""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE entity_type = $1
            AND $2 = ANY(aliases)
            AND merged_into IS NULL
            LIMIT 1
        """, entity.type, normalized_name)
    else:
        # Type-agnostic alias lookup (when type_hint not provided)
        # Risk: may return wrong entity if alias is reused across types
        logger.warning(f"Type-agnostic alias lookup for '{entity.name}' - consider providing type_hint")
        alias_match = await conn.fetchrow("""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE $1 = ANY(aliases)
            AND merged_into IS NULL
            LIMIT 1
        """, normalized_name)

    if alias_match:
        _veto = await is_vetoed_match(conn, normalized, alias_match['fuseki_uri'])
        if _veto:
            logger.info("Tier 1.1 alias match VETOED: '%s' -/-> '%s' (%s)",
                        entity.name, alias_match['entity_text'], _veto[:140])
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier1_1_alias", decision="rejected",
                candidate_uri=alias_match['fuseki_uri'],
                candidate_text=alias_match['entity_text'],
                score=1.0, reason=f"entity_non_match veto: {_veto[:160]}",
            )
            alias_match = None

    if alias_match:
        # Alias match = Tier-1 exact (short-circuit, don't enter contextual pool)
        logger.info(f"Tier 1.1 alias match: '{entity.name}' → '{alias_match['entity_text']}'")
        _log_resolver_decision(
            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
            tier="tier1_1_alias", decision="accepted",
            candidate_uri=alias_match['fuseki_uri'], candidate_text=alias_match['entity_text'],
            score=1.0, reason="registered alias match",
        )
        return CanonicalEntity(
            name=alias_match["entity_text"],
            uri=alias_match["fuseki_uri"],
            type=alias_match["entity_type"] or entity.type,
            is_new=False,
            merged_with=entity.name if alias_match["entity_text"] != entity.name else None,
            confidence=1.0,
        ), False

    # Tier 1.1b: Type-agnostic exact/alias fallback (cross-type dedup guard).
    # The type-gated Tier 1 + Tier 1.1 above can MISS a live survivor of a
    # DIFFERENT type that carries this name as its canonical text OR a registered
    # alias — e.g. an LLM extractor labels "Polis" as Concept/Project while the
    # survivor is schema:SoftwareApplication with alias "polis". Without this,
    # the type-gated tiers all miss and Tier 3 mints a fresh same-typed duplicate
    # on every ingest (observed recurring for Polis / Pol.is / Talk to the City /
    # Notion). Fall back to a TYPE-AGNOSTIC exact-or-alias lookup, but accept it
    # ONLY when EXACTLY ONE live (non-tombstoned) entity matches — otherwise the
    # name/alias is ambiguous across types (genuine polysemy) and we must NOT
    # guess; we fall through to fuzzy/semantic/create as before. This is no looser
    # than the pre-existing type-agnostic branch used when type_hint is absent
    # (lines above), and is strictly tighter (exactly-one + merged_into IS NULL).
    # Suppressed when skip_cross_type is set (register-entity force_type) so an
    # intentionally-distinct same-named entity of this type can be created.
    if entity.type and not skip_cross_type:
        cross_type_rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry
            WHERE merged_into IS NULL
              AND (normalized_text = $1 OR $2 = ANY(aliases))
            LIMIT 2
        """, normalized, normalized_name)

        if len(cross_type_rows) == 1 and await is_vetoed_match(
                conn, normalized, cross_type_rows[0]["fuseki_uri"]):
            _veto = await is_vetoed_match(conn, normalized, cross_type_rows[0]["fuseki_uri"])
            logger.info("Tier 1.1b cross-type match VETOED: '%s' -/-> '%s' (%s)",
                        entity.name, cross_type_rows[0]["entity_text"], _veto[:140])
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier1_1b_cross_type", decision="rejected",
                candidate_uri=cross_type_rows[0]["fuseki_uri"],
                candidate_text=cross_type_rows[0]["entity_text"],
                score=1.0, reason=f"entity_non_match veto: {_veto[:160]}",
            )
            cross_type_rows = []

        if len(cross_type_rows) == 1:
            row = cross_type_rows[0]
            logger.info(
                "Tier 1.1b cross-type match: '%s' (hint=%s) → '%s' (%s)",
                entity.name, entity.type, row["entity_text"], row["entity_type"],
            )
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier1_1b_cross_type", decision="accepted",
                candidate_uri=row['fuseki_uri'], candidate_text=row['entity_text'],
                score=1.0, reason=f"unique cross-type exact/alias match (found_type={row['entity_type']})",
            )
            return CanonicalEntity(
                name=row["entity_text"],
                uri=row["fuseki_uri"],
                type=row["entity_type"] or entity.type,
                is_new=False,
                merged_with=entity.name if row["entity_text"] != entity.name else None,
                confidence=1.0,
            ), False

    # Tier 1.5: Contextual co-occurrence match (ALL entity types, with phonetic boost)
    # Requirements:
    # - min_context_people from schema (default: Person=1, others=2)
    # - Two-tier threshold:
    #   - With phonetic match: combined_score ≥0.6 (phonetic is strong evidence)
    #   - Without phonetic match: combined_score ≥0.75 (stricter to avoid false positives)
    if context and context.associated_people:
        min_people = schema.min_context_people

        if len(context.associated_people) >= min_people:
            logger.info(f"Tier 1.5: Trying contextual match for '{entity.name}' ({entity.type}) "
                       f"with {len(context.associated_people)} associated people")

            contextual_candidates = await get_contextual_entity_candidates(
                conn,
                entity.name,
                entity.type,
                context.associated_people,
                context  # Pass full context for relationship checking
            )

            if contextual_candidates:
                best = contextual_candidates[0]
                has_phonetic = best.get('phonetic_match', False)

                # Token overlap guard for multi-token names (prevents "Silke Helfrich" -> "Soren Greywell")
                # If both names have 2+ tokens and share zero tokens, reject regardless of score
                best_norm = best.get('normalized_text', best['name'])
                query_tokens = set(normalized.lower().split())
                candidate_tokens = set(best_norm.lower().split())
                token_overlap = query_tokens & candidate_tokens
                if len(query_tokens) >= 2 and len(candidate_tokens) >= 2 and len(token_overlap) == 0:
                    logger.info(f"Tier 1.5 contextual match REJECTED (zero token overlap): "
                               f"'{entity.name}' -> '{best['name']}' "
                               f"(tokens: {query_tokens} vs {candidate_tokens})")
                    _log_resolver_decision(
                        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                        entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                        tier="tier1_5_contextual", decision="rejected",
                        candidate_uri=best.get('uri'), candidate_text=best.get('name'),
                        score=best.get("combined_score"), reason="zero token overlap",
                    )
                elif entity.type == "Person" and not passes_person_name_guard(normalized, best_norm):
                    # Person name guard: a bare first name must not collapse into
                    # a full name (or a different full name) on context alone.
                    # Registered aliases resolve at Tier 1.1 before this runs.
                    logger.info(f"Tier 1.5 contextual match REJECTED (person name guard): "
                               f"'{entity.name}' -> '{best['name']}'")
                    _log_resolver_decision(
                        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                        entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                        tier="tier1_5_contextual", decision="rejected",
                        candidate_uri=best.get('uri'), candidate_text=best.get('name'),
                        score=best.get("combined_score"), reason="person name guard",
                    )
                else:
                    # Two-tier threshold: phonetic matches get lower bar (strong evidence)
                    # Non-phonetic matches need higher score to avoid false positives
                    threshold_phonetic = 0.6      # "Brixley" -> "Brixly" (same sound)
                    threshold_no_phonetic = 0.75  # Stricter: avoid "Maddox" -> "Mabel Sedgewick"

                    effective_threshold = threshold_phonetic if has_phonetic else threshold_no_phonetic

                    _veto = await is_vetoed_match(conn, normalized, best["uri"])
                    if _veto:
                        logger.info("Tier 1.5 contextual match VETOED: '%s' -/-> '%s' (%s)",
                                    entity.name, best["name"], _veto[:140])
                        _log_resolver_decision(
                            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                            entity_type=entity.type, query_text=entity.name,
                            query_normalized=normalized, tier="tier1_5_contextual",
                            decision="rejected", candidate_uri=best["uri"],
                            candidate_text=best["name"], score=best.get("combined_score"),
                            reason=f"entity_non_match veto: {_veto[:160]}",
                        )
                    elif best["combined_score"] >= effective_threshold:
                        logger.info(f"Tier 1.5 contextual match: '{entity.name}' -> '{best['name']}' "
                                   f"(combined_score: {best['combined_score']:.3f}, "
                                   f"phonetic: {has_phonetic}, threshold: {effective_threshold})")
                        _log_resolver_decision(
                            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                            tier="tier1_5_contextual", decision="accepted",
                            candidate_uri=best["uri"], candidate_text=best["name"],
                            score=best["combined_score"],
                            reason=f"phonetic={has_phonetic} threshold={effective_threshold}",
                        )
                        return CanonicalEntity(
                            name=best["name"],
                            uri=best["uri"],
                            type=best.get("entity_type") or entity.type,
                            is_new=False,
                            merged_with=entity.name if best["name"] != entity.name else None,
                            confidence=best["combined_score"]  # Always 0-1 scale
                        ), False
                    else:
                        logger.info(f"Tier 1.5 contextual match REJECTED: '{entity.name}' -> '{best['name']}' "
                                   f"(score: {best['combined_score']:.3f} < threshold: {effective_threshold}, "
                                   f"phonetic: {has_phonetic})")
                        _log_resolver_decision(
                            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                            tier="tier1_5_contextual", decision="rejected",
                            candidate_uri=best.get("uri"), candidate_text=best.get("name"),
                            score=best["combined_score"],
                            reason=f"below threshold={effective_threshold} phonetic={has_phonetic}",
                        )

    # Tier 2a: Fuzzy match (Jaro-Winkler with token overlap check)
    if entity.type:
        candidates = await conn.fetch("""
            SELECT id, fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE entity_type = $1
            AND merged_into IS NULL
        """, entity.type)
    else:
        candidates = await conn.fetch("""
            SELECT id, fuseki_uri, entity_text, entity_type, normalized_text
            FROM entity_registry
            WHERE merged_into IS NULL
        """)

    best_match = None
    best_score = 0.0
    shadow = start_attempt(
        caller=resolution_caller,
        engine="personal_ingest",
        entity_type=entity.type or "",
        query_norm=normalized,
        active_policy="strict",
    )

    for candidate in candidates:
        score = jaro_winkler_similarity(normalized, candidate['normalized_text'])
        cand_norm = candidate['normalized_text']

        # The shadow evaluator reuses this candidate and score. It never repeats
        # the registry scan or embedding query.
        length_rejected = False
        if score >= threshold:
            len_shorter = min(len(normalized), len(cand_norm))
            len_longer = max(len(normalized), len(cand_norm))
            length_rejected = bool(
                len_shorter > 0
                and len_longer / len_shorter > 1.8
                and score < 0.95
            )
            if shadow.sampled and not length_rejected:
                shadow_started = time.perf_counter_ns()
                shadow.observe_candidate(
                    uri=candidate['fuseki_uri'],
                    score=score,
                    tier="fuzzy",
                    legacy_accepts=passes_token_overlap_legacy(
                        normalized, cand_norm, entity.type
                    ),
                    strict_accepts=passes_token_overlap_strict(
                        normalized, cand_norm, entity.type
                    ),
                    elapsed_ns=time.perf_counter_ns() - shadow_started,
                )

        if score >= threshold and score > best_score:
            # Length ratio guard: reject if candidate is much longer (prefix-match inflation)
            # e.g. "regen ai" (8) vs "regen ai bd sprint scope" (24) → ratio 3.0
            len_shorter = min(len(normalized), len(cand_norm))
            len_longer = max(len(normalized), len(cand_norm))
            if length_rejected:
                logger.info(f"Fuzzy match REJECTED (length ratio {len_longer/len_shorter:.1f}x): "
                           f"{entity.name} vs {candidate['entity_text']} | JW={score:.3f}")
                _log_resolver_decision(
                    db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                    entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                    tier="tier2a_fuzzy", decision="rejected",
                    candidate_uri=candidate['fuseki_uri'], candidate_text=candidate['entity_text'],
                    score=score, reason=f"length ratio {len_longer/len_shorter:.1f}x",
                )
                continue

            # Additional check: token overlap for Organization/Project/Concept
            overlap_ratio, overlap_count = compute_token_overlap(normalized, cand_norm)
            logger.info(f"Fuzzy candidate: {entity.name} vs {candidate['entity_text']} | JW={score:.3f} | overlap={overlap_count} ({overlap_ratio:.2f})")
            if not passes_token_overlap_strict(normalized, cand_norm, entity.type):
                logger.info(f"Fuzzy match REJECTED due to low token overlap: {entity.name} vs {candidate['entity_text']}")
                _log_resolver_decision(
                    db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                    entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                    tier="tier2a_fuzzy", decision="rejected",
                    candidate_uri=candidate['fuseki_uri'], candidate_text=candidate['entity_text'],
                    score=score, reason="low token overlap",
                )
                continue
            best_score = score
            best_match = candidate

    if best_match:
        _veto = await is_vetoed_match(conn, normalized, best_match['fuseki_uri'])
        if _veto:
            logger.info("Tier 2a fuzzy match VETOED: '%s' -/-> '%s' (JW=%.3f) (%s)",
                        entity.name, best_match['entity_text'], best_score, _veto[:140])
            _log_resolver_decision(
                db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                tier="tier2a_fuzzy", decision="rejected",
                candidate_uri=best_match['fuseki_uri'],
                candidate_text=best_match['entity_text'], score=best_score,
                reason=f"entity_non_match veto: {_veto[:160]}",
            )
            best_match = None

    if best_match:
        shadow.finish(
            active_uri=best_match['fuseki_uri'],
            active_outcome="fuzzy",
            legacy_fallback="fallthrough_unobserved",
            strict_fallback="fallthrough_unobserved",
        )
        _log_resolver_decision(
            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
            tier="tier2a_fuzzy", decision="accepted",
            candidate_uri=best_match['fuseki_uri'], candidate_text=best_match['entity_text'],
            score=best_score, reason="jaro-winkler >= threshold, passed length + token-overlap guards",
        )
        return CanonicalEntity(
            name=best_match['entity_text'],
            uri=best_match['fuseki_uri'],
            type=best_match['entity_type'] or entity.type,
            is_new=False,
            merged_with=entity.name if best_match['entity_text'] != entity.name else None,
            confidence=best_score
        ), False

    # Tier 2b: Semantic match (embeddings + pgvector)
    if embedding_provider and ENABLE_SEMANTIC_MATCHING:
        embedding = await generate_query_embedding(entity.name)
        if embedding:
            semantic_threshold = schema.semantic_threshold

            if entity.type:
                typed_sql = (
                    _TIER2B_SQL_TYPED
                    if await _tier2b_live_count(conn, entity.type) > _TIER2B_EXACT_MAX_ROWS
                    else _TIER2B_SQL_TYPED_EXACT
                )
                semantic_match = await _fetchrow_hnsw(
                    conn, typed_sql, str(embedding), entity.type, semantic_threshold)
            else:
                semantic_match = await _fetchrow_hnsw(
                    conn, _TIER2B_SQL_ANY_TYPE, str(embedding), semantic_threshold)

            if semantic_match:
                # Name-shape guard: embedding proximity alone conflates
                # same-first-name people, short generic names, and disjoint
                # multi-word names. Reject those and fall through to Tier 3.
                match_norm = normalize_entity_text(semantic_match['entity_text'])
                match_type = semantic_match['entity_type'] or entity.type
                similarity = float(semantic_match['similarity'])
                legacy_accepts = passes_semantic_match_guard_with_policy(
                    match_type,
                    normalized,
                    match_norm,
                    similarity,
                    semantic_threshold,
                    # STRICT since 2026-08-23; see api/resolution_primitives.py Tier 2b.
                    # 17 of 17 measured semantic divergences were legacy accepting a
                    # different-dated meeting. 0% divergence on every other type.
                    passes_token_overlap_strict,
                )
                if shadow.sampled:
                    shadow_started = time.perf_counter_ns()
                    strict_accepts = passes_semantic_match_guard_with_policy(
                        match_type,
                        normalized,
                        match_norm,
                        similarity,
                        semantic_threshold,
                        passes_token_overlap_strict,
                    )
                    shadow.observe_candidate(
                        uri=semantic_match['fuseki_uri'],
                        score=similarity,
                        tier="semantic",
                        legacy_accepts=legacy_accepts,
                        strict_accepts=strict_accepts,
                        elapsed_ns=time.perf_counter_ns() - shadow_started,
                    )
                _veto = await is_vetoed_match(
                    conn, normalized, semantic_match['fuseki_uri'])
                if _veto:
                    logger.info("Tier 2b semantic match VETOED: '%s' -/-> '%s' (%s)",
                                entity.name, semantic_match['entity_text'], _veto[:140])
                    _log_resolver_decision(
                        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                        entity_type=entity.type, query_text=entity.name,
                        query_normalized=normalized, tier="tier2b_semantic",
                        decision="rejected", candidate_uri=semantic_match['fuseki_uri'],
                        candidate_text=semantic_match['entity_text'], score=similarity,
                        reason=f"entity_non_match veto: {_veto[:160]}",
                    )
                elif not legacy_accepts:
                    logger.info(
                        f"Tier 2b REJECTED (name guard): '{entity.name}' -> "
                        f"'{semantic_match['entity_text']}' "
                        f"(similarity: {semantic_match['similarity']:.3f}); creating new entity"
                    )
                    _log_resolver_decision(
                        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                        entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                        tier="tier2b_semantic", decision="rejected",
                        candidate_uri=semantic_match['fuseki_uri'], candidate_text=semantic_match['entity_text'],
                        score=similarity, reason="name-shape guard",
                    )
                else:
                    logger.info(f"Tier 2b semantic match: '{entity.name}' -> '{semantic_match['entity_text']}' "
                               f"(similarity: {semantic_match['similarity']:.3f})")
                    shadow.finish(
                        active_uri=semantic_match['fuseki_uri'],
                        active_outcome="semantic",
                        legacy_fallback="create_unobserved",
                        strict_fallback="create_unobserved",
                    )
                    _log_resolver_decision(
                        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
                        entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
                        tier="tier2b_semantic", decision="accepted",
                        candidate_uri=semantic_match['fuseki_uri'], candidate_text=semantic_match['entity_text'],
                        score=similarity, reason="cosine similarity >= threshold, passed name-shape guard",
                    )
                    return CanonicalEntity(
                        name=semantic_match['entity_text'],
                        uri=semantic_match['fuseki_uri'],
                        type=semantic_match['entity_type'] or entity.type,
                        is_new=False,
                        merged_with=entity.name if semantic_match['entity_text'] != entity.name else None,
                        confidence=float(semantic_match['similarity'])
                    ), False

    # Tier 3: Create new entity
    new_uri = generate_entity_uri(entity.name, entity.type)
    create_outcome = f"create:{new_uri}"
    # Check the holder before the shadow record is finished, so a collision is not
    # recorded as a create of the tombstone URI. Both policies reach the same holder.
    holder = await _existing_holder_of_generated_uri(
        conn, entity, new_uri, skip_cross_type=skip_cross_type)
    if holder:
        held_outcome = f"uri_held:{holder.uri}"
        shadow.finish(
            active_uri=holder.uri,
            active_outcome="uri_held",
            legacy_fallback=held_outcome,
            strict_fallback=held_outcome,
        )
        logger.info("Tier 3 create for '%s' collides with %s; using %s",
                    entity.name, new_uri, holder.uri)
        _log_resolver_decision(
            db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
            entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
            tier="tier3_create", decision="accepted",
            candidate_uri=holder.uri, candidate_text=holder.name,
            score=1.0, reason=f"generated URI already held ({new_uri})",
        )
        return holder, False
    shadow.finish(
        active_uri=new_uri,
        active_outcome="create",
        legacy_fallback=create_outcome,
        strict_fallback=create_outcome,
    )
    _log_resolver_decision(
        db_pool, attempt_id=resolver_attempt_id, caller=resolution_caller,
        entity_type=entity.type, query_text=entity.name, query_normalized=normalized,
        tier="tier3_create", decision="created",
        candidate_uri=new_uri, reason="no tier matched",
    )

    return CanonicalEntity(
        name=entity.name,
        uri=new_uri,
        type=entity.type,
        is_new=True,
        confidence=entity.confidence
    ), True


def generate_entity_uri(name: str, entity_type: str) -> str:
    """Generate a deterministic URI for a new entity"""
    normalized = normalize_entity_text(name)
    # Create a stable hash-based ID
    hash_input = f"{entity_type}:{normalized}"
    hash_id = hashlib.sha256(hash_input.encode()).hexdigest()[:12]

    type_prefix = entity_type.lower()
    safe_name = normalized.replace(' ', '-').replace("'", '')[:50]

    return f"orn:personal-koi.entity:{type_prefix}-{safe_name}-{hash_id}"


async def store_new_entity(
    conn: asyncpg.Connection,
    entity: ExtractedEntity,
    canonical: CanonicalEntity,
    document_rid: str,
    source: str = 'personal-vault',
    origin: Optional[str] = None,
) -> bool:
    """Store a new entity in the registry with embedding and phonetic code.

    Returns True when a row was inserted, False when ON CONFLICT (fuseki_uri)
    DO NOTHING no-op'd because the URI was already taken (a concurrent create of
    the same name, or a merged-away row that resolve_entity did not catch). On a
    no-op nothing was created, so the caller must not report or emit NEW, and the
    TerminusDB outbox is not written for it.

    `origin` sets entity_registry.resolution_tier (migration 111). Precedence:
    if supplied it is used verbatim and the local classification below is
    SKIPPED; if None, this function classifies the create locally. That keeps
    resolver-driven callers at zero lines changed — only bulk importers, which
    traverse no resolver tier, pass an origin ('import').

    SCOPE: this records why THIS ROW was minted. It is not a resolver hit rate —
    store_new_entity runs only on creation, so matching tiers resolve to existing
    rows and stamp nothing. See the column comment in migration 111.
    """
    normalized = normalize_entity_text(entity.name)
    # Canonicalize the persisted type (strip schema:/bkc:, resolve plural/case)
    # so entity_registry never accumulates prefixed/variant type spellings.
    entity_type = canonicalize_entity_type(entity.type) if entity.type else entity.type

    import json as json_module
    metadata = json_module.dumps({
        'mentions': entity.mentions,
        'context': entity.context,
        'confidence': entity.confidence
    })

    # --- migration 111: classify WHY this row is being minted -----------------
    # Only when the caller did not supply an origin. One extra read on the
    # create path; no branch changes, no behaviour change.
    #
    # Threshold is >= 1 live row, NOT >= 2. Two is Tier 1.1b's *decline*
    # threshold (it binds at exactly one, declines at two) — a different
    # question. Here: if ANY live row already carries this name and we are
    # creating anyway, a duplicate has just been minted, and that is the
    # population worth counting.
    resolution_tier = origin
    if resolution_tier is None:
        try:
            async with conn.transaction():  # SAVEPOINT: a failed probe must not
                                            # poison the caller's transaction
                _existing = await conn.fetchval("""
                    -- 111 create-path classification
                    SELECT count(*) FROM entity_registry
                     WHERE merged_into IS NULL AND normalized_text = $1
                """, normalized)
        except Exception as _cls_exc:
            logger.warning(
                "resolution_tier classification failed for %r: %s", entity.name, _cls_exc
            )
            _existing = None
        if _existing is None:
            resolution_tier = None          # unknown stays NULL, never a guess
        elif _existing >= 1:
            resolution_tier = 'tier3_created_ambiguous'
        else:
            resolution_tier = 'tier3_created'

    # Generate embedding for new entity (enables future Tier 2 matching)
    embedding = None
    if embedding_provider and ENABLE_SEMANTIC_MATCHING:
        embedding = await generate_document_embedding(entity.name)
        if embedding:
            logger.info(f"Generated embedding for new entity: {entity.name}")

    # Compute phonetic code for types with phonetic_matching enabled (schema-driven)
    phonetic_code = None
    schema = get_schema_for_type(entity_type)
    if schema.phonetic_matching:
        # Use first significant token (skip stopwords)
        first_token = get_first_significant_token(normalized, schema.phonetic_stopwords)
        phonetic_code = get_phonetic_code(first_token)
        if phonetic_code:
            logger.info(f"Generated phonetic code for new {entity_type}: {entity.name} -> {phonetic_code}")

    if embedding:
        # Writes to embedding_3072 (post-2026-04-23 OpenAI 3072-dim migration).
        # Legacy embedding (1024) column is no longer populated for new rows;
        # existing 1024 vectors remain for back-compat reads but are not authoritative.
        insert_tag = await conn.execute("""
            INSERT INTO entity_registry (
                fuseki_uri, entity_text, entity_type, normalized_text,
                source, first_seen_rid, metadata, embedding_3072, phonetic_code,
                resolution_tier
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::vector(3072), $9, $10)
            ON CONFLICT (fuseki_uri) DO NOTHING
        """,
            canonical.uri,
            entity.name,
            entity_type,
            normalized,
            source,
            document_rid,
            metadata,
            str(embedding),
            phonetic_code,
            resolution_tier
        )
    else:
        insert_tag = await conn.execute("""
            INSERT INTO entity_registry (
                fuseki_uri, entity_text, entity_type, normalized_text,
                source, first_seen_rid, metadata, phonetic_code,
                resolution_tier
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
            ON CONFLICT (fuseki_uri) DO NOTHING
        """,
            canonical.uri,
            entity.name,
            entity_type,
            normalized,
            source,
            document_rid,
            metadata,
            phonetic_code,
            resolution_tier
        )

    # asyncpg's status tag is "INSERT 0 1" on a real insert, "INSERT 0 0" when
    # ON CONFLICT DO NOTHING no-op'd (same parse as knowledge_router._resolve_or_create).
    try:
        inserted = int(insert_tag.split()[-1]) > 0
    except (ValueError, IndexError, AttributeError):
        inserted = True  # unexpected tag shape: don't misreport a real insert as a no-op
    if not inserted:
        logger.warning(
            "store_new_entity: %s already exists (ON CONFLICT no-op); not created, "
            "no outbox write", canonical.uri)
        return False

    # Enqueue entity to TerminusDB outbox (same transaction as PG write)
    await enqueue_outbox(conn, "entity_upsert", {
        "fuseki_uri": canonical.uri,
        "entity_text": entity.name,
        "entity_type": entity_type,
        "normalized_text": normalized,
        "occurrence_count": 0,
        "phonetic_code": phonetic_code or "",
        "aliases": [],
        "created_by": "darren-personal",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "first_seen_rid": document_rid,
    }, rid=canonical.uri, source_rid=document_rid)
    return True


# =============================================================================
# TerminusDB Outbox Helpers
# =============================================================================

_terminusdb_schema_warned = False


def _terminusdb_schema_funcs():
    """The three helpers from scripts.terminusdb.schema, or None when that module is absent.

    The TerminusDB mirror is optional (TERMINUSDB_ENABLED, default false) and its
    schema module may not be installed. Callers run only when the mirror is enabled,
    so a missing module is worth one warning, not an exception that rolls back the
    vault write around it.
    """
    global _terminusdb_schema_warned
    try:
        from scripts.terminusdb.schema import (
            compute_assertion_hash,
            canonical_object_key,
            serialize_object_key,
        )
    except ImportError as e:
        if TERMINUSDB_ENABLED and not _terminusdb_schema_warned:
            logger.warning(
                "TERMINUSDB_ENABLED is on but scripts.terminusdb.schema cannot be imported "
                "(%s); relationship assertions are not enqueued for TerminusDB", e)
            _terminusdb_schema_warned = True
        return None
    return compute_assertion_hash, canonical_object_key, serialize_object_key


def _build_assertion_payload(row: dict, schema_funcs=None) -> dict:
    """Build a complete Assertion payload from an entity_relationships row.

    Computes assertion_hash and normalized_object_key so the outbox payload
    matches the full Assertion schema required by TerminusDB. schema_funcs is
    _terminusdb_schema_funcs()'s result; it is looked up when not given, and an
    absent schema module raises ImportError here.
    """
    funcs = schema_funcs or _terminusdb_schema_funcs()
    if funcs is None:
        raise ImportError("scripts.terminusdb.schema is not available")
    compute_assertion_hash, canonical_object_key, serialize_object_key = funcs

    subject = row["subject_uri"]
    predicate = row["predicate"]
    object_uri = row["object_uri"]
    source = row.get("source") or "personal-vault"
    source_rid = row.get("source_rid") or ""
    source_field = row.get("source_field") or ""
    raw_value = row.get("raw_value") or ""
    confidence = float(row.get("confidence") or 1.0)

    ahash = compute_assertion_hash(
        subject_uri=subject,
        predicate=predicate,
        object_kind="entity",
        object_uri=object_uri,
        literal_value="",
        literal_datatype="",
        literal_lang="",
        source=source,
        source_rid=source_rid,
        source_field=source_field,
        asserted_by="darren-personal",
    )

    assertion_dict = {
        "object_kind": "entity",
        "object_uri": object_uri,
        "literal_value": "",
        "literal_datatype": "",
        "literal_lang": "",
    }
    norm_key = serialize_object_key(canonical_object_key(assertion_dict))

    return {
        "assertion_hash": ahash,
        "subject_uri": subject,
        "predicate": predicate,
        "object_kind": "entity",
        "object_uri": object_uri,
        "literal_value": "",
        "literal_datatype": "",
        "literal_lang": "",
        "asserted_by": "darren-personal",
        "asserted_at": datetime.now(timezone.utc).isoformat(),
        "confidence": confidence,
        "source": source,
        "source_rid": source_rid,
        "source_field": source_field,
        "raw_value": raw_value,
        "status": "active",
        "normalized_object_key": norm_key,
    }


async def _enqueue_relationship_outbox(
    conn: asyncpg.Connection,
    entity_uri: str,
    vault_path: str,
) -> None:
    """After sync_vault_relationships, enqueue a retract + upserts for all current relationships."""
    schema_funcs = _terminusdb_schema_funcs()
    if schema_funcs is None:
        return  # warned once; enqueue nothing rather than a retract without its upserts
    # Retract old assertions from this source file
    await enqueue_outbox(conn, "assertion_retract", {},
                         rid=entity_uri, source_rid=vault_path)

    # Query the actual relationship rows just written by sync_vault_relationships
    rows = await conn.fetch("""
        SELECT subject_uri, predicate, object_uri, confidence,
               source, source_rid, source_field, raw_value
        FROM entity_relationships
        WHERE source_rid = $1
    """, vault_path)

    for row in rows:
        rel_payload = _build_assertion_payload(dict(row), schema_funcs)
        await enqueue_outbox(conn, "assertion_upsert", rel_payload,
                             rid=row["subject_uri"], source_rid=vault_path)


async def enqueue_outbox(
    conn: asyncpg.Connection,
    operation: str,
    payload: dict,
    rid: str,
    source_rid: str = "",
) -> bool:
    """Enqueue an operation to the TerminusDB outbox (same transaction as PG write).

    Returns True if enqueued, False if dedup skipped.
    """
    if not TERMINUSDB_ENABLED:
        return False
    payload_json = json_module_global.dumps(payload, sort_keys=True)
    payload_hash = hashlib.sha256(
        f"{operation}:{rid}:{payload_json}".encode()
    ).hexdigest()
    result = await conn.execute("""
        INSERT INTO terminusdb_outbox (operation, payload, payload_hash, rid, source_rid)
        VALUES ($1, $2::jsonb, $3, $4, $5)
        ON CONFLICT (payload_hash) WHERE status IN ('pending', 'processing') DO NOTHING
    """, operation, payload_json, payload_hash, rid, source_rid)
    return "INSERT" in result


# =============================================================================
# API Endpoints
# =============================================================================

@app.on_event("startup")
async def startup():
    """Initialize database connection pool and embedding provider"""
    global db_pool, embedding_provider, sql_ro_pool
    # Say which read-auth mode is live and whether its
    # configuration is valid, before the first request.
    try:
        from api.auth_observability import log_sink_description, warn_if_enforce_requested
        from api.read_auth import auth_mode, check_config
        warn_if_enforce_requested()
        logger.info("read-auth: mode=%s, observation log -> %s", auth_mode(), log_sink_description())
        _ra_problem = check_config()
        if _ra_problem:
            logger.error("read-auth configuration problem (affected routes return 503): %s", _ra_problem)
    except Exception as _ra_e:
        logger.error("read-auth startup check failed: %s", _ra_e)
    try:
        db_pool = await asyncpg.create_pool(
            DB_URL,
            min_size=2,
            max_size=10,
            command_timeout=60
        )
        logger.info(f"Connected to database (mode: {KOI_MODE})")

        # Least-privilege pool for POST /sql. Built only from
        # KOI_SQL_RO_DSN (the koi_sql_ro role). If unset or it fails to connect,
        # sql_ro_pool stays None and /sql fails closed with 503 — it never uses
        # the superuser db_pool for caller queries.
        sql_ro_pool = None
        _ro_dsn = os.getenv("KOI_SQL_RO_DSN", "").strip()
        if _ro_dsn:
            try:
                sql_ro_pool = await asyncpg.create_pool(
                    _ro_dsn, min_size=1, max_size=5, command_timeout=10)
                from api.routers.query_router import read_pool_is_least_privilege
                if await read_pool_is_least_privilege(sql_ro_pool):
                    logger.info("/sql: least-privilege read pool connected")
                else:
                    await sql_ro_pool.close()
                    sql_ro_pool = None
                    logger.error("/sql: KOI_SQL_RO_DSN logs in as a privileged role; "
                                 "pool refused, /sql will 503")
            except Exception as _ro_e:
                sql_ro_pool = None
                logger.warning("/sql: read pool unavailable (%s); /sql will 503", type(_ro_e).__name__)

        # 1. Init embedding provider (reads env vars only, no API call)
        embedding_provider = create_embedding_provider()

        # 2. Dimension guard: check provider dim matches ALL runtime vector columns
        #    that the query path reads. Uses explicit matrix — NOT discovery.
        #    Excludes koi_embeddings.dim_* (intentionally multi-dimension).
        #    2026-04-23: migrated to embedding_3072 for entities + chunks;
        #    session_chunks/knowledge_facts not migrated (unmigrated surfaces
        #    degrade to text/skip at query time, not blocked at startup).
        RUNTIME_VECTOR_COLUMNS = [
            ("entity_registry", "embedding_3072"),
            ("koi_memory_chunks", "embedding_3072"),
        ]
        if embedding_provider:
            async with db_pool.acquire() as conn:
                for table, column in RUNTIME_VECTOR_COLUMNS:
                    regclass = await conn.fetchval(
                        "SELECT to_regclass($1)", table
                    )
                    if regclass is None:
                        continue  # table doesn't exist yet (fresh install)
                    existing_dim = await conn.fetchval("""
                        SELECT atttypmod FROM pg_attribute
                        WHERE attrelid = to_regclass($1)
                        AND attname = $2 AND atttypmod > 0
                    """, table, column)
                    if existing_dim is not None and existing_dim > 0 and existing_dim != embedding_provider.dimension:
                        logger.fatal(
                            f"DIMENSION MISMATCH: provider outputs {embedding_provider.dimension}-dim "
                            f"but {table}.{column} is vector({existing_dim}). "
                            f"Cannot start. Re-embed data or change EMBEDDING_PROVIDER."
                        )
                        raise SystemExit(1)
            logger.info(f"Tier 2 semantic matching: ENABLED")
        else:
            logger.info("Tier 2 semantic matching: DISABLED (no embedding provider)")

        async with db_pool.acquire() as conn:
            facts_regclass = await conn.fetchval(
                "SELECT to_regclass($1)", "knowledge_facts"
            )
            episodes_regclass = await conn.fetchval(
                "SELECT to_regclass($1)", "knowledge_episodes"
            )
            app.state.facts_surface_available = bool(
                facts_regclass and episodes_regclass
            )
            logger.info(
                "facts_surface_available=%s (knowledge_facts=%s, knowledge_episodes=%s)",
                app.state.facts_surface_available,
                "present" if facts_regclass else "absent",
                "present" if episodes_regclass else "absent",
            )

        # 3. Ensure schema (uses provider dimension for new tables)
        dim = embedding_provider.dimension if embedding_provider else 1536
        async with db_pool.acquire() as conn:
            await ensure_schema(conn, embedding_dim=dim)

        # Verify graph traversal indexes
        async with db_pool.acquire() as conn:
            from api.graph_queries import verify_indexes
            await verify_indexes(conn)

        # Mount capability-gated routers (after pool init)
        if _caps is not None:
            # NOTE: graph_router is NOT mounted here because /graph/neighborhood
            # and /graph/shortest-path are already defined inline on app (lines 3755, 3805).
            # Mounting the router would create duplicate routes. The inline endpoints
            # will be removed in a future phase when the router is fully validated.
            # The router adds temporal endpoints (/graph/history, /graph/timeline) that
            # don't overlap — mount only those when assertion_history is enabled.
            if _caps.graph_queries and _caps.assertion_history:
                try:
                    from api.routers.graph_router import create_temporal_router
                    app.include_router(create_temporal_router(db_pool, _caps))
                    logger.info("Graph temporal router mounted (/graph/history, /graph/timeline)")
                except Exception as e:
                    logger.warning(f"Graph temporal router not mounted: {e}")

            if _caps.web_sensor:
                try:
                    from api.routers.web_router import create_router as create_web_router
                    app.include_router(create_web_router(db_pool, _caps))
                    logger.info("Web router mounted")
                except Exception as e:
                    logger.warning(f"Web router not mounted: {e}")

            # Commitment pooling router (always on, no capability gate)
            try:
                from api.routers.commitment_router import create_router as create_commitment_router, create_pool_router
                app.include_router(create_commitment_router(db_pool))
                app.include_router(create_pool_router(db_pool))
                logger.info("Commitment routers mounted (/commitments/, /pools/)")
            except Exception as e:
                logger.warning(f"Commitment routers not mounted: {e}")

            # Task router is always mounted (no capability gate — core feature)
            try:
                from api.routers.task_router import create_router as create_task_router
                app.include_router(create_task_router(db_pool, _caps), prefix="/tasks")
                logger.info("Task router mounted (/tasks)")
            except Exception as e:
                logger.warning(f"Task router not mounted: {e}")

            # Knowledge graph router (episodes + temporal facts)
            try:
                from api.routers.knowledge_router import create_router as create_knowledge_router
                app.include_router(create_knowledge_router(
                    db_pool,
                    generate_query_embedding=generate_query_embedding,
                    generate_document_embedding=generate_document_embedding,
                    generate_batch_embedding=generate_document_embeddings_batch,
                ), prefix="/knowledge")
                logger.info("Knowledge router mounted (/knowledge)")
            except Exception as e:
                logger.warning(f"Knowledge router not mounted: {e}")

            # Admin router (entity merge / redirect — always on, service-token gated)
            try:
                from api.routers.admin_router import create_router as create_admin_router
                app.include_router(create_admin_router(db_pool), prefix="/entities")
                logger.info("Admin router mounted (/entities)")
            except Exception as e:
                logger.warning(f"Admin router not mounted: {e}")

            # Project briefing router (always on, no capability gate)
            try:
                from api.routers.project_router import create_router as create_project_router
                app.include_router(create_project_router(db_pool), prefix="/project")
                logger.info("Project router mounted (/project)")
            except Exception as e:
                logger.warning(f"Project router not mounted: {e}")

            # Claims engine router (always on, no capability gate)
            try:
                from api.routers.claims_router import create_router as create_claims_router
                app.include_router(create_claims_router(db_pool))
                logger.info("Claims router mounted (/claims/)")
            except Exception as e:
                logger.warning(f"Claims router not mounted: {e}")

            # Document ingestion router (unified doc-ingest; background job; service-token gated)
            try:
                from api.routers.documents_router import create_router as create_documents_router
                app.include_router(create_documents_router(db_pool), prefix="/documents")
                logger.info("Documents router mounted (/documents)")
            except Exception as e:
                logger.warning(f"Documents router not mounted: {e}")

            # Calendar router — ICS-event date-range lookups (personal only)
            try:
                from api.routers.calendar_router import (
                    create_router as create_calendar_router,
                    ensure_try_ts,
                )
                await ensure_try_ts(db_pool)
                app.include_router(create_calendar_router(db_pool))
                logger.info("Calendar router mounted (/calendar/)")
            except Exception as e:
                logger.warning(f"Calendar router not mounted: {e}")

            # Intent registry router (always on, no capability gate)
            # Privacy note: the router is meant for local and private-network callers only.
            # Response model separation (Discovery/Detail/Coordinator) is the
            # enforcement mechanism at pilot scale. If the API ever becomes
            # externally accessible, add capability/auth gates to /detail and
            # /digest endpoints.
            try:
                from api.routers.intent_router import create_router as create_intent_router
                app.include_router(create_intent_router(db_pool), prefix="/intents")
                logger.info("Intent router mounted (/intents)")
            except Exception as e:
                logger.warning(f"Intent router not mounted: {e}")

            # Dynamic query endpoint (personal deployments only)
            if _caps.query_endpoint:
                try:
                    from api.routers.query_router import create_router as create_query_router
                    # /sql executes on the least-privilege pool (None -> fail closed),
                    # logs on the writable db_pool. Never executes caller SQL as superuser.
                    app.include_router(
                        create_query_router(sql_ro_pool, _caps, log_pool=db_pool),
                        prefix="/sql")
                    logger.info("Query router mounted (/sql)")
                except Exception as e:
                    logger.warning(f"Query router not mounted: {e}")

            # SeaTrees Bloom export router (always on, no capability gate)
            try:
                from api.routers.seatrees_router import create_router as create_seatrees_router
                app.include_router(create_seatrees_router(db_pool))
                logger.info("SeaTrees router mounted (/seatrees/)")
            except Exception as e:
                logger.warning(f"SeaTrees router not mounted: {e}")

            if _caps.github_sensor:
                try:
                    from api.github_sensor import GitHubSensor
                    gh_sensor = GitHubSensor(
                        pool=db_pool,
                        event_queue=getattr(app.state, 'event_queue', None),
                    )
                    gh_sensor._embed_fn = generate_document_embedding
                    await gh_sensor.start()
                    app.state.github_sensor = gh_sensor
                    logger.info("GitHub sensor started")
                except Exception as e:
                    logger.warning(f"GitHub sensor not started: {e}")
                try:
                    from api.routers.github_router import create_router as create_github_router
                    app.include_router(create_github_router(db_pool, _caps, app=app))
                    logger.info("GitHub router mounted")
                except Exception as e:
                    logger.warning(f"GitHub router not mounted: {e}")

            if _caps.mediawiki_sensor:
                try:
                    from api.mediawiki_sensor import MediaWikiSensor
                    mw_sensor = MediaWikiSensor(pool=db_pool, event_queue=getattr(app.state, 'event_queue', None))
                    await mw_sensor.start()
                    app.state.mediawiki_sensor = mw_sensor
                except Exception as e:
                    logger.warning(f"MediaWiki sensor not started: {e}")
                try:
                    from api.routers.mediawiki_router import create_router as create_mw_router
                    app.include_router(create_mw_router(db_pool, getattr(app.state, 'mediawiki_sensor', None)))
                    logger.info("MediaWiki router mounted")
                except Exception as e:
                    logger.warning(f"MediaWiki router not mounted: {e}")

            if _caps.coordinator_endpoints:
                try:
                    from api.routers.network_router import create_router as create_network_router
                    app.include_router(create_network_router(db_pool, _caps))
                    logger.info("Network router mounted")
                except Exception as e:
                    logger.warning(f"Network router not mounted: {e}")

        # Initialize KOI-net federation (if enabled)
        if KOI_NET_ENABLED:
            try:
                from api.koi_net_router import setup_koi_net
                await setup_koi_net(db_pool)
                # Wire event_queue for domain event emission from mutation endpoints
                from api.koi_net_router import _event_queue
                from api.federation_events import set_event_queue
                set_event_queue(_event_queue)
                logger.info("KOI-net federation initialized")
            except Exception as e:
                logger.warning(f"KOI-net federation failed to initialize: {e}")

        # Expose the DB pool for routers/workers that read app.state.
        app.state.db_pool = db_pool

        # C14: print every mounted (method, path) pair missing from the write-log
        # route map to stderr, so a route added to KOI later (id source "none"
        # until reviewed) is flagged at startup (RQ38, RQ41). Deploy requires
        # this to print nothing.
        try:
            if _koi_route_map is not None:
                from api.koi_write_log import unmapped_routes
                for _m, _pf in unmapped_routes(app.routes, _koi_route_map):
                    print(f"koi-write-log: unmapped route {_m} {_pf}", file=sys.stderr)
        except Exception as e:
            logger.warning(f"koi-write-log route check failed: {e}")

        # Agentic crawl background worker (Octo-only via table pre-flight).
        try:
            from api.crawl_worker import start_crawl_worker
            await start_crawl_worker(app)
        except Exception as e:
            logger.warning(f"crawl_worker start failed (non-fatal): {e}")

        # Initialize TerminusDB adapter (if enabled)
        if TERMINUSDB_ENABLED:
            global terminusdb_adapter
            try:
                from api.terminusdb_adapter import TerminusDBAdapter
                terminusdb_adapter = TerminusDBAdapter(
                    url=os.getenv('TERMINUSDB_URL', 'http://127.0.0.1:6363/'),
                    db_name=os.getenv('TERMINUSDB_DB', 'koi_knowledge_graph'),
                    team=os.getenv('TERMINUSDB_TEAM', 'admin'),
                    key=os.getenv('TERMINUSDB_KEY', 'root'),
                )
                health = terminusdb_adapter.health()
                if health.get("terminusdb_reachable"):
                    logger.info(f"TerminusDB connected (schema_hash={health['schema_hash'][:12]}...)")
                else:
                    logger.warning(f"TerminusDB not reachable: {health.get('error', 'unknown')}")
                    logger.info("Outbox will accumulate; worker will drain on recovery")
            except Exception as e:
                logger.warning(f"TerminusDB initialization failed (non-fatal): {e}")
                terminusdb_adapter = None

    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise


@app.on_event("shutdown")
async def shutdown():
    """Stop background tasks and close database connection pool"""
    shutdown_koi_net = _optional_federation_attr("api.koi_net_router", "shutdown_koi_net")
    await shutdown_koi_net()
    try:
        from api.crawl_worker import stop_crawl_worker
        await stop_crawl_worker(app)
    except Exception:
        pass
    global db_pool
    if db_pool:
        await db_pool.close()
    shutdown_emitter()


async def ensure_schema(conn: asyncpg.Connection, embedding_dim: int = 1536):
    """Ensure the entity_registry table exists"""
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS entity_registry (
            id SERIAL PRIMARY KEY,
            fuseki_uri TEXT UNIQUE NOT NULL,
            entity_text TEXT NOT NULL,
            entity_type TEXT,
            normalized_text TEXT NOT NULL,
            ledger_id TEXT,
            metadata_iri TEXT,
            admin_address TEXT,
            wallet_address TEXT,
            aliases TEXT[],
            jurisdiction TEXT,
            class_id TEXT,
            source TEXT DEFAULT 'personal-vault',
            first_seen_rid TEXT,
            metadata JSONB,
            embedding vector({embedding_dim}),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Create index on normalized_text for fast lookups
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_entity_registry_normalized
        ON entity_registry(normalized_text)
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_entity_registry_type
        ON entity_registry(entity_type)
    """)

    # Create HNSW index for vector similarity search (matches migration 020)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_entity_vector
        ON entity_registry USING hnsw (embedding vector_cosine_ops)
    """)

    # Create document_entity_links table
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS document_entity_links (
            id SERIAL PRIMARY KEY,
            document_rid TEXT NOT NULL,
            entity_uri TEXT NOT NULL,
            mention_count INT DEFAULT 1,
            context TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(document_rid, entity_uri)
        )
    """)

    # Create entity_rid_mappings table for vault entity registration
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_rid_mappings (
            id SERIAL PRIMARY KEY,
            vault_rid TEXT UNIQUE NOT NULL,
            vault_path TEXT NOT NULL,
            canonical_uri TEXT NOT NULL,
            entity_type TEXT,
            name TEXT,
            content_hash TEXT,
            sync_status TEXT DEFAULT 'linked',
            last_synced TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT valid_sync_status CHECK (
                sync_status IN ('linked', 'local_only', 'pending_sync', 'conflict')
            )
        )
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rid_mappings_canonical
        ON entity_rid_mappings(canonical_uri)
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rid_mappings_vault_path
        ON entity_rid_mappings(vault_path)
    """)

    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_rid_mappings_sync_status
        ON entity_rid_mappings(sync_status)
    """)

    # Add vault_rid column to entity_registry if not exists
    try:
        await conn.execute("""
            ALTER TABLE entity_registry
            ADD COLUMN IF NOT EXISTS vault_rid TEXT
        """)
    except Exception:
        pass  # Column may already exist

    # ==========================================================================
    # Entity Relationships Tables (for relationship-aware entity resolution)
    # ==========================================================================

    # Enable pg_trgm extension for fuzzy matching in pending resolution
    await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # Predicate allow-list (must be created first - FK target)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS allowed_predicates (
            predicate TEXT PRIMARY KEY,
            description TEXT,
            subject_types TEXT[],
            object_types TEXT[]
        )
    """)

    # Seed with canonical predicates (idempotent)
    await conn.execute("""
        INSERT INTO allowed_predicates (predicate, description, subject_types, object_types) VALUES
            ('affiliated_with', 'Person belongs to organization', ARRAY['Person'], ARRAY['Organization']),
            ('founded', 'Person founded org/project', ARRAY['Person'], ARRAY['Organization', 'Project']),
            ('has_founder', 'Org/project was founded by', ARRAY['Organization', 'Project'], ARRAY['Person']),
            ('knows', 'Person knows person (symmetric)', ARRAY['Person'], ARRAY['Person']),
            ('collaborates_with', 'Person collaborates with person (symmetric)', ARRAY['Person'], ARRAY['Person']),
            ('involves_person', 'Project involves person', ARRAY['Project', 'Meeting'], ARRAY['Person']),
            ('involves_organization', 'Project involves organization', ARRAY['Project'], ARRAY['Organization']),
            ('has_project', 'Organization has project', ARRAY['Organization'], ARRAY['Project']),
            ('attended', 'Person attended meeting', ARRAY['Person'], ARRAY['Meeting']),
            ('located_in', 'Entity is located in place', NULL, ARRAY['Location']),
            ('assigned_to', 'Task assigned to person', ARRAY['Task'], ARRAY['Person']),
            ('belongs_to_project', 'Task belongs to project', ARRAY['Task'], ARRAY['Project']),
            ('sourced_from', 'Task sourced from document', ARRAY['Task'], ARRAY['Meeting'])
        ON CONFLICT (predicate) DO NOTHING
    """)

    # Entity relationships table (resolved relationships)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS entity_relationships (
            id SERIAL PRIMARY KEY,
            subject_uri TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object_uri TEXT NOT NULL,
            confidence FLOAT DEFAULT 1.0,
            source TEXT DEFAULT 'vault',
            source_rid TEXT,
            source_field TEXT,
            raw_value TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(subject_uri, predicate, object_uri),
            CHECK (subject_uri != object_uri),
            CHECK (predicate ~ '^[a-z][a-z0-9_]*$')
        )
    """)

    # Add FK constraints if they don't exist (ignore errors if already exist)
    try:
        await conn.execute("""
            ALTER TABLE entity_relationships
            ADD CONSTRAINT fk_rel_predicate FOREIGN KEY (predicate)
                REFERENCES allowed_predicates(predicate)
        """)
    except Exception:
        pass

    try:
        await conn.execute("""
            ALTER TABLE entity_relationships
            ADD CONSTRAINT fk_rel_subject FOREIGN KEY (subject_uri)
                REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
        """)
    except Exception:
        pass

    try:
        await conn.execute("""
            ALTER TABLE entity_relationships
            ADD CONSTRAINT fk_rel_object FOREIGN KEY (object_uri)
                REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
        """)
    except Exception:
        pass

    # Pending relationships table (unresolved targets)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS pending_relationships (
            id SERIAL PRIMARY KEY,
            subject_uri TEXT,
            object_uri TEXT,
            predicate TEXT NOT NULL,
            raw_unknown_label TEXT NOT NULL,
            unknown_side TEXT NOT NULL CHECK (unknown_side IN ('subject', 'object')),
            target_type_hint TEXT,
            source TEXT DEFAULT 'vault',
            source_rid TEXT,
            source_field TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            CHECK ((subject_uri IS NOT NULL AND object_uri IS NULL) OR
                   (subject_uri IS NULL AND object_uri IS NOT NULL)),
            CHECK (
                (unknown_side = 'subject' AND subject_uri IS NULL AND object_uri IS NOT NULL) OR
                (unknown_side = 'object' AND object_uri IS NULL AND subject_uri IS NOT NULL)
            )
        )
    """)

    # Add unique index for pending edges (expression index)
    try:
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_unique_edge
            ON pending_relationships (
                COALESCE(subject_uri, ''),
                COALESCE(object_uri, ''),
                predicate,
                raw_unknown_label,
                unknown_side
            )
        """)
    except Exception:
        pass

    # Add FK constraints for pending
    try:
        await conn.execute("""
            ALTER TABLE pending_relationships
            ADD CONSTRAINT fk_pending_predicate FOREIGN KEY (predicate)
                REFERENCES allowed_predicates(predicate)
        """)
    except Exception:
        pass

    try:
        await conn.execute("""
            ALTER TABLE pending_relationships
            ADD CONSTRAINT fk_pending_subject FOREIGN KEY (subject_uri)
                REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
        """)
    except Exception:
        pass

    try:
        await conn.execute("""
            ALTER TABLE pending_relationships
            ADD CONSTRAINT fk_pending_object FOREIGN KEY (object_uri)
                REFERENCES entity_registry(fuseki_uri) ON DELETE CASCADE
        """)
    except Exception:
        pass

    # Indexes for entity_relationships
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_subject ON entity_relationships(subject_uri)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_object ON entity_relationships(object_uri)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_subject_predicate ON entity_relationships(subject_uri, predicate)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_object_predicate ON entity_relationships(object_uri, predicate)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_rel_source_rid ON entity_relationships(source_rid)")

    # Indexes for pending_relationships
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_unknown_label ON pending_relationships(raw_unknown_label)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_source_rid ON pending_relationships(source_rid)")

    # GIN trigram index for fuzzy matching
    try:
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_pending_unknown_label_trgm
            ON pending_relationships USING GIN (raw_unknown_label gin_trgm_ops)
        """)
    except Exception:
        pass  # May fail if pg_trgm extension not available

    logger.info("Schema verified/created (including relationship tables)")


EMBED_REPAIR_STATE = os.getenv(
    "EMBED_REPAIR_STATE_FILE",
    os.path.expanduser("~/projects/koi-processor-runtime/logs/embedding-repair-state.json"),
)
# A canary older than this is not evidence. The repair job runs every 300s, so three
# missed passes means the only scheduled probe on this machine has stopped.
EMBED_EVIDENCE_MAX_AGE_S = 1200


def _embedding_health() -> dict:
    """Whether embeddings ACTUALLY work, with the provenance of that claim.

    Reads the embedding-repair job's state file rather than calling the provider here:
    /health is polled by restart.sh and by monitoring, and must stay fast and never
    fail. That job already probes the provider with a one-token canary every 300s and
    records the result, so the measurement exists — it simply was not being surfaced.

    Fails CLOSED. Absent, unreadable, or stale state reports available=false with a
    reason, rather than falling back to "a provider object exists", because that
    fallback is precisely the bug being fixed: it answers a question about capacity
    with evidence about configuration. A preflight that cannot confirm should stop.
    """
    import json as _json
    import time as _time
    try:
        age = _time.time() - os.path.getmtime(EMBED_REPAIR_STATE)
        st = _json.loads(open(EMBED_REPAIR_STATE).read())
    except Exception as e:
        return {"embedding_available": False,
                "embedding_check": {"source": "unavailable", "reason": f"{type(e).__name__}"}}

    backoff_until = st.get("backoff_until")
    if backoff_until:
        return {"embedding_available": False,
                "embedding_check": {"source": "repair-canary", "reason": st.get("backoff_reason"),
                                    "backoff_until": backoff_until,
                                    "consecutive_failed_runs": st.get("consecutive_failed_runs", 0)}}
    if age > EMBED_EVIDENCE_MAX_AGE_S:
        return {"embedding_available": False,
                "embedding_check": {"source": "stale", "age_seconds": round(age),
                                    "reason": "no canary within the last 20 minutes; is "
                                              "com.personal-koi.embedding-repair running?"}}
    return {"embedding_available": True,
            "embedding_check": {"source": "repair-canary", "age_seconds": round(age),
                                "last_success_at": st.get("last_success_at")}}


# --- null-embed gauges: cached, single-flight -------------------------------
# These five COUNT(*) probes are drift detection, not liveness. Each one was added
# after a real silent failure (a dead chunk embedder unnoticed for three days;
# 3,464 entity rows invisible to Tier-2 resolution), so they are worth keeping —
# but they were being recomputed synchronously on EVERY /health call.
#
# 2026-09-01: measured on a loaded box they cost 285s in total for one call
# (session_chunks alone 198s). Because an abandoned HTTP request does not cancel
# the server-side query, short-timeout pollers stacked identical scans faster than
# they drained: 10+ concurrent copies observed, /health and then the whole API
# stopped answering while Postgres sat at 0.8% CPU — queued, not working.
#
# Two changes: results are cached for _NULL_EMBED_TTL, and a single-flight lock
# means a concurrent caller serves the stale value instead of starting another
# scan. Freshness is reported to the caller as null_embed_gauges_age_seconds, in
# the same spirit as embedding_check.age_seconds above. Partial indexes on each
# IS NULL predicate make the refresh itself cheap.
# 300s, not 60s: these gauges detect an embedder that has died, and the incidents
# they were built for ran for DAYS undetected — sub-minute resolution buys nothing.
# It matters here because two of the five probes (koi_memory_chunks, session_chunks)
# still lack their partial index, so a refresh is not yet cheap. Once
# scripts/ops/build_null_embed_indexes.sh has been run on a quiet box, this can
# safely drop back to 60.
_NULL_EMBED_TTL = 300.0
_null_embed_cache: dict = {"values": None, "at": 0.0}
_null_embed_lock = asyncio.Lock()
_NULL_EMBED_KEYS = ("db", "live", "chunks", "entities", "sessions")


async def _null_embed_gauges():
    """Return (values_dict, age_seconds). Never runs more than one refresh at a time."""
    now = time.monotonic()
    cached = _null_embed_cache["values"]
    age = now - _null_embed_cache["at"] if cached is not None else None

    if cached is not None and age < _NULL_EMBED_TTL:
        return cached, age

    # A refresh is already in flight: serve stale rather than pile on. This is the
    # specific behaviour that prevents the stampede described above.
    if _null_embed_lock.locked():
        if cached is not None:
            return cached, age
        return {k: -1 for k in _NULL_EMBED_KEYS}, None

    async with _null_embed_lock:
        # Re-check: another caller may have refreshed while we waited.
        now = time.monotonic()
        cached = _null_embed_cache["values"]
        if cached is not None and (now - _null_embed_cache["at"]) < _NULL_EMBED_TTL:
            return cached, now - _null_embed_cache["at"]
        try:
            async with db_pool.acquire() as conn:
                values = {
                    "db": await conn.fetchval(
                        "SELECT COUNT(*) FROM knowledge_facts WHERE fact_embedding_3072 IS NULL") or 0,
                    "live": await conn.fetchval(
                        "SELECT COUNT(*) FROM knowledge_facts WHERE fact_embedding_3072 IS NULL"
                        " AND valid_to IS NULL") or 0,
                    "chunks": await conn.fetchval(
                        "SELECT COUNT(*) FROM koi_memory_chunks WHERE embedding_3072 IS NULL") or 0,
                    "entities": await conn.fetchval(
                        "SELECT COUNT(*) FROM entity_registry WHERE embedding_3072 IS NULL") or 0,
                    "sessions": await conn.fetchval(
                        "SELECT COUNT(*) FROM session_chunks WHERE embedding_3072 IS NULL") or 0,
                }
        except Exception:
            values = {k: -1 for k in _NULL_EMBED_KEYS}  # signal "query failed"
        _null_embed_cache["values"] = values
        _null_embed_cache["at"] = time.monotonic()
        return values, 0.0


@app.get("/health")
async def health_check(request: Request):
    """Health check endpoint.

    Wave A A2 (2026-05-01): includes `null_embed_fact_count_db` (durable) and
    `null_embed_fact_counter_process` (transient; in-process counter accumulating
    since boot, cleared on restart). Non-zero values surface silent
    embedding-provider degradation; pair with `/diagnostics/embedding-health`
    for current-state failure rate.

    2026-08-01: `null_embed_fact_count_db` used to carry a hidden
    `AND valid_to IS NULL`, which the docstring did not mention. That made the
    gauge under-report 8.4x (13 reported against 109 actually present) and, worse,
    made it DECREASE with no repair — superseding a null-embed fact simply dropped
    it out of the count. A superseded fact is still a row; it still bypasses cosine
    dedup until re-embedded. Both numbers are now reported: `_db` is the honest
    total (matching this docstring), `_live` is the actionable subset.
    """
    try:
        # Liveness: a real round-trip to the DB, kept cheap and always on the hot path.
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        # Drift gauges: cached + single-flight, so /health cost is bounded no
        # matter how often it is polled. See _null_embed_gauges above.
        _gauges, _gauges_age = await _null_embed_gauges()
        null_embed_count_db = _gauges["db"]
        null_embed_count_live = _gauges["live"]
        null_embed_count_chunks = _gauges["chunks"]
        null_embed_count_entities = _gauges["entities"]
        null_embed_count_sessions = _gauges["sessions"]

        # Get loaded entity types from schema
        schemas = get_entity_schemas()
        entity_types = list(schemas.keys())

        process_counter = int(getattr(request.app.state, "null_embed_fact_counter", 0))

        return {
            "status": "healthy",
            "mode": KOI_MODE,
            "database": "connected",
            # `embedding_available` now means "embeddings actually work", which is what
            # the name promises and what its two consumers already assume — both
            # tests/eval/run_eval.py and scripts/koi_sustained_write.py use it as a
            # PREFLIGHT GATE. It used to be `embedding_provider is not None`, i.e.
            # whether a Python object had been constructed at startup, so while the
            # provider's credit was exhausted it reported true throughout and
            # both preflights would have passed into a run that could not embed anything.
            #
            # The honest handling already existed one file over: knowledge_router sets
            # "embedding_available": not degraded from real observed degradation.
            **_embedding_health(),
            "embedding_configured": embedding_provider is not None,
            "embedding_model": embedding_provider.model_name if embedding_provider else None,
            "embedding_dimension": embedding_provider.dimension if embedding_provider else None,
            "semantic_matching": embedding_provider is not None and ENABLE_SEMANTIC_MATCHING,
            "entity_types": entity_types,
            "schema_version": get_schema_version(),
            "resolution_tiers": {
                "tier1_exact": True,
                "tier1x_fuzzy": True,
                "tier15_contextual": True,
                "tier2_semantic": embedding_provider is not None and ENABLE_SEMANTIC_MATCHING,
                "tier3_create": True
            },
            # Behavior-neutral policy measurement. Includes the runtime kill
            # switch state, sample rate, bounded-queue depth, and dropped count.
            "resolver_shadow": emitter_status(),
            # How stale the five gauges below are (seconds); None = never computed.
            "null_embed_gauges_age_seconds": (round(_gauges_age, 1) if _gauges_age is not None else None),
            "null_embed_fact_count_db": null_embed_count_db,
            # actionable subset: excludes superseded rows (valid_to set). The _db
            # figure above is the honest total — see the docstring.
            "null_embed_fact_count_live": null_embed_count_live,
            # RAG chunks with no vector — sustained non-zero = the
            # chunk-embedding job is not running.
            "null_embed_chunk_count": null_embed_count_chunks,
            # Entities with no vector. Unlike facts and chunks this is not just
            # degraded retrieval: semantic entity reads filter IS NOT NULL, so these
            # rows are unreachable by Tier-2 resolution and will re-duplicate on the
            # next mention. Repair with scripts/backfill_entity_embeddings.py.
            "null_embed_entity_count": null_embed_count_entities,
            # Session-transcript chunks with no vector. Same invisibility as the
            # three above; see the query comment for why this reads 0 today.
            "null_embed_session_chunk_count": null_embed_count_sessions,
            "null_embed_fact_counter_process": process_counter,
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )


@app.get("/diagnostics/config")
async def diagnostics_config():
    """Non-sensitive configuration status for operational checks."""
    agentic_crawl_available = False
    try:
        from api import crawl_auth, ontology_registry

        feature_enabled = crawl_auth.is_feature_enabled()
        tokens_ok = crawl_auth.any_tokens_configured()
        ontology_ok = bool(ontology_registry.ALLOWED_ENTITY_TYPES) and bool(ontology_registry.ALLOWED_PREDICATES)
        db_ok = False
        if db_pool:
            async with db_pool.acquire() as conn:
                has_jobs = await conn.fetchval("SELECT to_regclass('web_crawl_jobs') IS NOT NULL")
                db_ok = bool(has_jobs)
        agentic_crawl_available = bool(feature_enabled and tokens_ok and ontology_ok and db_ok)
    except Exception:
        agentic_crawl_available = False
    return {
        "scraping_proxy_url_set": bool(os.environ.get("SCRAPING_PROXY_URL", "")),
        "playwright_available": bool(importlib.util.find_spec("playwright")),
        "scrapling_available": bool(importlib.util.find_spec("scrapling")),
        "trafilatura_available": bool(importlib.util.find_spec("trafilatura")),
        "embedding_provider": embedding_provider.model_name if embedding_provider else None,
        "agentic_crawl_available": agentic_crawl_available,
    }


@app.get("/diagnostics/embedding-preflight")
async def embedding_preflight():
    """Diagnostic canary for eval preflight — checks embedding config health.

    Returns dimension inventory, embedding service health, and gold entity canaries.
    Used by `run_eval.py --preflight` to catch config errors before a full eval run.
    """
    RUNTIME_VECTOR_COLUMNS = [
        ("entity_registry", "embedding_3072"),
        ("koi_memory_chunks", "embedding_3072"),
        ("session_chunks", "embedding_3072"),
        ("knowledge_facts", "fact_embedding_3072"),
    ]

    results = {
        "provider": None,
        "dimension_check": {"pass": True, "details": []},
        "canary_check": {"pass": True, "details": []},
    }

    # 1. Provider info
    if embedding_provider:
        results["provider"] = {
            "name": embedding_provider.model_name,
            "dimension": embedding_provider.dimension,
            "type": type(embedding_provider).__name__,
        }
    else:
        results["provider"] = None
        results["dimension_check"]["pass"] = False
        results["dimension_check"]["details"].append("No embedding provider configured")
        return results

    # 2. Dimension inventory
    if db_pool:
        async with db_pool.acquire() as conn:
            for table, column in RUNTIME_VECTOR_COLUMNS:
                regclass = await conn.fetchval("SELECT to_regclass($1)", table)
                if regclass is None:
                    results["dimension_check"]["details"].append(
                        {"table": table, "column": column, "status": "table_missing"}
                    )
                    continue
                dim = await conn.fetchval("""
                    SELECT atttypmod FROM pg_attribute
                    WHERE attrelid = to_regclass($1)
                    AND attname = $2 AND atttypmod > 0
                """, table, column)
                match = dim == embedding_provider.dimension if dim else None
                if dim and not match:
                    results["dimension_check"]["pass"] = False
                results["dimension_check"]["details"].append({
                    "table": table, "column": column,
                    "db_dimension": dim, "provider_dimension": embedding_provider.dimension,
                    "match": match,
                })

    # 3. Gold entity canaries — embed queries and check nearest neighbor
    canary_queries = [
        ("What is herring?", "herring"),
        ("Which organizations restore habitat?", None),
        ("What is commitment pooling?", "commitment"),
    ]

    try:
        async with db_pool.acquire() as conn:
            for query_text, expected_substr in canary_queries:
                emb = await generate_query_embedding(query_text)
                if not emb:
                    results["canary_check"]["pass"] = False
                    results["canary_check"]["details"].append({
                        "query": query_text, "error": "embedding failed"
                    })
                    continue

                top5 = await conn.fetch("""
                    SELECT fuseki_uri, entity_text,
                           1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
                    FROM entity_registry
                    WHERE embedding_3072 IS NOT NULL AND NOT node_private
                    ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                    LIMIT 5
                """, str(emb))

                names = [r["entity_text"] for r in top5]
                top_sim = float(top5[0]["similarity"]) if top5 else 0.0

                found = True
                if expected_substr:
                    found = any(expected_substr.lower() in n.lower() for n in names)

                if not found:
                    results["canary_check"]["pass"] = False

                results["canary_check"]["details"].append({
                    "query": query_text,
                    "top5": names,
                    "top_similarity": round(top_sim, 4),
                    "expected_found": found,
                })
    except Exception as e:
        results["canary_check"]["pass"] = False
        results["canary_check"]["details"].append({"error": str(e)})

    results["overall_pass"] = (
        results["dimension_check"]["pass"] and results["canary_check"]["pass"]
    )
    return results


@app.get("/diagnostics/embedding-health")
async def embedding_health(
    window: int = 20,
    fail_threshold: float = 0.20,
):
    """Wave A A1 (2026-05-01): proactive embedding-degradation surface.

    Reads `~/.koi/logs/embedding-tokens.jsonl` (Phase 8 B2 instrumentation;
    one record per call to `embed_or_none`/`embed_batch_or_none`) and computes
    the failure-rate over the last `window` records. No additional API spend —
    relies on real traffic. Use `/diagnostics/embedding-preflight` for explicit
    synthetic-canary checks.

    Query params:
      window:         How many recent records to inspect (default 20).
      fail_threshold: Failure rate above which `degraded=true` is set
                      (default 0.20 = 20%).

    Returns: {
      degraded: bool,
      failure_rate: float in [0, 1],
      recent_calls: int (actual count examined; <= window),
      ok: int,
      fail: int,
      window_start: ISO8601 or null,
      window_end: ISO8601 or null,
      last_error: str or null,
      log_path: str,
      log_present: bool,
    }
    """
    import os as _os
    from pathlib import Path as _Path

    log_path = _Path(_os.path.expanduser("~/.koi/logs/embedding-tokens.jsonl"))
    out = {
        "degraded": False,
        "failure_rate": 0.0,
        "recent_calls": 0,
        "ok": 0,
        "fail": 0,
        "window_start": None,
        "window_end": None,
        "last_error": None,
        "log_path": str(log_path),
        "log_present": log_path.exists(),
        "fail_threshold": fail_threshold,
        "window_requested": window,
    }
    if not log_path.exists():
        return out

    # Tail-read last `window` non-marker records. Files can grow large; use a
    # bounded tail scan instead of a full read.
    try:
        with log_path.open("rb") as f:
            f.seek(0, 2)
            file_size = f.tell()
            chunk = min(file_size, 256 * 1024)  # 256 KiB tail window
            f.seek(file_size - chunk)
            tail = f.read().decode("utf-8", errors="replace")
        lines = [ln for ln in tail.splitlines() if ln.startswith("{")]
        recent = lines[-window:] if len(lines) > window else lines
    except Exception as e:
        out["last_error"] = f"log_read_error: {e}"
        return out

    ok_count = 0
    fail_count = 0
    last_error_seen: Optional[str] = None
    window_start: Optional[str] = None
    window_end: Optional[str] = None
    # NOTE: module-level `json` is aliased to `json_module_global` (see line 22)
    # to dodge a name collision; rebind locally for readability inside the loop.
    import json as _json_local
    for ln in recent:
        try:
            r = _json_local.loads(ln)
        except Exception:
            continue
        ts = r.get("ts")
        if ts:
            if window_start is None:
                window_start = ts
            window_end = ts
        if r.get("ok"):
            ok_count += 1
        else:
            fail_count += 1
            # Track the most recent failure shape (provider/model/prompt_type).
            last_error_seen = (
                f"{r.get('provider')}:{r.get('model')}:"
                f"{r.get('prompt_type')}:fail@{ts}"
            )

    total = ok_count + fail_count
    out["recent_calls"] = total
    out["ok"] = ok_count
    out["fail"] = fail_count
    out["window_start"] = window_start
    out["window_end"] = window_end
    if total > 0:
        out["failure_rate"] = round(fail_count / total, 4)
        out["degraded"] = out["failure_rate"] > fail_threshold
    if last_error_seen:
        out["last_error"] = last_error_seen
    return out


@app.get("/graph-version")
async def graph_version_endpoint():
    """Return a deterministic hash of graph state for eval snapshot pinning.

    Hash = SHA-256(entity_count:rel_count:max_entity_updated:max_rel_created)[:16]
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                (SELECT COUNT(*) FROM entity_registry WHERE NOT node_private) AS entity_count,
                (SELECT COUNT(*) FROM entity_relationships) AS rel_count,
                (SELECT COALESCE(MAX(updated_at), '1970-01-01'::timestamptz) FROM entity_registry) AS max_entity_updated,
                (SELECT COALESCE(GREATEST(MAX(created_at), MAX(updated_at)), '1970-01-01'::timestamptz) FROM entity_relationships) AS max_rel_changed
        """)
        import hashlib as _hl
        state = f"{row['entity_count']}:{row['rel_count']}:{row['max_entity_updated']}:{row['max_rel_changed']}"
        version_hash = _hl.sha256(state.encode()).hexdigest()[:16]
        return {
            "graph_version": version_hash,
            "entity_count": row['entity_count'],
            "relationship_count": row['rel_count'],
        }


@app.get("/entity-types")
async def get_entity_types_endpoint():
    """
    Return entity type configs for MCP and external tools.

    This is the source of truth for entity type configuration.
    MCP and other clients should call this endpoint instead of
    maintaining their own hardcoded type mappings.

    Returns:
        version: Schema version hash for cache invalidation
        types: List of entity type configurations
    """
    schemas = get_entity_schemas()
    return {
        "version": get_schema_version(),
        "types": [
            {
                "type_key": s.type_key,
                "label": s.label,
                "folder": s.folder,
                "phonetic_matching": s.phonetic_matching,
                "min_context_people": s.min_context_people,
                "similarity_threshold": s.similarity_threshold,
                "semantic_threshold": s.semantic_threshold,
                "require_token_overlap": s.require_token_overlap,
            }
            for s in schemas.values()
        ]
    }


@app.post("/reload-schemas")
async def reload_schemas_endpoint(vault_path: Optional[str] = None):
    """
    Hot reload entity schemas from vault without restart.

    This endpoint reloads schemas from the vault Ontology/ folder.
    Use after adding or modifying schema files.

    Args:
        vault_path: Optional override for vault path (default: ~/Documents/Notes)

    Returns:
        version: New schema version hash
        types: Updated list of entity type keys
        phonetic_enabled: Types with phonetic matching enabled
    """
    schemas = reload_entity_schemas(vault_path)
    return {
        "success": True,
        "version": get_schema_version(),
        "types": list(schemas.keys()),
        "phonetic_enabled": get_phonetic_enabled_types(),
        "count": len(schemas)
    }


@app.post("/ingest", response_model=IngestResponse)
async def ingest_extraction(request: IngestRequest):
    """
    Ingest pre-extracted entities from Claude Code.

    This endpoint:
    1. Deduplicates entities against the personal KB
    2. Assigns canonical URIs to new entities
    3. Stores entities and document links
    4. Returns resolved entities for vault linking
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    # Idempotency entry-check: replay a prior identical request without re-ingesting.
    if request.request_id:
        async with db_pool.acquire() as conn:
            prior = await conn.fetchval(
                "SELECT response FROM ingest_idempotency WHERE request_id = $1",
                request.request_id)
        if prior is not None:
            import json as _json_replay
            data = prior if isinstance(prior, dict) else _json_replay.loads(prior)
            data["idempotent_replay"] = True
            logger.info("idempotent replay for /ingest request_id=%s", request.request_id)
            return IngestResponse(**data)

    canonical_entities: List[CanonicalEntity] = []
    new_count = 0
    resolved_count = 0
    failed_entities: List[dict] = []
    entity_uri_map: Dict[str, str] = {}  # normalized_name → canonical URI
    # (document_rid, entity_uri, context) for doclink federation emits — fired
    # AFTER the transaction below commits (2e rule). Group A site.
    doclink_emits: List[tuple] = []
    # (event type, canonical, extracted entity) for entity federation emits, fired after
    # the transaction commits for the same reason.
    entity_emits: List[tuple] = []

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            for entity in request.entities:
                # Wrap each entity in a nested transaction (savepoint) so any DB
                # failure rolls back only this one entity instead of poisoning
                # the outer transaction — which would cause every subsequent
                # entity + relationship to fail with InFailedSQLTransactionError
                # and bubble out as a 500 with no JSON body.
                try:
                    async with conn.transaction():
                        logger.info(f"Processing entity: {entity.name} ({entity.type})")

                        # Build context for this entity, merging global + per-entity (with deduplication)
                        global_people = request.context.associated_people if request.context else []
                        entity_people = entity.associated_people or []
                        global_orgs = request.context.organizations if request.context else []
                        entity_orgs = entity.associated_organizations or []

                        context_for_entity = ResolutionContext(
                            associated_people=list(set((global_people or []) + entity_people)),
                            organizations=list(set((global_orgs or []) + entity_orgs)),
                            project=request.context.project if request.context else None,
                            topics=request.context.topics if request.context else []
                        ) if (global_people or entity_people or global_orgs or entity_orgs or
                              (request.context and request.context.project)) else request.context

                        canonical, is_new = await resolve_entity(
                            conn,
                            entity,
                            context_for_entity,
                            resolution_caller="personal_ingest_api.ingest",
                        )
                        logger.info(f"Resolved: {canonical.name} -> {canonical.uri} (new={is_new})")

                        if is_new and not await store_new_entity(
                                conn, entity, canonical, request.document_rid, source=request.source):
                            # The URI was taken between resolve and insert (a concurrent
                            # create, or a merge). Nothing was created: link to the live
                            # holder and report it as resolved, not NEW (Phase 1b).
                            live_uri = await resolve_to_live_uri(conn, canonical.uri)
                            if live_uri != canonical.uri:
                                live = await conn.fetchrow(
                                    "SELECT entity_text, entity_type FROM entity_registry "
                                    "WHERE fuseki_uri = $1", live_uri)
                                if live:
                                    canonical.name = live["entity_text"]
                                    canonical.type = live["entity_type"] or canonical.type
                                    canonical.merged_with = (
                                        entity.name if live["entity_text"] != entity.name else None)
                                canonical.uri = live_uri
                            canonical.is_new = False
                            is_new = False
                        if is_new:
                            logger.info(f"Stored new entity: {canonical.uri}")
                        else:
                            # Vault-path annotation (P2, annotate-only). Mutates the
                            # already-appended canonical in place. One indexed query.
                            (canonical.vault_path,
                             canonical.vault_note_exists,
                             canonical.vault_folder) = await _annotate_vault_fields(
                                conn, canonical.uri, canonical.type
                            )
                            logger.info(f"Resolved to existing: {canonical.uri}")

                        # Link entity to document
                        await conn.execute("""
                            INSERT INTO document_entity_links (document_rid, entity_uri, context)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (document_rid, entity_uri)
                            DO UPDATE SET mention_count = document_entity_links.mention_count + 1
                        """, request.document_rid, canonical.uri, entity.context)
                        logger.info(f"Linked entity to document")
                        # Group A site: the upsert always moves mention_count by
                        # 1. Record for post-commit emit (this is the last stmt
                        # in the per-entity savepoint, so reaching here means it
                        # committed). Append only — emit happens post-txn below.
                        doclink_emits.append(
                            (request.document_rid, canonical.uri, entity.context)
                        )

                except Exception as e:
                    import traceback
                    logger.error(f"Error processing entity {entity.name}: {e}")
                    logger.error(traceback.format_exc())
                    failed_entities.append({"name": entity.name, "error": str(e)})
                    # Continue with other entities — nested transaction already
                    # rolled back so the outer txn is still healthy.
                else:
                    # The savepoint committed: only now does this entity count, and only
                    # now may it be reported, linked by a relationship or federated.
                    canonical_entities.append(canonical)
                    entity_uri_map[normalize_entity_text(entity.name)] = canonical.uri
                    if is_new:
                        new_count += 1
                    else:
                        resolved_count += 1
                    entity_emits.append(("NEW" if is_new else "UPDATE", canonical, entity))

            # --- Process relationships (still inside conn.transaction()) ---
            rel_count = 0
            if request.relationships:
                for rel in request.relationships:
                    subj_key = normalize_entity_text(rel.subject)
                    obj_key = normalize_entity_text(rel.object)
                    subj_uri = entity_uri_map.get(subj_key)
                    obj_uri = entity_uri_map.get(obj_key)

                    # Fallback: look up pre-existing entities by exact normalized_text
                    if not subj_uri:
                        rows = await conn.fetch(
                            "SELECT fuseki_uri FROM entity_registry WHERE normalized_text = $1 AND merged_into IS NULL LIMIT 2",
                            subj_key)
                        if len(rows) == 1:
                            subj_uri = rows[0]["fuseki_uri"]
                        elif len(rows) > 1:
                            logger.warning(
                                f"Ambiguous subject '{rel.subject}' in relationship "
                                f"'{rel.subject} → {rel.predicate} → {rel.object}': "
                                f"{len(rows)}+ matches, skipping")
                    if not obj_uri:
                        rows = await conn.fetch(
                            "SELECT fuseki_uri FROM entity_registry WHERE normalized_text = $1 AND merged_into IS NULL LIMIT 2",
                            obj_key)
                        if len(rows) == 1:
                            obj_uri = rows[0]["fuseki_uri"]
                        elif len(rows) > 1:
                            logger.warning(
                                f"Ambiguous object '{rel.object}' in relationship "
                                f"'{rel.subject} → {rel.predicate} → {rel.object}': "
                                f"{len(rows)}+ matches, skipping")

                    if not subj_uri or not obj_uri:
                        logger.warning(
                            f"Skipping relationship {rel.subject} → {rel.predicate} → {rel.object}: "
                            f"subject={'found' if subj_uri else 'NOT found'}, "
                            f"object={'found' if obj_uri else 'NOT found'}"
                        )
                        continue

                    if subj_uri == obj_uri:
                        logger.warning(
                            f"Skipping self-referential relationship: {rel.subject} → {rel.predicate} → {rel.object}")
                        continue

                    # Use a nested transaction (savepoint) so FK/check violations
                    # only roll back this one INSERT instead of poisoning the outer
                    # transaction (which would cause every subsequent statement to
                    # fail with InFailedSQLTransactionError → 500).
                    try:
                        async with conn.transaction():
                            await conn.execute("""
                                INSERT INTO entity_relationships
                                    (subject_uri, predicate, object_uri, source, source_rid)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
                            """, subj_uri, rel.predicate, obj_uri,
                                request.source, request.document_rid)
                            rel_count += 1
                    except asyncpg.exceptions.ForeignKeyViolationError as e:
                        logger.warning(f"Skipping relationship (FK violation): {e}")
                    except asyncpg.exceptions.CheckViolationError as e:
                        logger.warning(f"Skipping relationship (check violation): {e}")

    # Emit entity federation events only now, for entities whose savepoint committed.
    # The type is the resolved row's: a peer's apply overwrites entity_type, and a cross-type
    # match (Tier 1.1b, or a create collision followed to its survivor) would otherwise
    # retype the survivor on every peer.
    emit_domain_event = _optional_federation_attr("api.federation_events", "emit_domain_event")
    for kind, canonical, entity in entity_emits:
        await emit_domain_event("entity", kind, canonical.uri, {
            "fuseki_uri": canonical.uri,
            "entity_text": canonical.name,
            "entity_type": canonical.type or entity.type,
            "normalized_text": canonical.name.lower().strip(),
            "aliases": [],
            "metadata": {},
        })

    # Emit doclink federation events AFTER the transaction above commits (2e
    # rule — emitting inside the txn would queue events for rows a rollback
    # could remove). Group A site → mention_delta=1 per successful upsert.
    emit_doclink_event = _optional_federation_attr("api.federation_events", "emit_doclink_event")
    for document_rid, entity_uri, ctx in doclink_emits:
        await emit_doclink_event(document_rid, entity_uri, 1, context=ctx)

    success = len(failed_entities) == 0
    canonical_uris = [ce.uri for ce in canonical_entities]
    stats = IngestStats(
        entities_processed=len(request.entities),
        new_entities=new_count,
        resolved_entities=resolved_count,
        relationships_processed=rel_count,
        failed_entities=len(failed_entities),
        errors=failed_entities if failed_entities else None
    )
    if failed_entities:
        logger.warning(f"Ingest completed with {len(failed_entities)} failures: "
                      f"{[f['name'] for f in failed_entities]}")

    # Create real CAT receipt for provenance tracking
    doc_hash = hashlib.sha256(request.document_rid.encode()).hexdigest()[:16]
    output_rid = f"orn:personal-koi.ingest:{doc_hash}"
    content_hash = hashlib.sha256(
        request.model_dump_json().encode()
    ).hexdigest()

    receipt_persisted = False
    # Deterministic fallback receipt_id (same SHA-256 format as real receipts)
    receipt_id = generate_receipt_id("entity_ingest", request.document_rid, output_rid)
    try:
        async with db_pool.acquire() as receipt_conn:
            receipt = await create_receipt(
                receipt_conn,
                transformation_type="entity_ingest",
                input_rid=request.document_rid,
                output_rid=output_rid,
                processor_name="personal_ingest_api",
                source_sensor=request.source,
                parent_receipt_id=request.parent_receipt_id,
                metadata={
                    "entities_processed": len(request.entities),
                    "new_entities": new_count,
                    "resolved_entities": resolved_count,
                    "relationships_processed": rel_count,
                    "canonical_entity_uris": canonical_uris,
                },
                content_hash=content_hash,
            )
            receipt_id = receipt.receipt_id
            receipt_persisted = True
    except Exception as e:
        logger.error(f"Failed to create ingest CAT receipt: {e}")

    response = IngestResponse(
        success=success,
        canonical_entities=canonical_entities,
        receipt_id=receipt_id,
        receipt_rid=receipt_id,  # Backwards compatibility
        receipt_persisted=receipt_persisted,
        stats=stats
    )

    # Idempotency store-after-commit (best-effort). Unlike /knowledge/episodes,
    # /ingest cannot store inside the main write transaction because the CAT
    # receipt above is created afterwards. ON CONFLICT DO NOTHING so a concurrent
    # duplicate never errors; a crash between commit and here just means a retry
    # re-ingests (entity writes are dedup-idempotent). See IngestRequest.request_id.
    if request.request_id:
        try:
            async with db_pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO ingest_idempotency (request_id, endpoint, response) "
                    "VALUES ($1, $2, $3::jsonb) ON CONFLICT (request_id) DO NOTHING",
                    request.request_id, "/ingest",
                    json_module_global.dumps(response.model_dump()))
        except Exception as e:
            logger.warning(f"Failed to store ingest idempotency record: {e}")

    return response


@app.get("/receipts/{receipt_id}/chain")
async def get_receipt_chain_endpoint(receipt_id: str):
    """Walk the parent chain from a receipt back to the root.

    Returns receipts in reverse chronological order (most recent first).
    Useful for verifying provenance of ingest operations, TBFF settlements,
    and Hub Cultivator decision logging.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        chain = await get_receipt_chain(conn, receipt_id)
        if not chain:
            raise HTTPException(status_code=404, detail=f"Receipt {receipt_id} not found")

        return {
            "receipt_id": receipt_id,
            "chain_length": len(chain),
            "chain": [
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
                for r in chain
            ],
        }


@app.get("/entities")
async def list_entities(
    entity_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """List entities in the knowledge base"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        if entity_type:
            entities = await conn.fetch("""
                SELECT fuseki_uri, entity_text, entity_type, source, wallet_address, created_at
                FROM entity_registry
                WHERE entity_type = $1 AND NOT node_private
                  AND merged_into IS NULL
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
            """, entity_type, limit, offset)
        else:
            entities = await conn.fetch("""
                SELECT fuseki_uri, entity_text, entity_type, source, wallet_address, created_at
                FROM entity_registry
                WHERE NOT node_private
                  AND merged_into IS NULL
                ORDER BY created_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)

        return {
            "entities": [dict(e) for e in entities],
            "count": len(entities),
            "limit": limit,
            "offset": offset
        }


@app.api_route("/entity-search", methods=["GET", "POST"])
async def entity_search(request: Request, query: str = None, limit: int = 20, entity_type: Optional[str] = None):
    """Search entities by name (fuzzy text match + optional semantic).
    GET: query params (?query=...&limit=20&entity_type=Project)
    POST: JSON body ({"query": "...", "limit": 20, "entity_type": "Project"})
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    # For POST, merge body params over query params
    if request.method == "POST":
        try:
            body = await request.json()
            query = body.get("query", query)
            limit = body.get("limit", limit)
            entity_type = body.get("entity_type", entity_type)
        except Exception:
            pass

    if not query:
        return JSONResponse({"results": [], "count": 0})

    normalized_query = normalize_entity_text(query)
    async with db_pool.acquire() as conn:
        # Text search: ILIKE on normalized_text
        if entity_type:
            rows = await conn.fetch("""
                SELECT fuseki_uri, entity_text, entity_type, source, created_at, aliases, wallet_address,
                       CASE WHEN normalized_text = $1 THEN 1.0
                            WHEN normalized_text ILIKE $2 THEN 0.9
                            ELSE 0.7 END AS similarity
                FROM entity_registry
                WHERE normalized_text ILIKE $2 AND entity_type = $3 AND NOT node_private AND merged_into IS NULL
                ORDER BY
                    CASE WHEN normalized_text = $1 THEN 0 ELSE 1 END,
                    created_at DESC
                LIMIT $4
            """, normalized_query, f"%{normalized_query}%", entity_type, limit)
        else:
            rows = await conn.fetch("""
                SELECT fuseki_uri, entity_text, entity_type, source, created_at, aliases, wallet_address,
                       CASE WHEN normalized_text = $1 THEN 1.0
                            WHEN normalized_text ILIKE $2 THEN 0.9
                            ELSE 0.7 END AS similarity
                FROM entity_registry
                WHERE normalized_text ILIKE $2 AND NOT node_private AND merged_into IS NULL
                ORDER BY
                    CASE WHEN normalized_text = $1 THEN 0 ELSE 1 END,
                    created_at DESC
                LIMIT $3
            """, normalized_query, f"%{normalized_query}%", limit)

        # Get relationship counts for matched entities
        uris = [r["fuseki_uri"] for r in rows]
        rel_counts: Dict[str, int] = {}
        if uris:
            rel_rows = await conn.fetch("""
                SELECT uri, COUNT(*) as cnt FROM (
                    SELECT subject_uri AS uri FROM entity_relationships WHERE subject_uri = ANY($1)
                    UNION ALL
                    SELECT object_uri AS uri FROM entity_relationships WHERE object_uri = ANY($1)
                ) sub GROUP BY uri
            """, uris)
            rel_counts = {r["uri"]: r["cnt"] for r in rel_rows}

        results = []
        for row in rows:
            uri = row["fuseki_uri"]
            entity_type = row["entity_type"]
            entity_name = row["entity_text"]
            result_entry = {
                "fuseki_uri": uri,
                "name": entity_name,
                "entity_type": entity_type,
                "similarity": float(row["similarity"]),
                "aliases": list(row["aliases"] or []),
                "relationship_count": rel_counts.get(uri, 0),
                "quartz_url": quartz_url(entity_type, entity_name),
            }
            if row.get("wallet_address"):
                result_entry["wallet_address"] = row["wallet_address"]
            results.append(result_entry)

    return {"results": results, "count": len(results)}


async def _annotate_vault_fields(
    conn: asyncpg.Connection,
    canonical_uri: Optional[str],
    entity_type: Optional[str],
) -> Tuple[Optional[str], bool, Optional[str]]:
    """Return (vault_path, vault_note_exists, vault_folder) for a resolved entity.

    ANNOTATION ONLY — this never gates, demotes, or alters resolution; it only
    describes where (if anywhere) the resolved entity lives in the vault.

    - vault_path: from the entity_rid_mappings row (public/local scope only;
      node_private mappings are excluded). One indexed lookup on the existing
      idx_rid_mappings_canonical index. Null when there is no mapping. When more
      than one mapping shares the canonical, _pick_best_mapping decides — see
      that helper for why duplicates exist and how the winner is chosen.
    - vault_note_exists: best-effort filesystem check via _vault_note_exists
      (True when the vault isn't mounted here — see that helper). False when
      there is no mapping.
    - vault_folder: the schema folder for the entity type (e.g. Person ->
      People). Null when entity_type is missing.
    """
    vault_folder = None
    if entity_type:
        try:
            vault_folder = get_schema_for_type(entity_type).folder
        except Exception:
            vault_folder = None

    vault_path = None
    if canonical_uri:
        rows = await conn.fetch(
            """
            SELECT erm.vault_path, erm.name, er.entity_text AS canonical_name
            FROM entity_rid_mappings erm
            LEFT JOIN entity_registry er ON er.fuseki_uri = erm.canonical_uri
            WHERE erm.canonical_uri = $1
              AND COALESCE(erm.visibility_scope, 'public') != 'node_private'
            ORDER BY erm.last_synced DESC NULLS LAST, erm.id DESC
            """,
            canonical_uri,
        )
        vault_path = _pick_best_mapping(rows, vault_folder)
    vault_note_exists = _vault_note_exists(vault_path) if vault_path else False
    return vault_path, vault_note_exists, vault_folder


@app.get("/entity/resolve")
async def resolve_entity_get(
    label: str,
    type_hint: Optional[str] = None,
    limit: int = 5,
    persist: bool = False,
):
    """
    Resolve an entity label to canonical entity (GET - backward compatible).

    Query Parameters:
        label: Entity name to resolve
        type_hint: Optional entity type filter
        limit: Maximum candidates (default 5)
        persist: If true and is_new=true, commit the new row to
                 entity_registry before returning. Default false
                 (backwards-compatible; legacy callers unchanged).
                 Wave 2 B2 — see ResolveRequest.persist.

    Returns candidates with URIs, confidence scores, and `persisted` flag.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    entity = ExtractedEntity(name=label, type=type_hint or "")
    persisted = False
    async with db_pool.acquire() as conn:
        canonical, is_new = await resolve_entity(
            conn,
            entity,
            context=None,
            resolution_caller="personal_ingest_api.entity_resolve_get",
        )

        if canonical is None:
            return {"candidates": [], "is_new": False, "persisted": False}

        # Hide node_private entities from public resolution
        if not is_new:
            is_private = await conn.fetchval(
                "SELECT node_private FROM entity_registry WHERE fuseki_uri = $1",
                canonical.uri
            )
            if is_private:
                return {"candidates": [], "is_new": False, "persisted": False}

        # Wave 2 B2 — see resolve_entity_post for full rationale.
        if persist and is_new:
            try:
                await store_new_entity(
                    conn, entity, canonical,
                    document_rid="entity-resolve-api",
                    source="entity-resolve-api",
                )
                persisted = True
            except Exception as e:
                logger.warning(
                    f"persist=true: store_new_entity failed for {canonical.uri}: {e}"
                )

        # Phase G — surface exact-match siblings so callers can detect ambiguity
        # (e.g., two distinct "Patricia" Person rows preserved as keep_separate).
        # Skip for is_new (no siblings could exist) and when limit<=1.
        siblings = []
        if not is_new and limit > 1:
            sibling_rows = await conn.fetch(
                """
                SELECT fuseki_uri, entity_text, entity_type
                FROM entity_registry
                WHERE LOWER(entity_text) = LOWER($1)
                  AND fuseki_uri != $2
                  AND COALESCE(node_private, FALSE) = FALSE
                  AND merged_into IS NULL
                ORDER BY created_at ASC
                LIMIT $3
                """,
                label, canonical.uri, limit - 1
            )
            siblings = [
                {"name": r["entity_text"], "uri": r["fuseki_uri"],
                 "type": r["entity_type"], "confidence": 1.0, "merged_with": None}
                for r in sibling_rows
            ]

        # Vault-path annotation (P2, annotate-only) for the winning candidate
        # and each sibling. Still inside the connection block.
        p_vpath, p_vexists, p_vfolder = await _annotate_vault_fields(
            conn, canonical.uri, canonical.type
        )
        for s in siblings:
            s["vault_path"], s["vault_note_exists"], s["vault_folder"] = \
                await _annotate_vault_fields(conn, s["uri"], s["type"])

    return {
        "candidates": [{
            "name": canonical.name,
            "uri": canonical.uri,
            "type": canonical.type,
            "confidence": canonical.confidence,
            "merged_with": canonical.merged_with,
            "vault_path": p_vpath,
            "vault_note_exists": p_vexists,
            "vault_folder": p_vfolder,
        }] + siblings,
        "ambiguous": len(siblings) > 0,
        "is_new": is_new,
        "persisted": persisted,
    }


@app.post("/entity/resolve")
async def resolve_entity_post(request: ResolveRequest):
    """
    Resolve an entity label to canonical entity with optional context (POST).

    Request Body:
        label: Entity name to resolve
        type_hint: Optional entity type filter
        limit: Maximum candidates (default 5)
        context: Optional disambiguation context
            - associated_people: List of people co-occurring with this entity

    The context parameter enables Tier 1.5 contextual matching, which uses
    co-occurrence in documents to disambiguate entities. For example:
    - "Biocene Labs" with associated_people=["Orrin Tavish", "Mabel Patel"]
      may resolve to "Symbiocene Labs" if those people appear together in
      documents mentioning Symbiocene Labs.

    Returns candidates with URIs and confidence scores (same format as GET).
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    entity = ExtractedEntity(name=request.label, type=request.type_hint or "")
    persisted = False
    limit = request.limit if hasattr(request, "limit") and request.limit else 5
    async with db_pool.acquire() as conn:
        canonical, is_new = await resolve_entity(
            conn,
            entity,
            request.context,
            resolution_caller="personal_ingest_api.entity_resolve_post",
        )

        if canonical is None:
            return {"candidates": [], "is_new": False, "persisted": False}

        # Hide node_private entities from public resolution
        if not is_new:
            is_private = await conn.fetchval(
                "SELECT node_private FROM entity_registry WHERE fuseki_uri = $1",
                canonical.uri
            )
            if is_private:
                return {"candidates": [], "is_new": False, "persisted": False}

        # Wave 2 B2: opt-in persistence. When persist=true and is_new=true,
        # commit the row via store_new_entity() so callers can immediately
        # reference the URI in subsequent writes (subject_uri / object_uri
        # in /knowledge/episodes). store_new_entity is INSERT-IF-NOT-EXISTS
        # so re-runs are safe.
        if request.persist and is_new:
            try:
                # document_rid is a soft-required arg on store_new_entity;
                # for resolver-driven creation there's no source document,
                # so we pass a sentinel that won't accidentally collide
                # with real koi_memories rids.
                await store_new_entity(
                    conn, entity, canonical,
                    document_rid="entity-resolve-api",
                    source="entity-resolve-api",
                )
                persisted = True
            except Exception as e:
                logger.warning(
                    f"persist=true: store_new_entity failed for {canonical.uri}: {e}"
                )

        # Phase G — surface exact-match siblings so callers can detect ambiguity.
        siblings = []
        if not is_new and limit > 1:
            sibling_rows = await conn.fetch(
                """
                SELECT fuseki_uri, entity_text, entity_type
                FROM entity_registry
                WHERE LOWER(entity_text) = LOWER($1)
                  AND fuseki_uri != $2
                  AND COALESCE(node_private, FALSE) = FALSE
                  AND merged_into IS NULL
                ORDER BY created_at ASC
                LIMIT $3
                """,
                request.label, canonical.uri, limit - 1
            )
            siblings = [
                {"name": r["entity_text"], "uri": r["fuseki_uri"],
                 "type": r["entity_type"], "confidence": 1.0, "merged_with": None}
                for r in sibling_rows
            ]

        # Vault-path annotation (P2, annotate-only) for winner + siblings.
        p_vpath, p_vexists, p_vfolder = await _annotate_vault_fields(
            conn, canonical.uri, canonical.type
        )
        for s in siblings:
            s["vault_path"], s["vault_note_exists"], s["vault_folder"] = \
                await _annotate_vault_fields(conn, s["uri"], s["type"])

    return {
        "candidates": [{
            "name": canonical.name,
            "uri": canonical.uri,
            "type": canonical.type,
            "confidence": canonical.confidence,
            "merged_with": canonical.merged_with,
            "vault_path": p_vpath,
            "vault_note_exists": p_vexists,
            "vault_folder": p_vfolder,
        }] + siblings,
        "ambiguous": len(siblings) > 0,
        "is_new": is_new,
        "persisted": persisted,
    }


# =============================================================================
# Entity MentionedIn Endpoints (for bidirectional linking)
# IMPORTANT: These must be defined BEFORE the generic /entity/{uri:path} route
# =============================================================================

class EvidenceItem(BaseModel):
    subject_uri: str
    subject_name: str
    predicate: str
    object_uri: str
    object_name: str
    confidence: Optional[float] = None
    source: Optional[str] = None
    source_section: Optional[str] = None
    wiki_url: Optional[str] = None


class EvidenceResponse(BaseModel):
    entity_uri: str
    entity_name: str
    evidence: List[EvidenceItem]
    total: int


class MentionedInDocument(BaseModel):
    """A document that mentions an entity"""
    vault_path: str  # NO .md extension
    document_rid: str
    mention_count: int
    doc_date: Optional[str] = None
    first_seen: Optional[str] = None  # NULL for rows backfilled before created_at column existed
    # Informational: whether the backlinked note currently exists on disk.
    # NOT used to drop rows (a relocated note keeps its backlink under the old
    # path); clients that build mentionedIn arrays should filter on this.
    vault_note_exists: bool = True


class MentionedInResponse(BaseModel):
    """Response from mentioned-in endpoint"""
    entity_uri: str
    total_count: int
    truncated: bool
    documents: List[MentionedInDocument]


class BatchMentionedInRequest(BaseModel):
    """Request for batch mentioned-in query"""
    uris: List[str]
    limit_per_entity: int = 500


class BatchMentionedInResponse(BaseModel):
    """Response from batch mentioned-in query"""
    results: Dict[str, MentionedInResponse]
    total_entities: int


def extract_date_from_vault_path(vault_path: str) -> Optional[str]:
    """
    Extract date from vault path if present.
    Looks for YYYY-MM-DD pattern at start of filename.
    """
    import re
    # Get filename from path
    filename = vault_path.split('/')[-1] if '/' in vault_path else vault_path
    # Remove .md extension if present
    if filename.endswith('.md'):
        filename = filename[:-3]
    # Look for date pattern at start
    match = re.match(r'^(\d{4}-\d{2}-\d{2})', filename)
    return match.group(1) if match else None


@app.get("/entity/{entity_uri:path}/mentioned-in", response_model=MentionedInResponse)
async def get_entity_mentioned_in(
    entity_uri: str,
    limit: int = 500
):
    """
    Get documents that mention an entity.

    This endpoint queries document_entity_links to find all documents
    that mention the given entity. Used for populating mentionedIn
    frontmatter in entity notes.

    Args:
        entity_uri: Canonical entity URI (orn:personal-koi.entity:...)
        limit: Maximum documents to return (default 500, high to avoid silent truncation)

    Returns:
        MentionedInResponse with sorted document list (alphabetical by vault_path)
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Reject queries for node_private entities
        is_private = await conn.fetchval(
            "SELECT node_private FROM entity_registry WHERE fuseki_uri = $1",
            entity_uri
        )
        if is_private:
            raise HTTPException(status_code=404, detail="Entity not found")

        # Query document_entity_links for documents mentioning this entity.
        # Filter to vault-file RIDs only (orn:obsidian.entity:..., vault:..., or bare
        # vault paths). Sensor RIDs (claude-session:, email-message:, telegram:, slack:,
        # discourse:, gh-*:, etc.) are not vault files and would render as dangling
        # wikilinks if written into mentionedIn frontmatter — they belong in the
        # knowledge graph but not in entity-note backlinks. Filter is applied in SQL
        # rather than in Python so the LIMIT applies to vault rows, not sensor rows.
        rows = await conn.fetch("""
            SELECT del.document_rid, del.mention_count, del.created_at
            FROM document_entity_links del
            WHERE del.entity_uri = $1
              AND (
                del.document_rid LIKE 'orn:obsidian.entity:%'
                OR del.document_rid LIKE 'orn:obsidian.document:%'
                OR del.document_rid LIKE 'vault:%'
                OR del.document_rid NOT LIKE '%:%'
              )
            ORDER BY del.document_rid ASC
            LIMIT $2
        """, entity_uri, limit + 1)  # Fetch limit+1 to detect truncation

        truncated = len(rows) > limit
        if truncated:
            rows = rows[:limit]

        documents = []
        for row in rows:
            # Convert document_rid to vault_path
            # document_rid format: "orn:obsidian.entity:Notes/..." or "vault:notes/..."
            doc_rid = row['document_rid']

            # Extract vault path from RID
            # Handles various formats: orn:obsidian.entity:Notes/, vault:notes/, vault:
            if doc_rid.startswith('orn:obsidian.entity:Notes/'):
                vault_path = doc_rid.replace('orn:obsidian.entity:Notes/', '')
            elif doc_rid.startswith('orn:obsidian.entity:'):
                vault_path = doc_rid.replace('orn:obsidian.entity:', '')
            elif doc_rid.startswith('orn:obsidian.document:Notes/'):
                vault_path = doc_rid.replace('orn:obsidian.document:Notes/', '')
            elif doc_rid.startswith('orn:obsidian.document:'):
                vault_path = doc_rid.replace('orn:obsidian.document:', '')
            elif doc_rid.startswith('vault:notes/'):
                vault_path = doc_rid.replace('vault:notes/', '')
            elif doc_rid.startswith('vault:'):
                vault_path = doc_rid.replace('vault:', '')
            else:
                vault_path = doc_rid

            # Remove .md extension for Obsidian wikilink format
            if vault_path.endswith('.md'):
                vault_path = vault_path[:-3]

            # Extract date from filename if present
            doc_date = extract_date_from_vault_path(vault_path)

            documents.append(MentionedInDocument(
                vault_path=vault_path,
                document_rid=doc_rid,
                mention_count=row['mention_count'] or 1,
                doc_date=doc_date,
                first_seen=row['created_at'].isoformat() if row['created_at'] else None,
                vault_note_exists=_vault_note_exists(vault_path),
            ))

        return MentionedInResponse(
            entity_uri=entity_uri,
            total_count=len(documents),
            truncated=truncated,
            documents=documents
        )


@app.post("/entities/mentioned-in", response_model=BatchMentionedInResponse)
async def get_entities_mentioned_in_batch(request: BatchMentionedInRequest):
    """
    Batch query for documents mentioning multiple entities.

    This endpoint allows efficient querying of multiple entities at once,
    avoiding N+1 API calls when propagating mentionedIn to many entity notes.

    Args:
        uris: List of entity URIs to query
        limit_per_entity: Maximum documents per entity (default 500)

    Returns:
        BatchMentionedInResponse with results keyed by entity URI
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    if not request.uris:
        return BatchMentionedInResponse(results={}, total_entities=0)

    results = {}

    async with db_pool.acquire() as conn:
        # Filter out node_private entities from the batch
        private_uris = set()
        if request.uris:
            private_rows = await conn.fetch(
                "SELECT fuseki_uri FROM entity_registry WHERE fuseki_uri = ANY($1) AND node_private",
                request.uris
            )
            private_uris = {r['fuseki_uri'] for r in private_rows}

        for entity_uri in request.uris:
            if entity_uri in private_uris:
                continue
            # Vault-RID-only filter — see comment on get_entity_mentioned_in above.
            # Sensor RIDs are excluded so this endpoint is safe to use for
            # mentionedIn frontmatter population.
            rows = await conn.fetch("""
                SELECT del.document_rid, del.mention_count, del.created_at
                FROM document_entity_links del
                WHERE del.entity_uri = $1
                  AND (
                    del.document_rid LIKE 'orn:obsidian.entity:%'
                    OR del.document_rid LIKE 'vault:%'
                    OR del.document_rid NOT LIKE '%:%'
                  )
                ORDER BY del.document_rid ASC
                LIMIT $2
            """, entity_uri, request.limit_per_entity + 1)

            truncated = len(rows) > request.limit_per_entity
            if truncated:
                rows = rows[:request.limit_per_entity]

            documents = []
            for row in rows:
                doc_rid = row['document_rid']

                # Extract vault path from RID
                # Handles various formats: orn:obsidian.entity:Notes/, vault:notes/, vault:
                if doc_rid.startswith('orn:obsidian.entity:Notes/'):
                    vault_path = doc_rid.replace('orn:obsidian.entity:Notes/', '')
                elif doc_rid.startswith('orn:obsidian.entity:'):
                    vault_path = doc_rid.replace('orn:obsidian.entity:', '')
                elif doc_rid.startswith('vault:notes/'):
                    vault_path = doc_rid.replace('vault:notes/', '')
                elif doc_rid.startswith('vault:'):
                    vault_path = doc_rid.replace('vault:', '')
                else:
                    vault_path = doc_rid

                if vault_path.endswith('.md'):
                    vault_path = vault_path[:-3]

                doc_date = extract_date_from_vault_path(vault_path)

                documents.append(MentionedInDocument(
                    vault_path=vault_path,
                    document_rid=doc_rid,
                    mention_count=row['mention_count'] or 1,
                    doc_date=doc_date,
                    first_seen=row['created_at'].isoformat() if row['created_at'] else None,
                    vault_note_exists=_vault_note_exists(vault_path),
                ))

            results[entity_uri] = MentionedInResponse(
                entity_uri=entity_uri,
                total_count=len(documents),
                truncated=truncated,
                documents=documents
            )

    return BatchMentionedInResponse(
        results=results,
        total_entities=len(results)
    )


# =============================================================================
# Evidence Provenance Endpoint
# =============================================================================

@app.get("/entity/{entity_uri:path}/evidence", response_model=EvidenceResponse)
async def get_entity_evidence(entity_uri: str):
    """Get evidence provenance for an entity's relationships.

    Returns all relationships involving this entity with source provenance,
    including MediaWiki page links and wiki URLs where available.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Look up entity name
        entity = await conn.fetchrow(
            "SELECT entity_text FROM entity_registry WHERE fuseki_uri = $1 AND NOT node_private",
            entity_uri
        )
        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        entity_name = entity["entity_text"]

        # Get wiki base URL (if any wiki is registered)
        wiki_base_url = None
        try:
            wiki_base_url = await conn.fetchval(
                "SELECT base_url FROM mediawiki_wikis ORDER BY id LIMIT 1"
            )
        except asyncpg.UndefinedTableError:
            pass  # migration 063 not applied

        # Query relationships with provenance
        try:
            rows = await conn.fetch("""
                SELECT
                    er.subject_uri,
                    subj.entity_text AS subject_name,
                    er.predicate,
                    er.object_uri,
                    obj.entity_text AS object_name,
                    er.confidence,
                    er.source,
                    mpl.source_section,
                    mps.title AS wiki_page_title
                FROM entity_relationships er
                JOIN entity_registry subj ON subj.fuseki_uri = er.subject_uri
                JOIN entity_registry obj ON obj.fuseki_uri = er.object_uri
                LEFT JOIN mediawiki_page_state mps ON mps.source_rid = er.source_rid
                LEFT JOIN mediawiki_page_links mpl ON mpl.source_page_id = mps.id
                    AND mpl.target_title = obj.entity_text
                    AND mpl.predicate = er.predicate
                WHERE (er.subject_uri = $1 OR er.object_uri = $1)
                ORDER BY er.confidence DESC NULLS LAST
                LIMIT 100
            """, entity_uri)
        except asyncpg.UndefinedTableError:
            # mediawiki tables don't exist — fall back without provenance
            rows = await conn.fetch("""
                SELECT
                    er.subject_uri,
                    subj.entity_text AS subject_name,
                    er.predicate,
                    er.object_uri,
                    obj.entity_text AS object_name,
                    er.confidence,
                    er.source,
                    NULL::text AS source_section,
                    NULL::text AS wiki_page_title
                FROM entity_relationships er
                JOIN entity_registry subj ON subj.fuseki_uri = er.subject_uri
                JOIN entity_registry obj ON obj.fuseki_uri = er.object_uri
                WHERE (er.subject_uri = $1 OR er.object_uri = $1)
                ORDER BY er.confidence DESC NULLS LAST
                LIMIT 100
            """, entity_uri)

        evidence = []
        for row in rows:
            wiki_url = None
            if wiki_base_url and row["wiki_page_title"]:
                page_path = row["wiki_page_title"].replace(" ", "_")
                wiki_url = f"{wiki_base_url.rstrip('/')}/wiki/{page_path}"
                if row["source_section"]:
                    wiki_url += f"#{row['source_section']}"

            evidence.append(EvidenceItem(
                subject_uri=row["subject_uri"],
                subject_name=row["subject_name"],
                predicate=row["predicate"],
                object_uri=row["object_uri"],
                object_name=row["object_name"],
                confidence=row["confidence"],
                source=row["source"],
                source_section=row["source_section"],
                wiki_url=wiki_url,
            ))

        return EvidenceResponse(
            entity_uri=entity_uri,
            entity_name=entity_name,
            evidence=evidence,
            total=len(evidence),
        )


# =============================================================================
# Entity CRUD Endpoints
# =============================================================================

@app.get("/entity/{entity_uri:path}")
async def get_entity(entity_uri: str):
    """Get a specific entity by URI"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        entity = await conn.fetchrow("""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text,
                   source, first_seen_rid, metadata, wallet_address, created_at
            FROM entity_registry
            WHERE fuseki_uri = $1 AND NOT node_private
        """, entity_uri)

        if not entity:
            raise HTTPException(status_code=404, detail="Entity not found")

        # Get linked documents
        docs = await conn.fetch("""
            SELECT document_rid, mention_count, context, created_at
            FROM document_entity_links
            WHERE entity_uri = $1
            ORDER BY created_at DESC
        """, entity_uri)

        return {
            "entity": dict(entity),
            "documents": [dict(d) for d in docs]
        }


class WalletUpdateRequest(BaseModel):
    wallet_address: str = Field(..., description="Bech32 regen address (regen1...) or EVM hex (0x...)")


def _validate_wallet_address(address: str) -> bool:
    """Validate a wallet address (bech32 regen or EVM hex)."""
    # EVM hex address
    if re.match(r'^0x[0-9a-fA-F]{40}$', address):
        return True
    # Bech32 regen address
    try:
        import bech32
        hrp, data = bech32.bech32_decode(address)
        if hrp == "regen" and data is not None:
            return True
    except Exception:
        pass
    return False


@app.patch("/entities/{entity_uri:path}/wallet")
async def update_entity_wallet(entity_uri: str, body: WalletUpdateRequest):
    """Set or update the wallet address for an entity."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    if not _validate_wallet_address(body.wallet_address):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid wallet address: {body.wallet_address}. "
                   f"Must be bech32 regen address (regen1...) or EVM hex (0x...)",
        )

    async with db_pool.acquire() as conn:
        entity = await conn.fetchrow(
            "SELECT fuseki_uri FROM entity_registry WHERE fuseki_uri = $1",
            entity_uri,
        )
        if not entity:
            raise HTTPException(status_code=404, detail=f"Entity not found: {entity_uri}")

        try:
            await conn.execute(
                "UPDATE entity_registry SET wallet_address = $2, updated_at = NOW() WHERE fuseki_uri = $1",
                entity_uri, body.wallet_address,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(
                status_code=409,
                detail=f"Wallet address already registered to another entity: {body.wallet_address}",
            )

    return {"entity_uri": entity_uri, "wallet_address": body.wallet_address}


@app.get("/stats")
async def get_stats():
    """Get knowledge base statistics"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM entity_registry WHERE NOT node_private")

        by_type = await conn.fetch("""
            SELECT entity_type, COUNT(*) as count
            FROM entity_registry
            WHERE NOT node_private
            GROUP BY entity_type
            ORDER BY count DESC
        """)

        recent = await conn.fetch("""
            SELECT entity_text, entity_type, created_at
            FROM entity_registry
            WHERE NOT node_private
            ORDER BY created_at DESC
            LIMIT 10
        """)

        return {
            "total_entities": total,
            "by_type": {r['entity_type']: r['count'] for r in by_type},
            "recent_entities": [dict(r) for r in recent],
            "mode": KOI_MODE
        }


async def _recompute_node_private(conn, canonical_uri: str):
    """Set node_private=true only when ALL rid_mappings are node_private."""
    row = await conn.fetchrow("""
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE visibility_scope = 'node_private') AS private_count
        FROM entity_rid_mappings
        WHERE canonical_uri = $1
    """, canonical_uri)
    is_private = row['total'] > 0 and row['total'] == row['private_count']
    await conn.execute(
        "UPDATE entity_registry SET node_private = $1 WHERE fuseki_uri = $2",
        is_private, canonical_uri
    )


@app.post("/register-entity", response_model=RegisterEntityResponse)
async def register_vault_entity(request: RegisterEntityRequest):
    """
    Register a vault entity note with the backend.

    This endpoint:
    1. Checks if entity already exists (by name + type)
    2. Creates new or links to existing canonical entity
    3. Stores the vault RID → canonical URI mapping
    4. Returns canonical URI for frontmatter update
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Create an ExtractedEntity from the request for resolution
            entity = ExtractedEntity(
                name=request.name,
                type=request.entity_type,
                mentions=[request.name],
                confidence=1.0,
                context=None
            )

            # Resolve against existing entities. force_type suppresses only the
            # Tier 1.1b cross-type dedup fallback (advisory cross_type_warning
            # below still fires).
            try:
                canonical, is_new = await resolve_entity(
                    conn,
                    entity,
                    skip_fuzzy=request.exact_only,
                    skip_cross_type=request.force_type,
                    resolution_caller="personal_ingest_api.register_vault_entity",
                )
            except GeneratedUriHeldError as e:
                raise HTTPException(status_code=409, detail=str(e))

            # Backfill vault_rid / vault_path for alias-only updates.
            # When the caller omits them (e.g. alias auto-learning), reuse the
            # existing mapping so we update-in-place instead of 422-rejecting.
            eff_vault_rid = request.vault_rid
            eff_vault_path = request.vault_path
            if (eff_vault_rid is None or eff_vault_path is None) and not is_new:
                existing_rid_row = await conn.fetchrow("""
                    SELECT vault_rid, vault_path FROM entity_rid_mappings
                    WHERE canonical_uri = $1
                    ORDER BY last_synced DESC NULLS LAST
                    LIMIT 1
                """, canonical.uri)
                if existing_rid_row:
                    eff_vault_rid = eff_vault_rid or existing_rid_row["vault_rid"]
                    eff_vault_path = eff_vault_path or existing_rid_row["vault_path"]

            # Check for URI collision with different vault file
            collision_warning = None
            suppress_types = {'Meeting', 'Task'}
            suppress_paths = {'Tests/'}
            is_suppressed = (
                request.entity_type in suppress_types
                or (eff_vault_path is not None and any(eff_vault_path.startswith(p) for p in suppress_paths))
            )
            if not is_new and eff_vault_path is not None:
                existing_mapping = await conn.fetchrow("""
                    SELECT vault_path, name FROM entity_rid_mappings
                    WHERE canonical_uri = $1 AND vault_path != $2
                """, canonical.uri, eff_vault_path)

                if existing_mapping and not is_suppressed:
                    collision_warning = (
                        f"URI collision: '{request.name}' ({eff_vault_path}) "
                        f"shares URI with '{existing_mapping['name']}' ({existing_mapping['vault_path']})"
                    )
                    logger.warning(collision_warning)

            # Check for same name registered under a different entity type.
            # Live rows only: a merged-away row has already been deduplicated, so
            # it is not a possible duplicate to merge or cross-link.
            cross_type_warning = None
            if not is_suppressed:
                normalized_name = normalize_entity_text(request.name)
                cross_type_rows = await conn.fetch("""
                    SELECT er.entity_type, erm.vault_path
                    FROM entity_registry er
                    LEFT JOIN entity_rid_mappings erm ON erm.canonical_uri = er.fuseki_uri
                    WHERE er.normalized_text = $1 AND er.entity_type != $2
                      AND er.merged_into IS NULL
                    LIMIT 3
                """, normalized_name, request.entity_type)
                if cross_type_rows:
                    alts = ", ".join(
                        f"{r['entity_type']} ({r['vault_path']})" for r in cross_type_rows
                    )
                    cross_type_warning = (
                        f"'{request.name}' also exists as: {alts} — "
                        f"possible duplicate; consider merging or cross-linking"
                    )
                    logger.warning(cross_type_warning)

            if is_new:
                # Store new entity. first_seen_rid is informational; fall back to
                # a sentinel when the caller didn't supply a vault_rid.
                await store_new_entity(
                    conn, entity, canonical,
                    eff_vault_rid or "orn:personal-koi.source:register-entity"
                )
                logger.info(f"Registered new entity: {canonical.uri}")
            else:
                logger.info(f"Linked to existing entity: {canonical.uri}")

            # Store or update RID mapping — only when we have an actual vault RID
            # + path. Alias-only updates (no rid/path, no existing mapping to
            # backfill from) skip this and just merge aliases below.
            if eff_vault_rid and eff_vault_path:
                await conn.execute("""
                    INSERT INTO entity_rid_mappings (
                        vault_rid, vault_path, canonical_uri, entity_type,
                        name, content_hash, sync_status, last_synced, visibility_scope
                    ) VALUES ($1, $2, $3, $4, $5, $6, 'linked', NOW(), $7)
                    ON CONFLICT (vault_rid) DO UPDATE SET
                        vault_path = EXCLUDED.vault_path,
                        canonical_uri = EXCLUDED.canonical_uri,
                        entity_type = EXCLUDED.entity_type,
                        name = EXCLUDED.name,
                        content_hash = EXCLUDED.content_hash,
                        sync_status = 'linked',
                        last_synced = NOW(),
                        visibility_scope = EXCLUDED.visibility_scope
                """,
                    eff_vault_rid,
                    eff_vault_path,
                    canonical.uri,
                    request.entity_type,
                    request.name,
                    request.content_hash,
                    request.visibility_scope or "public"
                )

                # Prune sibling mappings for this canonical whose note is gone.
                # The upsert above conflicts on vault_rid, so registering a
                # second note for an existing canonical INSERTs rather than
                # repointing — leaving the old row to compete forever (that is
                # how People/Mabs.md kept beating People/Mabel Patel.md).
                # Only rows whose file no longer exists are removed: two LIVE
                # notes on one canonical is a real ambiguity for the operator,
                # surfaced as the collision warning, not something to silently
                # resolve by deletion. _vault_note_exists is fail-safe (True
                # when the vault isn't mounted), so a headless deploy prunes
                # nothing. document_entity_links hang off canonical_uri, not
                # these rows, so pruning never drops links.
                sibling_rows = await conn.fetch(
                    """
                    SELECT vault_rid, vault_path FROM entity_rid_mappings
                    WHERE canonical_uri = $1 AND vault_rid != $2
                    """,
                    canonical.uri, eff_vault_rid,
                )
                for sibling in sibling_rows:
                    if _vault_note_exists(sibling["vault_path"]):
                        continue
                    await conn.execute(
                        "DELETE FROM entity_rid_mappings WHERE vault_rid = $1",
                        sibling["vault_rid"],
                    )
                    logger.info(
                        "Pruned stale mapping %s -> %s (note missing) for %s",
                        sibling["vault_rid"], sibling["vault_path"], canonical.uri,
                    )

                # Update entity_registry with vault_rid if not set
                await conn.execute("""
                    UPDATE entity_registry
                    SET vault_rid = $1
                    WHERE fuseki_uri = $2 AND vault_rid IS NULL
                """, eff_vault_rid, canonical.uri)

            # Auto-assign koi_rid for federated publication scope
            final_koi_rid = None
            if request.publication_scope == "federated":
                existing_koi_rid = await conn.fetchval(
                    "SELECT koi_rid FROM entity_registry WHERE fuseki_uri = $1",
                    canonical.uri
                )
                if not existing_koi_rid:
                    import hashlib as _hashlib
                    type_slug = request.entity_type.lower()
                    h = _hashlib.sha256(canonical.uri.encode()).hexdigest()[:32]
                    koi_rid = f"orn:koi-net.{type_slug}:{h}"
                    await conn.execute(
                        "UPDATE entity_registry SET koi_rid = $1 WHERE fuseki_uri = $2",
                        koi_rid, canonical.uri
                    )
                    logger.info(f"Auto-assigned koi_rid for federated entity: {koi_rid}")
                final_koi_rid = await conn.fetchval(
                    "SELECT koi_rid FROM entity_registry WHERE fuseki_uri = $1",
                    canonical.uri
                )

            # Recompute node_private flag based on all mappings for this entity
            await _recompute_node_private(conn, canonical.uri)

            # Sync relationships from frontmatter if provided
            # Accept frontmatter OR properties (for older MCP clients)
            rel_stats = None
            relationship_sync_error: Optional[str] = None
            frontmatter_data = request.frontmatter or request.properties
            if frontmatter_data:
                # Relationship sync needs a vault_path; alias merge (below) does not.
                if eff_vault_path:
                    # SAVEPOINT, because `except Exception` does NOT undo a failed
                    # statement's effect on the transaction. This whole handler runs in one
                    # `async with conn.transaction()`. When sync_vault_relationships raised,
                    # PostgreSQL put the transaction in the aborted state; catching the
                    # exception let the handler keep going, but every later statement then
                    # failed with InFailedSQLTransactionError and the closing COMMIT was
                    # silently converted to a ROLLBACK. So the endpoint returned HTTP 200
                    # success=True having discarded the entity_registry and
                    # entity_rid_mappings writes it had just made — not merely the
                    # relationships. Rolling back to a savepoint is what makes the enclosing
                    # transaction usable again; the same pattern is already used per-edge in
                    # vault_parser.sync_vault_relationships ("SAVEPOINT rel_insert").
                    try:
                        await conn.execute("SAVEPOINT vault_rel_sync")
                        rel_stats = await sync_vault_relationships(
                            conn,
                            eff_vault_path,
                            canonical.uri,
                            frontmatter_data
                        )
                        logger.info(f"Synced relationships: {rel_stats}")

                        # Enqueue relationship changes to TerminusDB outbox
                        if rel_stats and TERMINUSDB_ENABLED:
                            await _enqueue_relationship_outbox(
                                conn, canonical.uri, eff_vault_path)
                        await conn.execute("RELEASE SAVEPOINT vault_rel_sync")
                    except Exception as e:
                        # Valid in the aborted state — this is what clears it.
                        try:
                            await conn.execute("ROLLBACK TO SAVEPOINT vault_rel_sync")
                        except Exception as rollback_error:  # savepoint never established
                            logger.error(
                                "Could not roll back to vault_rel_sync for %s; the "
                                "registration transaction is unusable and will roll back: %s",
                                eff_vault_path, rollback_error,
                            )
                        # Do NOT re-raise: registering the entity succeeded and is worth
                        # keeping. But this is a whole-note failure, not a per-edge one —
                        # sync_vault_relationships resolves every target in one batch, so
                        # one bad target loses every relationship in the note. Name the
                        # note and the exception type; "Failed to sync relationships: ..."
                        # with no path was unattributable across thousands of notes.
                        relationship_sync_error = f"{type(e).__name__}: {e}"
                        logger.warning(
                            "Relationship sync FAILED for %s (%s) — entity registered, "
                            "but every frontmatter relationship in this note was dropped: %s",
                            eff_vault_path, canonical.uri, relationship_sync_error,
                        )

                # Update aliases in entity_registry if provided in frontmatter
                raw_aliases = frontmatter_data.get('aliases', [])
                if raw_aliases:
                    if isinstance(raw_aliases, str):
                        raw_aliases = [raw_aliases]
                    normalized_aliases = [normalize_alias(a) for a in raw_aliases if a]

                    if normalized_aliases:
                        try:
                            # SAVEPOINT: same reasoning as vault_rel_sync above — an
                            # unhandled exception here would leave the transaction
                            # aborted and silently turn the closing COMMIT into a
                            # ROLLBACK, discarding the entity registration itself, not
                            # just the alias update.
                            await conn.execute("SAVEPOINT alias_update")
                            # Merge with existing aliases using DISTINCT to prevent duplicates
                            await conn.execute("""
                                UPDATE entity_registry
                                SET aliases = (
                                    SELECT ARRAY(
                                        SELECT DISTINCT unnest(
                                            array_cat(COALESCE(aliases, '{}'), $1::TEXT[])
                                        )
                                    )
                                )
                                WHERE fuseki_uri = $2
                            """, normalized_aliases, canonical.uri)
                            await conn.execute("RELEASE SAVEPOINT alias_update")
                            logger.info(f"Updated aliases for {canonical.uri}: {normalized_aliases}")
                        except Exception as e:
                            await conn.execute("ROLLBACK TO SAVEPOINT alias_update")
                            # Same shape as the relationship-sync handler above, smaller
                            # blast radius: this loses the aliases for one entity, not
                            # every relationship in the note, so it stays out of the
                            # response model. It must still be attributable.
                            logger.warning(
                                "Alias update FAILED for %s (%s) — aliases %s were not "
                                "written: %s: %s",
                                eff_vault_path, canonical.uri, normalized_aliases,
                                type(e).__name__, e,
                            )

            # Resolve any pending relationships that match this new entity
            pending_promoted = 0
            if is_new:
                try:
                    # SAVEPOINT: resolve_pending_relationships now savepoints its own
                    # promotion internally, but this wraps the call too as
                    # defense-in-depth against any exception outside that inner
                    # try/except (matching the vault_rel_sync / alias_update pattern
                    # above rather than assuming the inner function is exception-proof).
                    await conn.execute("SAVEPOINT pending_promotion_call")
                    pending_promoted = await resolve_pending_relationships(
                        conn,
                        canonical.uri,
                        request.name,
                        request.entity_type
                    )
                    await conn.execute("RELEASE SAVEPOINT pending_promotion_call")
                    if pending_promoted > 0:
                        logger.info(f"Promoted {pending_promoted} pending relationship(s)")
                except Exception as e:
                    await conn.execute("ROLLBACK TO SAVEPOINT pending_promotion_call")
                    # Leaves the pending rows in place, so this is recoverable on a later
                    # registration rather than lost — hence log-only. Attributable anyway.
                    logger.warning(
                        "Pending-relationship promotion FAILED for %s (%s) — rows remain "
                        "pending: %s: %s",
                        eff_vault_path, canonical.uri, type(e).__name__, e,
                    )

            result = RegisterEntityResponse(
                success=True,
                canonical_uri=canonical.uri,
                is_new=is_new,
                vault_rid=eff_vault_rid or "",
                merged_with=canonical.merged_with,
                collision_warning=collision_warning,
                cross_type_warning=cross_type_warning,
                koi_rid=final_koi_rid if request.publication_scope == "federated" else None,
                relationship_sync_error=relationship_sync_error,
            )

            # Emit federation event for entity replication. Name and type are the
            # resolved row's, which register-entity does not change: a peer's apply
            # overwrites both, so the request's would rename or retype the survivor.
            emit_domain_event = _optional_federation_attr("api.federation_events", "emit_domain_event")
            await emit_domain_event("entity", "NEW" if is_new else "UPDATE", canonical.uri, {
                "fuseki_uri": canonical.uri,
                "entity_text": canonical.name or request.name,
                "entity_type": canonical.type or request.entity_type,
                "normalized_text": canonical.name.lower().strip() if canonical.name else request.name.lower().strip(),
                "aliases": [],
                "metadata": {},
            })

            return result


@app.get("/vault-entities")
async def list_vault_entities(
    entity_type: Optional[str] = None,
    sync_status: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """List all vault entities registered with the backend"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Build query with optional filters
        conditions = ["COALESCE(visibility_scope, 'public') != 'node_private'"]
        params = []
        param_idx = 1

        if entity_type:
            conditions.append(f"entity_type = ${param_idx}")
            params.append(entity_type)
            param_idx += 1

        if sync_status:
            conditions.append(f"sync_status = ${param_idx}")
            params.append(sync_status)
            param_idx += 1

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        # Add limit and offset
        params.extend([limit, offset])

        query = f"""
            SELECT vault_rid, vault_path, canonical_uri, entity_type,
                   name, sync_status, content_hash, last_synced
            FROM entity_rid_mappings
            {where_clause}
            ORDER BY last_synced DESC
            LIMIT ${param_idx} OFFSET ${param_idx + 1}
        """

        entities = await conn.fetch(query, *params)

        # Get total count (exclude limit/offset params)
        count_query = f"SELECT COUNT(*) FROM entity_rid_mappings {where_clause}"
        count_params = params[:-2]  # Strip limit/offset
        if count_params:
            total = await conn.fetchval(count_query, *count_params)
        else:
            total = await conn.fetchval(count_query)

        return {
            "entities": [
                {
                    "vault_rid": e['vault_rid'],
                    "vault_path": e['vault_path'],
                    "canonical_uri": e['canonical_uri'],
                    "entity_type": e['entity_type'],
                    "name": e['name'],
                    "sync_status": e['sync_status'],
                    "content_hash": e['content_hash'],
                    "last_synced": e['last_synced'].isoformat() if e['last_synced'] else None
                }
                for e in entities
            ],
            "count": total,
            "limit": limit,
            "offset": offset
        }


@app.get("/vault-entity/{vault_rid:path}")
async def get_vault_entity(vault_rid: str):
    """Get a specific vault entity by its RID"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        mapping = await conn.fetchrow("""
            SELECT vault_rid, vault_path, canonical_uri, entity_type,
                   name, sync_status, content_hash, last_synced, created_at
            FROM entity_rid_mappings
            WHERE vault_rid = $1
              AND COALESCE(visibility_scope, 'public') != 'node_private'
        """, vault_rid)

        if not mapping:
            raise HTTPException(status_code=404, detail="Vault entity not found")

        # Get the canonical entity details
        entity = await conn.fetchrow("""
            SELECT fuseki_uri, entity_text, entity_type, normalized_text,
                   source, first_seen_rid, metadata, created_at
            FROM entity_registry
            WHERE fuseki_uri = $1
        """, mapping['canonical_uri'])

        return {
            "mapping": {
                "vault_rid": mapping['vault_rid'],
                "vault_path": mapping['vault_path'],
                "canonical_uri": mapping['canonical_uri'],
                "entity_type": mapping['entity_type'],
                "name": mapping['name'],
                "sync_status": mapping['sync_status'],
                "content_hash": mapping['content_hash'],
                "last_synced": mapping['last_synced'].isoformat() if mapping['last_synced'] else None,
                "created_at": mapping['created_at'].isoformat() if mapping['created_at'] else None
            },
            "entity": dict(entity) if entity else None
        }


@app.post("/resolve-to-vault")
async def resolve_canonical_to_vault(uris: List[str]):
    """
    Resolve canonical URIs to vault paths for wikilink generation.

    Given a list of canonical entity URIs, returns the corresponding vault paths
    that can be used to create wikilinks like [[People/Corin Ashgrove]].

    Example:
        POST /resolve-to-vault
        ["orn:personal-koi.entity:person-corin-ashgrove-abc123", ...]

        Returns:
        {
            "mappings": [
                {
                    "canonical_uri": "orn:personal-koi.entity:person-corin-ashgrove-abc123",
                    "vault_path": "People/Corin Ashgrove.md",
                    "name": "Corin Ashgrove",
                    "wikilink": "[[People/Corin Ashgrove]]"
                }
            ],
            "not_found": []
        }
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    mappings = []
    not_found = []
    orphaned = []  # registry row exists but its vault note is gone (phantom path)

    async with db_pool.acquire() as conn:
        for uri in uris:
            row = await conn.fetchrow("""
                SELECT vault_rid, vault_path, canonical_uri, entity_type, name
                FROM entity_rid_mappings
                WHERE canonical_uri = $1
                  AND COALESCE(visibility_scope, 'public') != 'node_private'
                LIMIT 1
            """, uri)

            if not row:
                not_found.append(uri)
                continue

            vault_path = row['vault_path']
            # Phantom-path guard: never emit a wikilink to a note that no longer
            # exists on disk. Such rows resolve to dangling links downstream.
            if not _vault_note_exists(vault_path):
                orphaned.append({
                    "canonical_uri": row['canonical_uri'],
                    "vault_path": vault_path,
                    "name": row['name'],
                    "entity_type": row['entity_type'],
                    "reason": "vault note missing on disk (orphaned entity_rid_mappings row)",
                })
                continue

            # Remove .md extension and use as wikilink
            wikilink_path = vault_path.replace('.md', '') if vault_path.endswith('.md') else vault_path
            mappings.append({
                "canonical_uri": row['canonical_uri'],
                "vault_path": row['vault_path'],
                "name": row['name'],
                "entity_type": row['entity_type'],
                "wikilink": f"[[{wikilink_path}]]",
                "vault_note_exists": True,
            })

    return {
        "mappings": mappings,
        "not_found": not_found,
        "orphaned": orphaned,
        "resolved": len(mappings),
        "orphaned_count": len(orphaned),
        "total": len(uris)
    }


class ContextualCandidatesRequest(BaseModel):
    """Request for contextual entity candidates based on meeting context"""
    project: Optional[str] = None
    attendees: Optional[List[str]] = None
    topics: Optional[List[str]] = None
    document_rid: Optional[str] = None  # Current document being processed
    entity_types: List[str] = Field(default_factory=lambda: ["Person"])  # Entity types to return


# Max documents an attendee name may match before it is skipped as context.
ATTENDEE_DOC_CAP = 1000


async def get_contextual_candidates_internal(
    conn: asyncpg.Connection,
    project: Optional[str] = None,
    attendees: Optional[List[str]] = None,
    attendee_uris: Optional[List[str]] = None,
    topics: Optional[List[str]] = None,
    document_rid: Optional[str] = None,
    entity_types: Optional[List[str]] = None
) -> dict:
    """
    Internal helper for finding contextual entity candidates.

    Returns: {"candidates": [...], "related_documents": [...], "context_types": [...]}
    """
    if entity_types is None:
        entity_types = ["Person"]

    candidates = {}  # uri -> candidate info
    related_docs = set()
    context_types = []

    # Strategy 1: Find meetings linked to the same project entity
    if project:
        project_normalized = project.lower().strip()
        project_docs = await conn.fetch("""
            SELECT DISTINCT del.document_rid
            FROM document_entity_links del
            JOIN entity_registry er ON del.entity_uri = er.fuseki_uri
            WHERE er.entity_type = 'Project'
              AND NOT er.node_private
              AND (LOWER(er.entity_text) LIKE $1
                   OR LOWER(er.normalized_text) LIKE $1)
        """, f"%{project_normalized}%")

        for row in project_docs:
            related_docs.add(row['document_rid'])
        if project_docs:
            context_types.append("project")

    # Strategy 2a: Find meetings by attendee URIs (most precise)
    if attendee_uris:
        uri_docs = await conn.fetch("""
            SELECT DISTINCT document_rid
            FROM document_entity_links
            WHERE entity_uri = ANY($1)
        """, attendee_uris)
        for row in uri_docs:
            related_docs.add(row['document_rid'])
        if uri_docs:
            context_types.append("attendee_uris")

    # Strategy 2b: Find meetings with common attendees (by name)
    skipped_attendees = []
    if attendees:
        for attendee in attendees:
            attendee_normalized = attendee.lower().strip()
            attendee_docs = await conn.fetch("""
                SELECT DISTINCT del.document_rid
                FROM document_entity_links del
                JOIN entity_registry er ON del.entity_uri = er.fuseki_uri
                WHERE er.entity_type = 'Person'
                  AND NOT er.node_private
                  AND (LOWER(er.entity_text) LIKE $1
                       OR LOWER(er.normalized_text) LIKE $1)
            """, f"%{attendee_normalized}%")

            # A name in more than ATTENDEE_DOC_CAP documents carries no
            # disambiguation signal. The operator is linked to ~4,800 of ~28,500
            # documents, so including that name pulled ~1,080 candidates into
            # every meeting's context. Skip it and report it.
            if len(attendee_docs) > ATTENDEE_DOC_CAP:
                skipped_attendees.append(attendee)
                continue

            for row in attendee_docs:
                related_docs.add(row['document_rid'])
        if attendees and related_docs:
            context_types.append("attendees")

    # Strategy 3: Find meetings with similar topics (linked Concept entities)
    if topics:
        for topic in topics:
            topic_normalized = topic.lower().strip()
            topic_docs = await conn.fetch("""
                SELECT DISTINCT del.document_rid
                FROM document_entity_links del
                JOIN entity_registry er ON del.entity_uri = er.fuseki_uri
                WHERE er.entity_type = 'Concept'
                  AND NOT er.node_private
                  AND (LOWER(er.entity_text) LIKE $1
                       OR LOWER(er.normalized_text) LIKE $1)
            """, f"%{topic_normalized}%")

            for row in topic_docs:
                related_docs.add(row['document_rid'])
        if topics and related_docs:
            context_types.append("topics")

    # Exclude current document from related docs
    if document_rid and document_rid in related_docs:
        related_docs.discard(document_rid)

    # Get entities of requested types from related documents. Tombstones are
    # excluded rather than followed: a merge rewires the loser's links to the
    # survivor, so the survivor is already a candidate from the same documents,
    # and Tier 1.5 returns the candidate URI as-is (Phase 1b, 2026-10-12).
    if related_docs:
        related_docs_list = list(related_docs)
        entities = await conn.fetch("""
            SELECT DISTINCT er.fuseki_uri, er.entity_text, er.entity_type,
                   er.normalized_text, er.phonetic_code, del.document_rid
            FROM entity_registry er
            JOIN document_entity_links del ON er.fuseki_uri = del.entity_uri
            WHERE er.entity_type = ANY($1)
              AND NOT er.node_private
              AND er.merged_into IS NULL
              AND del.document_rid = ANY($2)
        """, entity_types, related_docs_list)

        for entity in entities:
            uri = entity['fuseki_uri']
            if uri not in candidates:
                candidates[uri] = {
                    "name": entity['entity_text'],
                    "uri": uri,
                    "normalized_name": entity['normalized_text'],
                    "entity_type": entity['entity_type'],
                    "phonetic_code": entity['phonetic_code'],
                    "source_documents": []
                }
            candidates[uri]["source_documents"].append(entity['document_rid'])

    # Also include entities from vault registry (registered but not yet linked)
    # Only for Person type to keep backward compatibility
    if "Person" in entity_types:
        vault_people = await conn.fetch("""
            SELECT DISTINCT er.fuseki_uri, er.entity_text, er.normalized_text,
                   er.phonetic_code, erm.vault_path
            FROM entity_registry er
            LEFT JOIN entity_rid_mappings erm ON er.fuseki_uri = erm.canonical_uri
            WHERE er.entity_type = 'Person'
              AND NOT er.node_private
              AND er.merged_into IS NULL
              AND erm.vault_path IS NOT NULL
              -- NOT EXISTS, not NOT IN: NOT IN over the whole document_entity_links
              -- table re-scanned it per registry row (~76s on the live DB, 2026-10-13).
              -- NOT EXISTS is an index anti-join on idx_del_entity_uri (~15-100ms).
              AND NOT EXISTS (
                  SELECT 1 FROM document_entity_links del
                  WHERE del.entity_uri = er.fuseki_uri
              )
            LIMIT 50
        """)

        for person in vault_people:
            uri = person['fuseki_uri']
            if uri not in candidates:
                candidates[uri] = {
                    "name": person['entity_text'],
                    "uri": uri,
                    "normalized_name": person['normalized_text'],
                    "entity_type": "Person",
                    "phonetic_code": person['phonetic_code'],
                    "source_documents": [],
                    "vault_path": person['vault_path']
                }

    return {
        "candidates": list(candidates.values()),
        "related_documents": list(related_docs),
        "context_types": context_types,
        "skipped_attendees": skipped_attendees
    }


async def get_contextual_entity_candidates(
    conn: asyncpg.Connection,
    label: str,
    entity_type: str,
    associated_people: List[str],
    context: Optional[ResolutionContext] = None
) -> List[dict]:
    """
    Find entity candidates that co-occur with given people.

    Works for any entity type (Person, Organization, Project, Location, Concept).
    Uses unified scoring formula with phonetic boost for types with phonetic_matching=true.
    Now includes relationship-based relevance scoring when context includes project/organizations.

    Args:
        conn: Database connection
        label: The entity label to match against
        entity_type: Type of entity to search for
        associated_people: List of people names that should co-occur
        context: Optional resolution context with project/organizations for relationship checking

    Returns:
        List of scored candidate entities
    """
    target_normalized = normalize_entity_text(label)

    # Get schema-driven config for this entity type
    schema = get_schema_for_type(entity_type)

    # Exclude self from associated_people (prevents circular context)
    people = [
        p.strip() for p in associated_people
        if p.strip() and normalize_entity_text(p.strip()) != target_normalized
    ]
    people = list(set(people))  # Dedupe

    # Cap at 10 to avoid huge ANY() lists if extraction gets noisy
    if len(people) > 10:
        logger.warning(f"Contextual: truncating {len(people)} associated_people to 10")
        people = people[:10]

    # Schema-driven minimum people requirements
    min_people = schema.min_context_people

    if len(people) < min_people:
        logger.info(f"Contextual: need {min_people} associated people for {entity_type}, got {len(people)}")
        return []

    # Resolve people names to URIs
    # For 1-person case: require UNIQUE resolution (1 URI only)
    # For 2+ people: collect all URIs
    resolved_uris = []
    for person in people:
        rows = await conn.fetch("""
            SELECT fuseki_uri FROM entity_registry
            WHERE entity_type = 'Person' AND normalized_text = $1
            AND merged_into IS NULL
        """, normalize_entity_text(person))

        if len(people) == 1:
            # Single associated person: require unique resolution
            if len(rows) != 1:
                logger.info(f"Contextual: '{person}' resolves to {len(rows)} URIs, "
                           f"need exactly 1 for single-person context (blocking)")
                return []  # Ambiguous or not found
            resolved_uris.append(rows[0]['fuseki_uri'])
        else:
            # Multiple associated people: collect all URIs
            for row in rows:
                resolved_uris.append(row['fuseki_uri'])

    if len(resolved_uris) < min_people:
        logger.info(f"Contextual: resolved {len(resolved_uris)} URIs, need {min_people}")
        return []

    # Call internal helper with URIs
    response = await get_contextual_candidates_internal(
        conn,
        attendee_uris=resolved_uris,
        entity_types=[entity_type]
    )

    # Compute query's phonetic code (for types with phonetic_matching enabled)
    query_phonetic = None
    if schema.phonetic_matching:
        first_token = get_first_significant_token(target_normalized, schema.phonetic_stopwords)
        query_phonetic = get_phonetic_code(first_token)

    # Score candidates
    scored = []
    for candidate in response.get("candidates", []):
        candidate_normalized = candidate.get("normalized_name", normalize_entity_text(candidate["name"]))
        name_sim = jaro_winkler_similarity(target_normalized, candidate_normalized)
        doc_count = len(candidate.get("source_documents", []))

        # Phonetic bonus for types with phonetic_matching enabled (schema-driven)
        phonetic_bonus = 0.0
        phonetic_match = False
        if schema.phonetic_matching and query_phonetic:
            candidate_phonetic = candidate.get("phonetic_code")
            # Guard: if phonetic_code missing, compute on the fly
            if candidate_phonetic is None:
                candidate_first_token = get_first_significant_token(candidate_normalized, schema.phonetic_stopwords)
                candidate_phonetic = get_phonetic_code(candidate_first_token)
            if phonetic_codes_match(query_phonetic, candidate_phonetic):
                phonetic_bonus = 1.0
                phonetic_match = True

        # Minimum fuzzy threshold (relaxed if phonetic match)
        min_fuzzy = 0.4 if phonetic_bonus > 0 else 0.5
        if name_sim >= min_fuzzy:
            scored.append({
                **candidate,
                "name_similarity": name_sim,
                "phonetic_match": phonetic_match,
                "doc_count": doc_count
            })

    # Combined scoring (unified formula)
    # Stable doc_score normalization: min(doc_count / 3, 1.0)
    for c in scored:
        doc_score = min(c["doc_count"] / 3.0, 1.0)
        phonetic_score = 0.2 if c.get("phonetic_match") else 0.0

        # Unified formula: 0.5 * fuzzy + 0.2 * phonetic + 0.3 * doc
        c["combined_score"] = 0.5 * c["name_similarity"] + phonetic_score + 0.3 * doc_score

    # Relationship-based context relevance scoring
    # Only check if context includes project or organizations
    if context and (context.project or context.organizations):
        for c in scored:
            candidate_uri = c.get("uri")
            if not candidate_uri:
                continue

            # Gate context boost behind name similarity floor
            # Prevents weak name matches (e.g. "Regen Builder Lab" vs "Regen AI")
            # from being pushed over threshold by context alone
            min_name_sim_for_boost = 0.80 if c.get("phonetic_match") else 0.90
            if c["name_similarity"] < min_name_sim_for_boost:
                continue  # Skip boost for weak name matches

            relevance = await check_context_relevance(conn, candidate_uri, context)

            if relevance.signal == RelevanceSignal.POSITIVE:
                c["combined_score"] += relevance.score  # Boost
                c["relevance_detail"] = relevance.details
                logger.debug(f"Relevance POSITIVE for {c['name']}: {relevance.details}")

            elif relevance.signal == RelevanceSignal.NEGATIVE:
                # CRITICAL: Phonetic match alone is NOT enough to bypass penalty
                # Paul→Polly is a phonetic match, but Polly has no Regen/Gaia relationships
                # Require phonetic + high name similarity (>0.9) OR phonetic + semantic match
                has_strong_phonetic = (
                    c.get('phonetic_match') and
                    c.get('name_similarity', 0) >= 0.9  # "Sean"→"Shawn" = 0.93
                )

                if not has_strong_phonetic:
                    c["combined_score"] += relevance.score  # Penalty (negative value)
                    c["relevance_detail"] = relevance.details
                    logger.debug(f"Relevance NEGATIVE for {c['name']}: {relevance.details}")

            elif relevance.signal == RelevanceSignal.UNKNOWN:
                # Fallback to document co-occurrence
                fallback = await check_fallback_relevance(conn, candidate_uri, context)
                if fallback > 0:
                    c["combined_score"] += fallback
                    c["relevance_detail"] = f"doc co-occurrence (+{fallback:.2f})"

    # Clamp scores to valid confidence range [0, 1.0]
    for c in scored:
        c["combined_score"] = min(c["combined_score"], 1.0)

    # Short-name guard with explicit bypass conditions
    has_context = len(resolved_uris) > 0  # Context is present if we got here
    for c in scored:
        if len(c["name"]) < 8:
            # Bypass guard if phonetic match (strong evidence despite short name)
            if c.get("phonetic_match"):
                continue  # Allow short names with phonetic match
            # Bypass guard if: low min_context (like Person) + context present + high fuzzy
            if schema.min_context_people == 1 and has_context and c["name_similarity"] >= 0.85:
                continue  # Allow short names with high fuzzy + context
            # Otherwise apply strict guard
            if c["name_similarity"] < 0.7 or c["doc_count"] < 2:
                c["combined_score"] = 0  # Disqualify short names with weak signals

    scored = [c for c in scored if c["combined_score"] > 0]
    scored.sort(key=lambda x: -x["combined_score"])

    logger.info(f"Contextual {entity_type} candidates for '{label}': {len(scored)} candidates found")
    for c in scored[:3]:  # Log top 3
        phonetic_str = f", phonetic={c.get('phonetic_match', False)}" if schema.phonetic_matching else ""
        logger.info(f"  - {c['name']}: name_sim={c['name_similarity']:.3f}, "
                   f"docs={c['doc_count']}, combined={c['combined_score']:.3f}{phonetic_str}")

    return scored[:10]


# Backward compatibility alias
async def get_contextual_org_candidates(
    conn: asyncpg.Connection,
    label: str,
    associated_people: List[str]
) -> List[dict]:
    """Backward compatible wrapper - use get_contextual_entity_candidates instead."""
    return await get_contextual_entity_candidates(conn, label, 'Organization', associated_people)


@app.post("/get-contextual-candidates")
async def get_contextual_candidates(request: ContextualCandidatesRequest):
    """
    Get contextual entity candidates based on related meetings.

    This endpoint finds entities from related meetings that share:
    - The same project
    - Common attendees
    - Similar topics

    Use case: When processing a meeting that mentions "Orrin", this endpoint
    can return "Orrin Tavish" as a candidate if they attended other meetings
    for the same project.

    Example:
        POST /get-contextual-candidates
        {"project": "GLOTCHA", "attendees": ["Mabel Patel"], "entity_types": ["Person"]}

        Returns:
        {
            "candidates": [
                {"name": "Orrin Tavish", "uri": "orn:...", "source_documents": [...]}
            ],
            "related_documents": [...],
            "context_types": ["project"]
        }
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        result = await get_contextual_candidates_internal(
            conn,
            project=request.project,
            attendees=request.attendees,
            topics=request.topics,
            document_rid=request.document_rid,
            entity_types=request.entity_types
        )

    return {
        **result,
        "candidate_count": len(result["candidates"]),
        "related_document_count": len(result["related_documents"])
    }


# =============================================================================
# Relationship Endpoints
# =============================================================================

class SyncRelationshipsRequest(BaseModel):
    """Request to sync relationships from a vault file"""
    vault_path: str  # e.g., "People/Orrin Tavish.md"
    entity_uri: str  # Canonical URI of the entity
    frontmatter: Dict[str, Any]  # YAML frontmatter dict


@app.post("/sync-relationships")
async def sync_relationships_endpoint(request: SyncRelationshipsRequest):
    """
    Sync relationships from vault YAML frontmatter to the database.

    This endpoint:
    1. Deletes existing relationships from this file (replace-all strategy)
    2. Parses YAML fields (affiliation, founder, knows, etc.)
    3. Resolves targets to entity URIs
    4. Stores resolved relationships in entity_relationships
    5. Stores unresolved targets in pending_relationships

    Use Case: Backfill relationships for existing vault entities.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            stats = await sync_vault_relationships(
                conn,
                request.vault_path,
                request.entity_uri,
                request.frontmatter
            )

            # Enqueue relationship changes to TerminusDB outbox
            if stats and TERMINUSDB_ENABLED:
                await _enqueue_relationship_outbox(
                    conn, request.entity_uri, request.vault_path)

    return {
        "success": True,
        "vault_path": request.vault_path,
        "stats": stats
    }


@app.get("/relationships/{entity_uri:path}")
async def get_relationships_endpoint(
    entity_uri: str,
    predicate: Optional[str] = None,
    direction: Literal["incoming", "outgoing", "both"] = "both",
):
    """
    Get all relationships for an entity.

    Query Parameters:
        predicate: Optional filter by predicate (e.g., 'affiliated_with')
        direction: "both" (default), "incoming", or "outgoing"

    Returns relationships where the entity is subject and/or object.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Reject queries for node_private entities
        is_private = await conn.fetchval(
            "SELECT node_private FROM entity_registry WHERE fuseki_uri = $1",
            entity_uri
        )
        if is_private:
            raise HTTPException(status_code=404, detail="Entity not found")

        relationships = await get_entity_relationships(conn, entity_uri, predicate, direction)

        # Enrich with entity names, filtering out node_private neighbors
        enriched = []
        for rel in relationships:
            # Get subject name
            subject_row = await conn.fetchrow("""
                SELECT entity_text, entity_type, node_private FROM entity_registry
                WHERE fuseki_uri = $1
            """, rel['subject_uri'])

            # Get object name
            object_row = await conn.fetchrow("""
                SELECT entity_text, entity_type, node_private FROM entity_registry
                WHERE fuseki_uri = $1
            """, rel['object_uri'])

            # Skip relationships where either endpoint is node_private
            if (subject_row and subject_row['node_private']) or (object_row and object_row['node_private']):
                continue

            enriched.append({
                **rel,
                "subject_name": subject_row['entity_text'] if subject_row else None,
                "subject_type": subject_row['entity_type'] if subject_row else None,
                "object_name": object_row['entity_text'] if object_row else None,
                "object_type": object_row['entity_type'] if object_row else None,
            })

    return {
        "entity_uri": entity_uri,
        "relationships": enriched,
        "count": len(enriched)
    }


@app.get("/relationship-stats")
async def get_relationship_stats():
    """Get relationship statistics"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Total counts
        total_relationships = await conn.fetchval("SELECT COUNT(*) FROM entity_relationships")
        total_pending = await conn.fetchval("SELECT COUNT(*) FROM pending_relationships")

        # By predicate
        by_predicate = await conn.fetch("""
            SELECT predicate, COUNT(*) as count
            FROM entity_relationships
            GROUP BY predicate
            ORDER BY count DESC
        """)

        # Top pending (unresolved labels)
        top_pending = await conn.fetch("""
            SELECT raw_unknown_label, predicate, unknown_side, COUNT(*) as count
            FROM pending_relationships
            GROUP BY raw_unknown_label, predicate, unknown_side
            ORDER BY count DESC
            LIMIT 20
        """)

        return {
            "total_relationships": total_relationships,
            "total_pending": total_pending,
            "by_predicate": {r['predicate']: r['count'] for r in by_predicate},
            "top_pending": [dict(r) for r in top_pending]
        }


# =============================================================================
# Session Search Endpoints (for Claude Code session memory)
# =============================================================================

class SearchSessionsRequest(BaseModel):
    """Request to search Claude Code sessions"""
    query: str
    limit: int = 10
    session_id: Optional[str] = None  # Filter to specific session
    include_context: bool = True  # Include surrounding chunks


class SessionSearchResult(BaseModel):
    """A single session search result"""
    session_id: str
    session_rid: str
    chunk_index: int
    chunk_text: str
    similarity: Optional[float] = None
    summary: Optional[str] = None
    first_prompt: Optional[str] = None
    timestamp: Optional[str] = None


async def _session_exclusion_filter(conn, column: str) -> str:
    """SQL predicate that keeps excluded sessions out of the user-facing session endpoints.

    The sensor records each excluded session in session_exclusions (migration 136), and every
    endpoint that returns per-session rows filters on it. Aggregate counts are unchanged.
    Raises 503 when the table is missing or KOI_SESSION_READERS_DISABLED is set, so these
    endpoints fail closed (api/session_exclusions.py). `column` is always a literal from
    this module, never request input.
    """
    return await session_exclusion_filter(conn, column)


@app.post("/search-sessions")
async def search_sessions(request: SearchSessionsRequest):
    """
    Search Claude Code session transcripts.

    Performs semantic search over indexed session chunks.
    Returns matching chunks with session metadata.

    Example:
        POST /search-sessions
        {"query": "entity resolution", "limit": 10}
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    results = []

    async with db_pool.acquire() as conn:
        # Check if session_chunks table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'session_chunks'
            )
        """)

        if not table_exists:
            return {
                "results": [],
                "count": 0,
                "message": "Session chunks table not found. Run the session sensor first."
            }

        # Check if we have embeddings (3072-dim post-2026-04-23 OpenAI migration;
        # session_chunks is fully backfilled — see migration 094 for legacy column drop)
        has_embeddings = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM session_chunks WHERE embedding_3072 IS NOT NULL LIMIT 1
            )
        """)

        excl = await _session_exclusion_filter(conn, "sc.session_id")

        if has_embeddings and embedding_provider and ENABLE_SEMANTIC_MATCHING:
            # Semantic search with embeddings.
            # Reads from embedding_3072; halfvec cast required because pgvector
            # full-precision vector ANN caps at 2000 dims.
            # Uses idx_session_chunks_embedding_3072_hnsw.
            query_embedding = await generate_query_embedding(request.query)

            if query_embedding:
                if request.session_id:
                    # Search within specific session
                    rows = await conn.fetch(f"""
                        SELECT sc.session_id, sc.session_rid, sc.chunk_index,
                               sc.chunk_text, sc.timestamp,
                               1 - (sc.embedding_3072::halfvec(3072)
                                    <=> $1::halfvec(3072)) AS similarity,
                               sil.summary, sil.first_prompt
                        FROM session_chunks sc
                        LEFT JOIN session_ingestion_log sil ON sc.session_id = sil.session_id
                        WHERE sc.session_id = $2
                          AND sc.embedding_3072 IS NOT NULL
                          AND {excl}
                        ORDER BY similarity DESC
                        LIMIT $3
                    """, str(query_embedding), request.session_id, request.limit)
                else:
                    # Search all sessions
                    rows = await conn.fetch(f"""
                        SELECT sc.session_id, sc.session_rid, sc.chunk_index,
                               sc.chunk_text, sc.timestamp,
                               1 - (sc.embedding_3072::halfvec(3072)
                                    <=> $1::halfvec(3072)) AS similarity,
                               sil.summary, sil.first_prompt
                        FROM session_chunks sc
                        LEFT JOIN session_ingestion_log sil ON sc.session_id = sil.session_id
                        WHERE sc.embedding_3072 IS NOT NULL
                          AND {excl}
                        ORDER BY similarity DESC
                        LIMIT $2
                    """, str(query_embedding), request.limit)

                for row in rows:
                    results.append({
                        "session_id": row['session_id'],
                        "session_rid": row['session_rid'],
                        "chunk_index": row['chunk_index'],
                        "chunk_text": row['chunk_text'][:2000],  # Limit text size
                        "similarity": float(row['similarity']) if row['similarity'] else None,
                        "summary": row['summary'],
                        "first_prompt": row['first_prompt'][:200] if row['first_prompt'] else None,
                        "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None
                    })
        else:
            # Fallback: text search (basic ILIKE)
            search_pattern = f"%{request.query}%"

            if request.session_id:
                rows = await conn.fetch(f"""
                    SELECT sc.session_id, sc.session_rid, sc.chunk_index,
                           sc.chunk_text, sc.timestamp,
                           sil.summary, sil.first_prompt
                    FROM session_chunks sc
                    LEFT JOIN session_ingestion_log sil ON sc.session_id = sil.session_id
                    WHERE sc.session_id = $1
                      AND sc.chunk_text ILIKE $2
                      AND {excl}
                    ORDER BY sc.chunk_index
                    LIMIT $3
                """, request.session_id, search_pattern, request.limit)
            else:
                rows = await conn.fetch(f"""
                    SELECT sc.session_id, sc.session_rid, sc.chunk_index,
                           sc.chunk_text, sc.timestamp,
                           sil.summary, sil.first_prompt
                    FROM session_chunks sc
                    LEFT JOIN session_ingestion_log sil ON sc.session_id = sil.session_id
                    WHERE sc.chunk_text ILIKE $1
                      AND {excl}
                    ORDER BY sil.last_ingested_at DESC, sc.chunk_index
                    LIMIT $2
                """, search_pattern, request.limit)

            for row in rows:
                results.append({
                    "session_id": row['session_id'],
                    "session_rid": row['session_rid'],
                    "chunk_index": row['chunk_index'],
                    "chunk_text": row['chunk_text'][:2000],
                    "similarity": None,  # No similarity for text search
                    "summary": row['summary'],
                    "first_prompt": row['first_prompt'][:200] if row['first_prompt'] else None,
                    "timestamp": row['timestamp'].isoformat() if row['timestamp'] else None
                })

    return {
        "results": results,
        "count": len(results),
        "query": request.query,
        "search_type": "semantic" if has_embeddings and embedding_provider else "text"
    }


@app.get("/session-stats")
async def get_session_stats():
    """Get statistics about indexed Claude Code sessions."""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Check if tables exist
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'session_ingestion_log'
            )
        """)

        if not table_exists:
            return {
                "indexed": False,
                "message": "Session tables not found. Run the session sensor first."
            }

        total_sessions = await conn.fetchval(
            "SELECT COUNT(*) FROM session_ingestion_log"
        )
        total_chunks = await conn.fetchval(
            "SELECT COUNT(*) FROM session_chunks"
        )
        chunks_with_embeddings = await conn.fetchval(
            "SELECT COUNT(*) FROM session_chunks WHERE embedding_3072 IS NOT NULL"
        )

        excl = await _session_exclusion_filter(conn, "session_ingestion_log.session_id")
        recent_sessions = await conn.fetch(f"""
            SELECT session_id, summary, first_prompt, message_count, chunk_count, last_ingested_at
            FROM session_ingestion_log
            WHERE {excl}
            ORDER BY last_ingested_at DESC
            LIMIT 5
        """)

        return {
            "indexed": True,
            "total_sessions": total_sessions,
            "total_chunks": total_chunks,
            "chunks_with_embeddings": chunks_with_embeddings,
            "embedding_coverage": f"{(chunks_with_embeddings / total_chunks * 100):.1f}%" if total_chunks > 0 else "0%",
            "recent_sessions": [
                {
                    "session_id": r['session_id'],
                    "summary": r['summary'],
                    "first_prompt": r['first_prompt'][:100] if r['first_prompt'] else None,
                    "message_count": r['message_count'],
                    "chunk_count": r['chunk_count'],
                    "last_ingested_at": r['last_ingested_at'].isoformat() if r['last_ingested_at'] else None
                }
                for r in recent_sessions
            ]
        }


@app.get("/session-tools")
async def get_session_tools(
    tool: Optional[str] = None,
    mcp_server: Optional[str] = None,
    limit: int = 20
):
    """
    Query sessions by tool usage.

    Examples:
        GET /session-tools?tool=Bash
        GET /session-tools?mcp_server=personal-koi
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Check if table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'session_tool_usage'
            )
        """)

        if not table_exists:
            return {
                "results": [],
                "message": "Session tool usage table not found. Re-run sensor to extract metadata."
            }

        excl = await _session_exclusion_filter(conn, "stu.session_id")
        if tool:
            # Find sessions using a specific tool
            rows = await conn.fetch(f"""
                SELECT stu.session_id, stu.tool_name, stu.call_count,
                       sil.summary, sil.first_prompt, sil.last_ingested_at
                FROM session_tool_usage stu
                JOIN session_ingestion_log sil ON stu.session_id = sil.session_id
                WHERE stu.tool_name ILIKE $1
                  AND {excl}
                ORDER BY stu.call_count DESC, sil.last_ingested_at DESC
                LIMIT $2
            """, f"%{tool}%", limit)
        elif mcp_server:
            # Find sessions using a specific MCP server
            rows = await conn.fetch(f"""
                SELECT stu.session_id, stu.tool_name, stu.call_count, stu.mcp_server,
                       sil.summary, sil.first_prompt, sil.last_ingested_at
                FROM session_tool_usage stu
                JOIN session_ingestion_log sil ON stu.session_id = sil.session_id
                WHERE stu.mcp_server ILIKE $1
                  AND {excl}
                ORDER BY sil.last_ingested_at DESC
                LIMIT $2
            """, f"%{mcp_server}%", limit)
        else:
            # Return overall tool usage stats
            rows = await conn.fetch("""
                SELECT tool_name, SUM(call_count) as total_calls,
                       COUNT(DISTINCT session_id) as session_count,
                       is_mcp, mcp_server
                FROM session_tool_usage
                GROUP BY tool_name, is_mcp, mcp_server
                ORDER BY total_calls DESC
                LIMIT $1
            """, limit)

            return {
                "tool_stats": [
                    {
                        "tool_name": r['tool_name'],
                        "total_calls": r['total_calls'],
                        "session_count": r['session_count'],
                        "is_mcp": r['is_mcp'],
                        "mcp_server": r['mcp_server']
                    }
                    for r in rows
                ]
            }

        return {
            "results": [
                {
                    "session_id": r['session_id'],
                    "tool_name": r['tool_name'],
                    "call_count": r['call_count'],
                    "mcp_server": r.get('mcp_server'),
                    "summary": r['summary'],
                    "first_prompt": r['first_prompt'][:100] if r['first_prompt'] else None,
                    "last_ingested_at": r['last_ingested_at'].isoformat() if r['last_ingested_at'] else None
                }
                for r in rows
            ],
            "count": len(rows),
            "filter": {"tool": tool, "mcp_server": mcp_server}
        }


@app.get("/session-files")
async def get_session_files(
    path_contains: Optional[str] = None,
    limit: int = 20
):
    """
    Query sessions by files accessed.

    Examples:
        GET /session-files?path_contains=koi-processor
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        excl = await _session_exclusion_filter(conn, "session_ingestion_log.session_id")
        if path_contains:
            # Find sessions that accessed files matching pattern
            rows = await conn.fetch(f"""
                SELECT session_id, summary, first_prompt, files_accessed, last_ingested_at
                FROM session_ingestion_log
                WHERE files_accessed IS NOT NULL
                  AND {excl}
                  AND EXISTS (
                      SELECT 1 FROM unnest(files_accessed) f WHERE f ILIKE $1
                  )
                ORDER BY last_ingested_at DESC
                LIMIT $2
            """, f"%{path_contains}%", limit)
        else:
            # Return sessions with most files accessed
            rows = await conn.fetch(f"""
                SELECT session_id, summary, first_prompt,
                       files_accessed, array_length(files_accessed, 1) as file_count,
                       last_ingested_at
                FROM session_ingestion_log
                WHERE files_accessed IS NOT NULL
                  AND {excl}
                  AND array_length(files_accessed, 1) > 0
                ORDER BY array_length(files_accessed, 1) DESC
                LIMIT $1
            """, limit)

        return {
            "results": [
                {
                    "session_id": r['session_id'],
                    "summary": r['summary'],
                    "first_prompt": r['first_prompt'][:100] if r['first_prompt'] else None,
                    "files_accessed": r['files_accessed'][:20] if r['files_accessed'] else [],
                    "file_count": len(r['files_accessed']) if r['files_accessed'] else 0,
                    "last_ingested_at": r['last_ingested_at'].isoformat() if r['last_ingested_at'] else None
                }
                for r in rows
            ],
            "count": len(rows),
            "filter": {"path_contains": path_contains}
        }


# =============================================================================
# Knowledge Base Search (Emails, Vault, etc.)
# =============================================================================

BGE_SERVER_URL = os.getenv('BGE_SERVER_URL', 'http://localhost:8091/encode')


class SearchRequest(BaseModel):
    """Request for knowledge base search"""
    query: str
    limit: int = 10
    source: Optional[str] = None  # Filter by source: 'email', 'vault', etc.
    include_chunks: bool = False  # Also search chunk-level embeddings


class SearchResult(BaseModel):
    """Single search result"""
    rid: str
    title: Optional[str] = None
    content_preview: str
    similarity: float
    source: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


async def get_bge_embedding(text: str) -> Optional[List[float]]:
    """Get embedding from local BGE server."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                BGE_SERVER_URL,
                json={"text": text}
            )
            if response.status_code == 200:
                return response.json().get("embedding")
            else:
                logger.warning(f"BGE server error: {response.status_code}")
                return None
    except Exception as e:
        logger.error(f"BGE embedding error: {e}")
        return None


# Tranche-1 read auth. db_pool is read late for session lookup.
from api.read_auth import make_read_scope_dependency  # noqa: E402
from api.read_scope import ReadScope, chunk_visible_sql  # noqa: E402

_search_scope = make_read_scope_dependency("search", lambda: db_pool)
_query_scope = make_read_scope_dependency("query", lambda: db_pool)


@app.post("/search")
async def search_knowledge_base(request: SearchRequest,
                                scope: ReadScope = Depends(_search_scope)):
    """
    Search the personal knowledge base (emails, vault notes, etc.).

    Performs semantic search using BGE embeddings over koi_memories.

    Args:
        query: Search query text
        limit: Max results (default 10)
        source: Filter by source ('email', 'vault', etc.)
        include_chunks: Also search chunk-level embeddings

    Example:
        POST /search
        {"query": "hackathon ethereum", "limit": 10, "source": "email"}
    """
    # Internal callers must pass the caller's scope; a missing one is an error,
    # never a silent personal read.
    if not isinstance(scope, ReadScope):
        raise HTTPException(status_code=500, detail="search called without a read scope")
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    # Generate query embedding using the active provider (OpenAI 3072 via
    # generate_query_embedding — the same path unified_search/entity search use).
    # Was get_bge_embedding (dim_1024), but the BGE server is not running so it
    # returned None and forced a text-only fallback; and dim_1024 doc embeddings
    # are not populated for newer sources (substack). Fix 2026-06-01.
    query_embedding = await generate_query_embedding(request.query)

    results = []
    search_type = "text"  # fallback

    async with db_pool.acquire() as conn:
        if query_embedding:
            search_type = "semantic"
            embedding_str = '[' + ','.join(str(x) for x in query_embedding) + ']'

            # Build query with optional source filter (fail-closed on unknown)
            if request.source and request.source not in SOURCE_TO_FILTER:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown source: {request.source!r}. Valid values: {list(SOURCE_TO_FILTER.keys())}",
                )
            source_filter = SOURCE_TO_FILTER.get(request.source or '', "")
            # Caller visibility inside the ANN WHERE, before the
            # 200-candidate LIMIT, so filtering precedes ranking and limiting.
            # Personal is the literal TRUE: today's query and plan.
            vis_sql, vis_params = chunk_visible_sql(scope, "c.document_rid", 3)

            # Search chunk-level embedding_3072 (the live OpenAI 3072 surface,
            # populated for ALL sources incl. substack/email; dim_1024 doc-level
            # is legacy BGE and missing for newer sources). Pipeline: HNSW finds
            # the top candidate chunks by vector distance (fast), dedup keeps the
            # best chunk per document, then re-sort by similarity for the page.
            query = f"""
                SELECT * FROM (
                    SELECT DISTINCT ON (document_rid)
                        document_rid as rid, title, content_preview,
                        similarity, source_sensor, metadata, created_at
                    FROM (
                        SELECT c.document_rid,
                               m.content->>'title' as title,
                               LEFT(c.content->>'text', 500) as content_preview,
                               1 - (c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) as similarity,
                               m.source_sensor, m.metadata, m.created_at
                        FROM koi_memory_chunks c
                        JOIN koi_memories m ON m.rid = c.document_rid
                        WHERE c.embedding_3072 IS NOT NULL
                          AND {vis_sql}
                        {source_filter}
                        ORDER BY c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                        LIMIT 200
                    ) cand
                    ORDER BY document_rid, similarity DESC
                ) ded
                ORDER BY similarity DESC
                LIMIT $2
            """

            rows = await conn.fetch(query, embedding_str, request.limit, *vis_params)

            for row in rows:
                # Handle metadata - asyncpg returns JSONB as dict already
                metadata = row['metadata']
                if metadata is None:
                    metadata = {}
                elif isinstance(metadata, str):
                    import json as json_module
                    metadata = json_module.loads(metadata)

                result = {
                    "rid": row['rid'],
                    "title": row['title'],
                    "content_preview": row['content_preview'],
                    "similarity": float(row['similarity']) if row['similarity'] else 0,
                    "source": row['source_sensor'],
                    "metadata": metadata,
                }

                # ics-event rows: no email_metadata lookup (base schema only)
                if row['source_sensor'] == 'ics-event':
                    pass
                # Add email-specific metadata if available
                elif row['source_sensor'] == 'email-sensor':
                    email_meta = await conn.fetchrow("""
                        SELECT subject, from_name, from_address, date_sent
                        FROM email_metadata
                        WHERE rid = $1
                    """, row['rid'])

                    if email_meta:
                        result['email'] = {
                            "subject": email_meta['subject'],
                            "from_name": email_meta['from_name'],
                            "from_address": email_meta['from_address'],
                            "date_sent": email_meta['date_sent'].isoformat() if email_meta['date_sent'] else None
                        }

                results.append(result)

            # Optionally search chunks for more coverage
            if request.include_chunks and len(results) < request.limit:
                chunk_vis_sql, chunk_vis_params = chunk_visible_sql(scope, "c.document_rid", 4)
                chunk_query = f"""
                    SELECT DISTINCT ON (c.document_rid)
                        c.document_rid as rid,
                        m.content->>'title' as title,
                        LEFT(c.content->>'text', 500) as content_preview,
                        1 - (c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) as similarity,
                        m.source_sensor,
                        m.metadata
                    FROM koi_memory_chunks c
                    JOIN koi_memories m ON m.rid = c.document_rid
                    WHERE c.embedding_3072 IS NOT NULL
                      AND c.document_rid NOT IN (SELECT rid FROM unnest($3::text[]) as rid)
                      AND {chunk_vis_sql}
                    {source_filter.replace('m.source_sensor', 'm.source_sensor')}
                    ORDER BY c.document_rid, c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
                    LIMIT $2
                """

                existing_rids = [r['rid'] for r in results]
                chunk_rows = await conn.fetch(
                    chunk_query,
                    embedding_str,
                    request.limit - len(results),
                    existing_rids,
                    *chunk_vis_params,
                )

                for row in chunk_rows:
                    chunk_metadata = row['metadata'] if row['metadata'] else {}
                    results.append({
                        "rid": row['rid'],
                        "title": row['title'],
                        "content_preview": row['content_preview'],
                        "similarity": float(row['similarity']) if row['similarity'] else 0,
                        "source": row['source_sensor'],
                        "metadata": chunk_metadata,
                        "matched_via": "chunk"
                    })

        else:
            # Fallback: text search
            search_type = "text"
            search_pattern = f"%{request.query}%"

            if request.source and request.source not in SOURCE_TO_FILTER:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown source: {request.source!r}. Valid values: {list(SOURCE_TO_FILTER.keys())}",
                )
            source_filter = SOURCE_TO_FILTER.get(request.source or '', "")
            vis_sql, vis_params = chunk_visible_sql(scope, "m.rid", 3)

            query = f"""
                SELECT
                    m.rid,
                    m.content->>'title' as title,
                    LEFT(m.content->>'text', 500) as content_preview,
                    m.source_sensor,
                    m.metadata,
                    m.created_at
                FROM koi_memories m
                WHERE m.content->>'text' ILIKE $1
                  AND {vis_sql}
                {source_filter}
                ORDER BY m.created_at DESC
                LIMIT $2
            """

            rows = await conn.fetch(query, search_pattern, request.limit, *vis_params)

            for row in rows:
                text_metadata = row['metadata'] if row['metadata'] else {}
                results.append({
                    "rid": row['rid'],
                    "title": row['title'],
                    "content_preview": row['content_preview'],
                    "similarity": None,
                    "source": row['source_sensor'],
                    "metadata": text_metadata
                })

    return {
        "results": results,
        "count": len(results),
        "query": request.query,
        "search_type": search_type,
        "source_filter": request.source
    }


@app.get("/search")
async def search_knowledge_base_get(
    q: str,
    limit: int = 10,
    source: Optional[str] = None,
    scope: ReadScope = Depends(_search_scope),
):
    """GET version of search for convenience."""
    request = SearchRequest(query=q, limit=limit, source=source)
    return await search_knowledge_base(request, scope=scope)


# =============================================================================
# /query Endpoint (Compatible with regen-koi-mcp)
# =============================================================================

class QueryRequest(BaseModel):
    """Query request compatible with regen-koi-mcp format"""
    query: Optional[str] = None
    question: Optional[str] = None  # Alternative field name
    limit: int = 10
    intent: Optional[str] = None
    source: Optional[str] = None
    filters: Optional[Dict[str, Any]] = None
    published_from: Optional[str] = None
    published_to: Optional[str] = None


@app.post("/query")
async def query_knowledge_base(request: QueryRequest,
                               scope: ReadScope = Depends(_query_scope)):
    """
    Query endpoint compatible with regen-koi-mcp format.

    This wraps the /search endpoint to provide compatibility with the MCP client.
    Accepts both 'query' and 'question' parameters.

    Example:
        POST /query
        {"query": "hackathon", "limit": 10, "source": "email"}
    """
    # Use query or question parameter
    query_text = request.query or request.question or ""

    if not query_text or query_text == "warmup":
        # Return empty results for warmup or empty queries
        return {"results": [], "count": 0, "query": query_text}

    # Map source filter
    source = request.source
    if request.filters and request.filters.get('source'):
        source = request.filters['source']

    # Call the search endpoint
    search_request = SearchRequest(
        query=query_text,
        limit=request.limit,
        source=source,
        include_chunks=True  # Include chunks for better coverage
    )

    search_result = await search_knowledge_base(search_request, scope=scope)

    # Transform results to match expected format
    results = []
    for r in search_result.get("results", []):
        result = {
            "rid": r.get("rid"),
            "title": r.get("title") or r.get("email", {}).get("subject") or "Untitled",
            "content": r.get("content_preview", ""),
            "similarity": r.get("similarity", 0),
            "source": r.get("source"),
            "url": None,  # Emails don't have URLs
            "published_at": r.get("email", {}).get("date_sent") if r.get("email") else None,
            "metadata": r.get("metadata", {}),
        }

        # Add email-specific fields if present
        if r.get("email"):
            result["email"] = r["email"]

        results.append(result)

    return {
        "results": results,
        "count": len(results),
        "query": query_text,
        "search_type": search_result.get("search_type", "semantic")
    }


# =============================================================================
# TerminusDB Graph Endpoints
# =============================================================================

GRAPH_ALLOWED_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _check_graph_auth(request: Request):
    """Restrict /graph/* endpoints to localhost and the 10.100.0.0/24 subnet."""
    client_host = request.client.host if request.client else None
    if client_host in GRAPH_ALLOWED_HOSTS:
        return
    # Allow the 10.100.0.0/24 subnet
    if client_host and client_host.startswith("10.100.0."):
        return
    raise HTTPException(status_code=403, detail="Graph endpoints restricted to local access")


@app.get("/graph/health")
async def graph_health(request: Request):
    """TerminusDB connection status, schema hash, sync lag metrics."""
    _check_graph_auth(request)
    if not TERMINUSDB_ENABLED:
        return {"terminusdb_enabled": False}

    result = {}
    if terminusdb_adapter:
        result = terminusdb_adapter.health()
    else:
        result = {"terminusdb_reachable": False, "error": "adapter not initialized"}

    # Add outbox metrics from PostgreSQL
    if db_pool:
        async with db_pool.acquire() as conn:
            pending = await conn.fetchval(
                "SELECT COUNT(*) FROM terminusdb_outbox WHERE status = 'pending'")
            dead = await conn.fetchval(
                "SELECT COUNT(*) FROM terminusdb_outbox WHERE status = 'dead_letter'")
            oldest = await conn.fetchval("""
                SELECT EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))
                FROM terminusdb_outbox WHERE status = 'pending'
            """)
            result["pending_outbox_count"] = pending or 0
            result["dead_letter_count"] = dead or 0
            result["oldest_pending_age_s"] = round(oldest, 1) if oldest else None

    return result


@app.get("/graph/conflicts")
async def graph_conflicts(request: Request, limit: int = 50, offset: int = 0):
    """All conflicts (grouped by subject+predicate)."""
    _check_graph_auth(request)
    if not TERMINUSDB_ENABLED or not terminusdb_adapter:
        raise HTTPException(status_code=503, detail="TerminusDB not enabled")

    limit = min(max(limit, 1), 500)
    offset = max(offset, 0)
    conflicts, total = terminusdb_adapter.get_conflicts(limit=limit, offset=offset)
    return {"conflicts": conflicts, "total": total, "limit": limit, "offset": offset}


@app.get("/graph/conflicts/{entity_rid:path}")
async def graph_conflicts_for_entity(request: Request, entity_rid: str):
    """Conflicts for a specific entity."""
    _check_graph_auth(request)
    if not TERMINUSDB_ENABLED or not terminusdb_adapter:
        raise HTTPException(status_code=503, detail="TerminusDB not enabled")

    conflicts, total = terminusdb_adapter.get_conflicts(entity_rid=entity_rid)
    return {"entity_rid": entity_rid, "conflicts": conflicts, "total": total}


@app.get("/graph/assertions/{entity_rid:path}")
async def graph_assertions(request: Request, entity_rid: str, limit: int = 100, offset: int = 0):
    """All assertions about an entity."""
    _check_graph_auth(request)
    if not TERMINUSDB_ENABLED or not terminusdb_adapter:
        raise HTTPException(status_code=503, detail="TerminusDB not enabled")

    limit = min(max(limit, 1), 500)
    offset = max(offset, 0)
    assertions, total = terminusdb_adapter.get_assertions(
        entity_rid=entity_rid, limit=limit, offset=offset)
    return {"entity_rid": entity_rid, "assertions": assertions,
            "total": total, "limit": limit, "offset": offset}


# =============================================================================
# Graph Traversal Endpoints
# =============================================================================


@app.get("/graph/neighborhood/{entity_uri:path}", response_model=NeighborhoodResponse)
async def graph_neighborhood(
    request: Request,
    entity_uri: str,
    max_depth: int = 2,
    direction: Literal["incoming", "outgoing", "both"] = "both",
    predicate: Optional[str] = None,
    entity_type: Optional[str] = None,
    max_nodes: int = 200,
    max_edges: int = 1000,
):
    """
    Return the neighborhood graph around an entity via recursive traversal.

    Discovers reachable nodes up to `max_depth` hops, returns nodes + edges
    between them. Safety caps prevent runaway queries.

    - **max_depth**: 1-4 (default 2, silently clamped)
    - **direction**: which edges to traverse ("both", "incoming", "outgoing")
    - **predicate**: only traverse edges with this predicate
    - **entity_type**: post-filter nodes by type (root always included)
    - **max_nodes**: cap on returned nodes (default 200, max 500)
    - **max_edges**: cap on returned edges (default 1000, max 2000)

    When truncated, `total_nodes_discovered` / `total_edges_discovered`
    show the uncapped counts.
    """
    _check_graph_auth(request)

    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Check entity exists
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM entity_registry WHERE fuseki_uri = $1)",
            entity_uri,
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"Entity not found: {entity_uri}")

        from api.graph_queries import get_neighborhood
        result = await get_neighborhood(
            conn, entity_uri, max_depth, direction, predicate,
            entity_type, max_nodes, max_edges,
        )

    return result


@app.get("/graph/shortest-path", response_model=ShortestPathResponse)
async def graph_shortest_path(
    request: Request,
    source: str,
    target: str,
    max_depth: int = 6,
    direction: Literal["incoming", "outgoing", "both"] = "both",
):
    """
    Find the shortest path between two entities via BFS.

    - **source**: URI of the starting entity
    - **target**: URI of the destination entity
    - **max_depth**: maximum hops (1-8, default 6, silently clamped)
    - **direction**: which edges to follow ("both", "incoming", "outgoing")

    When `source == target`, returns `path_length: 0` with an empty steps list.

    Each step shows the edge connecting consecutive nodes. When multiple edges
    exist between two nodes, the highest-confidence edge is chosen (ties broken
    alphabetically by predicate) for deterministic results.
    """
    _check_graph_auth(request)

    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")

    async with db_pool.acquire() as conn:
        # Check both entities exist
        for uri, label in [(source, "source"), (target, "target")]:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM entity_registry WHERE fuseki_uri = $1)",
                uri,
            )
            if not exists:
                raise HTTPException(status_code=404, detail=f"{label.capitalize()} entity not found: {uri}")

        from api.graph_queries import get_shortest_path
        result = await get_shortest_path(conn, source, target, max_depth, direction)

    return result


# =============================================================================
# /chat Endpoint — RAG-powered conversational interface
# =============================================================================


# ── B2 GraphRAG: graph-guided retrieval ──────────────────────────────

async def _compute_graph_version_hash(conn) -> str:
    """Compute deterministic graph state hash for cache invalidation."""
    import hashlib as _hashlib
    row = await conn.fetchrow("""
        SELECT
            (SELECT COUNT(*) FROM entity_registry) AS ec,
            (SELECT COUNT(*) FROM entity_relationships) AS rc,
            (SELECT MAX(updated_at) FROM entity_registry) AS meu,
            (SELECT GREATEST(MAX(created_at), MAX(updated_at)) FROM entity_relationships) AS mrc
    """)
    state = f"{row['ec']}:{row['rc']}:{row['meu']}:{row['mrc']}"
    return _hashlib.sha256(state.encode()).hexdigest()[:16]


async def _ensure_graph_metrics(conn) -> bool:
    """Ensure entity_graph_metrics is populated and fresh. Returns True if available."""
    try:
        row = await conn.fetchrow(
            "SELECT graph_version, COUNT(*) as cnt FROM entity_graph_metrics GROUP BY graph_version LIMIT 1"
        )
        if not row or row['cnt'] == 0:
            return False
        current_version = await _compute_graph_version_hash(conn)
        return row['graph_version'] == current_version
    except Exception:
        return False


async def _graph_guided_retrieval(
    query: str,
    query_embedding: list,
    conn,
    top_k: int = 10,
) -> tuple:
    """B2 GraphRAG: community-aware, centrality-weighted retrieval.

    Returns (sources, relationships_ctx, doc_chunks, web_sources) matching
    the B1 interface so the LLM prompt builder works unchanged.

    Strategy:
    1. Semantic search for seed entities (same as B1)
    2. Look up community_l1 + betweenness for each seed from entity_graph_metrics
    3. Expand: entities in same L1 community, sorted by betweenness DESC
    4. Follow edges from seeds via entity_relationships (predicate-aware)
    5. Rank: semantic_score * 0.4 + centrality * 0.3 + community_overlap * 0.3
    """
    sources = []
    relationships_ctx = []
    doc_chunks = []
    web_sources = []

    if not query_embedding:
        return sources, relationships_ctx, doc_chunks, web_sources

    embedding_str = '[' + ','.join(str(x) for x in query_embedding) + ']'

    # ── Step 1: Seed entities via semantic search (like B1, with fallback) ──
    try:
        seed_rows = await conn.fetch("""
            SELECT id, fuseki_uri, entity_text, entity_type, metadata,
                   1 - (embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
            FROM entity_registry
            WHERE embedding_3072 IS NOT NULL AND NOT node_private
              AND merged_into IS NULL
            ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT $2
        """, embedding_str, top_k)
    except (asyncpg.exceptions.UndefinedColumnError,
            asyncpg.exceptions.UndefinedFunctionError,
            asyncpg.exceptions.DataError) as e:
        logger.warning("entity_registry.embedding vector search failed, falling back to text search: %s", e)
        # Fallback: text search when vector column/extension is unavailable
        words = [w for w in query.lower().split() if len(w) >= 3]
        if words:
            conditions = " OR ".join(f"normalized_text ILIKE ${i+1}" for i in range(len(words)))
            match_score = " + ".join(
                f"CASE WHEN normalized_text ILIKE ${i+1} THEN 1 ELSE 0 END"
                for i in range(len(words))
            )
            params = [f"%{w}%" for w in words]
            params.append(top_k)
            seed_rows = await conn.fetch(f"""
                SELECT id, fuseki_uri, entity_text, entity_type, metadata,
                       ({match_score})::float / {len(words)} AS similarity
                FROM entity_registry
                WHERE ({conditions}) AND NOT node_private
                  AND merged_into IS NULL
                ORDER BY ({match_score}) DESC, created_at DESC
                LIMIT ${len(words)+1}
            """, *params)
        else:
            seed_rows = []

    if not seed_rows:
        return sources, relationships_ctx, doc_chunks, web_sources

    seed_ids = [r['id'] for r in seed_rows]
    seed_uris = [r['fuseki_uri'] for r in seed_rows]
    seed_scores = {r['id']: float(r['similarity']) for r in seed_rows}

    # ── Step 2: Get community + centrality for seeds ──
    metrics_available = await _ensure_graph_metrics(conn)
    seed_communities = {}
    seed_betweenness = {}

    if metrics_available:
        metric_rows = await conn.fetch("""
            SELECT entity_id, community_l1, betweenness
            FROM entity_graph_metrics
            WHERE entity_id = ANY($1)
        """, seed_ids)
        for mr in metric_rows:
            seed_communities[mr['entity_id']] = mr['community_l1']
            seed_betweenness[mr['entity_id']] = mr['betweenness']

    # Determine dominant communities from seeds
    comm_counts = {}
    for eid in seed_ids:
        c = seed_communities.get(eid, -1)
        if c >= 0:
            comm_counts[c] = comm_counts.get(c, 0) + 1
    dominant_communities = sorted(comm_counts, key=comm_counts.get, reverse=True)[:3]

    # ── Step 3: Expand via community — get high-centrality entities in same communities ──
    expanded_entities = {}  # id -> {uri, label, type, score, description}

    if metrics_available and dominant_communities:
        community_rows = await conn.fetch("""
            SELECT
                er.id, er.fuseki_uri, er.entity_text, er.entity_type, er.metadata,
                egm.community_l1, egm.betweenness
            FROM entity_graph_metrics egm
            JOIN entity_registry er ON er.id = egm.entity_id
            WHERE egm.community_l1 = ANY($1) AND NOT er.node_private
            ORDER BY egm.betweenness DESC
            LIMIT $2
        """, dominant_communities, top_k * 2)

        for cr in community_rows:
            eid = cr['id']
            if eid not in expanded_entities:
                expanded_entities[eid] = {
                    'id': eid,
                    'uri': cr['fuseki_uri'],
                    'label': cr['entity_text'],
                    'type': cr['entity_type'],
                    'metadata': cr['metadata'],
                    'betweenness': float(cr['betweenness'] or 0),
                    'community': cr['community_l1'],
                    'semantic_score': seed_scores.get(eid, 0.0),
                }

    # Add seeds that might not be in expanded set
    for row in seed_rows:
        eid = row['id']
        if eid not in expanded_entities:
            meta = row['metadata'] or {}
            if isinstance(meta, str):
                meta = json_module_global.loads(meta)
            expanded_entities[eid] = {
                'id': eid,
                'uri': row['fuseki_uri'],
                'label': row['entity_text'],
                'type': row['entity_type'],
                'metadata': meta,
                'betweenness': seed_betweenness.get(eid, 0.0),
                'community': seed_communities.get(eid, -1),
                'semantic_score': float(row['similarity']),
            }

    # ── Step 4: Composite ranking ──
    # score = semantic * 0.4 + centrality * 0.3 + community_overlap * 0.3
    max_bc = max((e['betweenness'] for e in expanded_entities.values()), default=1.0) or 1.0

    for eid, ent in expanded_entities.items():
        sem = ent['semantic_score']
        bc_norm = ent['betweenness'] / max_bc
        comm_overlap = 1.0 if ent['community'] in dominant_communities else 0.0
        ent['composite_score'] = sem * 0.4 + bc_norm * 0.3 + comm_overlap * 0.3

    # Sort by composite score, take top_k
    ranked = sorted(expanded_entities.values(), key=lambda x: x['composite_score'], reverse=True)[:top_k]

    # Build sources list
    entity_uris = []
    for ent in ranked:
        meta = ent.get('metadata', {})
        if isinstance(meta, str):
            meta = json_module_global.loads(meta)
        description = meta.get('description', '') if isinstance(meta, dict) else ''
        sources.append({
            "uri": ent['uri'],
            "label": ent['label'],
            "entity_type": ent['type'],
            "score": round(ent['composite_score'], 4),
            "description": description,
            "retrieval_mode": "graphrag",
            "community": ent.get('community', -1),
            "betweenness": round(ent.get('betweenness', 0), 6),
        })
        entity_uris.append(ent['uri'])

    # ── Step 5: Relationships — predicate-aware, community-guided ──
    if entity_uris:
        rel_rows = await conn.fetch("""
            WITH RECURSIVE traverse AS (
                SELECT r.subject_uri, r.object_uri, r.predicate, 1 AS depth
                FROM entity_relationships r
                WHERE r.subject_uri = ANY($1) OR r.object_uri = ANY($1)
                UNION
                SELECT r2.subject_uri, r2.object_uri, r2.predicate, t.depth + 1
                FROM traverse t
                JOIN entity_relationships r2
                    ON r2.subject_uri IN (t.subject_uri, t.object_uri)
                    OR r2.object_uri IN (t.subject_uri, t.object_uri)
                WHERE t.depth < 2
            )
            SELECT DISTINCT ON (t.subject_uri, t.predicate, t.object_uri)
                t.subject_uri,
                s.entity_text AS subject_label,
                t.predicate,
                t.object_uri,
                o.entity_text AS object_label,
                t.depth
            FROM traverse t
            LEFT JOIN entity_registry s ON s.fuseki_uri = t.subject_uri
            LEFT JOIN entity_registry o ON o.fuseki_uri = t.object_uri
            WHERE NOT COALESCE(s.node_private, false)
              AND NOT COALESCE(o.node_private, false)
            ORDER BY t.subject_uri, t.predicate, t.object_uri, t.depth
            LIMIT 50
        """, entity_uris)
        for rr in rel_rows:
            subj = rr['subject_label'] or rr['subject_uri']
            obj = rr['object_label'] or rr['object_uri']
            relationships_ctx.append(f"{subj} --[{rr['predicate']}]--> {obj}")

    # ── Step 6: Document chunks (same as B1) ──
    try:
        chunk_rows = await conn.fetch("""
            SELECT
                c.document_rid,
                m.content->>'title' AS title,
                LEFT(c.content->>'text', 500) AS chunk_text,
                c.content->>'section_id' AS section_id,
                c.content->>'section_title' AS section_title,
                c.content->>'wiki_url' AS wiki_url,
                1 - (c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS similarity
            FROM koi_memory_chunks c
            JOIN koi_memories m ON m.rid = c.document_rid
            WHERE c.embedding_3072 IS NOT NULL
            ORDER BY c.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)
            LIMIT 8
        """, embedding_str)
        for cr in chunk_rows:
            if float(cr['similarity']) > 0.3:
                doc_chunks.append({
                    "rid": cr['document_rid'],
                    "title": cr['title'] or cr['document_rid'],
                    "text": cr['chunk_text'] or "",
                    "score": round(float(cr['similarity']), 4),
                    "section_id": cr['section_id'],
                    "section_title": cr['section_title'],
                    "wiki_url": cr['wiki_url'],
                })
                sources.append({
                    "uri": cr['document_rid'],
                    "label": cr['title'] or cr['document_rid'],
                    "entity_type": "Document",
                    "score": round(float(cr['similarity']), 4),
                    "description": (cr['chunk_text'] or "")[:200],
                    "url": cr['wiki_url'],
                })
    except Exception:
        pass

    # ── Step 7: Web sources (same as B1) ──
    if entity_uris:
        try:
            ws_rows = await conn.fetch("""
                SELECT DISTINCT ON (ws.url) ws.url, ws.title, ws.description
                FROM web_submissions ws
                JOIN document_entity_links del ON del.document_rid = 'web:' || ws.rid::text
                WHERE del.entity_uri = ANY($1) AND ws.status IN ('ingested', 'monitoring')
                LIMIT 5
            """, entity_uris)
            for wr in ws_rows:
                desc = wr['description'] or ""
                web_sources.append({"url": wr['url'], "title": wr['title'] or wr['url'], "summary": desc})
                sources.append({
                    "uri": wr['url'],
                    "label": wr['title'] or wr['url'],
                    "entity_type": "WebSource",
                    "score": 0.8,
                    "description": desc[:200],
                })
        except Exception:
            pass

    return sources, relationships_ctx, doc_chunks, web_sources


# ---------------------------------------------------------------------------
# Structured graph query (Step 7) — template-based entity/relationship lookup
# ---------------------------------------------------------------------------
import re as _re_module

# Known entity types for pattern matching
_ENTITY_TYPES = {
    "organization": "Organization", "organizations": "Organization",
    "project": "Project", "projects": "Project",
    "location": "Location", "locations": "Location",
    "concept": "Concept", "concepts": "Concept",
    "practice": "Practice", "practices": "Practice",
    "person": "Person", "people": "Person",
    "bioregion": "Bioregion", "bioregions": "Bioregion",
}

# Patterns: (regex, template_name)
_GRAPH_QUERY_PATTERNS = [
    # "which/what organizations work on/related to/involved in X"
    (_re_module.compile(
        r"(?:which|what|list|show)\s+(\w+)\s+(?:work on|related to|involved in|involved with|associated with|connected to)\s+(.+?)[\?.]?$",
        _re_module.IGNORECASE
    ), "related_entities_typed"),
    # "what is related to X" / "what relates to X"
    (_re_module.compile(
        r"(?:what|which|who)\s+(?:is|are)\s+(?:related|connected|linked)\s+to\s+(.+?)[\?.]?$",
        _re_module.IGNORECASE
    ), "related_entities_all"),
    # "how many X in Y" / "how many X"
    (_re_module.compile(
        r"how many\s+(\w+)(?:\s+in\s+(.+?))?[\?.]?$",
        _re_module.IGNORECASE
    ), "count_entities"),
]


async def _resolve_entity_uri(name: str, conn) -> Optional[str]:
    """Resolve a free-text entity name to its fuseki_uri.

    Called by `_try_structured_graph_query`, which runs unconditionally on every POST
    /chat, so this is the widest-traffic name lookup in the service.

    Both queries are `ILIKE ... LIMIT 1` with no ORDER BY on the first, and merges keep
    the tombstoned row's entity_text — so for any merged name the winner was arbitrary.
    "Talk to the City" resolved to a tombstone this way. Following merged_into rather than
    filtering it out is deliberate: a tombstone hit means the name IS this entity, just
    under its pre-merge identity, and excluding it would turn a correct-but-stale answer
    into no answer at all.
    """
    row = await conn.fetchrow(
        "SELECT fuseki_uri FROM entity_registry WHERE entity_text ILIKE $1 AND NOT node_private LIMIT 1",
        name.strip()
    )
    if row:
        return await resolve_to_live_uri(conn, row["fuseki_uri"])
    # Try fuzzy match with %
    row = await conn.fetchrow(
        "SELECT fuseki_uri FROM entity_registry WHERE entity_text ILIKE $1 AND NOT node_private ORDER BY LENGTH(entity_text) LIMIT 1",
        f"%{name.strip()}%"
    )
    return await resolve_to_live_uri(conn, row["fuseki_uri"]) if row else None


async def _try_structured_graph_query(query: str, conn, db_pool) -> str:
    """
    Attempt to match the query against structured graph patterns.
    Returns a formatted block of results, or empty string if no pattern matches.
    """
    if conn is None:
        conn = await db_pool.acquire()
        _acquired = True
    else:
        _acquired = False

    try:
        for pattern, template_name in _GRAPH_QUERY_PATTERNS:
            m = pattern.search(query)
            if not m:
                continue

            if template_name == "related_entities_typed":
                type_word = m.group(1).lower()
                entity_name = m.group(2).strip()
                target_type = _ENTITY_TYPES.get(type_word)
                if not target_type:
                    continue

                uri = await _resolve_entity_uri(entity_name, conn)
                if not uri:
                    continue

                rows = await conn.fetch("""
                    SELECT DISTINCT e.entity_text, e.entity_type, e.description
                    FROM entity_registry e
                    JOIN entity_relationships r
                        ON (e.fuseki_uri = r.subject_uri OR e.fuseki_uri = r.object_uri)
                    WHERE (r.subject_uri = $1 OR r.object_uri = $1)
                      AND e.fuseki_uri != $1
                      AND e.entity_type = $2
                      AND NOT COALESCE(e.node_private, false)
                    ORDER BY e.entity_text
                    LIMIT 20
                """, uri, target_type)

                if not rows:
                    return ""

                lines = [f"Graph query: {target_type}s related to \"{entity_name}\""]
                for r in rows:
                    desc = f": {r['description'][:100]}" if r['description'] else ""
                    lines.append(f"- {r['entity_text']} ({r['entity_type']}){desc}")
                logger.info(f"Structured graph query matched: {template_name}, {len(rows)} results")
                return "\n".join(lines)

            elif template_name == "related_entities_all":
                entity_name = m.group(1).strip()
                uri = await _resolve_entity_uri(entity_name, conn)
                if not uri:
                    continue

                rows = await conn.fetch("""
                    SELECT DISTINCT e.entity_text, e.entity_type, r.predicate
                    FROM entity_registry e
                    JOIN entity_relationships r
                        ON (e.fuseki_uri = r.subject_uri OR e.fuseki_uri = r.object_uri)
                    WHERE (r.subject_uri = $1 OR r.object_uri = $1)
                      AND e.fuseki_uri != $1
                      AND NOT COALESCE(e.node_private, false)
                    ORDER BY e.entity_type, e.entity_text
                    LIMIT 20
                """, uri)

                if not rows:
                    return ""

                lines = [f"Graph query: entities related to \"{entity_name}\""]
                for r in rows:
                    lines.append(f"- {r['entity_text']} ({r['entity_type']}) [{r['predicate']}]")
                logger.info(f"Structured graph query matched: {template_name}, {len(rows)} results")
                return "\n".join(lines)

            elif template_name == "count_entities":
                type_word = m.group(1).lower()
                location_name = m.group(2).strip() if m.group(2) else None
                target_type = _ENTITY_TYPES.get(type_word)
                if not target_type:
                    continue

                if location_name:
                    # Count entities of type related to a location
                    loc_uri = await _resolve_entity_uri(location_name, conn)
                    if not loc_uri:
                        continue
                    row = await conn.fetchrow("""
                        SELECT COUNT(DISTINCT e.fuseki_uri) as cnt
                        FROM entity_registry e
                        JOIN entity_relationships r
                            ON (e.fuseki_uri = r.subject_uri OR e.fuseki_uri = r.object_uri)
                        WHERE (r.subject_uri = $1 OR r.object_uri = $1)
                          AND e.fuseki_uri != $1
                          AND e.entity_type = $2
                          AND NOT COALESCE(e.node_private, false)
                    """, loc_uri, target_type)
                    count = row['cnt']
                    logger.info(f"Structured graph query matched: {template_name}, count={count}")
                    return f"Graph query: {count} {target_type}(s) related to \"{location_name}\""
                else:
                    row = await conn.fetchrow(
                        "SELECT COUNT(*) as cnt FROM entity_registry WHERE entity_type = $1 AND NOT COALESCE(node_private, false)",
                        target_type
                    )
                    count = row['cnt']
                    logger.info(f"Structured graph query matched: {template_name}, count={count}")
                    return f"Graph query: {count} {target_type}(s) in the knowledge graph"

        return ""  # No pattern matched
    finally:
        if _acquired:
            await db_pool.release(conn)


# ── B7: Cross-encoder reranking via FlashRank ──
_reranker = None

def _get_reranker():
    global _reranker
    if _reranker is None:
        try:
            from flashrank import Ranker
            _reranker = Ranker(model_name="ms-marco-MiniLM-L-12-v2")
        except ImportError:
            logger.warning("flashrank not installed — reranking disabled")
    return _reranker

def _rerank_chunks(query: str, chunks: list, top_k: int = 8) -> list:
    """Rerank doc_chunks dicts using FlashRank cross-encoder (B7)."""
    if not chunks:
        return chunks
    ranker = _get_reranker()
    if ranker is None:
        return chunks[:top_k]
    from flashrank import RerankRequest
    passages = [{"id": i, "text": ((c.get("context") or "") + "\n" + c.get("text", ""))[:1500]} for i, c in enumerate(chunks)]
    req = RerankRequest(query=query, passages=passages)
    results = ranker.rerank(req)
    reranked = []
    for r in results[:top_k]:
        idx = r["id"]
        chunk = chunks[idx].copy()
        chunk["rerank_score"] = float(r["score"])
        reranked.append(chunk)
    return reranked


EXPANSION_MODEL = os.getenv("EXPANSION_MODEL", "gpt-4o-mini")


async def _expand_queries(query: str, n: int = 3) -> list:
    """Generate n query reformulations for multi-query retrieval (B8b).
    Returns [original_query] + up to n reformulations.
    """
    if not expansion_provider:
        return [query]
    try:
        result = await expansion_provider.complete(
            [{
                "role": "user",
                "content": f"""Generate {n} alternative search queries for a bioregional knowledge commons.
Original query: "{query}"

Return ONLY the alternative queries, one per line. No numbering, no explanation.
Each should rephrase the question using different terminology to find relevant documents."""
            }],
            model=EXPANSION_MODEL,
            max_tokens=200,
            temperature=0.7,
        )
        lines = [l.strip() for l in result.strip().split('\n') if l.strip()]
        return [query] + lines[:n]
    except Exception as e:
        logger.warning(f"B8b query expansion failed: {e}")
        return [query]


# ── P3 system prompts ──
_SYSTEM_PROMPT_DEFAULT = (
    "You are a knowledgeable assistant for a bioregional knowledge commons "
    "focused on ecological stewardship, regenerative practices, and community "
    "governance in bioregions.\n\n"
    "INSTRUCTIONS:\n"
    "- Answer the user's question using ONLY the entity, relationship, document, "
    "and web source context provided below. You may synthesize across multiple "
    "context items, but do not add facts from outside the provided context.\n"
    "- The Relevant Documents section is your primary evidence source — it often "
    "contains detailed answers not captured in entity summaries or relationship "
    "lists. Always synthesize from document text before concluding the context "
    "is insufficient.\n"
    "- Cite specific entities and sources in your answer.\n"
    "- When referencing wiki sources, cite them as [Page > Section](url).\n"
    "- If the provided context partially answers the question, answer the supported "
    "part and clearly state what information is missing.\n"
    "- If the provided context does not answer the question at all, say: "
    '"The available context does not contain sufficient information to answer '
    'this question."\n'
    "- Be concise and factual."
)

_SYSTEM_PROMPT_EXPLAINER = (
    "You are a knowledgeable assistant for a bioregional knowledge commons "
    "focused on ecological stewardship, regenerative practices, and community "
    "governance in bioregions.\n\n"
    "INSTRUCTIONS:\n"
    "- Answer the user's question using ONLY the entity, relationship, document, "
    "and web source context provided below. You may synthesize across multiple "
    "context items, but do not add facts from outside the provided context.\n"
    "- The Relevant Documents section is your primary evidence source — it often "
    "contains detailed answers not captured in entity summaries or relationship "
    "lists. Always synthesize from document text before concluding the context "
    "is insufficient.\n"
    "- Context items are labeled with reference IDs: [S1], [S2], … for entities, "
    "documents, and web sources; [R1], [R2], … for relationships; "
    "[G1], [G2], … for graph-query results. "
    "Use these IDs when citing evidence in claim blocks and Key Sources.\n\n"
    "FORMAT: Produce a structured brief. Omit any section that cannot be "
    "populated from the provided context.\n\n"
    "## Bottom Line\n"
    "2-4 sentences summarizing the best-supported answer. If the context "
    "is insufficient to answer the question, state that here and omit the "
    "Well-Supported Claims and Partial / Tentative Claims sections.\n\n"
    "## Well-Supported Claims\n"
    "List specific claims directly backed by the context. Each claim:\n"
    "- **Claim:** one short, atomic statement\n"
    "- **Evidence:** cite reference IDs, e.g. `S1, S3` or `R2`\n"
    "- **Support:** `well-supported`\n\n"
    "## Partial / Tentative Claims\n"
    "List claims the context suggests but cannot fully confirm. Each claim:\n"
    "- **Claim:** tentative statement\n"
    "- **Evidence:** cite reference IDs\n"
    "- **Support:** `partially supported`\n"
    "- **Missing:** what would be needed to strengthen this claim\n\n"
    "## Open Questions\n"
    "Explicit unknowns, ambiguities, or missing data the brief cannot resolve "
    "from the current context.\n\n"
    "## Key Sources\n"
    "List the most relevant sources using their reference IDs "
    "(e.g. `- S1: [Page > Section](url)` or `- S2: EntityName`). "
    "When referencing wiki sources, cite as [Page > Section](url).\n\n"
    "Support label guide: `well-supported` = evidence is sufficient and specific; "
    "`partially supported` = evidence points in the direction but leaves a gap. "
    "If a statement has no contextual support, route it to Open Questions instead "
    "of asserting it as a claim."
)


class ChatRequest(BaseModel):
    """Request for RAG chat."""
    query: str
    max_context_entities: int = Field(default=5, ge=1, le=20)
    retrieval_mode: str = Field(default="hybrid", description="hybrid (B1 default) or graphrag (B2 experimental)")
    include_code: bool = Field(default=False, description="Include code entity chunks in retrieval (default: exclude)")
    multi_query: bool = Field(default=False, description="Enable multi-query expansion for broader retrieval (B8b)")
    planner: bool = Field(default=False, description="B9a QueryPlan IR path (experimental)")
    debug_prompt: bool = Field(default=False, description="Include assembled prompt in response (requires CHAT_DEBUG_PROMPT env)")
    answer_mode: Literal["default", "explainer"] = Field(default="default", description="Answer format: 'default' for concise, 'explainer' for structured brief format")


@app.post("/chat")
async def chat_endpoint(request: ChatRequest):
    """
    RAG chat: semantic-search the knowledge graph, build context from matched
    entities (labels, types, descriptions, relationships), then call an LLM to
    generate a grounded answer.

    Returns ``{ answer, sources, intent }`` matching the web-dashboard
    ChatResponse contract.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    # Lazy init chat providers (classifier, chat answer, expansion)
    global classifier_provider, chat_answer_provider, expansion_provider
    if not classifier_provider or not chat_answer_provider or not expansion_provider:
        try:
            if not classifier_provider:
                classifier_provider = create_classifier_provider()
            if not chat_answer_provider:
                chat_answer_provider = create_chat_provider()
            if not expansion_provider:
                expansion_provider = create_expansion_provider()
        except (ValueError, ImportError) as e:
            raise HTTPException(
                status_code=503,
                detail=f"LLM provider not available: {e}",
            )

    # ------------------------------------------------------------------
    # 1. Semantic search over entity embeddings to find relevant entities
    # ------------------------------------------------------------------
    query_embedding = await generate_query_embedding(request.query)

    # ── B9a Planner path (opt-in via planner=true) ──
    planner_requested = request.planner and request.retrieval_mode != "graphrag"
    planner_executed = False
    plan_trace = None

    if planner_requested:
        from api.query_classifier import classify_query, CLASSIFIER_CONFIDENCE_THRESHOLD
        from api.query_planner import assemble_plan
        from api.plan_executor import execute_plan as _execute_plan
        from api.retrieval_executors import evidence_bundles_to_legacy_format
        from api.schemas.query_plan import QueryTaxonomy

        classifier_output = await classify_query(request.query, classifier_provider)

        if classifier_output.query_taxonomy == QueryTaxonomy.OUT_OF_DOMAIN and \
           classifier_output.confidence >= CLASSIFIER_CONFIDENCE_THRESHOLD:
            # ABSTENTION: early-return, bypasses prompt builder + LLM entirely
            return {
                "answer": "This question appears to be outside the scope of bioregional knowledge. "
                          "I can help with questions about ecology, governance, organizations, "
                          "commitments, and projects in the Bioregional Knowledge Commons.",
                "sources": [],
                "intent": {"intent": "out_of_domain", "entities": [], "confidence": classifier_output.confidence},
                "retrieval_mode": request.retrieval_mode,
                "plan_trace": {
                    "plan_id": None, "taxonomy": "out_of_domain",
                    "confidence": classifier_output.confidence,
                    "fallback": False, "abstained": True,
                },
            }
        elif classifier_output.confidence >= CLASSIFIER_CONFIDENCE_THRESHOLD:
            planner_executed = True
            plan = assemble_plan(classifier_output, request.query)

            from api.confidence_gate import (
                compute_signals as _compute_signals,
                assess as _assess_confidence,
            )
            from api.schemas.query_plan import RetrievalOp as _RetrievalOp

            # Check which executor types the plan includes
            _plan_had_entity_lookup = any(
                s.op == _RetrievalOp.ENTITY_LOOKUP for s in plan.steps
            )
            _plan_had_text_search = any(
                s.op == _RetrievalOp.TEXT_SEARCH for s in plan.steps
            )

            async with db_pool.acquire() as conn:
                evidence, traces = await _execute_plan(
                    plan, conn, query_embedding,
                    generate_embedding_fn=generate_query_embedding,
                    expand_queries_fn=_expand_queries,
                    rerank_fn=_rerank_chunks,
                    quartz_url_fn=quartz_url,
                )

                # ── B9.5 CRAG confidence gate (telemetry-only) ──
                # Compute signals and gate decision for logging.
                # Retry and abstention behavior disabled after Sprint 2
                # eval showed quality-neutral results with latency cost.
                # The signals are logged to plan_trace for future analysis.
                _crag_signals = _compute_signals(evidence, _plan_had_entity_lookup, _plan_had_text_search)
                _gate_decision = _assess_confidence(_crag_signals)

                sources, relationships_ctx, doc_chunks, web_sources = evidence_bundles_to_legacy_format(evidence)
            plan_trace = {
                "plan_id": plan.plan_id,
                "taxonomy": plan.query_taxonomy.value,
                "confidence": classifier_output.confidence,
                "depth_tier": classifier_output.depth_tier.value,
                "steps": len(plan.steps),
                "fallback": False,
                "gate_decision": _gate_decision.value,
                "confidence_signals": _crag_signals.to_dict(),
            }
        else:
            # HARD FALLBACK: low confidence -> run full monolithic baseline
            logger.info(f"Planner confidence {classifier_output.confidence:.2f} < {CLASSIFIER_CONFIDENCE_THRESHOLD}, falling back")
            plan_trace = {
                "fallback": True,
                "confidence": classifier_output.confidence,
                "taxonomy": classifier_output.query_taxonomy.value,
                "fallback_reason": "low_confidence",
            }
            # Falls through to monolithic path below

    # ── B2 GraphRAG dispatch ──
    _use_graphrag = request.retrieval_mode == "graphrag"
    _graphrag_done = False  # True when graphrag produced usable results

    if _use_graphrag:
        async with db_pool.acquire() as conn:
            _gr_sources, _gr_rels, _gr_docs, _gr_web = await _graph_guided_retrieval(
                request.query, query_embedding, conn, top_k=request.max_context_entities
            )
        # Only use graphrag results if it actually found sources;
        # otherwise fall through to B1 so we don't serve empty context.
        if _gr_sources:
            sources = _gr_sources
            relationships_ctx = _gr_rels
            doc_chunks = _gr_docs
            web_sources = _gr_web
            _graphrag_done = True

    # ── B1 hybrid retrieval via extracted executors (B9a refactor) ──
    # Skipped when planner already executed or graphrag produced results
    if not planner_executed and not _graphrag_done:
      from api.retrieval_executors import (
          entity_lookup as _entity_lookup,
          relationship_traverse as _relationship_traverse,
          text_search as _text_search,
          web_source_lookup as _web_source_lookup,
          evidence_bundles_to_legacy_format,
          CHAT_EXCLUDE_TYPES,
      )
      async with db_pool.acquire() as conn:
        # Step 1: Entity lookup (exclude internal tracking types for /chat)
        entity_bundles = await _entity_lookup(
            request.query, query_embedding, conn,
            max_results=request.max_context_entities,
            exclude_types=CHAT_EXCLUDE_TYPES,
            quartz_url_fn=quartz_url,
        )
        entity_uris = [b.source_uri for b in entity_bundles]

        # Step 2: Relationship traverse
        rel_bundles = await _relationship_traverse(entity_uris, conn)

        # Step 3: Text search (hybrid BM25+vector + reranking)
        text_bundles = await _text_search(
            request.query, query_embedding, conn,
            multi_query=request.multi_query,
            include_code=request.include_code,
            top_k=8,
            generate_embedding_fn=generate_query_embedding,
            expand_queries_fn=_expand_queries,
            rerank_fn=_rerank_chunks,
        )

        # Step 4: Web source enrichment (deterministic post-step)
        web_bundles = await _web_source_lookup(entity_uris, conn)

      # Convert to legacy format for prompt builder
      all_bundles = entity_bundles + rel_bundles + text_bundles + web_bundles
      sources, relationships_ctx, doc_chunks, web_sources = evidence_bundles_to_legacy_format(all_bundles)

    # ------------------------------------------------------------------
    # 2d. Structured graph query (template-based, Step 7)
    # ------------------------------------------------------------------
    graph_query_block = ""
    try:
        graph_query_block = await _try_structured_graph_query(
            request.query, None, db_pool
        )
    except Exception as e:
        logger.warning(f"Structured graph query failed (non-fatal): {e}")

    # ------------------------------------------------------------------
    # 3. Build LLM prompt with entity context
    # ------------------------------------------------------------------
    ref_to_source: dict = {}

    if request.answer_mode == "explainer":
        from api.brief_payload import (
            assign_source_refs as _assign_source_refs,
            assign_and_render_graph_query_refs as _assign_gq_refs,
            render_entity_block as _render_entity_block,
            render_doc_block as _render_doc_block,
            render_web_block as _render_web_block,
            render_rel_block as _render_rel_block,
        )
        ref_to_source = _assign_source_refs(sources, doc_chunks, web_sources, relationships_ctx)
        entity_block = _render_entity_block(sources)
        rel_block = _render_rel_block(relationships_ctx, ref_to_source)
        doc_block = _render_doc_block(doc_chunks)
        web_block = _render_web_block(web_sources)
        if graph_query_block:
            graph_query_block = _assign_gq_refs(graph_query_block, ref_to_source)
        system_prompt = _SYSTEM_PROMPT_EXPLAINER
        answer_max_tokens = 1536
    else:
        entity_block = "\n".join(
            f"- {s['label']} ({s['entity_type']})"
            + (f": {s['description']}" if s.get('description') else "")
            for s in sources
            if s['entity_type'] not in ('Document', 'WebSource')
        ) or "(no matching entities found)"
        rel_block = "\n".join(f"- {r}" for r in relationships_ctx) or "(none)"
        doc_block = "\n".join(
            f"- **{d['title']}**"
            + (f" (Section: {d['section_title']})" if d.get('section_title') else "")
            + (f" [source]({d['wiki_url']})" if d.get('wiki_url') else "")
            + (f" Context: {d['context']}." if d.get('context') else "")
            + f": {d['text'][:1500]}"
            for d in doc_chunks
        ) if doc_chunks else ""
        web_block = "\n".join(
            f"- [{w['title']}]({w['url']}): {w['summary'][:500]}"
            for w in web_sources
        ) if web_sources else ""
        system_prompt = _SYSTEM_PROMPT_DEFAULT
        answer_max_tokens = 1024

    prompt_sections = [f"## Relevant Entities\n{entity_block}"]
    prompt_sections.append(f"## Relationships\n{rel_block}")
    if graph_query_block:
        prompt_sections.append(f"## Graph Query Results\n{graph_query_block}")
    if doc_block:
        prompt_sections.append(f"## Relevant Documents\n{doc_block}")
    if web_block:
        prompt_sections.append(f"## Web Sources\n{web_block}")
    prompt_sections.append(f"## Question\n{request.query}")
    user_prompt = "\n\n".join(prompt_sections)

    # ------------------------------------------------------------------
    # 4. Call LLM
    # ------------------------------------------------------------------
    try:
        answer = await chat_answer_provider.complete(
            [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=answer_max_tokens,
        )
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"LLM call failed: {e}",
        )

    # ------------------------------------------------------------------
    # 5. Build intent object (lightweight classification)
    # ------------------------------------------------------------------
    entity_labels = [s['label'] for s in sources]
    intent = {
        "intent": "knowledge_query",
        "entities": entity_labels,
        "confidence": round(max((s['score'] for s in sources), default=0.0), 4),
    }

    response = {
        "answer": answer,
        "sources": sources,
        "intent": intent,
        "retrieval_mode": request.retrieval_mode,
        "answer_mode": request.answer_mode,
    }
    # Always emit plan_trace when planner was requested (including fallback)
    if planner_requested and plan_trace is not None:
        response["plan_trace"] = plan_trace
    # Brief payload for explainer mode (additive — does not affect default mode)
    if request.answer_mode == "explainer":
        from api.brief_payload import parse_brief as _parse_brief
        try:
            brief_payload = _parse_brief(answer, ref_to_source)
        except Exception as e:
            logger.warning(f"Brief payload parsing failed: {e}")
            brief_payload = {
                "bottom_line": "",
                "claims": [],
                "open_questions": [],
                "key_sources": [],
                "parse_warnings": [f"Parser exception: {e}"],
            }
        response["brief_payload_version"] = "v1"
        response["brief_payload"] = brief_payload
    # Debug prompt capture (double-gated: request flag + server env var)
    if request.debug_prompt and os.getenv("CHAT_DEBUG_PROMPT"):
        response["_debug_prompt"] = {
            "system_prompt": system_prompt,
            "user_prompt": user_prompt,
        }
    return response


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv('KOI_API_PORT', '8351'))
    uvicorn.run(app, host="0.0.0.0", port=port)
