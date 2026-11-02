"""
B9a — QueryPlan IR schema.

Typed retrieval plan emitted by the planner and executed by trusted tool runners.
The LLM emits typed operations; trusted executors compile these to SQL, Cypher,
or MCP tool calls. The LLM never writes raw SQL.

See: docs/specs/b9a-query-plan-spec.md for decision matrix and router contract.
See: BioregionalKnowledgeCommoning/docs/foundations/federated-memory-architecture.md
     for architectural context.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class QueryTaxonomy(str, Enum):
    """Domain query taxonomy — 7 classes that determine retrieval strategy."""
    ENTITY_DEFINITION = "entity_definition"
    RELATIONSHIP_PATH = "relationship_path"
    GOVERNANCE_POLICY = "governance_policy"
    ROADMAP_STATUS = "roadmap_status"
    COMMITMENT_CLAIM = "commitment_claim"
    CROSS_NODE_PROVENANCE = "cross_node_provenance"
    OUT_OF_DOMAIN = "out_of_domain"


class RetrievalOp(str, Enum):
    """Typed retrieval operations. Each maps to a trusted executor function.
    B9a implements: entity_lookup, relationship_traverse, text_search.
    Stubs: graph_query (B9c), structured_sql (B9b), peer_query (B12)."""
    ENTITY_LOOKUP = "entity_lookup"
    RELATIONSHIP_TRAVERSE = "relationship_traverse"
    TEXT_SEARCH = "text_search"
    GRAPH_QUERY = "graph_query"
    STRUCTURED_SQL = "structured_sql"
    PEER_QUERY = "peer_query"


class SourceType(str, Enum):
    """Provenance classification for evidence bundles."""
    LOCAL_AUTHORITATIVE = "local_authoritative"  # entity_registry canonical descriptions
    LOCAL_DOCUMENT = "local_document"              # koi_memory_chunks (wiki, docs)
    LOCAL_WEB = "local_web"                        # web_submissions linked to entities
    # Future (B12):
    # TRUSTED_PEER = "trusted_peer"
    # PUBLIC_PEER = "public_peer"


class DepthTier(str, Enum):
    """Retrieval depth budget tier."""
    SHALLOW = "shallow"    # entity_lookup only (~200ms)
    STANDARD = "standard"  # entity_lookup + text_search (~2-5s)
    DEEP = "deep"          # all applicable ops + multi_query (~5-10s)


# ---------------------------------------------------------------------------
# Core models
# ---------------------------------------------------------------------------

class PolicyScope(BaseModel):
    """Trusted system constraint derived from caller identity and visibility rules.
    Deterministic — never LLM-authored. The planner operates within a PolicyScope
    and cannot widen it.

    v0: local-only. Visibility derived from existing node_private filtering.
    Future: caller_node from auth, eligible_peers from edge governance,
    consent from commons_memberships."""

    visibility_tier: Literal["public", "node_private"] = "public"
    include_node_private: bool = False

    # Future fields (not implemented in B9a):
    # caller_node: str | None = None
    # eligible_peers: list[str] = Field(default_factory=list)
    # consent_constraints: list[str] = Field(default_factory=list)


class StepBudget(BaseModel):
    """Resource limits for a single retrieval step."""
    max_results: int = 20
    max_tokens: int = 4000
    timeout_ms: int = 5000


class PlanStep(BaseModel):
    """A single retrieval operation in a QueryPlan."""
    op: RetrievalOp
    target: str = ""               # table/index/peer targeted (informational)
    params: dict = Field(default_factory=dict)  # op-specific parameters
    budget: StepBudget = Field(default_factory=StepBudget)
    depends_on: list[int] = Field(default_factory=list)  # step indices


class SafetyGuards(BaseModel):
    """Hard limits on plan execution. Violations halt execution with logging."""
    max_steps: int = 6
    max_total_tokens: int = 16000
    timeout_ms: int = 15000
    max_peer_fanout: int = 0  # v0: local only


class EntityCandidate(BaseModel):
    """An entity mention extracted from the query with candidate URIs."""
    name: str
    type: str | None = None
    candidates: list[str] = Field(default_factory=list)  # entity URIs


class EvidenceBundle(BaseModel):
    """Internal evidence unit with provenance. Produced by retrieval ops,
    consumed by answer generation.

    Internal through Phase 2. External exposure deferred to B12."""

    source_uri: str                     # entity URI, chunk ID, or web URL
    source_type: SourceType
    source_node: str = "local"          # v0: always "local"
    retrieval_op: RetrievalOp           # which op produced this
    confidence: float                   # retrieval score (RRF, similarity, rerank)
    text: str                           # the actual content
    metadata: dict = Field(default_factory=dict)  # op-specific (entity_type, chunk_source, etc.)
    freshness: datetime | None = None   # last_modified or created_at


class StepTrace(BaseModel):
    """Execution trace for a single plan step. Populated during execution."""
    step_index: int
    op: RetrievalOp
    started_at: datetime
    completed_at: datetime
    results_count: int
    tokens_used: int = 0
    error: str | None = None


class QueryPlan(BaseModel):
    """A typed retrieval plan emitted by the planner and executed by trusted
    tool runners. Contains both the plan and its execution trace."""

    plan_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    policy_scope: PolicyScope = Field(default_factory=PolicyScope)
    query_taxonomy: QueryTaxonomy
    original_query: str
    depth_tier: DepthTier = DepthTier.STANDARD
    entities: list[EntityCandidate] = Field(default_factory=list)
    steps: list[PlanStep] = Field(default_factory=list)
    safety_guards: SafetyGuards = Field(default_factory=SafetyGuards)
    trace: list[StepTrace] | None = None


# ---------------------------------------------------------------------------
# Classifier output (what the LLM returns)
# ---------------------------------------------------------------------------

class ClassifierOutput(BaseModel):
    """Structured output from the query classifier (GPT-4o-mini).
    Layer 2 of the router: taxonomy classification + entity extraction."""

    query_taxonomy: QueryTaxonomy
    depth_tier: DepthTier = DepthTier.STANDARD
    entities: list[EntityCandidate] = Field(default_factory=list)
    reasoning: str = ""  # brief explanation of classification choice
    confidence: float = Field(
        default=0.0, ge=0.0, le=1.0,
        description="Classifier self-reported confidence. Below threshold triggers fallback to baseline.",
    )
