"""
B9a — Plan executor.

Dispatches each PlanStep to its corresponding retrieval executor,
enforces SafetyGuards, captures StepTrace, and runs web-source
enrichment as a deterministic post-step.

See: docs/specs/b9a-query-plan-spec.md §4 for op definitions.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable

import asyncpg

from api.retrieval_executors import (
    entity_lookup,
    relationship_traverse,
    text_search,
    web_source_lookup,
    graph_query,
    structured_sql,
    peer_query,
    CHAT_EXCLUDE_TYPES,
)
from api.schemas.query_plan import (
    EvidenceBundle,
    QueryPlan,
    QueryTaxonomy,
    RetrievalOp,
    StepTrace,
)

logger = logging.getLogger(__name__)


async def execute_plan(
    plan: QueryPlan,
    conn: asyncpg.Connection,
    query_embedding: list[float] | None,
    *,
    generate_embedding_fn: Callable,
    expand_queries_fn: Callable,
    rerank_fn: Callable,
    quartz_url_fn: Callable | None = None,
) -> tuple[list[EvidenceBundle], list[StepTrace]]:
    """Execute a QueryPlan by dispatching each step to its executor.

    After all plan steps complete, runs web_source_lookup as a
    deterministic post-step (not dispatched via PlanStep.op).

    Enforces SafetyGuards: halts if step count exceeds max_steps
    or wall-clock time exceeds timeout_ms.

    Returns (all_evidence_bundles including web, step_traces).
    """
    all_evidence: list[EvidenceBundle] = []
    traces: list[StepTrace] = []
    entity_uris: list[str] = []
    plan_start = time.monotonic()
    guards = plan.safety_guards

    for step_idx, step in enumerate(plan.steps):
        # Safety: max steps
        if step_idx >= guards.max_steps:
            logger.warning(f"Plan halted: max_steps ({guards.max_steps}) exceeded at step {step_idx}")
            break

        # Safety: timeout
        elapsed_ms = (time.monotonic() - plan_start) * 1000
        if elapsed_ms >= guards.timeout_ms:
            logger.warning(f"Plan halted: timeout ({guards.timeout_ms}ms) exceeded at step {step_idx}")
            break

        step_start = datetime.now(timezone.utc)
        t0 = time.monotonic()
        error = None
        step_evidence: list[EvidenceBundle] = []

        try:
            if step.op == RetrievalOp.ENTITY_LOOKUP:
                # Roadmap queries need WorkItem/Milestone/etc — skip type filter
                _exclude = None if plan.query_taxonomy == QueryTaxonomy.ROADMAP_STATUS else CHAT_EXCLUDE_TYPES
                step_evidence = await entity_lookup(
                    plan.original_query,
                    query_embedding,
                    conn,
                    max_results=step.budget.max_results,
                    exclude_types=_exclude,
                    quartz_url_fn=quartz_url_fn,
                )
                # Collect entity URIs for downstream steps
                entity_uris.extend(b.source_uri for b in step_evidence)

            elif step.op == RetrievalOp.RELATIONSHIP_TRAVERSE:
                step_evidence = await relationship_traverse(
                    entity_uris,
                    conn,
                    max_hops=step.params.get("max_hops", 2),
                    max_results=step.budget.max_results,
                )

            elif step.op == RetrievalOp.TEXT_SEARCH:
                step_evidence = await text_search(
                    plan.original_query,
                    query_embedding,
                    conn,
                    multi_query=step.params.get("multi_query", False),
                    include_code=step.params.get("include_code", False),
                    top_k=step.params.get("top_k", 8),
                    generate_embedding_fn=generate_embedding_fn,
                    expand_queries_fn=expand_queries_fn,
                    rerank_fn=rerank_fn,
                )

            elif step.op == RetrievalOp.GRAPH_QUERY:
                step_evidence = await graph_query()

            elif step.op == RetrievalOp.STRUCTURED_SQL:
                step_evidence = await structured_sql(
                    plan.original_query,
                    conn,
                    template=step.params.get("template", "commitment"),
                    entity_uris=entity_uris if entity_uris else None,
                    max_results=step.budget.max_results,
                )

            elif step.op == RetrievalOp.PEER_QUERY:
                step_evidence = await peer_query()

            all_evidence.extend(step_evidence)

        except Exception as e:
            error = str(e)
            logger.error(f"Plan step {step_idx} ({step.op.value}) failed: {e}")

        step_end = datetime.now(timezone.utc)
        traces.append(StepTrace(
            step_index=step_idx,
            op=step.op,
            started_at=step_start,
            completed_at=step_end,
            results_count=len(step_evidence),
            error=error,
        ))

    # Deterministic post-step: web source enrichment
    # (not dispatched via PlanStep.op — always runs after plan steps)
    if entity_uris:
        try:
            web_evidence = await web_source_lookup(entity_uris, conn)
            all_evidence.extend(web_evidence)
        except Exception as e:
            logger.warning(f"Web source post-step failed (non-fatal): {e}")

    return all_evidence, traces
