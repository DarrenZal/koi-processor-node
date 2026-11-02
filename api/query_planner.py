"""
B9a — Deterministic plan assembly.

Layer 3 of the router: maps ClassifierOutput to a QueryPlan via
decision matrix lookup. No LLM calls. Depth tier modifies step
parameters.

See: docs/specs/b9a-query-plan-spec.md §3 for the decision matrix.
"""

from __future__ import annotations

import math

from api.schemas.query_plan import (
    ClassifierOutput,
    DepthTier,
    PlanStep,
    PolicyScope,
    QueryPlan,
    QueryTaxonomy,
    RetrievalOp,
    SafetyGuards,
    StepBudget,
)


def _step(
    op: RetrievalOp,
    target: str = "",
    params: dict | None = None,
    max_results: int = 20,
    depends_on: list[int] | None = None,
) -> PlanStep:
    return PlanStep(
        op=op,
        target=target,
        params=params or {},
        budget=StepBudget(max_results=max_results),
        depends_on=depends_on or [],
    )


# ---------------------------------------------------------------------------
# Decision matrix: QueryTaxonomy -> list of PlanStep factories
# Each entry is (op, target, params, max_results, depends_on_indices)
# ---------------------------------------------------------------------------

def _entity_definition_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=5),
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"top_k": 8}, max_results=20, depends_on=[0]),
    ]


def _relationship_path_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=3),
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"multi_query": True, "top_k": 8}, max_results=20, depends_on=[0]),
    ]


def _governance_policy_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"multi_query": True, "top_k": 12}, max_results=20),
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=8),
        _step(RetrievalOp.RELATIONSHIP_TRAVERSE, "entity_relationships",
              params={"max_hops": 1}, max_results=30, depends_on=[1]),
    ]


def _roadmap_status_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=5),
        _step(RetrievalOp.STRUCTURED_SQL, "entity_registry",
              params={"template": "roadmap"}, max_results=15, depends_on=[0]),
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"top_k": 6}, max_results=15, depends_on=[0]),
    ]


def _commitment_claim_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=3),
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"multi_query": True, "top_k": 8}, max_results=20, depends_on=[0]),
    ]


def _cross_node_provenance_steps() -> list[PlanStep]:
    return [
        _step(RetrievalOp.ENTITY_LOOKUP, "entity_registry", max_results=5),
        _step(RetrievalOp.TEXT_SEARCH, "koi_memory_chunks",
              params={"top_k": 8}, max_results=20, depends_on=[0]),
    ]


DECISION_MATRIX = {
    QueryTaxonomy.ENTITY_DEFINITION: _entity_definition_steps,
    QueryTaxonomy.RELATIONSHIP_PATH: _relationship_path_steps,
    QueryTaxonomy.GOVERNANCE_POLICY: _governance_policy_steps,
    QueryTaxonomy.ROADMAP_STATUS: _roadmap_status_steps,
    QueryTaxonomy.COMMITMENT_CLAIM: _commitment_claim_steps,
    QueryTaxonomy.CROSS_NODE_PROVENANCE: _cross_node_provenance_steps,
    QueryTaxonomy.OUT_OF_DOMAIN: lambda: [],  # empty = abstention
}


def _apply_depth_overrides(steps: list[PlanStep], depth: DepthTier) -> list[PlanStep]:
    """Apply depth tier overrides to plan steps."""
    if depth == DepthTier.STANDARD:
        return steps

    if depth == DepthTier.SHALLOW:
        # Keep only entity_lookup steps
        return [s for s in steps if s.op == RetrievalOp.ENTITY_LOOKUP]

    if depth == DepthTier.DEEP:
        for s in steps:
            if s.op == RetrievalOp.TEXT_SEARCH:
                s.params["multi_query"] = True
            # Increase max_results by 50%
            s.budget.max_results = math.ceil(s.budget.max_results * 1.5)
        return steps

    return steps


def assemble_plan(
    classifier_output: ClassifierOutput,
    original_query: str,
    policy_scope: PolicyScope | None = None,
) -> QueryPlan:
    """Deterministic plan assembly from decision matrix.

    Maps taxonomy + depth_tier to ordered PlanSteps. No LLM calls.
    """
    taxonomy = classifier_output.query_taxonomy
    step_factory = DECISION_MATRIX.get(taxonomy, lambda: [])
    steps = step_factory()

    # Apply depth tier overrides
    steps = _apply_depth_overrides(steps, classifier_output.depth_tier)

    return QueryPlan(
        policy_scope=policy_scope or PolicyScope(),
        query_taxonomy=taxonomy,
        original_query=original_query,
        depth_tier=classifier_output.depth_tier,
        entities=classifier_output.entities,
        steps=steps,
        safety_guards=SafetyGuards(),
    )
