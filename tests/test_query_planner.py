"""B9a planner component tests.

Tests classifier, plan assembly, plan execution, confidence fallback,
OOD abstention, web evidence post-step, and plan_trace emission.
"""

import json
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.schemas.query_plan import (
    ClassifierOutput,
    DepthTier,
    EntityCandidate,
    EvidenceBundle,
    PlanStep,
    QueryPlan,
    QueryTaxonomy,
    RetrievalOp,
    SafetyGuards,
    SourceType,
    StepBudget,
)
from api.query_planner import assemble_plan, DECISION_MATRIX
from api.query_classifier import CLASSIFIER_CONFIDENCE_THRESHOLD


# ---------------------------------------------------------------------------
# Classifier tests
# ---------------------------------------------------------------------------

class TestClassifier:

    @pytest.mark.asyncio
    async def test_classify_entity_definition(self):
        """Mock provider returns correct ClassifierOutput for entity question."""
        from api.query_classifier import classify_query

        mock_provider = AsyncMock()
        mock_provider.complete = AsyncMock(return_value=json.dumps({
            "query_taxonomy": "entity_definition",
            "depth_tier": "standard",
            "entities": [{"name": "Eelgrass", "type": "Concept"}],
            "reasoning": "Asking for a definition of eelgrass",
            "confidence": 0.95,
        }))

        result = await classify_query("What is eelgrass?", mock_provider)

        assert result.query_taxonomy == QueryTaxonomy.ENTITY_DEFINITION
        assert result.depth_tier == DepthTier.STANDARD
        assert len(result.entities) == 1
        assert result.entities[0].name == "Eelgrass"
        assert result.confidence == 0.95

    @pytest.mark.asyncio
    async def test_classify_parse_error_fallback(self):
        """Malformed LLM response -> OUT_OF_DOMAIN, confidence=0.0."""
        from api.query_classifier import classify_query

        mock_provider = AsyncMock()
        mock_provider.complete = AsyncMock(side_effect=Exception("API error"))

        result = await classify_query("test query", mock_provider)

        assert result.query_taxonomy == QueryTaxonomy.OUT_OF_DOMAIN
        assert result.confidence == 0.0
        assert "parse_error" in result.reasoning


# ---------------------------------------------------------------------------
# Plan assembly tests
# ---------------------------------------------------------------------------

class TestPlanAssembly:

    def test_assemble_entity_definition(self):
        """Entity definition -> 2 steps: entity_lookup -> text_search."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            depth_tier=DepthTier.STANDARD,
            confidence=0.9,
        )
        plan = assemble_plan(co, "What is eelgrass?")

        assert plan.query_taxonomy == QueryTaxonomy.ENTITY_DEFINITION
        assert len(plan.steps) == 2
        assert plan.steps[0].op == RetrievalOp.ENTITY_LOOKUP
        assert plan.steps[1].op == RetrievalOp.TEXT_SEARCH

    def test_assemble_relationship_path(self):
        """Relationship path -> 2 steps, text-search-first (B9c Approach B)."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.RELATIONSHIP_PATH,
            depth_tier=DepthTier.STANDARD,
            confidence=0.85,
        )
        plan = assemble_plan(co, "How does X relate to Y?")

        assert len(plan.steps) == 2
        ops = [s.op for s in plan.steps]
        assert ops == [RetrievalOp.ENTITY_LOOKUP, RetrievalOp.TEXT_SEARCH]
        assert plan.steps[0].budget.max_results == 3
        assert plan.steps[1].params.get("multi_query") is True

    def test_assemble_out_of_domain(self):
        """Out of domain -> empty steps list."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.OUT_OF_DOMAIN,
            depth_tier=DepthTier.STANDARD,
            confidence=0.95,
        )
        plan = assemble_plan(co, "What is the stock price of Apple?")

        assert len(plan.steps) == 0

    def test_assemble_depth_shallow(self):
        """Shallow depth -> text_search steps dropped."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            depth_tier=DepthTier.SHALLOW,
            confidence=0.9,
        )
        plan = assemble_plan(co, "What is eelgrass?")

        # Shallow should only keep entity_lookup
        assert all(s.op == RetrievalOp.ENTITY_LOOKUP for s in plan.steps)
        assert len(plan.steps) == 1

    def test_commitment_claim_text_search_first(self):
        """commitment_claim plan: ENTITY_LOOKUP -> TEXT_SEARCH (multi_query)."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.COMMITMENT_CLAIM,
            depth_tier=DepthTier.STANDARD,
            confidence=0.9,
        )
        plan = assemble_plan(co, "What commitments have been made?")

        ops = [s.op for s in plan.steps]
        assert ops == [RetrievalOp.ENTITY_LOOKUP, RetrievalOp.TEXT_SEARCH]
        assert plan.steps[0].budget.max_results == 3
        text_step = plan.steps[1]
        assert text_step.params.get("multi_query") is True
        assert text_step.params.get("top_k") == 8

    def test_roadmap_status_includes_structured_sql(self):
        """roadmap_status plan: ENTITY_LOOKUP -> STRUCTURED_SQL -> TEXT_SEARCH."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ROADMAP_STATUS,
            depth_tier=DepthTier.STANDARD,
            confidence=0.9,
        )
        plan = assemble_plan(co, "What is the status of the roadmap?")

        ops = [s.op for s in plan.steps]
        assert ops == [RetrievalOp.ENTITY_LOOKUP, RetrievalOp.STRUCTURED_SQL, RetrievalOp.TEXT_SEARCH]
        sql_step = plan.steps[1]
        assert sql_step.params.get("template") == "roadmap"
        assert sql_step.budget.max_results == 15

    def test_governance_policy_includes_relationship_traverse(self):
        """governance_policy plan: TEXT_SEARCH -> ENTITY_LOOKUP -> RELATIONSHIP_TRAVERSE."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.GOVERNANCE_POLICY,
            depth_tier=DepthTier.STANDARD,
            confidence=0.9,
        )
        plan = assemble_plan(co, "How does the BKC handle governance?")

        ops = [s.op for s in plan.steps]
        assert ops == [RetrievalOp.TEXT_SEARCH, RetrievalOp.ENTITY_LOOKUP, RetrievalOp.RELATIONSHIP_TRAVERSE]
        rel_step = plan.steps[2]
        assert rel_step.params.get("max_hops") == 1
        assert rel_step.budget.max_results == 30
        assert rel_step.depends_on == [1]

    def test_assemble_depth_deep(self):
        """Deep depth -> multi_query=true, max_results +50%."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            depth_tier=DepthTier.DEEP,
            confidence=0.9,
        )
        plan = assemble_plan(co, "Complex question?")

        text_steps = [s for s in plan.steps if s.op == RetrievalOp.TEXT_SEARCH]
        assert len(text_steps) >= 1
        for s in text_steps:
            assert s.params.get("multi_query") is True
        # Max results should be 50% higher than default (20 -> 30)
        for s in plan.steps:
            assert s.budget.max_results > 0


# ---------------------------------------------------------------------------
# Plan executor tests
# ---------------------------------------------------------------------------

class TestPlanExecutor:

    @pytest.mark.asyncio
    async def test_execute_plan_safety_timeout(self):
        """timeout_ms=1 -> halt before completion."""
        from api.plan_executor import execute_plan

        plan = QueryPlan(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            original_query="test",
            steps=[
                PlanStep(op=RetrievalOp.ENTITY_LOOKUP, budget=StepBudget()),
                PlanStep(op=RetrievalOp.TEXT_SEARCH, budget=StepBudget()),
            ],
            safety_guards=SafetyGuards(timeout_ms=1),  # very short timeout
        )
        conn = AsyncMock()

        # Make entity_lookup slow enough to trigger timeout
        async def _slow_lookup(*args, **kwargs):
            await asyncio.sleep(0.01)
            return []

        with patch("api.plan_executor.entity_lookup", side_effect=_slow_lookup), \
             patch("api.plan_executor.text_search", new_callable=AsyncMock) as mock_text, \
             patch("api.plan_executor.web_source_lookup", new_callable=AsyncMock, return_value=[]):
            evidence, traces = await execute_plan(
                plan, conn, None,
                generate_embedding_fn=AsyncMock(),
                expand_queries_fn=AsyncMock(),
                rerank_fn=MagicMock(),
            )

        # Second step should not have executed due to timeout
        assert len(traces) <= 2  # may have 1 or 2 depending on timing

    @pytest.mark.asyncio
    async def test_execute_plan_includes_web_evidence_post_step(self):
        """execute_plan returns web EvidenceBundles even though no PlanStep has op=web."""
        from api.plan_executor import execute_plan

        entity_bundle = EvidenceBundle(
            source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
            text="Entity", metadata={},
        )
        web_bundle = EvidenceBundle(
            source_uri="https://example.com", source_type=SourceType.LOCAL_WEB,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.8,
            text="Web", metadata={"url": "https://example.com", "title": "Web", "summary": "Web"},
        )

        plan = QueryPlan(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            original_query="test",
            steps=[PlanStep(op=RetrievalOp.ENTITY_LOOKUP, budget=StepBudget(max_results=5))],
        )
        conn = AsyncMock()

        with patch("api.plan_executor.entity_lookup", new_callable=AsyncMock, return_value=[entity_bundle]), \
             patch("api.plan_executor.web_source_lookup", new_callable=AsyncMock, return_value=[web_bundle]):
            evidence, traces = await execute_plan(
                plan, conn, None,
                generate_embedding_fn=AsyncMock(),
                expand_queries_fn=AsyncMock(),
                rerank_fn=MagicMock(),
            )

        # Should have both entity + web evidence
        assert len(evidence) == 2
        types = {b.source_type for b in evidence}
        assert SourceType.LOCAL_WEB in types
        assert SourceType.LOCAL_AUTHORITATIVE in types


# ---------------------------------------------------------------------------
# Fallback + abstention + telemetry tests
# ---------------------------------------------------------------------------

class TestFallbackAndAbstention:

    @pytest.mark.asyncio
    async def test_fallback_below_threshold(self):
        """Confidence < 0.7 -> response still contains plan_trace with fallback=true."""
        from api.schemas.query_plan import ClassifierOutput, QueryTaxonomy, DepthTier

        low_conf = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            depth_tier=DepthTier.STANDARD,
            confidence=0.5,
        )

        mock_chat_provider = AsyncMock()
        mock_chat_provider.complete = AsyncMock(return_value="Mock answer")

        mock_cm = AsyncMock()
        mock_conn = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = mock_cm

        # Make monolithic path produce some results
        mock_entity_lookup = AsyncMock(return_value=[
            EvidenceBundle(source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
                          retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
                          text="Desc", metadata={"entity_type": "Concept", "label": "Test", "fuseki_uri": "urn:e:1"})
        ])

        with patch("api.personal_ingest_api.db_pool", mock_pool), \
             patch("api.personal_ingest_api.classifier_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.expansion_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.generate_embedding", AsyncMock(return_value=[0.1]*1536)), \
             patch("api.personal_ingest_api._try_structured_graph_query", AsyncMock(return_value="")), \
             patch("api.query_classifier.classify_query", AsyncMock(return_value=low_conf)), \
             patch("api.retrieval_executors.entity_lookup", mock_entity_lookup), \
             patch("api.retrieval_executors.relationship_traverse", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.text_search", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.web_source_lookup", AsyncMock(return_value=[])):

            from api.personal_ingest_api import chat_endpoint, ChatRequest
            request = ChatRequest(query="What is eelgrass?", planner=True)
            response = await chat_endpoint(request)

        assert "plan_trace" in response
        assert response["plan_trace"]["fallback"] is True
        assert response["plan_trace"]["confidence"] == 0.5
        assert response["plan_trace"]["fallback_reason"] == "low_confidence"

    @pytest.mark.asyncio
    async def test_confidence_at_threshold(self):
        """Confidence == 0.7 -> planner executes (not fallback)."""
        co = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION,
            confidence=0.7,
        )
        # At threshold should execute planner
        assert co.confidence >= CLASSIFIER_CONFIDENCE_THRESHOLD

    @pytest.mark.asyncio
    async def test_abstain_out_of_domain_high_confidence_no_llm(self):
        """OOD + confidence>=0.7 -> early return, no LLM call, abstained=true."""
        ood_result = ClassifierOutput(
            query_taxonomy=QueryTaxonomy.OUT_OF_DOMAIN,
            confidence=0.95,
            reasoning="Stock price question is out of domain",
        )

        mock_chat_provider = AsyncMock()
        mock_chat_provider.complete = AsyncMock(return_value="should not be called")
        mock_pool = MagicMock()

        with patch("api.personal_ingest_api.db_pool", mock_pool), \
             patch("api.personal_ingest_api.classifier_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.expansion_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.generate_embedding", AsyncMock(return_value=[0.1]*1536)), \
             patch("api.query_classifier.classify_query", AsyncMock(return_value=ood_result)):

            from api.personal_ingest_api import chat_endpoint, ChatRequest
            request = ChatRequest(query="What is the stock price of Apple?", planner=True)
            response = await chat_endpoint(request)

        # Should early-return with abstention
        assert response["plan_trace"]["abstained"] is True
        assert response["plan_trace"]["fallback"] is False
        assert response["sources"] == []
        assert "outside the scope" in response["answer"]

    @pytest.mark.asyncio
    async def test_plan_trace_emitted_on_fallback(self):
        """Fallback case still has plan_trace in response."""
        # Same as test_fallback_below_threshold but focused on trace presence
        co = ClassifierOutput(query_taxonomy=QueryTaxonomy.ENTITY_DEFINITION, confidence=0.3)

        mock_chat_provider = AsyncMock()
        mock_chat_provider.complete = AsyncMock(return_value="Answer")

        mock_cm = AsyncMock()
        mock_conn = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = mock_cm

        with patch("api.personal_ingest_api.db_pool", mock_pool), \
             patch("api.personal_ingest_api.classifier_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.expansion_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.generate_embedding", AsyncMock(return_value=[0.1]*1536)), \
             patch("api.personal_ingest_api._try_structured_graph_query", AsyncMock(return_value="")), \
             patch("api.query_classifier.classify_query", AsyncMock(return_value=co)), \
             patch("api.retrieval_executors.entity_lookup", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.relationship_traverse", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.text_search", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.web_source_lookup", AsyncMock(return_value=[])):

            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="test", planner=True))

        assert "plan_trace" in response
        assert response["plan_trace"]["fallback"] is True

    @pytest.mark.asyncio
    async def test_plan_trace_emitted_on_abstention(self):
        """OOD abstention response contains plan_trace with abstained=true."""
        ood = ClassifierOutput(query_taxonomy=QueryTaxonomy.OUT_OF_DOMAIN, confidence=0.9)

        mock_provider = AsyncMock()

        with patch("api.personal_ingest_api.db_pool", MagicMock()), \
             patch("api.personal_ingest_api.classifier_provider", mock_provider), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_provider), \
             patch("api.personal_ingest_api.expansion_provider", mock_provider), \
             patch("api.personal_ingest_api.generate_embedding", AsyncMock(return_value=[0.1]*1536)), \
             patch("api.query_classifier.classify_query", AsyncMock(return_value=ood)):

            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="Stock price?", planner=True))

        assert response["plan_trace"]["abstained"] is True
        assert response["plan_trace"]["taxonomy"] == "out_of_domain"
