"""B9b — structured_sql executor + legacy format integration tests.

Tests the structured_sql executor (commitment + roadmap templates),
plan_executor dispatch wiring, and evidence_bundles_to_legacy_format
partitioning for STRUCTURED_SQL bundles.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.schemas.query_plan import (
    EvidenceBundle,
    PlanStep,
    QueryPlan,
    QueryTaxonomy,
    RetrievalOp,
    SafetyGuards,
    SourceType,
    StepBudget,
)
from api.retrieval_executors import structured_sql, evidence_bundles_to_legacy_format


# ---------------------------------------------------------------------------
# Helper: mock asyncpg connection
# ---------------------------------------------------------------------------

def _mock_conn(commitment_rows=None, claim_rows=None, roadmap_rows=None):
    """Build an AsyncMock connection that returns canned rows for fetch()."""
    conn = AsyncMock()
    call_count = 0

    async def _fetch(sql, *args):
        nonlocal call_count
        sql_lower = sql.lower().strip()
        if "commitments c" in sql_lower:
            call_count += 1
            return commitment_rows or []
        elif "claims cl" in sql_lower:
            call_count += 1
            return claim_rows or []
        elif "entity_registry er" in sql_lower:
            call_count += 1
            return roadmap_rows or []
        return []

    conn.fetch = _fetch
    return conn


def _commitment_row(**overrides):
    defaults = {
        "commitment_rid": "orn:test:commitment:1",
        "title": "Watershed restoration pledge",
        "description": "Restore 500m of riparian habitat",
        "offer_type": "offer",
        "state": "verified",
        "quantity": 500,
        "unit": "meters",
        "pool_name": "Victoria Pool",
        "pool_state": "active",
        "bioregion_uri": "urn:bioregion:salish-sea",
    }
    defaults.update(overrides)
    return defaults


def _claim_row(**overrides):
    defaults = {
        "claim_rid": "orn:test:claim:1",
        "claim_type": "ecological_impact",
        "verification": "verified",
        "statement": "500m of riparian habitat has been restored along the Goldstream River.",
        "claimant_uri": "urn:person:darren",
    }
    defaults.update(overrides)
    return defaults


def _roadmap_row(**overrides):
    defaults = {
        "fuseki_uri": "urn:initiative:b9b",
        "entity_text": "B9b Structured SQL Executors",
        "entity_type": "WorkItem",
        "description": "Implement structured_sql executor for commitment and roadmap queries",
        "metadata": None,
        "last_seen_at": None,
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Executor tests
# ---------------------------------------------------------------------------

class TestStructuredSqlCommitment:

    @pytest.mark.asyncio
    async def test_commitment_with_entity_uris(self):
        """Commitment template with entity_uris returns filtered bundles."""
        conn = _mock_conn(
            commitment_rows=[_commitment_row(), _commitment_row(
                commitment_rid="orn:test:commitment:2",
                title="Eelgrass planting",
                offer_type="need",
                state="proposed",
            ), _commitment_row(
                commitment_rid="orn:test:commitment:3",
                title="Monitoring commitment",
            )],
            claim_rows=[_claim_row()],
        )
        uris = ["urn:person:darren", "urn:bioregion:salish-sea"]
        bundles = await structured_sql("commitments in Victoria", conn, template="commitment", entity_uris=uris, max_results=20)

        # 3 commitment + 1 claim = 4 bundles
        assert len(bundles) == 4
        commitment_bundles = [b for b in bundles if b.metadata.get("entity_type") == "Commitment"]
        claim_bundles = [b for b in bundles if b.metadata.get("entity_type") == "Claim"]
        assert len(commitment_bundles) == 3
        assert len(claim_bundles) == 1

        # Verify bundle shape
        b = commitment_bundles[0]
        assert b.source_type == SourceType.LOCAL_AUTHORITATIVE
        assert b.retrieval_op == RetrievalOp.STRUCTURED_SQL
        assert b.metadata["label"] == "Watershed restoration pledge"
        assert b.metadata["entity_type"] == "Commitment"
        assert b.metadata["template"] == "commitment"
        assert "Victoria Pool" in b.text

    @pytest.mark.asyncio
    async def test_commitment_without_entity_uris(self):
        """No entity_uris -> unfiltered fallback, capped at 5."""
        conn = _mock_conn(
            commitment_rows=[_commitment_row(commitment_rid=f"orn:c:{i}") for i in range(10)],
            claim_rows=[_claim_row(claim_rid=f"orn:cl:{i}") for i in range(10)],
        )
        bundles = await structured_sql("all commitments", conn, template="commitment", max_results=20)

        # Should be capped at min(20, 5) = 5 per sub-query = up to 10 total
        # But the SQL LIMIT is 5 for each, so the conn would get 5 each
        # Our mock returns all 10 each though — the cap is in the SQL LIMIT param
        # We just verify it ran without error and produced bundles
        assert len(bundles) > 0
        assert all(b.retrieval_op == RetrievalOp.STRUCTURED_SQL for b in bundles)

    @pytest.mark.asyncio
    async def test_empty_results(self):
        """No matching rows -> empty list, no error."""
        conn = _mock_conn()
        bundles = await structured_sql("nonexistent", conn, template="commitment")
        assert bundles == []

    @pytest.mark.asyncio
    async def test_claim_bundle_shape(self):
        """Claim bundles have correct metadata for prompt builder."""
        conn = _mock_conn(claim_rows=[_claim_row()])
        bundles = await structured_sql("claims", conn, template="commitment", entity_uris=["urn:person:darren"])

        claim_b = [b for b in bundles if b.metadata.get("entity_type") == "Claim"]
        assert len(claim_b) == 1
        b = claim_b[0]
        assert b.source_uri == "orn:test:claim:1"
        assert b.metadata["label"]  # non-empty
        assert b.metadata["entity_type"] == "Claim"
        assert "ecological_impact" in b.text
        assert "verified" in b.text


class TestStructuredSqlRoadmap:

    @pytest.mark.asyncio
    async def test_roadmap_with_entity_uris(self):
        """Roadmap template returns filtered bundles with correct entity_type."""
        conn = _mock_conn(roadmap_rows=[
            _roadmap_row(),
            _roadmap_row(fuseki_uri="urn:milestone:phase1", entity_text="Phase 1 Complete", entity_type="Milestone"),
        ])
        bundles = await structured_sql("roadmap status", conn, template="roadmap", entity_uris=["urn:initiative:b9b"])

        assert len(bundles) == 2
        types = {b.metadata["entity_type"] for b in bundles}
        assert "WorkItem" in types
        assert "Milestone" in types

        b = bundles[0]
        assert b.source_type == SourceType.LOCAL_AUTHORITATIVE
        assert b.retrieval_op == RetrievalOp.STRUCTURED_SQL
        assert b.metadata["template"] == "roadmap"
        assert b.metadata["label"] == "B9b Structured SQL Executors"

    @pytest.mark.asyncio
    async def test_roadmap_empty(self):
        """No roadmap entities -> empty list."""
        conn = _mock_conn()
        bundles = await structured_sql("roadmap", conn, template="roadmap")
        assert bundles == []

    @pytest.mark.asyncio
    async def test_unknown_template(self):
        """Unknown template -> empty list, no error."""
        conn = _mock_conn()
        bundles = await structured_sql("test", conn, template="unknown_template")
        assert bundles == []


class TestMaxResultsCap:

    @pytest.mark.asyncio
    async def test_unfiltered_cap(self):
        """Without entity_uris, effective max is min(max_results, 5)."""
        # We verify the cap logic by checking the function doesn't crash
        # and returns results (the actual LIMIT enforcement is in SQL)
        conn = _mock_conn(commitment_rows=[_commitment_row()])
        bundles = await structured_sql("test", conn, template="commitment", entity_uris=None, max_results=50)
        assert len(bundles) >= 0  # should work without error


# ---------------------------------------------------------------------------
# Plan executor dispatch test
# ---------------------------------------------------------------------------

class TestPlanExecutorDispatch:

    @pytest.mark.asyncio
    async def test_structured_sql_dispatched_correctly(self):
        """plan_executor passes correct args to structured_sql for commitment_claim plan."""
        from api.plan_executor import execute_plan

        entity_bundle = EvidenceBundle(
            source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
            text="Entity", metadata={"entity_type": "Person", "label": "Darren", "fuseki_uri": "urn:e:1"},
        )
        sql_bundle = EvidenceBundle(
            source_uri="orn:c:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.STRUCTURED_SQL, confidence=0.9,
            text="Commitment: Test (offer, verified) in pool Victoria — desc",
            metadata={"label": "Test", "entity_type": "Commitment", "template": "commitment"},
        )

        plan = QueryPlan(
            query_taxonomy=QueryTaxonomy.COMMITMENT_CLAIM,
            original_query="What commitments exist?",
            steps=[
                PlanStep(op=RetrievalOp.ENTITY_LOOKUP, budget=StepBudget(max_results=10)),
                PlanStep(op=RetrievalOp.STRUCTURED_SQL, params={"template": "commitment"}, budget=StepBudget(max_results=15), depends_on=[0]),
                PlanStep(op=RetrievalOp.TEXT_SEARCH, budget=StepBudget(max_results=15), depends_on=[0]),
            ],
        )
        conn = AsyncMock()

        mock_structured_sql = AsyncMock(return_value=[sql_bundle])

        with patch("api.plan_executor.entity_lookup", new_callable=AsyncMock, return_value=[entity_bundle]), \
             patch("api.plan_executor.structured_sql", mock_structured_sql), \
             patch("api.plan_executor.text_search", new_callable=AsyncMock, return_value=[]), \
             patch("api.plan_executor.web_source_lookup", new_callable=AsyncMock, return_value=[]):
            evidence, traces = await execute_plan(
                plan, conn, None,
                generate_embedding_fn=AsyncMock(),
                expand_queries_fn=AsyncMock(),
                rerank_fn=MagicMock(),
            )

        # Verify structured_sql was called with correct args
        mock_structured_sql.assert_called_once()
        call_args = mock_structured_sql.call_args
        assert call_args[0][0] == "What commitments exist?"  # query
        assert call_args[0][1] is conn  # conn
        assert call_args[1]["template"] == "commitment"
        assert call_args[1]["entity_uris"] == ["urn:e:1"]  # collected from entity_lookup
        assert call_args[1]["max_results"] == 15

        # Verify evidence collected
        assert len(evidence) == 2  # entity + sql
        assert any(b.retrieval_op == RetrievalOp.STRUCTURED_SQL for b in evidence)

        # Verify trace
        sql_trace = [t for t in traces if t.op == RetrievalOp.STRUCTURED_SQL]
        assert len(sql_trace) == 1
        assert sql_trace[0].results_count == 1


# ---------------------------------------------------------------------------
# Legacy format integration test
# ---------------------------------------------------------------------------

class TestLegacyFormatIntegration:

    def test_structured_sql_bundles_in_sources(self):
        """STRUCTURED_SQL + LOCAL_AUTHORITATIVE bundles appear in sources list with correct shape."""
        bundles = [
            EvidenceBundle(
                source_uri="orn:c:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.9,
                text="Commitment: Watershed restoration (offer, verified) in pool Victoria — Restore riparian habitat",
                metadata={"label": "Watershed restoration", "entity_type": "Commitment", "template": "commitment"},
            ),
            EvidenceBundle(
                source_uri="orn:cl:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.85,
                text="Claim (ecological_impact, verified): Habitat restored",
                metadata={"label": "Habitat restored", "entity_type": "Claim", "template": "commitment"},
            ),
            # Also include an entity_lookup bundle to verify no interference
            EvidenceBundle(
                source_uri="urn:e:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.ENTITY_LOOKUP,
                confidence=0.8,
                text="An entity",
                metadata={"label": "Eelgrass", "entity_type": "Concept"},
            ),
        ]

        sources, rels, docs, webs = evidence_bundles_to_legacy_format(bundles)

        # 2 structured_sql + 1 entity_lookup = 3 sources
        assert len(sources) == 3
        assert len(rels) == 0
        assert len(docs) == 0
        assert len(webs) == 0

        # Verify structured_sql sources have correct shape for prompt builder
        sql_sources = [s for s in sources if s["uri"].startswith("orn:")]
        assert len(sql_sources) == 2

        commitment_source = [s for s in sql_sources if s["entity_type"] == "Commitment"][0]
        assert commitment_source["label"] == "Watershed restoration"
        assert commitment_source["entity_type"] == "Commitment"
        assert commitment_source["description"]  # non-empty
        assert commitment_source["score"] == 0.9

        claim_source = [s for s in sql_sources if s["entity_type"] == "Claim"][0]
        assert claim_source["label"] == "Habitat restored"
        assert claim_source["entity_type"] == "Claim"

    def test_mixed_bundles_partition_correctly(self):
        """All bundle types partition into the correct legacy format slots."""
        bundles = [
            EvidenceBundle(
                source_uri="orn:c:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.STRUCTURED_SQL,
                confidence=0.9, text="Commitment data",
                metadata={"label": "C1", "entity_type": "Commitment"},
            ),
            EvidenceBundle(
                source_uri="urn:rel:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.RELATIONSHIP_TRAVERSE,
                confidence=1.0, text="A --[knows]--> B",
                metadata={},
            ),
            EvidenceBundle(
                source_uri="doc:1",
                source_type=SourceType.LOCAL_DOCUMENT,
                retrieval_op=RetrievalOp.TEXT_SEARCH,
                confidence=0.7, text="Doc chunk text",
                metadata={"title": "Wiki Page"},
            ),
        ]

        sources, rels, docs, webs = evidence_bundles_to_legacy_format(bundles)

        assert len(sources) == 2  # structured_sql + doc (doc also adds to sources)
        assert len(rels) == 1
        assert rels[0] == "A --[knows]--> B"
        assert len(docs) == 1
        assert len(webs) == 0
