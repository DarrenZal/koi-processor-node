"""Behavioral tests for B9a retrieval executors.

Tests the extracted executor functions from api/retrieval_executors.py
using mocked asyncpg connections. These replace the earlier source-text
pattern tests and provide a real safety net for the refactor.
"""

import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.schemas.query_plan import EvidenceBundle, RetrievalOp, SourceType
from api.retrieval_executors import (
    entity_lookup,
    relationship_traverse,
    text_search,
    web_source_lookup,
    evidence_bundles_to_legacy_format,
)


# ---------------------------------------------------------------------------
# Helpers: mock DB row factories
# ---------------------------------------------------------------------------

def _make_entity_row(uri="urn:entity:1", text="Salish Sea", etype="Bioregion",
                     similarity=0.9, metadata=None):
    """Build a mock entity_registry row (dict-like)."""
    return {
        "fuseki_uri": uri,
        "entity_text": text,
        "entity_type": etype,
        "similarity": similarity,
        "metadata": metadata or json.dumps({"description": f"Description of {text}"}),
    }


def _make_rel_row(subj_uri="urn:entity:1", subj_label="Salish Sea",
                  pred="located_in", obj_uri="urn:entity:2",
                  obj_label="Pacific Northwest", depth=1):
    return {
        "subject_uri": subj_uri,
        "subject_label": subj_label,
        "predicate": pred,
        "object_uri": obj_uri,
        "object_label": obj_label,
        "depth": depth,
    }


def _make_chunk_row(chunk_id=1, doc_rid="doc:123", title="Herring Report",
                    chunk_text="The herring population in the Salish Sea...",
                    rrf_score=0.032, context="Marine ecology context",
                    section_id=None, section_title=None, wiki_url=None):
    return {
        "id": chunk_id,
        "document_rid": doc_rid,
        "title": title,
        "chunk_text": chunk_text,
        "chunk_context": context,
        "rrf_score": rrf_score,
        "section_id": section_id,
        "section_title": section_title,
        "wiki_url": wiki_url,
    }


def _make_web_row(url="https://example.com/salish", title="Salish Sea Info",
                  description="Overview of the Salish Sea bioregion."):
    return {
        "url": url,
        "title": title,
        "description": description,
    }


# ---------------------------------------------------------------------------
# entity_lookup tests
# ---------------------------------------------------------------------------

class TestEntityLookup:

    @pytest.mark.asyncio
    async def test_entity_lookup_returns_evidence_bundles(self):
        """Vector search returns properly typed EvidenceBundles."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_entity_row(uri="urn:e:1", text="Eelgrass", etype="Concept", similarity=0.92),
            _make_entity_row(uri="urn:e:2", text="Salish Sea", etype="Bioregion", similarity=0.85),
        ])

        bundles = await entity_lookup(
            "What is eelgrass?",
            [0.1] * 1536,  # mock embedding
            conn,
            max_results=5,
        )

        assert len(bundles) == 2
        assert all(isinstance(b, EvidenceBundle) for b in bundles)
        assert bundles[0].source_uri == "urn:e:1"
        assert bundles[0].source_type == SourceType.LOCAL_AUTHORITATIVE
        assert bundles[0].retrieval_op == RetrievalOp.ENTITY_LOOKUP
        assert bundles[0].confidence == 0.92
        assert bundles[0].metadata["entity_type"] == "Concept"
        assert bundles[0].metadata["label"] == "Eelgrass"
        assert "Description of Eelgrass" in bundles[0].text

    @pytest.mark.asyncio
    async def test_entity_lookup_keyword_fallback(self):
        """When vector search raises UndefinedColumnError, falls back to keyword search."""
        import asyncpg.exceptions

        call_count = 0
        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncpg.exceptions.UndefinedColumnError("embedding column missing")
            return [_make_entity_row(uri="urn:e:fallback", text="Herring", similarity=0.5)]

        conn = AsyncMock()
        conn.fetch = AsyncMock(side_effect=_side_effect)

        bundles = await entity_lookup(
            "What is herring?",
            [0.1] * 1536,
            conn,
            max_results=5,
        )

        assert call_count == 2  # first call failed, second was keyword fallback
        assert len(bundles) == 1
        assert bundles[0].metadata["label"] == "Herring"

    @pytest.mark.asyncio
    async def test_entity_lookup_empty_results(self):
        """No embedding and no keywords -> empty list, no crash."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])

        bundles = await entity_lookup("ab", None, conn, max_results=5)

        assert bundles == []


# ---------------------------------------------------------------------------
# relationship_traverse tests
# ---------------------------------------------------------------------------

class TestRelationshipTraverse:

    @pytest.mark.asyncio
    async def test_relationship_traverse_returns_bundles(self):
        """CTE results become EvidenceBundles with formatted edge text."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_rel_row(subj_uri="urn:e:1", subj_label="Salish Sea",
                         pred="located_in", obj_uri="urn:e:2",
                         obj_label="Pacific Northwest", depth=1),
            _make_rel_row(subj_uri="urn:e:2", subj_label="Pacific Northwest",
                         pred="contains", obj_uri="urn:e:3",
                         obj_label="Fraser River", depth=2),
        ])

        bundles = await relationship_traverse(["urn:e:1"], conn, max_hops=2)

        assert len(bundles) == 2
        assert all(b.retrieval_op == RetrievalOp.RELATIONSHIP_TRAVERSE for b in bundles)
        assert "Salish Sea --[located_in]--> Pacific Northwest" in bundles[0].text
        assert "Pacific Northwest --[contains]--> Fraser River" in bundles[1].text
        # Closer hops have higher confidence
        assert bundles[0].confidence > bundles[1].confidence
        assert bundles[0].metadata["depth"] == 1
        assert bundles[1].metadata["depth"] == 2

    @pytest.mark.asyncio
    async def test_relationship_traverse_no_entities(self):
        """Empty entity_uris -> immediate empty return, no DB call."""
        conn = AsyncMock()
        conn.fetch = AsyncMock()

        bundles = await relationship_traverse([], conn)

        assert bundles == []
        conn.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# text_search tests
# ---------------------------------------------------------------------------

class TestTextSearch:

    @pytest.mark.asyncio
    async def test_text_search_rrf_fusion(self):
        """RRF fusion combines vector + BM25 results correctly."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_chunk_row(chunk_id=1, doc_rid="doc:a", title="Doc A",
                           chunk_text="Content A", rrf_score=0.035),
            _make_chunk_row(chunk_id=2, doc_rid="doc:b", title="Doc B",
                           chunk_text="Content B", rrf_score=0.028),
        ])

        bundles = await text_search(
            "herring population",
            [0.1] * 1536,
            conn,
            top_k=8,
        )

        assert len(bundles) == 2
        assert all(b.source_type == SourceType.LOCAL_DOCUMENT for b in bundles)
        assert all(b.retrieval_op == RetrievalOp.TEXT_SEARCH for b in bundles)
        assert bundles[0].source_uri == "doc:a"
        assert bundles[0].metadata["title"] == "Doc A"

    @pytest.mark.asyncio
    async def test_text_search_with_reranking(self):
        """When rerank_fn provided, it filters to top_k."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_chunk_row(chunk_id=i, doc_rid=f"doc:{i}", title=f"Doc {i}",
                           chunk_text=f"Content {i}", rrf_score=0.03 - i * 0.001)
            for i in range(10)
        ])

        def mock_reranker(query, chunks, top_k=8):
            # Simulate reranker: reverse order, add rerank_score, take top_k
            for i, c in enumerate(chunks[:top_k]):
                c['rerank_score'] = 0.9 - i * 0.05
            return chunks[:top_k]

        bundles = await text_search(
            "herring", [0.1] * 1536, conn,
            top_k=3,
            rerank_fn=mock_reranker,
        )

        # Reranker was called with top_k=3
        assert len(bundles) <= 3

    @pytest.mark.asyncio
    async def test_text_search_bm25_fallback(self):
        """DataError on vector path triggers BM25-only fallback."""
        import asyncpg.exceptions

        call_count = 0
        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncpg.exceptions.DataError("dimension mismatch")
            return [_make_chunk_row(chunk_id=99, doc_rid="doc:bm25",
                                   title="BM25 Result", rrf_score=0.5)]

        conn = AsyncMock()
        conn.fetch = AsyncMock(side_effect=_side_effect)

        bundles = await text_search(
            "herring", [0.1] * 1536, conn, top_k=8,
        )

        assert call_count == 2  # first failed, second was BM25 fallback
        assert len(bundles) == 1
        assert bundles[0].source_uri == "doc:bm25"

    @pytest.mark.asyncio
    async def test_text_search_code_filtering(self):
        """With include_code=False, code_filter SQL fragment is applied."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])

        await text_search("test", [0.1] * 1536, conn, include_code=False)

        # The SQL should contain the code filter
        call_args = conn.fetch.call_args
        sql = call_args[0][0]
        assert "entity_name" in sql  # code filter checks entity_name IS NULL


# ---------------------------------------------------------------------------
# Option B: field-membership scoping (document_field_membership)
# ---------------------------------------------------------------------------

class TestTextSearchFieldScoping:

    @pytest.mark.asyncio
    async def test_fields_none_no_membership_join(self):
        """Default (fields=None) is globally visible: no membership JOIN, and
        only the two base positional params (embedding_str, query_text)."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])

        await text_search("test", [0.1] * 1536, conn, fields=None)

        sql, params = conn.fetch.call_args[0][0], conn.fetch.call_args[0][1:]
        assert "document_field_membership" not in sql
        # main hybrid query binds exactly $1 embedding + $2 query_text
        assert len(params) == 2

    @pytest.mark.asyncio
    async def test_fields_empty_list_no_membership_join(self):
        """An empty list is treated like None (unchanged behavior)."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])

        await text_search("test", [0.1] * 1536, conn, fields=[])

        sql, params = conn.fetch.call_args[0][0], conn.fetch.call_args[0][1:]
        assert "document_field_membership" not in sql
        assert len(params) == 2

    @pytest.mark.asyncio
    async def test_fields_set_adds_membership_join_and_param(self):
        """Non-empty fields scopes via a membership JOIN bound to $3=field list."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_chunk_row(chunk_id=1, doc_rid="doc:sheaf", title="Sheaf",
                            chunk_text="sheaf content", rrf_score=0.04),
        ])

        bundles = await text_search(
            "sheaf", [0.1] * 1536, conn, fields=["sheaf-explorer"],
        )

        sql, params = conn.fetch.call_args[0][0], conn.fetch.call_args[0][1:]
        assert "document_field_membership" in sql
        assert "fm.field_id = ANY($3::text[])" in sql
        # $3 carries the field list
        assert params[2] == ["sheaf-explorer"]
        assert len(bundles) == 1
        assert bundles[0].source_uri == "doc:sheaf"

    @pytest.mark.asyncio
    async def test_fields_set_bm25_fallback_binds_param_2(self):
        """On the BM25-only fallback path query_text is $1, so the membership
        filter must bind to $2 (not $3)."""
        import asyncpg.exceptions

        call_count = 0
        async def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncpg.exceptions.DataError("dimension mismatch")
            return [_make_chunk_row(chunk_id=7, doc_rid="doc:bm25",
                                    title="BM25", rrf_score=0.5)]

        conn = AsyncMock()
        conn.fetch = AsyncMock(side_effect=_side_effect)

        bundles = await text_search(
            "sheaf", [0.1] * 1536, conn, fields=["sheaf-explorer"],
        )

        assert call_count == 2
        # inspect the fallback (2nd) call
        fallback_sql = conn.fetch.call_args_list[1][0][0]
        fallback_params = conn.fetch.call_args_list[1][0][1:]
        assert "document_field_membership" in fallback_sql
        assert "fm.field_id = ANY($2::text[])" in fallback_sql
        assert fallback_params[1] == ["sheaf-explorer"]
        assert len(bundles) == 1
        assert bundles[0].source_uri == "doc:bm25"


# ---------------------------------------------------------------------------
# Option B: ingest membership upsert (additive, idempotent)
# ---------------------------------------------------------------------------

class _FakePKConn:
    """Minimal asyncpg-like conn enforcing PRIMARY KEY (document_rid, field_id)
    with ON CONFLICT DO NOTHING semantics, for the membership upsert."""

    def __init__(self):
        self.rows: set[tuple[str, str]] = set()
        self.execute_sqls: list[str] = []

    async def execute(self, sql, *args):
        self.execute_sqls.append(sql)
        # upsert_field_membership passes (document_rid, field_id)
        document_rid, field_id = args[0], args[1]
        key = (document_rid, field_id)
        if key in self.rows:
            return "INSERT 0 0"  # ON CONFLICT DO NOTHING
        self.rows.add(key)
        return "INSERT 0 1"


class TestFieldMembershipUpsert:

    @pytest.mark.asyncio
    async def test_upsert_idempotent_same_field(self):
        from scripts.ingest_document import upsert_field_membership
        conn = _FakePKConn()
        meta = {"group_id": "sheaf-explorer"}

        f1 = await upsert_field_membership(conn, "document:abc", meta)
        f2 = await upsert_field_membership(conn, "document:abc", meta)

        assert f1 == "sheaf-explorer"
        assert f2 == "sheaf-explorer"
        # second call is a no-op: still exactly one row
        assert conn.rows == {("document:abc", "sheaf-explorer")}
        assert "ON CONFLICT DO NOTHING" in conn.execute_sqls[0]

    @pytest.mark.asyncio
    async def test_reingest_into_second_field_keeps_first(self):
        """A doc re-ingested into field B must KEEP its field-A membership."""
        from scripts.ingest_document import upsert_field_membership
        conn = _FakePKConn()

        await upsert_field_membership(conn, "document:abc", {"group_id": "field-a"})
        await upsert_field_membership(conn, "document:abc", {"group_id": "field-b"})

        assert conn.rows == {
            ("document:abc", "field-a"),
            ("document:abc", "field-b"),
        }

    @pytest.mark.asyncio
    async def test_default_group_id_personal(self):
        from scripts.ingest_document import upsert_field_membership
        conn = _FakePKConn()

        field = await upsert_field_membership(conn, "document:abc", {})

        assert field == "personal"
        assert ("document:abc", "personal") in conn.rows


# ---------------------------------------------------------------------------
# web_source_lookup tests
# ---------------------------------------------------------------------------

class TestWebSourceLookup:

    @pytest.mark.asyncio
    async def test_web_source_lookup_returns_evidence_bundles(self):
        """Web submissions become EvidenceBundles with source_type=LOCAL_WEB."""
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[
            _make_web_row(url="https://example.com/a", title="Page A",
                         description="Description A"),
        ])

        bundles = await web_source_lookup(["urn:e:1"], conn)

        assert len(bundles) == 1
        assert bundles[0].source_type == SourceType.LOCAL_WEB
        assert bundles[0].source_uri == "https://example.com/a"
        assert bundles[0].metadata["title"] == "Page A"
        assert bundles[0].confidence == 0.8

    @pytest.mark.asyncio
    async def test_web_source_lookup_graceful_missing_table(self):
        """UndefinedTableError -> empty list, no crash."""
        import asyncpg.exceptions

        conn = AsyncMock()
        conn.fetch = AsyncMock(
            side_effect=asyncpg.exceptions.UndefinedTableError("web_submissions does not exist")
        )

        bundles = await web_source_lookup(["urn:e:1"], conn)

        assert bundles == []


# ---------------------------------------------------------------------------
# Legacy adapter tests
# ---------------------------------------------------------------------------

class TestLegacyAdapter:

    def test_evidence_bundles_to_legacy_format(self):
        """Flat bundle list partitions correctly into legacy 4-tuple."""
        bundles = [
            EvidenceBundle(
                source_uri="urn:e:1",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.ENTITY_LOOKUP,
                confidence=0.9,
                text="Description of Eelgrass",
                metadata={"entity_type": "Concept", "label": "Eelgrass", "fuseki_uri": "urn:e:1"},
            ),
            EvidenceBundle(
                source_uri="urn:e:1:located_in:urn:e:2",
                source_type=SourceType.LOCAL_AUTHORITATIVE,
                retrieval_op=RetrievalOp.RELATIONSHIP_TRAVERSE,
                confidence=1.0,
                text="Eelgrass --[located_in]--> Salish Sea",
                metadata={"subject_uri": "urn:e:1", "predicate": "located_in", "object_uri": "urn:e:2",
                          "subject_label": "Eelgrass", "object_label": "Salish Sea", "depth": 1},
            ),
            EvidenceBundle(
                source_uri="doc:123",
                source_type=SourceType.LOCAL_DOCUMENT,
                retrieval_op=RetrievalOp.TEXT_SEARCH,
                confidence=0.035,
                text="Eelgrass beds are critical habitat...",
                metadata={"title": "Marine Ecology", "context": "Habitat section",
                          "section_id": "s1", "section_title": "Habitats", "wiki_url": "https://wiki/eelgrass"},
            ),
            EvidenceBundle(
                source_uri="https://example.com/eelgrass",
                source_type=SourceType.LOCAL_WEB,
                retrieval_op=RetrievalOp.ENTITY_LOOKUP,
                confidence=0.8,
                text="Comprehensive eelgrass resource",
                metadata={"url": "https://example.com/eelgrass", "title": "Eelgrass Guide",
                          "summary": "Comprehensive eelgrass resource"},
            ),
        ]

        sources, rels, docs, web = evidence_bundles_to_legacy_format(bundles)

        # Entity sources
        entity_sources = [s for s in sources if s['entity_type'] not in ('Document', 'WebSource')]
        assert len(entity_sources) == 1
        assert entity_sources[0]['label'] == "Eelgrass"
        assert entity_sources[0]['score'] == 0.9

        # Relationships
        assert len(rels) == 1
        assert "Eelgrass --[located_in]--> Salish Sea" in rels[0]

        # Doc chunks
        assert len(docs) == 1
        assert docs[0]['rid'] == "doc:123"
        assert docs[0]['title'] == "Marine Ecology"
        assert docs[0]['wiki_url'] == "https://wiki/eelgrass"

        # Web sources
        assert len(web) == 1
        assert web[0]['url'] == "https://example.com/eelgrass"
        assert web[0]['title'] == "Eelgrass Guide"

        # Web also added to sources
        web_in_sources = [s for s in sources if s['entity_type'] == 'WebSource']
        assert len(web_in_sources) == 1

        # Doc also added to sources
        doc_in_sources = [s for s in sources if s['entity_type'] == 'Document']
        assert len(doc_in_sources) == 1


# ---------------------------------------------------------------------------
# Orchestration test: chat_endpoint wires executors correctly
# ---------------------------------------------------------------------------

class TestChatEndpointOrchestration:
    """Verify that chat_endpoint calls the extracted executors with correct
    args and feeds evidence_bundles_to_legacy_format correctly."""

    @pytest.mark.asyncio
    async def test_chat_endpoint_calls_executors_and_adapter(self):
        """Patch all 4 executors + adapter, verify chat_endpoint calls them
        with expected arguments and produces a response."""
        from api.schemas.query_plan import EvidenceBundle, RetrievalOp, SourceType

        # Build mock return bundles
        entity_bundle = EvidenceBundle(
            source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
            text="Mock entity", metadata={"entity_type": "Concept", "label": "Eelgrass", "fuseki_uri": "urn:e:1"},
        )
        rel_bundle = EvidenceBundle(
            source_uri="urn:e:1:rel:urn:e:2", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.RELATIONSHIP_TRAVERSE, confidence=1.0,
            text="Eelgrass --[located_in]--> Salish Sea",
            metadata={"subject_uri": "urn:e:1", "predicate": "located_in", "object_uri": "urn:e:2",
                      "subject_label": "Eelgrass", "object_label": "Salish Sea", "depth": 1},
        )
        text_bundle = EvidenceBundle(
            source_uri="doc:1", source_type=SourceType.LOCAL_DOCUMENT,
            retrieval_op=RetrievalOp.TEXT_SEARCH, confidence=0.03,
            text="Eelgrass beds...", metadata={"title": "Marine Doc", "context": "", "section_id": None,
                                                "section_title": None, "wiki_url": None},
        )
        web_bundle = EvidenceBundle(
            source_uri="https://example.com", source_type=SourceType.LOCAL_WEB,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.8,
            text="Web desc", metadata={"url": "https://example.com", "title": "Web Page", "summary": "Web desc"},
        )

        mock_entity = AsyncMock(return_value=[entity_bundle])
        mock_rel = AsyncMock(return_value=[rel_bundle])
        mock_text = AsyncMock(return_value=[text_bundle])
        mock_web = AsyncMock(return_value=[web_bundle])

        # Mock the chat answer provider
        mock_chat_provider = AsyncMock()
        mock_chat_provider.complete = AsyncMock(return_value="Mock answer about eelgrass")

        # Mock DB pool with proper async context manager
        mock_conn = AsyncMock()
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = mock_cm

        # Mock generate_embedding
        mock_embed = AsyncMock(return_value=[0.1] * 1536)

        # Mock _try_structured_graph_query
        mock_graph_query = AsyncMock(return_value="")

        with patch("api.personal_ingest_api.db_pool", mock_pool), \
             patch("api.personal_ingest_api.classifier_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.expansion_provider", mock_chat_provider), \
             patch("api.personal_ingest_api.generate_embedding", mock_embed), \
             patch("api.personal_ingest_api._try_structured_graph_query", mock_graph_query), \
             patch("api.retrieval_executors.entity_lookup", mock_entity), \
             patch("api.retrieval_executors.relationship_traverse", mock_rel), \
             patch("api.retrieval_executors.text_search", mock_text), \
             patch("api.retrieval_executors.web_source_lookup", mock_web):

            # Import after patching
            from api.personal_ingest_api import chat_endpoint, ChatRequest

            request = ChatRequest(query="What is eelgrass?", max_context_entities=5)
            response = await chat_endpoint(request)

        # Verify all 4 executors were called
        mock_entity.assert_called_once()
        mock_rel.assert_called_once()
        mock_text.assert_called_once()
        mock_web.assert_called_once()

        # Verify entity_lookup received correct args
        entity_call = mock_entity.call_args
        assert entity_call[0][0] == "What is eelgrass?"  # query
        assert entity_call[1]['max_results'] == 5

        # Verify relationship_traverse received entity URIs from entity_lookup
        rel_call = mock_rel.call_args
        assert rel_call[0][0] == ["urn:e:1"]  # entity_uris from entity_bundle

        # Verify web_source_lookup received same entity URIs
        web_call = mock_web.call_args
        assert web_call[0][0] == ["urn:e:1"]

        # Verify response has expected structure
        assert "answer" in response
        assert "sources" in response
        assert "intent" in response
        assert response["answer"] == "Mock answer about eelgrass"
        assert len(response["sources"]) > 0


# ---------------------------------------------------------------------------
# debug_prompt gating tests
# ---------------------------------------------------------------------------

def _chat_endpoint_patches():
    """Common patches for chat endpoint tests. Returns a context manager stack."""
    from api.schemas.query_plan import EvidenceBundle, RetrievalOp, SourceType

    entity_bundle = EvidenceBundle(
        source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
        retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
        text="Mock entity",
        metadata={"entity_type": "Concept", "label": "Test", "fuseki_uri": "urn:e:1"},
    )

    mock_chat_provider = AsyncMock()
    mock_chat_provider.complete = AsyncMock(return_value="Mock answer")

    mock_cm = AsyncMock()
    mock_conn = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    mock_pool = MagicMock()
    mock_pool.acquire.return_value = mock_cm

    return {
        "api.personal_ingest_api.db_pool": mock_pool,
        "api.personal_ingest_api.classifier_provider": mock_chat_provider,
        "api.personal_ingest_api.chat_answer_provider": mock_chat_provider,
        "api.personal_ingest_api.expansion_provider": mock_chat_provider,
        "api.personal_ingest_api.generate_embedding": AsyncMock(return_value=[0.1] * 1536),
        "api.personal_ingest_api._try_structured_graph_query": AsyncMock(return_value=""),
        "api.retrieval_executors.entity_lookup": AsyncMock(return_value=[entity_bundle]),
        "api.retrieval_executors.relationship_traverse": AsyncMock(return_value=[]),
        "api.retrieval_executors.text_search": AsyncMock(return_value=[]),
        "api.retrieval_executors.web_source_lookup": AsyncMock(return_value=[]),
    }


class TestDebugPromptGating:

    @pytest.mark.asyncio
    async def test_debug_prompt_absent_by_default(self):
        """Default request has no _debug_prompt in response."""
        patches = _chat_endpoint_patches()
        import contextlib
        with contextlib.ExitStack() as stack:
            for target, mock_obj in patches.items():
                stack.enter_context(patch(target, mock_obj))
            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="test"))
        assert "_debug_prompt" not in response

    @pytest.mark.asyncio
    async def test_debug_prompt_gated_by_env(self):
        """debug_prompt=True but no CHAT_DEBUG_PROMPT env -> no _debug_prompt."""
        patches = _chat_endpoint_patches()
        import contextlib
        with contextlib.ExitStack() as stack:
            for target, mock_obj in patches.items():
                stack.enter_context(patch(target, mock_obj))
            stack.enter_context(patch.dict("os.environ", {}, clear=False))
            # Ensure CHAT_DEBUG_PROMPT is not set
            import os
            os.environ.pop("CHAT_DEBUG_PROMPT", None)
            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="test", debug_prompt=True))
        assert "_debug_prompt" not in response

    @pytest.mark.asyncio
    async def test_debug_prompt_present_when_gated(self):
        """debug_prompt=True + CHAT_DEBUG_PROMPT=1 -> _debug_prompt with prompts."""
        patches = _chat_endpoint_patches()
        import contextlib
        with contextlib.ExitStack() as stack:
            for target, mock_obj in patches.items():
                stack.enter_context(patch(target, mock_obj))
            stack.enter_context(patch.dict("os.environ", {"CHAT_DEBUG_PROMPT": "1"}, clear=False))
            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="test", debug_prompt=True))
        assert "_debug_prompt" in response
        assert "system_prompt" in response["_debug_prompt"]
        assert "user_prompt" in response["_debug_prompt"]
        assert len(response["_debug_prompt"]["system_prompt"]) > 0
        assert len(response["_debug_prompt"]["user_prompt"]) > 0


# ===========================================================================
# Piece C — source-link surfacing (derive_source_url) + Piece B (--fields)
# (added by the ingestion-governor / retrieval-source-link plan, 2026-06-27)
# ===========================================================================

from api.routers.knowledge_router import (  # noqa: E402
    derive_source_url,
    create_router,
)
from scripts.ingest_document import (  # noqa: E402
    effective_field_membership,
    upsert_field_membership,
)


class TestDeriveSourceUrl:
    """AC4 precedence: document-rid url_map > http(s) in metadata > bare arXiv id > None.
    Never fabricates; non-document source_node_rids skip url_map entirely."""

    def test_document_rid_uses_url_map_over_conflicting_metadata(self):
        url_map = {"document:abc": "https://arxiv.org/abs/2005.12798"}
        meta = {"source_url": "https://wrong.example/other"}
        # url_map (authoritative) wins for a document: rid.
        assert derive_source_url("document:abc", "sources/x.md", meta, url_map) \
            == "https://arxiv.org/abs/2005.12798"

    def test_http_in_metadata_when_no_url_map_hit(self):
        meta = {"source_url": "https://golem.ph.utexas.edu/x.html"}
        assert derive_source_url("document:missing", "sources/x.md", meta, {}) \
            == "https://golem.ph.utexas.edu/x.html"

    def test_http_scanned_from_arbitrary_metadata_value(self):
        meta = {"note": "see https://example.org/paper for details"}
        assert derive_source_url(None, None, meta, None) == "https://example.org/paper"

    def test_bare_arxiv_id_extracted(self):
        assert derive_source_url(None, "arXiv:2605.15778v1 preprint", {}, None) \
            == "https://arxiv.org/abs/2605.15778v1"

    def test_non_document_rid_skips_url_map_returns_null(self):
        # A session-sourced fact: never looked up in url_map, no fabrication.
        url_map = {"document:abc": "https://arxiv.org/abs/1"}
        assert derive_source_url("claude-session:123", "session-123", {}, url_map) is None

    def test_all_null_returns_none(self):
        assert derive_source_url(None, None, {}, None) is None
        assert derive_source_url("substack-corpus:42", None, {}, {}) is None


class TestEffectiveFieldMembership:
    """AC3: dedup(union([group_id] + fields)); group_id always primary/first."""

    def test_no_fields_is_single_membership(self):
        assert effective_field_membership("sheaf-explorer", None) == ["sheaf-explorer"]
        assert effective_field_membership("sheaf-explorer", []) == ["sheaf-explorer"]

    def test_union_dedup_keeps_group_id_first(self):
        assert effective_field_membership(
            "X", ["X", "Y", "Z"]) == ["X", "Y", "Z"]
        assert effective_field_membership(
            "personal", ["spore", "spore", "personal", "sheaf"]) \
            == ["personal", "spore", "sheaf"]

    def test_empty_tokens_filtered(self):
        assert effective_field_membership("X", ["", "  ", "Y"]) == ["X", "Y"]


class TestUpsertFieldMembership:
    """The additive INSERT loop runs once per effective field (idempotent)."""

    @pytest.mark.asyncio
    async def test_multi_field_inserts_union(self):
        conn = AsyncMock()
        conn.execute = AsyncMock()
        # Returns the PRIMARY (group_id) str; inserts the full deduped union.
        primary = await upsert_field_membership(
            conn, "document:rid1",
            {"group_id": "sheaf-explorer", "fields": ["spore", "sheaf-explorer", "bkc"]})
        assert primary == "sheaf-explorer"
        inserted = [call.args[2] for call in conn.execute.call_args_list]
        assert inserted == ["sheaf-explorer", "spore", "bkc"]

    @pytest.mark.asyncio
    async def test_no_fields_single_insert(self):
        conn = AsyncMock()
        conn.execute = AsyncMock()
        primary = await upsert_field_membership(
            conn, "document:rid2", {"group_id": "personal"})
        assert primary == "personal"
        assert conn.execute.call_count == 1


# --- Endpoint-shaping: search_facts + unified_search emit AC4 fields ---------

def _route(router, name):
    for r in router.routes:
        if getattr(r, "name", None) == name:
            return r.endpoint
    raise KeyError(name)


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


def _fake_request():
    req = MagicMock()
    req.app.state.facts_surface_available = True
    return req


async def _embed(_q):
    return [0.1] * 16


class _FactsConn:
    def __init__(self, fact_rows, url_rows):
        self._facts = fact_rows
        self._urls = url_rows

    async def fetch(self, sql, *a):
        if "document_ingestion_log" in sql:
            return self._urls
        if "knowledge_facts" in sql:
            return self._facts
        return []

    async def fetchval(self, sql, *a):
        return "EntityName"


class TestSearchFactsSourceLinks:

    @pytest.mark.asyncio
    async def test_search_facts_emits_source_document_and_source_url(self):
        fact_rows = [
            {"id": "1", "episode_id": "e1", "episode_name": "Ep1",
             "subject_uri": "urn:s", "predicate": "p", "object_uri": "urn:o",
             "object_literal": None, "fact_text": "f1",
             "valid_from": None, "valid_to": None, "created_at": None,
             "source_node_rid": "document:abc", "source_document": "sources/x.md",
             "ep_metadata": {}, "similarity": 0.9},
            {"id": "2", "episode_id": "e2", "episode_name": "Ep2",
             "subject_uri": "urn:s2", "predicate": "p", "object_uri": None,
             "object_literal": "lit", "fact_text": "f2",
             "valid_from": None, "valid_to": None, "created_at": None,
             "source_node_rid": "claude-session:9", "source_document": "session-9",
             "ep_metadata": {}, "similarity": 0.8},
        ]
        url_rows = [{"document_rid": "document:abc",
                     "source_url": "https://arxiv.org/abs/2005.12798"}]
        router = create_router(_FakePool(_FactsConn(fact_rows, url_rows)),
                               generate_embedding=_embed)
        handler = _route(router, "search_facts")
        out = await handler(_fake_request(), query="sheaf", limit=10,
                            group_id=None, include_expired=False)
        facts = out["facts"]
        # AC4: both keys always present.
        for f in facts:
            assert "source_document" in f and "source_url" in f
        # Document-sourced fact derives a real URL; session-sourced is null.
        assert facts[0]["source_document"] == "sources/x.md"
        assert facts[0]["source_url"] == "https://arxiv.org/abs/2005.12798"
        assert facts[1]["source_url"] is None
        assert facts[1]["source_document"] == "session-9"


class _UnifiedConn:
    def __init__(self, entity_rows, fact_rows, url_rows):
        self._entities = entity_rows
        self._facts = fact_rows
        self._urls = url_rows

    async def execute(self, sql, *a):
        return None  # SET ivfflat.probes

    async def fetch(self, sql, *a):
        if "document_ingestion_log" in sql:
            return self._urls
        # The facts query joins entity_registry for the subject's closure, so it is
        # matched first.
        if "knowledge_facts" in sql:
            return self._facts
        if "entity_registry" in sql:
            return self._entities
        return []

    async def fetchval(self, sql, *a):
        return False  # session_chunks table_exists → skip sessions surface


from api.read_scope import personal_scope  # noqa: E402


class TestUnifiedSearchSourceLinks:

    @pytest.mark.asyncio
    async def test_unified_search_entities_and_facts_carry_links(self):
        from fastapi import Response
        entity_rows = [
            {"fuseki_uri": "urn:e1", "entity_text": "Sheaf Theory",
             "entity_type": "Concept", "vault_path": "Concepts/Sheaf Theory.md", "closure": None,
             "score": 0.91},
        ]
        fact_rows = [
            {"id": "1", "subject_uri": "urn:s", "predicate": "p",
             "object_uri": "urn:o", "fact_text": "f1",
             "source_node_rid": "document:abc", "episode_name": "Ep1",
             "source_document": "sources/x.md", "ep_metadata": {}, "subject_closure": None,
             "score": 0.88},
        ]
        url_rows = [{"document_rid": "document:abc",
                     "source_url": "https://arxiv.org/abs/2005.12798"}]
        router = create_router(_FakePool(_UnifiedConn(entity_rows, fact_rows, url_rows)),
                               generate_embedding=_embed)
        handler = _route(router, "unified_search")
        out = await handler(_fake_request(), Response(), query="sheaf",
                            limit=10, include="entities,facts", doc_kind=None,
                            status=None, is_governed=None, repo=None,
                            rerank="rrf", mmr_lambda=0.5, scope=personal_scope())
        ents = [r for r in out["results"] if r["source"] == "entity"]
        facts = out["facts"]
        # AC4: entity items gain vault_path + quartz_url (str|null), keys present.
        assert ents and "vault_path" in ents[0] and "quartz_url" in ents[0]
        assert ents[0]["vault_path"] == "Concepts/Sheaf Theory.md"
        # AC4: fact items gain source_document + source_url.
        assert facts and facts[0]["source_document"] == "sources/x.md"
        assert facts[0]["source_url"] == "https://arxiv.org/abs/2005.12798"

    @pytest.mark.asyncio
    async def test_recall_inherits_unified_search_fields(self):
        """`recall` forwards the unified-search response verbatim, so asserting the
        unified-search output shape IS the recall-inheritance guarantee."""
        from fastapi import Response
        entity_rows = [
            {"fuseki_uri": "urn:e1", "entity_text": "X", "entity_type": "Concept",
             "vault_path": None, "closure": None, "score": 0.5},
        ]
        router = create_router(_FakePool(_UnifiedConn(entity_rows, [], [])),
                               generate_embedding=_embed)
        handler = _route(router, "unified_search")
        out = await handler(_fake_request(), Response(), query="x",
                            limit=5, include="entities", doc_kind=None,
                            status=None, is_governed=None, repo=None,
                            rerank="rrf", mmr_lambda=0.5, scope=personal_scope())
        ents = [r for r in out["results"] if r["source"] == "entity"]
        assert ents and "vault_path" in ents[0] and "quartz_url" in ents[0]
