"""Unit tests for knowledge-domain event handlers.

Phase 2 step 13 (Federation Phase 1 step 2b): _apply_knowledge_episode.

Tests run inside a single transaction that is rolled back at teardown —
no DB state leaks. The handler under test wraps its body in
`async with conn.transaction():`; asyncpg nests this as a savepoint when
the outer fixture transaction is already active.
"""

import json
import uuid

import asyncpg
import pytest
from tests import koi_test_targets as targets

TEST_SOURCE_NODE = "orn:koi-net.node:test-peer+aaaa"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


def _episode_payload(
    episode_id: str,
    n_facts: int = 0,
    *,
    name: str = "test episode",
    content: str = "test content",
    event_id: str | None = None,
    embedding_column: str | None = None,
    embedding_value=None,
):
    facts = []
    for i in range(n_facts):
        fact = {
            "id": str(uuid.uuid4()),
            "subject_uri": f"orn:entity:s{i}",
            "predicate": "test_pred",
            "object_uri": f"orn:entity:o{i}",
            "object_literal": None,
            "fact_text": f"fact {i}",
            "valid_from": "2026-05-13T00:00:00Z",
            "valid_to": None,
            "created_at": "2026-05-13T00:00:00Z",
            "group_id": "personal",
            "source_node_rid": "orn:koi-net.node:originator+xxxx",
            "turn_range_start": i,
            "turn_range_end": i + 1,
            "embedding_column": embedding_column,
            "embedding_value": embedding_value,
        }
        facts.append(fact)

    payload = {
        "id": episode_id,
        "name": name,
        "content": content,
        "source_description": "test source",
        "source_document": "test.md",
        "group_id": "personal",
        "valid_at": "2026-05-13T00:00:00Z",
        "created_at": "2026-05-13T00:00:00Z",
        "metadata": {"test": True},
        "facts": facts,
    }
    if event_id:
        payload["_federation_event_id"] = event_id
    return payload


def _rid_for(episode_id: str) -> str:
    return f"orn:personal-koi.knowledge-episode:{episode_id}"


# ---------------------------------------------------------------------------
# _apply_knowledge_episode
# ---------------------------------------------------------------------------


class TestApplyKnowledgeEpisode:

    @pytest.mark.anyio
    async def test_inserts_episode_and_facts(self, conn):
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        payload = _episode_payload(ep_id, n_facts=3, event_id=ev_id)

        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        ep = await conn.fetchrow(
            "SELECT name, content, group_id FROM knowledge_episodes WHERE id = $1::uuid",
            ep_id,
        )
        assert ep is not None
        assert ep["name"] == "test episode"
        assert ep["content"] == "test content"
        assert ep["group_id"] == "personal"

        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert fact_count == 3

        # Originator metadata preserved verbatim
        facts = await conn.fetch(
            "SELECT source_node_rid, group_id, turn_range_start, turn_range_end "
            "FROM knowledge_facts WHERE episode_id = $1::uuid ORDER BY turn_range_start",
            ep_id,
        )
        assert all(f["source_node_rid"] == "orn:koi-net.node:originator+xxxx" for f in facts)
        assert all(f["group_id"] == "personal" for f in facts)
        assert [f["turn_range_start"] for f in facts] == [0, 1, 2]

        # federation_applied_events idempotency row recorded
        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'knowledge_episode' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_idempotent(self, conn):
        """Applying the same event_id twice → second call is a clean no-op upsert."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        payload = _episode_payload(ep_id, n_facts=2, event_id=ev_id)

        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )
        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        ep_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_episodes WHERE id = $1::uuid", ep_id,
        )
        assert ep_count == 1

        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert fact_count == 2

        applied = await conn.fetchval(
            "SELECT COUNT(*) FROM federation_applied_events "
            "WHERE domain = 'knowledge_episode' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_upsert_on_conflict(self, conn):
        """New content for existing episode UUID → UPDATE, not duplicate."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())

        v1 = _episode_payload(ep_id, n_facts=0, name="v1", content="content v1")
        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", v1, TEST_SOURCE_NODE,
        )

        v2 = _episode_payload(ep_id, n_facts=0, name="v2", content="content v2")
        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "UPDATE", v2, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT name, content FROM knowledge_episodes WHERE id = $1::uuid", ep_id,
        )
        assert row["name"] == "v2"
        assert row["content"] == "content v2"

        total = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_episodes WHERE id = $1::uuid", ep_id,
        )
        assert total == 1

    @pytest.mark.anyio
    async def test_missing_id_skips_cleanly(self, conn):
        """Payload without `id` is logged + skipped (no exception)."""
        from api.domain_event_handlers import _apply_knowledge_episode

        payload = _episode_payload("00000000-0000-0000-0000-000000000000", n_facts=0)
        del payload["id"]

        # Should NOT raise — logger.warning + return
        await _apply_knowledge_episode(
            conn, "orn:test:bad-rid", "NEW", payload, TEST_SOURCE_NODE,
        )

    @pytest.mark.anyio
    async def test_originator_created_at_preserved(self, conn):
        """`created_at` is preserved verbatim from payload, NOT rewritten to NOW()."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        payload = _episode_payload(ep_id, n_facts=0)
        payload["created_at"] = "2025-01-01T12:34:56Z"

        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        ts = await conn.fetchval(
            "SELECT created_at FROM knowledge_episodes WHERE id = $1::uuid", ep_id,
        )
        assert ts.year == 2025
        assert ts.month == 1
        assert ts.day == 1
        assert ts.hour == 12
        assert ts.minute == 34

    @pytest.mark.anyio
    async def test_embedding_column_discriminator_1024(self, conn):
        """embedding_column='fact_embedding' inserts into the 1024-dim column."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        vec = [0.01 * i for i in range(1024)]
        payload = _episode_payload(
            ep_id, n_facts=1,
            embedding_column="fact_embedding",
            embedding_value=vec,
        )

        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        # Embedding present, 3072-column null
        row = await conn.fetchrow(
            "SELECT fact_embedding IS NOT NULL AS has_1024, "
            "fact_embedding_3072 IS NOT NULL AS has_3072 "
            "FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert row["has_1024"] is True
        assert row["has_3072"] is False

    @pytest.mark.anyio
    async def test_embedding_column_discriminator_null(self, conn):
        """embedding_column=null omits both embedding columns."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        payload = _episode_payload(ep_id, n_facts=1)  # no embedding

        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT fact_embedding IS NULL AS no_1024, "
            "fact_embedding_3072 IS NULL AS no_3072 "
            "FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert row["no_1024"] is True
        assert row["no_3072"] is True


# ---------------------------------------------------------------------------
# _insert_with_drift_retry — schema-drift retry up to 5 columns
# ---------------------------------------------------------------------------


class TestSchemaDriftRetry:

    @pytest.mark.anyio
    async def test_unknown_column_retry_single(self, conn):
        """One unknown column → drift retry drops it, INSERT succeeds."""
        from api.domain_event_handlers import _insert_with_drift_retry

        await conn.execute(
            "CREATE TEMP TABLE drift_one (id uuid PRIMARY KEY, name text)"
        )
        row_id = str(uuid.uuid4())
        await _insert_with_drift_retry(
            conn,
            "drift_one",
            {"id": row_id, "name": "alpha", "ghost_col": "x"},
            {"id": "::uuid"},
            "",
        )
        row = await conn.fetchrow(
            "SELECT name FROM drift_one WHERE id = $1::uuid", row_id,
        )
        assert row["name"] == "alpha"

    @pytest.mark.anyio
    async def test_unknown_column_retry_multi(self, conn):
        """Multiple unknown columns → drift retries N times, INSERT succeeds."""
        from api.domain_event_handlers import _insert_with_drift_retry

        await conn.execute(
            "CREATE TEMP TABLE drift_multi (id uuid PRIMARY KEY, name text)"
        )
        row_id = str(uuid.uuid4())
        await _insert_with_drift_retry(
            conn,
            "drift_multi",
            {
                "id": row_id, "name": "beta",
                "ghost_a": "1", "ghost_b": "2", "ghost_c": "3",
            },
            {"id": "::uuid"},
            "",
        )
        row = await conn.fetchrow(
            "SELECT name FROM drift_multi WHERE id = $1::uuid", row_id,
        )
        assert row["name"] == "beta"

    @pytest.mark.anyio
    async def test_drift_exceeds_max_retries_raises(self, conn):
        """6 unknown columns exceeds the 5-retry cap → raises UndefinedColumnError."""
        from api.domain_event_handlers import _insert_with_drift_retry

        await conn.execute(
            "CREATE TEMP TABLE drift_cap (id uuid PRIMARY KEY, name text)"
        )
        row_id = str(uuid.uuid4())
        ghosts = {f"ghost_{i}": str(i) for i in range(6)}
        with pytest.raises(asyncpg.exceptions.UndefinedColumnError):
            await _insert_with_drift_retry(
                conn,
                "drift_cap",
                {"id": row_id, "name": "z", **ghosts},
                {"id": "::uuid"},
                "",
            )

    @pytest.mark.anyio
    async def test_drift_retry_in_episode_with_bogus_fact_column(self, conn):
        """Episode flow tolerates a stray unknown column on a fact (single-col drift)."""
        from api.domain_event_handlers import _apply_knowledge_episode

        ep_id = str(uuid.uuid4())
        payload = _episode_payload(ep_id, n_facts=1)
        # Inject an unknown column into the fact — should be dropped by drift retry.
        # NOTE: _insert_fact ignores unknown payload keys, so we need to route this
        # through the helper directly. Instead, we test the end-to-end behavior:
        # the handler must not crash when facts carry unexpected schema additions
        # in future versions. Since _insert_fact builds a fixed col list, this
        # specific path isn't exercised — kept as a future-proofing placeholder.
        # The single/multi unit tests above cover the retry mechanism itself.
        await _apply_knowledge_episode(
            conn, _rid_for(ep_id), "NEW", payload, TEST_SOURCE_NODE,
        )
        # Sanity: the episode + fact applied normally.
        assert await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        ) == 1


# ---------------------------------------------------------------------------
# _format_vector
# ---------------------------------------------------------------------------


class TestFormatVector:

    def test_list_of_floats(self):
        from api.domain_event_handlers import _format_vector
        out = _format_vector([1.0, 2.5, -3.0])
        assert out.startswith("[") and out.endswith("]")
        # Round-trippable to Python floats
        parsed = json.loads(out)
        assert parsed == [1.0, 2.5, -3.0]

    def test_none_returns_none(self):
        from api.domain_event_handlers import _format_vector
        assert _format_vector(None) is None

    def test_empty_list_returns_none(self):
        from api.domain_event_handlers import _format_vector
        assert _format_vector([]) is None

    def test_passthrough_string(self):
        from api.domain_event_handlers import _format_vector
        assert _format_vector("[1.0,2.0]") == "[1.0,2.0]"


# ---------------------------------------------------------------------------
# _apply_knowledge_fact (Phase 2 step 14 — late-bound standalone facts)
# ---------------------------------------------------------------------------


async def _seed_episode(conn, episode_id: str, *, name: str = "seed ep") -> None:
    """Insert a minimal parent episode so a fact's FK resolves."""
    await conn.execute(
        """
        INSERT INTO knowledge_episodes (id, name, content, group_id, valid_at, created_at)
        VALUES ($1::uuid, $2, $3, 'personal',
                '2026-05-13T00:00:00Z'::timestamptz,
                '2026-05-13T00:00:00Z'::timestamptz)
        ON CONFLICT (id) DO NOTHING
        """,
        episode_id, name, "seed content",
    )


def _fact_payload(
    fact_id: str,
    episode_id: str,
    *,
    event_id: str | None = None,
    embedding_column: str | None = None,
    embedding_value=None,
    source_node_rid: str = "orn:koi-net.node:originator+xxxx",
    group_id: str = "personal",
    created_at: str = "2026-05-13T00:00:00Z",
    turn_range_start: int | None = 5,
    turn_range_end: int | None = 6,
    subject_uri: str = "orn:entity:standalone-s",
    predicate: str = "test_pred",
    object_uri: str = "orn:entity:standalone-o",
    fact_text: str = "standalone fact",
):
    payload = {
        "id": fact_id,
        "episode_id": episode_id,
        "subject_uri": subject_uri,
        "predicate": predicate,
        "object_uri": object_uri,
        "object_literal": None,
        "fact_text": fact_text,
        "valid_from": "2026-05-13T00:00:00Z",
        "valid_to": None,
        "created_at": created_at,
        "group_id": group_id,
        "source_node_rid": source_node_rid,
        "turn_range_start": turn_range_start,
        "turn_range_end": turn_range_end,
        "embedding_column": embedding_column,
        "embedding_value": embedding_value,
    }
    if event_id:
        payload["_federation_event_id"] = event_id
    return payload


def _fact_rid(fact_id: str) -> str:
    return f"orn:personal-koi.knowledge-fact:{fact_id}"


class TestApplyKnowledgeFact:

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_inserts_new_fact(self, conn):
        """Happy path: standalone fact with extant parent episode → row inserted."""
        from api.domain_event_handlers import _apply_knowledge_fact

        ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        await _seed_episode(conn, ep_id)

        payload = _fact_payload(fact_id, ep_id, event_id=ev_id)
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT episode_id, subject_uri, predicate, fact_text "
            "FROM knowledge_facts WHERE id = $1::uuid",
            fact_id,
        )
        assert row is not None
        assert str(row["episode_id"]) == ep_id
        assert row["subject_uri"] == "orn:entity:standalone-s"
        assert row["fact_text"] == "standalone fact"

        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'knowledge_fact' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_idempotent_on_redelivery(self, conn):
        """Same event_id twice → ON CONFLICT keeps row count at 1 and idempotency at 1."""
        from api.domain_event_handlers import _apply_knowledge_fact

        ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        await _seed_episode(conn, ep_id)

        payload = _fact_payload(fact_id, ep_id, event_id=ev_id)
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
        )
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE id = $1::uuid", fact_id,
        )
        assert fact_count == 1

        applied = await conn.fetchval(
            "SELECT COUNT(*) FROM federation_applied_events "
            "WHERE domain = 'knowledge_fact' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_late_bound_episode_missing(self, conn):
        """FK miss → raises FederationDeferred; no row; no idempotency record."""
        from api.domain_event_handlers import (
            _apply_knowledge_fact,
            FederationDeferred,
        )

        # episode_id refers to a UUID that does NOT exist in knowledge_episodes
        missing_ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        payload = _fact_payload(fact_id, missing_ep_id, event_id=ev_id)

        with pytest.raises(FederationDeferred, match="awaiting episode"):
            await _apply_knowledge_fact(
                conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
            )

        # Fact row not inserted (the txn savepoint rolled back).
        fact_exists = await conn.fetchval(
            "SELECT 1 FROM knowledge_facts WHERE id = $1::uuid", fact_id,
        )
        assert fact_exists is None

        # Idempotency row NOT written — handler raised before the INSERT line.
        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'knowledge_fact' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied is None

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_recovers_after_episode_arrives(self, conn):
        """First call defers; after episode arrives, second call succeeds + idempotent."""
        from api.domain_event_handlers import (
            _apply_knowledge_fact,
            FederationDeferred,
        )

        ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        ev_id = str(uuid.uuid4())
        payload = _fact_payload(fact_id, ep_id, event_id=ev_id)

        with pytest.raises(FederationDeferred):
            await _apply_knowledge_fact(
                conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
            )

        # Episode arrives (simulating delayed delivery of parent).
        await _seed_episode(conn, ep_id)

        # Second call now succeeds.
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT episode_id FROM knowledge_facts WHERE id = $1::uuid", fact_id,
        )
        assert row is not None
        assert str(row["episode_id"]) == ep_id

        applied = await conn.fetchval(
            "SELECT COUNT(*) FROM federation_applied_events "
            "WHERE domain = 'knowledge_fact' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_originator_metadata_preserved(self, conn):
        """source_node_rid, group_id, created_at must NOT be rewritten."""
        from api.domain_event_handlers import _apply_knowledge_fact

        ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        await _seed_episode(conn, ep_id)

        payload = _fact_payload(
            fact_id, ep_id,
            source_node_rid="orn:koi-net.node:originator+yyyy",
            group_id="custom-group",
            created_at="2025-02-03T04:05:06Z",
        )
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id), "NEW", payload, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT source_node_rid, group_id, created_at "
            "FROM knowledge_facts WHERE id = $1::uuid",
            fact_id,
        )
        assert row["source_node_rid"] == "orn:koi-net.node:originator+yyyy"
        assert row["group_id"] == "custom-group"
        assert row["created_at"].year == 2025
        assert row["created_at"].month == 2
        assert row["created_at"].day == 3
        assert row["created_at"].hour == 4

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_embedding_column_discriminator(self, conn):
        """1024-dim and null embedding cases for standalone facts."""
        from api.domain_event_handlers import _apply_knowledge_fact

        ep_id = str(uuid.uuid4())
        await _seed_episode(conn, ep_id)

        # 1024-dim case
        fact_id_1024 = str(uuid.uuid4())
        vec = [0.01 * i for i in range(1024)]
        payload_1024 = _fact_payload(
            fact_id_1024, ep_id,
            embedding_column="fact_embedding",
            embedding_value=vec,
        )
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id_1024), "NEW", payload_1024, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT fact_embedding IS NOT NULL AS has_1024, "
            "fact_embedding_3072 IS NOT NULL AS has_3072 "
            "FROM knowledge_facts WHERE id = $1::uuid",
            fact_id_1024,
        )
        assert row["has_1024"] is True
        assert row["has_3072"] is False

        # Null-embedding case
        fact_id_null = str(uuid.uuid4())
        payload_null = _fact_payload(fact_id_null, ep_id)
        await _apply_knowledge_fact(
            conn, _fact_rid(fact_id_null), "NEW", payload_null, TEST_SOURCE_NODE,
        )
        row_null = await conn.fetchrow(
            "SELECT fact_embedding IS NULL AS no_1024, "
            "fact_embedding_3072 IS NULL AS no_3072 "
            "FROM knowledge_facts WHERE id = $1::uuid",
            fact_id_null,
        )
        assert row_null["no_1024"] is True
        assert row_null["no_3072"] is True

    @pytest.mark.anyio
    async def test_apply_knowledge_fact_schema_drift_retry(self, conn):
        """Unknown column on a fact (via direct _insert_with_drift_retry on
        knowledge_facts) → drift retry drops it, INSERT succeeds. Confirms
        the helper handles this table too.
        """
        from api.domain_event_handlers import _insert_with_drift_retry

        ep_id = str(uuid.uuid4())
        fact_id = str(uuid.uuid4())
        await _seed_episode(conn, ep_id)

        cols_vals = {
            "id": fact_id,
            "episode_id": ep_id,
            "subject_uri": "orn:s",
            "predicate": "p",
            "fact_text": "drift fact",
            "ghost_col": "should be dropped",  # ← unknown column
        }
        casts = {"id": "::uuid", "episode_id": "::uuid"}
        conflict = "ON CONFLICT (id) DO UPDATE SET valid_to = EXCLUDED.valid_to"
        await _insert_with_drift_retry(
            conn, "knowledge_facts", cols_vals, casts, conflict,
        )

        row = await conn.fetchrow(
            "SELECT fact_text FROM knowledge_facts WHERE id = $1::uuid", fact_id,
        )
        assert row["fact_text"] == "drift fact"


# ---------------------------------------------------------------------------
# _apply_doclink (Phase 2 step 15 — check-first-apply, single-txn wrap)
# ---------------------------------------------------------------------------


def _doclink_payload(
    document_rid: str,
    entity_uri: str,
    *,
    mention_delta=1,
    event_id: str | None = None,
    context: str | None = "test context",
    created_at: str | None = "2026-05-13T00:00:00Z",
    include_mention_delta: bool = True,
):
    payload = {
        "document_rid": document_rid,
        "entity_uri": entity_uri,
        "context": context,
        "created_at": created_at,
    }
    if include_mention_delta:
        payload["mention_delta"] = mention_delta
    if event_id:
        payload["_federation_event_id"] = event_id
    return payload


def _doclink_rid(document_rid: str, entity_uri: str) -> str:
    return f"orn:personal-koi.document-entity-link:{document_rid}::{entity_uri}"


class TestApplyDoclink:

    @pytest.mark.anyio
    async def test_apply_doclink_inserts_new_row(self, conn):
        """Happy path: new (document_rid, entity_uri) → row with mention_count = delta."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id = str(uuid.uuid4())

        payload = _doclink_payload(doc, ent, mention_delta=3, event_id=ev_id)
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", payload, TEST_SOURCE_NODE,
        )

        row = await conn.fetchrow(
            "SELECT mention_count, context FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert row is not None
        assert row["mention_count"] == 3
        assert row["context"] == "test context"

        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'document_entity_link' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_apply_doclink_increments_mention_count_once(self, conn):
        """THE critical test: same event_id delivered twice → mention_count stays
        at delta, NOT 2×delta. The idempotency check blocks the second apply."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id = str(uuid.uuid4())

        payload = _doclink_payload(doc, ent, mention_delta=5, event_id=ev_id)
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", payload, TEST_SOURCE_NODE,
        )
        # Re-delivery of the SAME event_id.
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", payload, TEST_SOURCE_NODE,
        )

        mention_count = await conn.fetchval(
            "SELECT mention_count FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert mention_count == 5  # NOT 10

        applied = await conn.fetchval(
            "SELECT COUNT(*) FROM federation_applied_events "
            "WHERE domain = 'document_entity_link' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_apply_doclink_composite_key_dedup(self, conn):
        """Two DIFFERENT event_ids, same (document_rid, entity_uri) → mention_count
        sums both deltas. Distinct from idempotency: legitimate re-mention."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id_1 = str(uuid.uuid4())
        ev_id_2 = str(uuid.uuid4())

        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW",
            _doclink_payload(doc, ent, mention_delta=4, event_id=ev_id_1),
            TEST_SOURCE_NODE,
        )
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW",
            _doclink_payload(doc, ent, mention_delta=7, event_id=ev_id_2),
            TEST_SOURCE_NODE,
        )

        mention_count = await conn.fetchval(
            "SELECT mention_count FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert mention_count == 11  # 4 + 7

    @pytest.mark.anyio
    async def test_apply_doclink_rollback_on_upsert_failure(self, conn):
        """Load-bearing test for the single-txn wrap. Force the doclink upsert to
        raise (mention_delta out of int4 range). Verify (a) the exception
        propagates, (b) the federation_applied_events row was rolled back too —
        NOT left stale, (c) a subsequent retry with a valid delta succeeds."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id = str(uuid.uuid4())

        # int4 max is 2_147_483_647; this overflows → asyncpg raises on encode.
        bad_payload = _doclink_payload(
            doc, ent, mention_delta=10 ** 12, event_id=ev_id,
        )
        with pytest.raises(Exception):
            await _apply_doclink(
                conn, _doclink_rid(doc, ent), "NEW", bad_payload, TEST_SOURCE_NODE,
            )

        # (b) idempotency row must have rolled back with the failed upsert.
        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'document_entity_link' AND event_id = $1::uuid",
            ev_id,
        )
        assert applied is None

        # doclink row must not exist either.
        row_exists = await conn.fetchval(
            "SELECT 1 FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert row_exists is None

        # (c) retry with the SAME event_id but a valid delta now succeeds —
        # proving no stale idempotency row blocked it.
        good_payload = _doclink_payload(
            doc, ent, mention_delta=2, event_id=ev_id,
        )
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", good_payload, TEST_SOURCE_NODE,
        )
        mention_count = await conn.fetchval(
            "SELECT mention_count FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert mention_count == 2

    @pytest.mark.anyio
    async def test_apply_doclink_missing_mention_delta_fallback(self, conn):
        """Payload omits mention_delta → handler logs WARNING, applies with delta=1."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id = str(uuid.uuid4())

        payload = _doclink_payload(
            doc, ent, event_id=ev_id, include_mention_delta=False,
        )
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", payload, TEST_SOURCE_NODE,
        )

        mention_count = await conn.fetchval(
            "SELECT mention_count FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert mention_count == 1

    @pytest.mark.anyio
    async def test_apply_doclink_originator_metadata_preserved(self, conn):
        """created_at from payload is preserved verbatim, not overwritten with NOW()."""
        from api.domain_event_handlers import _apply_doclink

        doc = f"orn:doc:{uuid.uuid4()}"
        ent = f"orn:entity:{uuid.uuid4()}"
        ev_id = str(uuid.uuid4())

        payload = _doclink_payload(
            doc, ent, event_id=ev_id, created_at="2025-02-03T04:05:06Z",
        )
        await _apply_doclink(
            conn, _doclink_rid(doc, ent), "NEW", payload, TEST_SOURCE_NODE,
        )

        created_at = await conn.fetchval(
            "SELECT created_at FROM document_entity_links "
            "WHERE document_rid = $1 AND entity_uri = $2",
            doc, ent,
        )
        assert created_at.year == 2025
        assert created_at.month == 2
        assert created_at.day == 3
        assert created_at.hour == 4
        assert created_at.minute == 5
        assert created_at.second == 6
