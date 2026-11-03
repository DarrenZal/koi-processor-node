"""Unit tests for knowledge_router.py — Federation Phase 1 step 2e.

Covers the `knowledge_episode` bundled emit added to `POST /knowledge/episodes`:
the post-commit emit ordering, the feature-flag gate, the publisher→subscriber
payload roundtrip, the `_federation_event_id` chain, the embedding-column
discriminator, and a regression check that the endpoint's existing contract is
unchanged with the flag off.

Federation plan, Phase 1 step 8 / 2e.

Style mirrors tests/unit/test_domain_event_handlers.py — a single asyncpg
connection wrapped in a transaction that is rolled back at teardown, so no DB
state leaks. The endpoint opens no explicit transaction of its own (asyncpg
auto-commits per statement); wrapping the one connection in an outer
transaction keeps every write inside the rollback boundary.
"""

import uuid

import asyncpg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

import api.federation_events as federation_events
from tests import koi_test_targets as targets

TEST_SOURCE_NODE = "orn:koi-net.node:test-peer+aaaa"
EMBED_DIM = 3072


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------

class _InstrumentedPool:
    """Wraps a single asyncpg.Connection to quack like asyncpg.Pool.

    Records whether the `async with pool.acquire()` block has exited — used by
    `test_emit_fires_after_commit` to prove the emit happens OUTSIDE that block.
    """

    def __init__(self, conn):
        self._conn = conn
        self.acquire_exited = False

    class _CM:
        def __init__(self, pool):
            self.pool = pool

        async def __aenter__(self):
            return self.pool._conn

        async def __aexit__(self, *exc):
            self.pool.acquire_exited = True

    def acquire(self):
        return self._CM(self)


class _FakeQueue:
    """Stand-in for EventQueue. Records `.add()` calls instead of touching the
    DB, and snapshots the pool's acquire-exited flag at call time so a test can
    assert the emit fired post-commit (outside the acquire block).
    """

    def __init__(self, pool):
        self._pool = pool
        self.added = []
        self.acquire_exited_at_add = None

    async def add(self, **kwargs):
        self.acquire_exited_at_add = self._pool.acquire_exited
        self.added.append(kwargs)
        return kwargs.get("event_id")


async def _fake_embed(text, **kwargs):
    """Deterministic, distinct-per-text, non-zero 3072-dim embedding."""
    seed = hash(text)
    return [float(((seed + i) % 97) + 1) / 97.0 for i in range(EMBED_DIM)]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def anyio_backend():
    return "asyncio"


# ---------------------------------------------------------------------------
# Service-token auth for the test client.
#
# POST /knowledge/episodes carries `Depends(require_service_auth)`
# (api/routers/knowledge_router.py:729). Tests written before that dependency
# existed posted with no Authorization header and began failing `401 != 201`.
# The endpoint is behaving correctly; the tests were not sending credentials.
#
# `make_service_token_auth` reads the env var at REQUEST time
# (api/auth_deps.py:212 `os.getenv(env_var, "")`), so setting it from a fixture
# is sufficient — no import-time ordering concern.
#
# Defined here, above the fixtures that use it, so there is exactly one auth
# path in this module. The P4 section below aliases these rather than defining
# a second token.
# ---------------------------------------------------------------------------
SVC_TOKEN = "test-token-i"
SVC_AUTH = {"Authorization": f"Bearer {SVC_TOKEN}"}


@pytest.fixture
async def harness(monkeypatch):
    """Yield (client, conn, fake_queue, pool) wired against a rolled-back txn."""
    from api.routers.knowledge_router import create_router

    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", SVC_TOKEN)

    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    pool = _InstrumentedPool(conn)
    fake_queue = _FakeQueue(pool)

    # Wire the publisher's module-global event queue to the fake.
    prev_queue = federation_events._event_queue
    federation_events.set_event_queue(fake_queue)

    router = create_router(pool, generate_document_embedding=_fake_embed)
    app = FastAPI()
    app.include_router(router, prefix="/knowledge")

    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers=SVC_AUTH
    ) as client:
        yield client, conn, fake_queue, pool

    federation_events.set_event_queue(prev_queue)
    await tx.rollback()
    await conn.close()


def _episode_body(n_facts=2, *, group_id=None, source_document=None):
    """Build a POST /knowledge/episodes request body with unique entity names
    so entity resolution always mints fresh entities (no collision with
    committed production rows visible through the rolled-back txn).
    """
    tag = uuid.uuid4().hex[:10]
    facts = []
    for i in range(n_facts):
        facts.append({
            "subject": f"TestSubj_{tag}_{i}",
            "predicate": "TEST_PREDICATE",
            "object": f"TestObj_{tag}_{i}",
            "fact_text": f"Test fact {i} for {tag} — unique sentence {uuid.uuid4()}.",
            "valid_from": "2026-05-13T00:00:00+00:00",
        })
    return {
        "name": f"Test Episode {tag}",
        "content": f"content {tag}",
        "source_description": "unit-test",
        "source_document": source_document or f"test-doc-{tag}.md",
        "group_id": group_id or f"test-{tag}",
        "valid_at": "2026-05-13T00:00:00+00:00",
        "metadata": {"test": True, "tag": tag},
        "facts": facts,
        "create_entities": True,
    }


# ---------------------------------------------------------------------------
# _fact_embedding_discriminator — pure unit, no DB
# ---------------------------------------------------------------------------

class TestEmbeddingDiscriminator:

    def test_3072_dim(self):
        from api.routers.knowledge_router import _fact_embedding_discriminator

        vec = [0.1] * 3072
        col, val = _fact_embedding_discriminator(fact_embedding_3072=vec)
        assert col == "fact_embedding_3072"
        assert val == vec
        assert isinstance(val, list)

    def test_1024_dim(self):
        from api.routers.knowledge_router import _fact_embedding_discriminator

        vec = [0.2] * 1024
        col, val = _fact_embedding_discriminator(fact_embedding=vec)
        assert col == "fact_embedding"
        assert val == vec

    def test_neither(self):
        from api.routers.knowledge_router import _fact_embedding_discriminator

        col, val = _fact_embedding_discriminator()
        assert col is None
        assert val is None

        col, val = _fact_embedding_discriminator(
            fact_embedding_3072=None, fact_embedding=[]
        )
        assert col is None
        assert val is None

    def test_3072_preferred_over_1024(self):
        from api.routers.knowledge_router import _fact_embedding_discriminator

        col, val = _fact_embedding_discriminator(
            fact_embedding_3072=[0.3] * 3072, fact_embedding=[0.4] * 1024
        )
        assert col == "fact_embedding_3072"
        assert len(val) == 3072


# ---------------------------------------------------------------------------
# Emit behaviour
# ---------------------------------------------------------------------------

class TestKnowledgeEpisodeEmit:

    @pytest.mark.anyio
    async def test_emit_does_not_fire_when_flag_off(self, harness, monkeypatch):
        """KOI_FEDERATE_KNOWLEDGE unset → emit is a perfect no-op (zero adds)."""
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness

        resp = await client.post("/knowledge/episodes", json=_episode_body(2))
        assert resp.status_code == 201
        assert len(fake_queue.added) == 0

    @pytest.mark.anyio
    async def test_emit_fires_after_commit(self, harness, monkeypatch):
        """The emit must fire OUTSIDE the `async with pool.acquire()` block.

        The instrumented pool flips `acquire_exited` True in the context
        manager's `__aexit__`; the fake queue snapshots that flag when `.add()`
        is called. If the emit were still inside the block the snapshot would
        be False.
        """
        monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
        client, conn, fake_queue, pool = harness

        resp = await client.post("/knowledge/episodes", json=_episode_body(3))
        assert resp.status_code == 201
        assert len(fake_queue.added) == 1
        assert fake_queue.acquire_exited_at_add is True

        # And the rows it describes are all present on the connection by then.
        ep_id = resp.json()["episode_id"]
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert fact_count == 3

    @pytest.mark.anyio
    async def test_emit_payload_event_id_present(self, harness, monkeypatch):
        """Gate 2: the emitted event carries `_federation_event_id` in payload
        contents, equal to the queue-level `event_id` — the idempotency chain.
        """
        monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
        client, conn, fake_queue, pool = harness

        resp = await client.post("/knowledge/episodes", json=_episode_body(1))
        assert resp.status_code == 201

        add = fake_queue.added[0]
        queue_event_id = add["event_id"]
        payload = add["contents"]["payload"]
        assert queue_event_id is not None
        # round-trips: queue event_id == payload._federation_event_id
        assert payload["_federation_event_id"] == queue_event_id
        # and it's a real UUID
        uuid.UUID(queue_event_id)
        assert add["contents"]["_koi_domain"] == "knowledge_episode"

    @pytest.mark.anyio
    async def test_emit_payload_roundtrips_through_subscriber(
        self, harness, monkeypatch
    ):
        """THE integration-point test: the publisher payload feeds cleanly
        through the 2b subscriber handler `_apply_knowledge_episode`.

        The POST already wrote the episode + facts; we DELETE them (cascade),
        then replay the captured payload through the subscriber and assert the
        rows are faithfully reconstructed. A publisher/subscriber shape drift
        (e.g. a missing fact `id`) would surface here as a dropped fact.
        """
        monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
        client, conn, fake_queue, pool = harness
        from api.domain_event_handlers import _apply_knowledge_episode

        resp = await client.post("/knowledge/episodes", json=_episode_body(3))
        assert resp.status_code == 201
        ep_id = resp.json()["episode_id"]
        assert resp.json()["facts_created"] == 3

        add = fake_queue.added[0]
        payload = add["contents"]["payload"]
        rid = add["rid"]
        event_type = add["event_type"]
        assert event_type == "NEW"

        # Wipe the locally-written rows so the subscriber applies onto a clean
        # slate — proving the payload alone reconstructs episode + facts.
        await conn.execute(
            "DELETE FROM knowledge_episodes WHERE id = $1::uuid", ep_id
        )
        gone = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert gone == 0

        await _apply_knowledge_episode(
            conn, rid, event_type, payload, TEST_SOURCE_NODE
        )

        ep = await conn.fetchrow(
            "SELECT name, content, group_id FROM knowledge_episodes "
            "WHERE id = $1::uuid",
            ep_id,
        )
        assert ep is not None
        assert ep["name"] == payload["name"]
        assert ep["content"] == payload["content"]

        facts = await conn.fetch(
            "SELECT id, subject_uri, predicate, fact_text, group_id, "
            "fact_embedding_3072 IS NOT NULL AS has_embed "
            "FROM knowledge_facts WHERE episode_id = $1::uuid",
            ep_id,
        )
        assert len(facts) == 3
        for f in facts:
            assert f["predicate"] == "TEST_PREDICATE"
            assert f["has_embed"] is True
            assert f["group_id"] == payload["group_id"]

        # idempotency row recorded by the subscriber
        applied = await conn.fetchval(
            "SELECT 1 FROM federation_applied_events "
            "WHERE domain = 'knowledge_episode' AND event_id = $1::uuid",
            payload["_federation_event_id"],
        )
        assert applied == 1

    @pytest.mark.anyio
    async def test_emit_event_type_update_on_reused_episode(
        self, harness, monkeypatch
    ):
        """Reusing an episode (same source_document + group_id) emits UPDATE."""
        monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
        client, conn, fake_queue, pool = harness

        body1 = _episode_body(1)
        resp1 = await client.post("/knowledge/episodes", json=body1)
        assert resp1.status_code == 201
        assert fake_queue.added[0]["event_type"] == "NEW"

        # Second write with same source_document + group_id → reuse → UPDATE.
        body2 = _episode_body(1)
        body2["source_document"] = body1["source_document"]
        body2["group_id"] = body1["group_id"]
        resp2 = await client.post("/knowledge/episodes", json=body2)
        assert resp2.status_code == 201
        assert resp2.json()["episode_reused"] is True
        assert fake_queue.added[1]["event_type"] == "UPDATE"
        assert resp2.json()["episode_id"] == resp1.json()["episode_id"]

    @pytest.mark.anyio
    async def test_existing_episode_endpoint_unchanged(self, harness, monkeypatch):
        """REGRESSION: with the flag off, POST /knowledge/episodes still returns
        the same response shape and still commits episode + facts.
        """
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness

        resp = await client.post("/knowledge/episodes", json=_episode_body(2))
        assert resp.status_code == 201
        data = resp.json()

        # Response shape unchanged — every EpisodeCreateResponse field present.
        for key in (
            "episode_id", "episode_reused", "facts_created", "facts_skipped",
            "facts_superseded", "entities_resolved", "entities_created",
            "facts_null_embed",
        ):
            assert key in data, f"missing response key: {key}"

        assert data["episode_reused"] is False
        assert data["facts_created"] == 2
        assert data["facts_null_embed"] == 0

        # Episode + facts were actually written.
        ep_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_episodes WHERE id = $1::uuid",
            data["episode_id"],
        )
        assert ep_count == 1
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid",
            data["episode_id"],
        )
        assert fact_count == 2

        # Flag off → no emit.
        assert len(fake_queue.added) == 0


# ---------------------------------------------------------------------------
# Type-hint divergence (subject_type/object_type → type_mismatches)
# ---------------------------------------------------------------------------

class TestTypeMismatch:
    """Coverage for the entity-type-hint feature: subject_type/object_type on
    fact ingest, and the type_mismatches surfaced when a hint conflicts with an
    existing registry row's entity_type. type_hint is create-time-only —
    resolution against an existing row preserves the existing type and reports
    the divergence rather than mutating it.
    """

    def _typed_body(self, subject, *, subject_type=None, object_type=None):
        tag = uuid.uuid4().hex[:10]
        fact = {
            "subject": subject,
            "predicate": "TEST_PREDICATE",
            "object_literal": "some literal value",
            "fact_text": f"Type-hint test fact {tag} — {uuid.uuid4()}.",
            "valid_from": "2026-05-13T00:00:00+00:00",
        }
        if subject_type is not None:
            fact["subject_type"] = subject_type
        if object_type is not None:
            fact["object_type"] = object_type
        return {
            "name": f"TypeHint Episode {tag}",
            "content": f"content {tag}",
            "source_description": "unit-test",
            "source_document": f"test-doc-{tag}.md",
            "group_id": f"test-{tag}",
            "valid_at": "2026-05-13T00:00:00+00:00",
            "metadata": {"test": True, "tag": tag},
            "facts": [fact],
            "create_entities": True,
        }

    @pytest.mark.anyio
    async def test_mismatch_surfaced_when_hint_conflicts_with_existing(
        self, harness, monkeypatch
    ):
        """Subject already exists as Concept; fact asserts subject_type=Person.
        Resolution keeps Concept and surfaces a single type_mismatch entry.
        """
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness
        from api.personal_ingest_api import (
            normalize_entity_text, generate_entity_uri,
        )

        tag = uuid.uuid4().hex[:10]
        name = f"TypeProbe {tag}"
        normalized = normalize_entity_text(name)
        seeded_uri = generate_entity_uri(name, "Concept")

        # Seed an existing Concept row inside the rolled-back txn.
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, normalized_text, entity_type, source)
            VALUES ($1, $2, $3, 'Concept', 'unit-test')
            ON CONFLICT (fuseki_uri) DO NOTHING
            """,
            seeded_uri, name, normalized,
        )

        resp = await client.post(
            "/knowledge/episodes",
            json=self._typed_body(name, subject_type="Person"),
        )
        assert resp.status_code == 201
        data = resp.json()

        mismatches = data["type_mismatches"]
        assert len(mismatches) == 1
        m = mismatches[0]
        assert m["role"] == "subject"
        assert m["requested_type"] == "Person"
        assert m["resolved_type"] == "Concept"
        assert m["resolved_uri"] == seeded_uri
        assert m["name"] == name

        # Contract: the existing entity's type is preserved, not mutated.
        still_concept = await conn.fetchval(
            "SELECT entity_type FROM entity_registry WHERE fuseki_uri = $1",
            seeded_uri,
        )
        assert still_concept == "Concept"

    @pytest.mark.anyio
    async def test_no_mismatch_when_entity_created_with_hint(
        self, harness, monkeypatch
    ):
        """Fresh subject + subject_type=Person → created as Person (Tier-3),
        resolved type equals the hint → no mismatch reported.
        """
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness
        from api.personal_ingest_api import normalize_entity_text

        tag = uuid.uuid4().hex[:10]
        name = f"FreshPerson {tag}"

        resp = await client.post(
            "/knowledge/episodes",
            json=self._typed_body(name, subject_type="Person"),
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["type_mismatches"] == []

        # The new entity was created with the hinted type.
        created_type = await conn.fetchval(
            "SELECT entity_type FROM entity_registry WHERE normalized_text = $1",
            normalize_entity_text(name),
        )
        assert created_type == "Person"

    @pytest.mark.anyio
    async def test_concept_hint_bypasses_cross_type_exact_homonym(
        self, harness, monkeypatch
    ):
        """Scientific concepts may share names with organizations/products.

        A Concept hint should prefer a same-type create over an exact cross-type
        organization row, avoiding document-ingest collisions like mathematical
        "discord" resolving to the Discord organization.
        """
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness
        from api.personal_ingest_api import (
            normalize_entity_text, generate_entity_uri,
        )

        tag = uuid.uuid4().hex[:10]
        name = f"HomonymProbe {tag}"
        normalized = normalize_entity_text(name)
        org_uri = generate_entity_uri(name, "Organization")

        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, normalized_text, entity_type, source)
            VALUES ($1, $2, $3, 'Organization', 'unit-test')
            ON CONFLICT (fuseki_uri) DO NOTHING
            """,
            org_uri, name, normalized,
        )

        resp = await client.post(
            "/knowledge/episodes",
            json=self._typed_body(name, subject_type="Concept"),
        )
        assert resp.status_code == 201
        data = resp.json()

        assert data["type_mismatches"] == []

        subject_uri = await conn.fetchval(
            "SELECT subject_uri FROM knowledge_facts WHERE episode_id = $1::uuid",
            data["episode_id"],
        )
        subject_type = await conn.fetchval(
            "SELECT entity_type FROM entity_registry WHERE fuseki_uri = $1",
            subject_uri,
        )
        assert subject_uri != org_uri
        assert subject_type == "Concept"

    @pytest.mark.anyio
    async def test_no_mismatch_when_no_hint_provided(self, harness, monkeypatch):
        """Backward-compat: a fact with no subject_type never produces a
        type_mismatch, even when the subject resolves to an existing row.
        """
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, fake_queue, pool = harness
        from api.personal_ingest_api import (
            normalize_entity_text, generate_entity_uri,
        )

        tag = uuid.uuid4().hex[:10]
        name = f"NoHintProbe {tag}"
        seeded_uri = generate_entity_uri(name, "Concept")
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, normalized_text, entity_type, source)
            VALUES ($1, $2, $3, 'Concept', 'unit-test')
            ON CONFLICT (fuseki_uri) DO NOTHING
            """,
            seeded_uri, name, normalize_entity_text(name),
        )

        resp = await client.post(
            "/knowledge/episodes", json=self._typed_body(name),
        )
        assert resp.status_code == 201
        assert resp.json()["type_mismatches"] == []


# ---------------------------------------------------------------------------
# P4: batch-embed prefetch + single transaction + request_id idempotency
# ---------------------------------------------------------------------------

# Aliases of the single auth path defined at the top of this module.
P4_SVC_TOKEN = SVC_TOKEN
P4_AUTH = SVC_AUTH


def _fake_vec(text):
    seed = hash(text)
    return [float(((seed + i) % 97) + 1) / 97.0 for i in range(EMBED_DIM)]


class _EmbedCounter:
    """Counts single vs batch embed calls; returns valid 3072-dim vectors."""

    def __init__(self):
        self.single = 0
        self.batch = 0

    async def single_embed(self, text, **kwargs):
        self.single += 1
        return _fake_vec(text)

    async def batch_embed(self, texts, **kwargs):
        self.batch += 1
        return [_fake_vec(t) for t in texts]


@pytest.fixture
async def p4_harness(monkeypatch):
    """(client, conn, counter) wired against a rolled-back txn, with batch embed
    + an in-txn ingest_idempotency table + service-token auth satisfied.
    """
    from api.routers.knowledge_router import create_router

    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", P4_SVC_TOKEN)

    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()
    # Idempotency ledger lives inside the rolled-back txn (migration 106 not
    # applied to the live DB in tests).
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_idempotency (
            request_id TEXT PRIMARY KEY,
            endpoint   TEXT NOT NULL,
            response   JSONB NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now()
        )
    """)

    prev_queue = federation_events._event_queue
    fake_queue = _FakeQueue(_InstrumentedPool(conn))
    federation_events.set_event_queue(fake_queue)

    counter = _EmbedCounter()
    pool = _InstrumentedPool(conn)
    router = create_router(
        pool,
        generate_document_embedding=counter.single_embed,
        generate_batch_embedding=counter.batch_embed,
    )
    app = FastAPI()
    app.include_router(router, prefix="/knowledge")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, conn, counter

    federation_events.set_event_queue(prev_queue)
    await tx.rollback()
    await conn.close()


class TestP4BatchTransactionIdempotency:

    @pytest.mark.anyio
    async def test_batch_embed_one_call_no_per_text(self, p4_harness, monkeypatch):
        """A 20-fact episode uses ONE batch embed call and zero per-text calls."""
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, counter = p4_harness

        resp = await client.post(
            "/knowledge/episodes", json=_episode_body(20), headers=P4_AUTH)
        assert resp.status_code == 201, resp.text
        assert resp.json()["facts_created"] == 20
        # ≤2 batch calls (implementation does exactly 1) and no per-text fallback.
        assert counter.batch == 1, f"expected 1 batch call, got {counter.batch}"
        assert counter.single == 0, f"expected 0 per-text calls, got {counter.single}"

    @pytest.mark.anyio
    async def test_idempotent_replay(self, p4_harness, monkeypatch):
        """Same request_id replays the stored response without new writes."""
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, counter = p4_harness

        body = _episode_body(3)
        body["request_id"] = f"idem-{uuid.uuid4()}"

        r1 = await client.post("/knowledge/episodes", json=body, headers=P4_AUTH)
        assert r1.status_code == 201, r1.text
        d1 = r1.json()
        assert d1["idempotent_replay"] is False
        assert d1["facts_created"] == 3
        ep_id = d1["episode_id"]

        # Replay: identical request_id → stored response, no new writes.
        r2 = await client.post("/knowledge/episodes", json=body, headers=P4_AUTH)
        assert r2.status_code == 201, r2.text
        d2 = r2.json()
        assert d2["idempotent_replay"] is True
        assert d2["episode_id"] == ep_id
        assert d2["facts_created"] == 3
        # Discriminator: a fresh episode-reuse would report episode_reused=True;
        # a replay returns the STORED first response (episode_reused=False).
        assert d2["episode_reused"] is False

        # Exactly one idempotency row, one episode, three facts.
        rows = await conn.fetchval(
            "SELECT COUNT(*) FROM ingest_idempotency WHERE request_id = $1",
            body["request_id"])
        assert rows == 1
        ep_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_episodes WHERE id = $1::uuid", ep_id)
        assert ep_count == 1
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid", ep_id)
        assert fact_count == 3

    @pytest.mark.anyio
    async def test_mid_transaction_failure_rolls_back_everything(
        self, p4_harness, monkeypatch
    ):
        """An exception after partial writes rolls back the whole episode."""
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, counter = p4_harness
        import api.routers.knowledge_router as kr

        calls = {"n": 0}
        orig = kr._fact_embedding_discriminator

        def boom(**kwargs):
            calls["n"] += 1
            if calls["n"] >= 2:
                raise RuntimeError("simulated mid-transaction failure")
            return orig(**kwargs)

        monkeypatch.setattr(kr, "_fact_embedding_discriminator", boom)

        body = _episode_body(3)
        src = body["source_document"]

        raised = False
        status = None
        try:
            resp = await client.post(
                "/knowledge/episodes", json=body, headers=P4_AUTH)
            status = resp.status_code
        except RuntimeError:
            raised = True
        assert raised or status == 500, f"expected failure, got status={status}"

        # Nothing persisted — episode + all facts rolled back.
        ep_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_episodes WHERE source_document = $1", src)
        assert ep_count == 0
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts f "
            "JOIN knowledge_episodes e ON e.id = f.episode_id "
            "WHERE e.source_document = $1", src)
        assert fact_count == 0

    @pytest.mark.anyio
    async def test_single_transaction_semantics_normal_commit(
        self, p4_harness, monkeypatch
    ):
        """Sanity: with batch embedding wired, a normal episode commits fully."""
        monkeypatch.delenv("KOI_FEDERATE_KNOWLEDGE", raising=False)
        client, conn, counter = p4_harness

        body = _episode_body(2)
        resp = await client.post("/knowledge/episodes", json=body, headers=P4_AUTH)
        assert resp.status_code == 201, resp.text
        ep_id = resp.json()["episode_id"]
        fact_count = await conn.fetchval(
            "SELECT COUNT(*) FROM knowledge_facts WHERE episode_id = $1::uuid", ep_id)
        assert fact_count == 2
