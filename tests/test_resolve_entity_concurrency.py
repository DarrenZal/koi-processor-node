"""Regression test for ``resolve_entity`` concurrency invariant (AC40b).

The invariant: N concurrent ``resolve_entity`` + ``store_new_entity`` calls
for the same novel ``(name, type)`` pair produce exactly one row in
``entity_registry`` and all callers receive the same ``fuseki_uri``.

That property is provided by two mechanisms working together, which this
file locks in:

  1. ``generate_entity_uri(name, type)`` is deterministic:
     ``sha256(f"{type}:{normalize_entity_text(name)}")`` → same URI for
     same normalized inputs. Verified by ``test_generate_entity_uri_is_deterministic``.

  2. ``store_new_entity`` INSERTs with
     ``ON CONFLICT (fuseki_uri) DO NOTHING`` against the
     ``entity_registry_fuseki_uri_key`` UNIQUE constraint, so concurrent
     INSERTs of the same URI serialize at the DB layer and only one row
     survives. Verified by ``test_concurrent_resolve_and_store_single_row``.

Either mechanism silently disappearing (e.g. a future refactor that adds a
timestamp to the URI hash, splits URI generation from insert, or drops the
``ON CONFLICT`` clause) will fail these tests.

This is the belt-and-suspenders for AC40b. The prior plan wording called
for a ``pg_advisory_xact_lock`` in ``resolve_entity``; after inspection we
confirmed the deterministic-URI + ON CONFLICT pattern delivers the same
correctness property atomically at the DB layer, without the lock-wait
latency. See plan commentary on AC40b.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from pathlib import Path

import pytest
from tests import koi_test_targets as targets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")


# ---- Unit test: deterministic URI (no DB) --------------------------------


def test_generate_entity_uri_is_deterministic():
    """Repeated calls with the same ``(name, type)`` return the same URI.

    Covers mechanism #1 of the concurrency invariant. Any change that makes
    URI generation non-deterministic (adding a timestamp, a random seed,
    etc.) breaks concurrency safety and is caught here.
    """
    from api.personal_ingest_api import generate_entity_uri
    from api.resolution_primitives import normalize_entity_text

    name = "NovelTestOrg"
    type_hint = "Organization"

    uris = [generate_entity_uri(name, type_hint) for _ in range(50)]
    assert len(set(uris)) == 1, f"URI is not deterministic: {set(uris)}"

    # Different type → different URI (same name, different hash input).
    assert generate_entity_uri(name, "Person") != uris[0]

    # Different surface form with same normalized form → same URI.
    assert normalize_entity_text("Novel-Test_Org") == normalize_entity_text(
        "novel test org"
    )
    assert generate_entity_uri("Novel-Test_Org", type_hint) == generate_entity_uri(
        "novel test org", type_hint
    )


# ---- Integration test: concurrent gather → 1 row, 1 URI ------------------


_integration_skip = targets.requires_database


async def _create_minimal_schema(pool, schema: str) -> None:
    """Create a minimal ``entity_registry`` mirror sufficient for this test.

    We deliberately use ``TEXT`` for ``embedding`` (rather than
    ``vector(1024)``) so the test does not require the pgvector extension
    on the test DB. The embedding column is never written by this test
    because we force ``embedding_provider = None``.

    THIS TABLE MUST CARRY EVERY COLUMN ``store_new_entity`` WRITES. It is a
    hand-maintained mirror in its own schema, so no migration ever reaches it
    and nothing fails until the INSERT does. Migration 111 added
    ``resolution_tier`` to the write path and this mirror was not updated,
    which broke the test with ``UndefinedColumnError`` -- and went unnoticed
    because this file is outside the 8-file red-baseline node set. If you add
    a column to the ``store_new_entity`` INSERT, add it here in the same
    commit.
    """
    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}", public')
        await conn.execute(
            """
            CREATE TABLE entity_registry (
                id              SERIAL PRIMARY KEY,
                fuseki_uri      TEXT UNIQUE NOT NULL,
                entity_text     TEXT NOT NULL,
                entity_type     TEXT,
                normalized_text TEXT NOT NULL,
                source          TEXT,
                first_seen_rid  TEXT,
                metadata        JSONB,
                embedding       TEXT,
                phonetic_code   TEXT,
                aliases         TEXT[],
                node_private    BOOLEAN DEFAULT false,
                merged_into     TEXT,
                resolution_tier TEXT,
                created_at      TIMESTAMP DEFAULT now(),
                updated_at      TIMESTAMP DEFAULT now()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX idx_entity_registry_normalized "
            "ON entity_registry(normalized_text)"
        )
        await conn.execute(
            "CREATE INDEX idx_entity_registry_type "
            "ON entity_registry(entity_type)"
        )


@_integration_skip
@pytest.mark.asyncio
async def test_concurrent_resolve_and_store_single_row(monkeypatch):
    """10 concurrent ``resolve_entity`` + ``store_new_entity`` → 1 row, 1 URI.

    Covers mechanism #2 (ON CONFLICT) of the concurrency invariant, and
    exercises mechanism #1 end-to-end via the real tier chain. The novel
    name is UUID-suffixed so it cannot collide with any pre-existing data.
    """
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import (
        ExtractedEntity,
        resolve_entity,
        store_new_entity,
    )

    schema = f"conc_{uuid.uuid4().hex[:10]}"

    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        await _create_minimal_schema(pool0, schema)
    finally:
        await pool0.close()

    pool = await asyncpg.create_pool(
        targets.database_url(),
        min_size=2,
        max_size=20,
        server_settings={"search_path": f'"{schema}", public'},
    )

    # Force Tier 2b (semantic) off so the test does not require embeddings
    # or the pgvector extension. Tier 1.5 is disabled by passing context=None.
    # enqueue_outbox is stubbed to a noop because we do not create the
    # outbox table in the minimal schema.
    monkeypatch.setattr(pia, "embedding_provider", None, raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)

    async def _noop_enqueue_outbox(*args, **kwargs):
        return None

    monkeypatch.setattr(pia, "enqueue_outbox", _noop_enqueue_outbox, raising=False)

    novel_name = f"NovelTestOrg_{uuid.uuid4().hex[:8]}"
    novel_type = "Organization"

    async def one_resolve() -> str:
        async with pool.acquire() as conn:
            entity = ExtractedEntity(
                name=novel_name,
                type=novel_type,
                confidence=1.0,
            )
            canonical, is_new = await resolve_entity(conn, entity, context=None)
            if is_new:
                await store_new_entity(
                    conn,
                    entity,
                    canonical,
                    document_rid="orn:test.doc:concurrency",
                    source="test",
                )
            return canonical.uri

    try:
        uris = await asyncio.gather(*(one_resolve() for _ in range(10)))
        distinct_uris = set(uris)
        assert len(distinct_uris) == 1, (
            f"All 10 callers must receive the same fuseki_uri; got {distinct_uris}"
        )

        async with pool.acquire() as conn:
            row_count = await conn.fetchval(
                "SELECT COUNT(*) FROM entity_registry "
                "WHERE entity_text = $1 AND entity_type = $2",
                novel_name,
                novel_type,
            )
        assert row_count == 1, (
            f"Expected exactly 1 row in entity_registry, got {row_count}"
        )
    finally:
        try:
            async with pool.acquire() as conn:
                await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        except Exception:
            pass
        await pool.close()
