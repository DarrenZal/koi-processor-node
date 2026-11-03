"""
Tests for assertion_history table (migration 051).

Validates bi-temporal constraints, immutability triggers, dedup indexes,
and object-type CHECK constraints.

Uses a transaction that rolls back after each test — no persistent data changes.
Requires KOI_TEST_DATABASE_URL naming a test database with the KOI schema and
migration 051 applied; the tests skip when it is unset.
"""

import uuid

import pytest
import asyncpg
from tests import koi_test_targets as targets



@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    """Single connection with a transaction that rolls back."""
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


# =============================================================================
# Helpers
# =============================================================================

async def insert_assertion(conn, **overrides):
    """Insert an assertion with sensible defaults, return the row."""
    defaults = {
        "assertion_id": uuid.uuid4(),
        "subject": "orn:test:entity-a",
        "predicate": "related_to",
        "object_uri": "orn:test:entity-b",
        "object_literal": None,
        "object_datatype": None,
        "object_lang": None,
        "asserted_by_node_rid": "orn:node:peer-a",
        "valid_from": None,
        "valid_to": None,
        "supersedes_assertion_id": None,
        "provenance_doc_rid": None,
        "source_event_id": None,
        "source_node_rid": None,
    }
    defaults.update(overrides)
    row = await conn.fetchrow(
        """
        INSERT INTO assertion_history (
            assertion_id, subject, predicate,
            object_uri, object_literal, object_datatype, object_lang,
            asserted_by_node_rid,
            valid_from, valid_to,
            supersedes_assertion_id, provenance_doc_rid,
            source_event_id, source_node_rid
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        RETURNING *
        """,
        defaults["assertion_id"],
        defaults["subject"],
        defaults["predicate"],
        defaults["object_uri"],
        defaults["object_literal"],
        defaults["object_datatype"],
        defaults["object_lang"],
        defaults["asserted_by_node_rid"],
        defaults["valid_from"],
        defaults["valid_to"],
        defaults["supersedes_assertion_id"],
        defaults["provenance_doc_rid"],
        defaults["source_event_id"],
        defaults["source_node_rid"],
    )
    return row


# =============================================================================
# tx_recorded_at immutability
# =============================================================================

@pytest.mark.anyio
async def test_tx_recorded_at_is_set_on_insert(conn):
    """tx_recorded_at should be auto-populated on INSERT."""
    row = await insert_assertion(conn)
    assert row["tx_recorded_at"] is not None


@pytest.mark.anyio
async def test_tx_recorded_at_immutable_rejects_update(conn):
    """UPDATE that changes tx_recorded_at must be rejected by trigger."""
    row = await insert_assertion(conn)
    with pytest.raises(asyncpg.RaiseError, match="tx_recorded_at is immutable"):
        await conn.execute(
            "UPDATE assertion_history SET tx_recorded_at = NOW() - INTERVAL '1 day' WHERE assertion_id = $1",
            row["assertion_id"],
        )


@pytest.mark.anyio
async def test_tx_recorded_at_noop_update_allowed(conn):
    """UPDATE that does not change tx_recorded_at should succeed."""
    row = await insert_assertion(conn)
    # Setting tx_recorded_at to same value is a no-op (IS DISTINCT FROM = false)
    await conn.execute(
        "UPDATE assertion_history SET tx_recorded_at = $2 WHERE assertion_id = $1",
        row["assertion_id"],
        row["tx_recorded_at"],
    )


# =============================================================================
# tx_retracted_at write-once
# =============================================================================

@pytest.mark.anyio
async def test_tx_retracted_at_null_to_timestamp_allowed(conn):
    """Setting tx_retracted_at from NULL to a timestamp should succeed."""
    row = await insert_assertion(conn)
    assert row["tx_retracted_at"] is None
    await conn.execute(
        "UPDATE assertion_history SET tx_retracted_at = NOW() WHERE assertion_id = $1",
        row["assertion_id"],
    )
    updated = await conn.fetchrow(
        "SELECT tx_retracted_at FROM assertion_history WHERE assertion_id = $1",
        row["assertion_id"],
    )
    assert updated["tx_retracted_at"] is not None


@pytest.mark.anyio
async def test_tx_retracted_at_change_after_set_rejected(conn):
    """Changing tx_retracted_at after it's been set must be rejected."""
    row = await insert_assertion(conn)
    await conn.execute(
        "UPDATE assertion_history SET tx_retracted_at = NOW() WHERE assertion_id = $1",
        row["assertion_id"],
    )
    with pytest.raises(asyncpg.RaiseError, match="tx_retracted_at is write-once"):
        await conn.execute(
            "UPDATE assertion_history SET tx_retracted_at = NOW() + INTERVAL '1 day' WHERE assertion_id = $1",
            row["assertion_id"],
        )


@pytest.mark.anyio
async def test_tx_retracted_at_before_recorded_rejected(conn):
    """tx_retracted_at before tx_recorded_at violates CHECK constraint."""
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            """
            INSERT INTO assertion_history (
                subject, predicate, object_uri, asserted_by_node_rid,
                tx_recorded_at, tx_retracted_at
            ) VALUES (
                'orn:test:s', 'pred', 'orn:test:o', 'orn:node:p',
                '2026-01-15T00:00:00Z', '2026-01-14T00:00:00Z'
            )
            """
        )


# =============================================================================
# Replay dedup (source_node_rid + source_event_id)
# =============================================================================

@pytest.mark.anyio
async def test_replay_dedup_rejects_duplicate(conn):
    """Same source_node_rid + source_event_id cannot create two assertions."""
    event_id = uuid.uuid4()
    await insert_assertion(
        conn,
        source_node_rid="orn:node:peer-x",
        source_event_id=event_id,
        subject="orn:test:entity-1",
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_assertion(
            conn,
            source_node_rid="orn:node:peer-x",
            source_event_id=event_id,
            subject="orn:test:entity-2",  # different subject, same event
        )


@pytest.mark.anyio
async def test_replay_dedup_allows_different_events(conn):
    """Different source_event_id from same node should succeed."""
    await insert_assertion(
        conn,
        source_node_rid="orn:node:peer-x",
        source_event_id=uuid.uuid4(),
        subject="orn:test:entity-1",
    )
    await insert_assertion(
        conn,
        source_node_rid="orn:node:peer-x",
        source_event_id=uuid.uuid4(),
        subject="orn:test:entity-2",
    )


@pytest.mark.anyio
async def test_replay_dedup_allows_null_source(conn):
    """NULL source_node_rid or source_event_id should not trigger dedup."""
    await insert_assertion(conn, source_node_rid=None, source_event_id=None)
    await insert_assertion(
        conn,
        assertion_id=uuid.uuid4(),
        source_node_rid=None,
        source_event_id=None,
        subject="orn:test:entity-other",
    )


# =============================================================================
# Active assertion dedup
# =============================================================================

@pytest.mark.anyio
async def test_active_dedup_rejects_duplicate_active(conn):
    """Same (subject, predicate, object, node) while both active -> rejected."""
    await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-a",
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await insert_assertion(
            conn,
            subject="orn:test:regen",
            predicate="has_type",
            object_uri="orn:type:org",
            asserted_by_node_rid="orn:node:peer-a",
        )


@pytest.mark.anyio
async def test_active_dedup_allows_different_nodes(conn):
    """Same triple from different nodes should succeed (conflicting claims)."""
    await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-a",
    )
    await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-b",
    )


@pytest.mark.anyio
async def test_active_dedup_allows_after_retraction(conn):
    """Retracted assertion should not block new active assertion."""
    row = await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-a",
    )
    # Retract it
    await conn.execute(
        "UPDATE assertion_history SET tx_retracted_at = NOW() WHERE assertion_id = $1",
        row["assertion_id"],
    )
    # New active assertion with same triple should succeed
    await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-a",
    )


@pytest.mark.anyio
async def test_conflicting_claims_preserved(conn):
    """Three peers asserting different types for same entity — all preserved."""
    for node, obj in [
        ("orn:node:peer-a", "orn:type:organization"),
        ("orn:node:peer-b", "orn:type:project"),
        ("orn:node:peer-c", "orn:type:dao"),
    ]:
        await insert_assertion(
            conn,
            subject="orn:test:regen-network",
            predicate="has_type",
            object_uri=obj,
            asserted_by_node_rid=node,
        )
    rows = await conn.fetch(
        "SELECT * FROM assertion_history WHERE subject = 'orn:test:regen-network' AND tx_retracted_at IS NULL"
    )
    assert len(rows) == 3
    nodes = {r["asserted_by_node_rid"] for r in rows}
    assert nodes == {"orn:node:peer-a", "orn:node:peer-b", "orn:node:peer-c"}


# =============================================================================
# Object type CHECK constraint
# =============================================================================

@pytest.mark.anyio
async def test_object_both_null_rejected(conn):
    """Both object_uri and object_literal NULL should be rejected."""
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_assertion(conn, object_uri=None, object_literal=None)


@pytest.mark.anyio
async def test_object_both_set_rejected(conn):
    """Both object_uri and object_literal set should be rejected."""
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_assertion(conn, object_uri="orn:test:x", object_literal="some value")


@pytest.mark.anyio
async def test_object_literal_with_datatype(conn):
    """Literal assertion with datatype should succeed."""
    row = await insert_assertion(
        conn,
        object_uri=None,
        object_literal="42",
        object_datatype="xsd:integer",
    )
    assert row["object_literal"] == "42"
    assert row["object_datatype"] == "xsd:integer"


@pytest.mark.anyio
async def test_object_literal_with_lang(conn):
    """Literal assertion with language tag should succeed."""
    row = await insert_assertion(
        conn,
        object_uri=None,
        object_literal="Regeneration",
        object_lang="en",
    )
    assert row["object_lang"] == "en"


@pytest.mark.anyio
async def test_datatype_without_literal_rejected(conn):
    """object_datatype set when object_literal is NULL should be rejected."""
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_assertion(
            conn,
            object_uri="orn:test:x",
            object_literal=None,
            object_datatype="xsd:string",
        )


@pytest.mark.anyio
async def test_lang_without_literal_rejected(conn):
    """object_lang set when object_literal is NULL should be rejected."""
    with pytest.raises(asyncpg.CheckViolationError):
        await insert_assertion(
            conn,
            object_uri="orn:test:x",
            object_literal=None,
            object_lang="en",
        )


# =============================================================================
# Valid time constraints
# =============================================================================

@pytest.mark.anyio
async def test_valid_to_before_valid_from_rejected(conn):
    """valid_to before valid_from violates CHECK constraint."""
    with pytest.raises(asyncpg.CheckViolationError):
        await conn.execute(
            """
            INSERT INTO assertion_history (
                subject, predicate, object_uri, asserted_by_node_rid,
                valid_from, valid_to
            ) VALUES (
                'orn:test:s', 'pred', 'orn:test:o', 'orn:node:p',
                '2026-03-01T00:00:00Z', '2026-02-01T00:00:00Z'
            )
            """
        )


@pytest.mark.anyio
async def test_valid_time_range_accepted(conn):
    """Valid time range with from < to should succeed."""
    row = await conn.fetchrow(
        """
        INSERT INTO assertion_history (
            subject, predicate, object_uri, asserted_by_node_rid,
            valid_from, valid_to
        ) VALUES (
            'orn:test:s', 'pred', 'orn:test:o', 'orn:node:p',
            '2026-01-01T00:00:00Z', '2026-06-01T00:00:00Z'
        ) RETURNING *
        """
    )
    assert row["valid_from"] is not None
    assert row["valid_to"] is not None


# =============================================================================
# Supersedes chain (provenance)
# =============================================================================

@pytest.mark.anyio
async def test_supersedes_chain(conn):
    """New assertion can reference a prior one via supersedes_assertion_id."""
    original = await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:org",
        asserted_by_node_rid="orn:node:peer-a",
    )
    # Retract original
    await conn.execute(
        "UPDATE assertion_history SET tx_retracted_at = NOW() WHERE assertion_id = $1",
        original["assertion_id"],
    )
    # Create correction that supersedes original
    correction = await insert_assertion(
        conn,
        subject="orn:test:regen",
        predicate="has_type",
        object_uri="orn:type:dao",
        asserted_by_node_rid="orn:node:peer-a",
        supersedes_assertion_id=original["assertion_id"],
    )
    assert correction["supersedes_assertion_id"] == original["assertion_id"]
    # Original is retracted, correction is active
    active = await conn.fetch(
        "SELECT * FROM assertion_history WHERE subject = 'orn:test:regen' AND tx_retracted_at IS NULL"
    )
    assert len(active) == 1
    assert active[0]["object_uri"] == "orn:type:dao"
