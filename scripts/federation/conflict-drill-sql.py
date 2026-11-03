#!/usr/bin/env python3
"""
ADR-001 Conflict Drill — Layer A: Direct SQL

Tests assertion_history schema correctness for federation conflict scenarios.

Determinism: DRILL_SEED=42 → fixed UUIDs via uuid5.
Isolation: All operations in a transaction that rolls back.
Rerunnable: Running twice produces identical results.

Usage:
    python scripts/federation/conflict-drill-sql.py
    POSTGRES_URL=postgresql://... python scripts/federation/conflict-drill-sql.py
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime, timezone

import asyncpg

DB_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://koi:@localhost:5432/personal_koi",
)

# Fixed seed for reproducible UUIDs
DRILL_SEED = int(os.getenv("DRILL_SEED", "42"))
NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def seeded_uuid(label: str) -> uuid.UUID:
    """Deterministic UUID from seed + label."""
    return uuid.uuid5(NAMESPACE, f"{DRILL_SEED}:{label}")


# Pre-generate all UUIDs for reproducibility
PEER_A_NODE = "orn:node:drill-peer-a"
PEER_B_NODE = "orn:node:drill-peer-b"
PEER_C_NODE = "orn:node:drill-peer-c"

EVENT_A = seeded_uuid("event-a")
EVENT_B = seeded_uuid("event-b")
EVENT_C = seeded_uuid("event-c")

ASSERTION_A = seeded_uuid("assertion-a")
ASSERTION_B = seeded_uuid("assertion-b")
ASSERTION_C = seeded_uuid("assertion-c")

SUBJECT = "orn:entity:regen-network"
PREDICATE = "has_type"

# Conflicting type assertions
TYPE_ORG = "orn:type:organization"
TYPE_PROJECT = "orn:type:project"
TYPE_DAO = "orn:type:dao"


async def insert_assertion(conn, assertion_id, subject, predicate, object_uri,
                           asserted_by, source_event_id=None, source_node_rid=None,
                           supersedes=None):
    """Insert an assertion, return the row."""
    return await conn.fetchrow(
        """
        INSERT INTO assertion_history (
            assertion_id, subject, predicate, object_uri, object_literal,
            asserted_by_node_rid, source_event_id, source_node_rid,
            supersedes_assertion_id
        ) VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8)
        RETURNING *
        """,
        assertion_id, subject, predicate, object_uri,
        asserted_by, source_event_id, source_node_rid, supersedes,
    )


async def test_c1_conflicting_claims(conn) -> dict:
    """C1: Three peers assert conflicting types — all preserved, no silent overwrite."""
    result = {"test": "C1_conflicting_claims", "status": "FAIL", "details": ""}
    try:
        # Peer A: Regen Network is ORGANIZATION
        await insert_assertion(
            conn, ASSERTION_A, SUBJECT, PREDICATE, TYPE_ORG,
            PEER_A_NODE, EVENT_A, PEER_A_NODE,
        )
        # Peer B: Regen Network is PROJECT
        await insert_assertion(
            conn, ASSERTION_B, SUBJECT, PREDICATE, TYPE_PROJECT,
            PEER_B_NODE, EVENT_B, PEER_B_NODE,
        )
        # Peer C: Regen Network is DAO
        await insert_assertion(
            conn, ASSERTION_C, SUBJECT, PREDICATE, TYPE_DAO,
            PEER_C_NODE, EVENT_C, PEER_C_NODE,
        )

        # Verify: all 3 active assertions exist
        rows = await conn.fetch(
            """
            SELECT asserted_by_node_rid, object_uri
            FROM assertion_history
            WHERE subject = $1 AND predicate = $2 AND tx_retracted_at IS NULL
            ORDER BY asserted_by_node_rid
            """,
            SUBJECT, PREDICATE,
        )
        assert len(rows) == 3, f"Expected 3 active assertions, got {len(rows)}"

        nodes = {r["asserted_by_node_rid"] for r in rows}
        assert nodes == {PEER_A_NODE, PEER_B_NODE, PEER_C_NODE}

        types = {r["object_uri"] for r in rows}
        assert types == {TYPE_ORG, TYPE_PROJECT, TYPE_DAO}

        result["status"] = "PASS"
        result["details"] = "3 conflicting type assertions preserved with distinct provenance"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_c2_provenance_replay(conn) -> dict:
    """C2: Full audit trail replay from assertion history."""
    result = {"test": "C2_provenance_replay", "status": "FAIL", "details": ""}
    try:
        # Insert assertions (reuse from C1 context — we're in same transaction)
        # Query full audit trail ordered by tx_recorded_at
        rows = await conn.fetch(
            """
            SELECT assertion_id, subject, predicate, object_uri,
                   asserted_by_node_rid, source_event_id, source_node_rid,
                   tx_recorded_at, tx_retracted_at
            FROM assertion_history
            WHERE subject = $1
            ORDER BY tx_recorded_at ASC
            """,
            SUBJECT,
        )

        # All 3 should have source_event_id and source_node_rid populated
        for row in rows:
            assert row["source_event_id"] is not None, \
                f"Missing source_event_id for {row['asserted_by_node_rid']}"
            assert row["source_node_rid"] is not None, \
                f"Missing source_node_rid for {row['asserted_by_node_rid']}"
            assert row["tx_recorded_at"] is not None, \
                f"Missing tx_recorded_at for {row['asserted_by_node_rid']}"

        # Verify replay ordering is deterministic (tx_recorded_at monotonic)
        timestamps = [r["tx_recorded_at"] for r in rows]
        assert timestamps == sorted(timestamps), "Audit trail not in chronological order"

        result["status"] = "PASS"
        result["details"] = f"Full audit trail of {len(rows)} assertions with provenance"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_c3_sovereign_edits(conn) -> dict:
    """C3: Retraction + supersedes chain for sovereign edit scenario."""
    result = {"test": "C3_sovereign_edits", "status": "FAIL", "details": ""}
    try:
        # Peer A decides to change their assertion from ORGANIZATION to DAO
        # Step 1: Retract original assertion
        await conn.execute(
            "UPDATE assertion_history SET tx_retracted_at = NOW() WHERE assertion_id = $1",
            ASSERTION_A,
        )

        # Step 2: Create correction that supersedes original
        correction_id = seeded_uuid("correction-a")
        correction_event = seeded_uuid("correction-event-a")
        await insert_assertion(
            conn, correction_id, SUBJECT, PREDICATE, TYPE_DAO,
            PEER_A_NODE, correction_event, PEER_A_NODE,
            supersedes=ASSERTION_A,
        )

        # Verify: original is retracted
        original = await conn.fetchrow(
            "SELECT tx_retracted_at FROM assertion_history WHERE assertion_id = $1",
            ASSERTION_A,
        )
        assert original["tx_retracted_at"] is not None, "Original not retracted"

        # Verify: correction references original via supersedes
        correction = await conn.fetchrow(
            "SELECT supersedes_assertion_id, object_uri FROM assertion_history WHERE assertion_id = $1",
            correction_id,
        )
        assert correction["supersedes_assertion_id"] == ASSERTION_A
        assert correction["object_uri"] == TYPE_DAO

        # Verify: active assertions now show Peer A as DAO (matching Peer C)
        active = await conn.fetch(
            """
            SELECT asserted_by_node_rid, object_uri
            FROM assertion_history
            WHERE subject = $1 AND predicate = $2 AND tx_retracted_at IS NULL
            ORDER BY asserted_by_node_rid
            """,
            SUBJECT, PREDICATE,
        )
        assert len(active) == 3, f"Expected 3 active assertions, got {len(active)}"

        # Peer A now asserts DAO (changed from ORG)
        peer_a_row = [r for r in active if r["asserted_by_node_rid"] == PEER_A_NODE][0]
        assert peer_a_row["object_uri"] == TYPE_DAO, \
            f"Peer A should now assert DAO, got {peer_a_row['object_uri']}"

        result["status"] = "PASS"
        result["details"] = "Retraction + supersedes chain verified; sovereign edit preserved"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_replay_idempotency(conn) -> dict:
    """Replay idempotency: same event replayed = no duplicate."""
    result = {"test": "replay_idempotency", "status": "FAIL", "details": ""}
    try:
        # Try to replay EVENT_B (already inserted in C1)
        # Use savepoint so the expected error doesn't abort the transaction
        sp = conn.transaction()
        await sp.start()
        try:
            await insert_assertion(
                conn, seeded_uuid("replay-b"), SUBJECT, PREDICATE, TYPE_PROJECT,
                PEER_B_NODE, EVENT_B, PEER_B_NODE,
            )
            await sp.commit()
            result["details"] = "Duplicate insert should have been rejected"
            return result
        except asyncpg.UniqueViolationError:
            await sp.rollback()  # Rollback savepoint, outer tx continues

        # Verify count unchanged
        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM assertion_history
            WHERE source_node_rid = $1 AND source_event_id = $2
            """,
            PEER_B_NODE, EVENT_B,
        )
        assert count == 1, f"Expected exactly 1 assertion for EVENT_B, got {count}"

        result["status"] = "PASS"
        result["details"] = "Replay dedup index correctly rejected duplicate event"
    except Exception as e:
        result["details"] = str(e)
    return result


async def run_drill():
    """Run all Layer A tests in a single rolled-back transaction."""
    conn = await asyncpg.connect(DB_URL)
    tx = conn.transaction()
    await tx.start()

    results = {}
    try:
        # Run tests sequentially (C2 and C3 depend on C1's data)
        r1 = await test_c1_conflicting_claims(conn)
        results["C1_conflicting_claims"] = r1["status"]

        r2 = await test_c2_provenance_replay(conn)
        results["C2_provenance_replay"] = r2["status"]

        r3 = await test_c3_sovereign_edits(conn)
        results["C3_sovereign_edits"] = r3["status"]

        r4 = await test_replay_idempotency(conn)
        results["replay_idempotency"] = r4["status"]

        # Print details
        for r in [r1, r2, r3, r4]:
            status_icon = "✓" if r["status"] == "PASS" else "✗"
            print(f"  {status_icon} {r['test']}: {r['status']} — {r['details']}")

    finally:
        await tx.rollback()
        await conn.close()

    return results


def main():
    print("=" * 60)
    print("ADR-001 Conflict Drill — Layer A: Direct SQL")
    print("=" * 60)
    print(f"  Seed: {DRILL_SEED}")
    print(f"  DB: {DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL}")
    print()

    results = asyncio.run(run_drill())

    overall = "PASS" if all(v == "PASS" for v in results.values()) else "FAIL"
    print()
    print(f"  Layer A overall: {overall}")

    # Return results dict for orchestrator
    output = {
        "layer": "A",
        "seed": DRILL_SEED,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": results,
        "overall": overall,
    }
    # Write to stdout as JSON on last line (orchestrator can capture)
    print()
    print(f"DRILL_JSON:{json.dumps(output)}")

    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
