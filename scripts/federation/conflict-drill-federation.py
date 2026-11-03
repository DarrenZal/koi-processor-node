#!/usr/bin/env python3
"""
ADR-001 Conflict Drill — Layer B: KOI-net Event Replay

Tests the federation behavior by constructing KOI-net event envelopes
with conflicting assertions from simulated peers and feeding them through
the event-processing pipeline.

Determinism: DRILL_SEED=42 → fixed UUIDs via uuid5.
Isolation: All operations in a transaction that rolls back.
Rerunnable: Running twice produces identical results.

Usage:
    python scripts/federation/conflict-drill-federation.py
    POSTGRES_URL=postgresql://... python scripts/federation/conflict-drill-federation.py
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
NAMESPACE = uuid.UUID("b2c3d4e5-f6a7-8901-bcde-f12345678901")


def seeded_uuid(label: str) -> uuid.UUID:
    """Deterministic UUID from seed + label."""
    return uuid.uuid5(NAMESPACE, f"{DRILL_SEED}:{label}")


# Simulated peer nodes
PEERS = {
    "peer-a": {
        "node_rid": "orn:node:drill-fed-peer-a",
        "node_name": "Drill Peer A",
        "type_claim": "orn:type:organization",
    },
    "peer-b": {
        "node_rid": "orn:node:drill-fed-peer-b",
        "node_name": "Drill Peer B",
        "type_claim": "orn:type:project",
    },
    "peer-c": {
        "node_rid": "orn:node:drill-fed-peer-c",
        "node_name": "Drill Peer C",
        "type_claim": "orn:type:dao",
    },
}

SUBJECT = "orn:entity:regen-network"
PREDICATE = "has_type"


def build_event_envelope(peer_key: str) -> dict:
    """Build a KOI-net-style event envelope for an assertion.

    Simulates what a real peer would send via POST /koi-net/events/broadcast.
    The envelope contains the assertion data that the event processor
    would extract and insert into assertion_history.
    """
    peer = PEERS[peer_key]
    event_id = seeded_uuid(f"fed-event-{peer_key}")

    return {
        "event_id": str(event_id),
        "rid": f"orn:assertion:{peer_key}-regen-type",
        "event_type": "NEW",
        "source_node_rid": peer["node_rid"],
        "manifest": {
            "rid": f"orn:assertion:{peer_key}-regen-type",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sha256_hash": f"drill-hash-{peer_key}",
        },
        "contents": {
            "assertion": {
                "subject": SUBJECT,
                "predicate": PREDICATE,
                "object_uri": peer["type_claim"],
            },
        },
    }


async def process_event_to_assertion(conn, envelope: dict) -> asyncpg.Record:
    """Simulate the event processing pipeline's assertion insertion.

    In production, the koi_net_router receives events via broadcast/poll
    and the processing logic extracts assertion data from the envelope.
    Here we replicate that insertion path using the same SQL.
    """
    assertion = envelope["contents"]["assertion"]
    assertion_id = seeded_uuid(f"assertion-{envelope['source_node_rid']}")
    event_id = uuid.UUID(envelope["event_id"])

    return await conn.fetchrow(
        """
        INSERT INTO assertion_history (
            assertion_id, subject, predicate, object_uri, object_literal,
            asserted_by_node_rid, source_event_id, source_node_rid
        ) VALUES ($1, $2, $3, $4, NULL, $5, $6, $7)
        RETURNING *
        """,
        assertion_id,
        assertion["subject"],
        assertion["predicate"],
        assertion["object_uri"],
        envelope["source_node_rid"],
        event_id,
        envelope["source_node_rid"],
    )


async def test_event_envelope_processing(conn) -> dict:
    """Process event envelopes from 3 simulated peers with conflicting assertions."""
    result = {"test": "event_envelope_processing", "status": "FAIL", "details": ""}
    try:
        envelopes = [build_event_envelope(k) for k in ["peer-a", "peer-b", "peer-c"]]

        rows = []
        for env in envelopes:
            row = await process_event_to_assertion(conn, env)
            rows.append(row)
            assert row is not None, f"Insert failed for {env['source_node_rid']}"

        assert len(rows) == 3, f"Expected 3 inserted assertions, got {len(rows)}"

        result["status"] = "PASS"
        result["details"] = "3 event envelopes processed into assertion_history"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_poller_idempotency(conn) -> dict:
    """Replay same events — assert no duplicates created."""
    result = {"test": "poller_idempotency", "status": "FAIL", "details": ""}
    try:
        envelopes = [build_event_envelope(k) for k in ["peer-a", "peer-b", "peer-c"]]

        duplicates = 0
        for env in envelopes:
            # Use savepoint so expected errors don't abort the transaction
            sp = conn.transaction()
            await sp.start()
            try:
                await process_event_to_assertion(conn, env)
                await sp.commit()
                duplicates += 1
            except asyncpg.UniqueViolationError:
                await sp.rollback()  # Rollback savepoint, outer tx continues

        assert duplicates == 0, f"{duplicates} duplicate(s) were incorrectly accepted"

        # Verify exactly 3 assertions exist
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM assertion_history WHERE subject = $1",
            SUBJECT,
        )
        assert count == 3, f"Expected 3 assertions, got {count}"

        result["status"] = "PASS"
        result["details"] = "All 3 replays correctly rejected by dedup index"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_provenance_from_envelope(conn) -> dict:
    """Verify source_event_id + source_node_rid populated from envelope."""
    result = {"test": "provenance_from_envelope", "status": "FAIL", "details": ""}
    try:
        rows = await conn.fetch(
            """
            SELECT source_event_id, source_node_rid, asserted_by_node_rid
            FROM assertion_history
            WHERE subject = $1
            ORDER BY asserted_by_node_rid
            """,
            SUBJECT,
        )

        for row in rows:
            assert row["source_event_id"] is not None, \
                f"Missing source_event_id for {row['asserted_by_node_rid']}"
            assert row["source_node_rid"] is not None, \
                f"Missing source_node_rid for {row['asserted_by_node_rid']}"
            # source_node_rid should match asserted_by_node_rid (peer is the asserter)
            assert row["source_node_rid"] == row["asserted_by_node_rid"], \
                f"Provenance mismatch: source={row['source_node_rid']} != asserter={row['asserted_by_node_rid']}"

        result["status"] = "PASS"
        result["details"] = "All assertions have correct provenance from envelopes"
    except Exception as e:
        result["details"] = str(e)
    return result


async def test_end_to_end_query(conn) -> dict:
    """End-to-end: query assertion_history shows all 3 claims with correct provenance."""
    result = {"test": "end_to_end_query", "status": "FAIL", "details": ""}
    try:
        rows = await conn.fetch(
            """
            SELECT asserted_by_node_rid, object_uri, source_event_id, source_node_rid,
                   tx_recorded_at, tx_retracted_at
            FROM assertion_history
            WHERE subject = $1 AND predicate = $2 AND tx_retracted_at IS NULL
            ORDER BY asserted_by_node_rid
            """,
            SUBJECT, PREDICATE,
        )

        assert len(rows) == 3, f"Expected 3 active assertions, got {len(rows)}"

        expected_claims = {
            PEERS["peer-a"]["node_rid"]: PEERS["peer-a"]["type_claim"],
            PEERS["peer-b"]["node_rid"]: PEERS["peer-b"]["type_claim"],
            PEERS["peer-c"]["node_rid"]: PEERS["peer-c"]["type_claim"],
        }

        for row in rows:
            node = row["asserted_by_node_rid"]
            assert node in expected_claims, f"Unexpected node: {node}"
            assert row["object_uri"] == expected_claims[node], \
                f"Wrong type for {node}: expected {expected_claims[node]}, got {row['object_uri']}"
            assert row["source_event_id"] is not None
            assert row["source_node_rid"] is not None
            assert row["tx_recorded_at"] is not None
            assert row["tx_retracted_at"] is None

        result["status"] = "PASS"
        result["details"] = "All 3 claims with correct types and provenance verified end-to-end"
    except Exception as e:
        result["details"] = str(e)
    return result


async def run_drill():
    """Run all Layer B tests in a single rolled-back transaction."""
    conn = await asyncpg.connect(DB_URL)
    tx = conn.transaction()
    await tx.start()

    results = {}
    try:
        # Sequential: later tests depend on data from earlier ones
        r1 = await test_event_envelope_processing(conn)
        results["event_envelope_processing"] = r1["status"]

        r2 = await test_poller_idempotency(conn)
        results["poller_idempotency"] = r2["status"]

        r3 = await test_provenance_from_envelope(conn)
        results["provenance_from_envelope"] = r3["status"]

        r4 = await test_end_to_end_query(conn)
        results["end_to_end_query"] = r4["status"]

        for r in [r1, r2, r3, r4]:
            status_icon = "✓" if r["status"] == "PASS" else "✗"
            print(f"  {status_icon} {r['test']}: {r['status']} — {r['details']}")

    finally:
        await tx.rollback()
        await conn.close()

    return results


def main():
    print("=" * 60)
    print("ADR-001 Conflict Drill — Layer B: KOI-net Event Replay")
    print("=" * 60)
    print(f"  Seed: {DRILL_SEED}")
    print(f"  DB: {DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL}")
    print()

    results = asyncio.run(run_drill())

    overall = "PASS" if all(v == "PASS" for v in results.values()) else "FAIL"
    print()
    print(f"  Layer B overall: {overall}")

    output = {
        "layer": "B",
        "seed": DRILL_SEED,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": results,
        "overall": overall,
    }
    print()
    print(f"DRILL_JSON:{json.dumps(output)}")

    return 0 if overall == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
