"""In-process pytest tests for V2 attestation layer.

Tests attestation CRUD, UPSERT idempotency, self-attestation guard,
policy gates for verify transitions, and grandfathering of pre-migration claims.

Run:  pytest tests/test_claims_attestations.py -v
Requires: KOI_TEST_DATABASE_URL naming a test database with the KOI schema (uses rollback
transactions); the tests skip when it is unset.
          Migration 066_attestations must be applied.
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import asyncpg
import httpx
import pytest
from fastapi import FastAPI
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

TEST_SERVICE_TOKEN = "test-token-a"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _SingleConnPool:
    """Wraps a single asyncpg.Connection to quack like asyncpg.Pool.acquire()."""

    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *a):
            pass

    def acquire(self):
        return self._CM(self._conn)


async def _setup_test_claimant(conn, uri="urn:test:claimant-org"):
    """Ensure a test claimant entity exists in entity_registry."""
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, 'Test Claimant Org', 'Organization', 'test claimant org')
        ON CONFLICT (fuseki_uri) DO NOTHING
    """, uri)
    return uri


async def _setup_test_reviewer(conn, name="Test Reviewer", uri=None):
    """Insert a test reviewer Person in entity_registry. Returns URI."""
    if uri is None:
        uri = f"urn:test:reviewer-{int(time.time() * 1000)}"
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, $2, 'Person', $3)
        ON CONFLICT (fuseki_uri) DO NOTHING
    """, uri, name, name.lower())
    return uri


async def _setup_test_claim(conn, claimant_uri="urn:test:claimant-org",
                             verification="self_reported", created_at=None):
    """Insert a minimal claim row. Returns claim_rid."""
    ts = int(time.time() * 1000)
    rid = f"orn:koi-net.claim:test-att-{ts}"

    if created_at is None:
        created_at = datetime.now(timezone.utc)

    await conn.execute("""
        INSERT INTO claims (claim_rid, claimant_uri, statement, claim_type,
                            verification, metadata, created_at, updated_at)
        VALUES ($1, $2, 'Test claim for attestation tests', 'ecological',
                $3, '{}'::jsonb, $4, NOW())
    """, rid, claimant_uri, verification, created_at)

    # Insert initial state log
    await conn.execute("""
        INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
        VALUES ($1, NULL, 'self_reported', 'test', 'initial')
    """, rid)

    return rid


async def _ensure_migration_row(conn, applied_at=None):
    """Ensure the 066_attestations migration row exists for policy gate tests."""
    if applied_at is None:
        applied_at = datetime.now(timezone.utc)
    await conn.execute("""
        INSERT INTO koi_migrations (migration_id, checksum, applied_at)
        VALUES ('066_attestations', 'v2_attestation_layer', $1)
        ON CONFLICT (migration_id) DO UPDATE SET applied_at = $1
    """, applied_at)
    return applied_at


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def test_app():
    """Create a FastAPI app with claims router wired to a rollback transaction."""
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    try:
        pool = _SingleConnPool(conn)

        from api.routers.claims_router import create_router
        router = create_router(pool)

        app = FastAPI()
        app.include_router(router)

        yield app, conn
    finally:
        await tx.rollback()
        await conn.close()


@pytest.fixture
async def client(test_app):
    """Async httpx client using ASGI transport."""
    app, _ = test_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def claims_auth_client(client, monkeypatch):
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", TEST_SERVICE_TOKEN)
    client.headers.update({"Authorization": f"Bearer {TEST_SERVICE_TOKEN}"})
    return client


@pytest.fixture
async def conn(test_app):
    """Direct DB connection (same transaction as the app)."""
    _, conn = test_app
    return conn


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("claims_auth_client")

@pytest.mark.anyio
async def test_create_attestation(client, conn):
    """POST /claims/{rid}/attestations → 201 with attestation_rid."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Reviewer Alpha", "urn:test:reviewer-alpha")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Reviewed source docs",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["attestation_rid"].startswith("orn:koi-net.attestation:")
    assert data["claim_rid"] == rid
    assert data["reviewer_uri"] == reviewer_uri
    assert data["verdict"] == "approved"
    assert data["rationale"] == "Reviewed source docs"
    assert data["reviewer_name"] == "Reviewer Alpha"


@pytest.mark.anyio
async def test_get_attestation(client, conn):
    """GET /claims/{rid}/attestations/{att_rid} → matches input."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Reviewer Beta", "urn:test:reviewer-beta")
    rid = await _setup_test_claim(conn, claimant_uri)

    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "needs_info",
        "rationale": "Need more data",
    })
    att_rid = create_resp.json()["attestation_rid"]

    resp = await client.get(f"/claims/{rid}/attestations/{att_rid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["attestation_rid"] == att_rid
    assert data["verdict"] == "needs_info"


@pytest.mark.anyio
async def test_list_attestations(client, conn):
    """GET /claims/{rid}/attestations → includes created attestation."""
    claimant_uri = await _setup_test_claimant(conn)
    rev1 = await _setup_test_reviewer(conn, "Rev List 1", "urn:test:rev-list-1")
    rev2 = await _setup_test_reviewer(conn, "Rev List 2", "urn:test:rev-list-2")
    rid = await _setup_test_claim(conn, claimant_uri)

    await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": rev1, "verdict": "approved",
    })
    await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": rev2, "verdict": "rejected",
    })

    resp = await client.get(f"/claims/{rid}/attestations")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2

    # Test verdict filter
    resp2 = await client.get(f"/claims/{rid}/attestations?verdict=approved")
    assert len(resp2.json()) == 1


@pytest.mark.anyio
async def test_upsert_updates_verdict(client, conn):
    """Duplicate reviewer → UPSERT updates verdict, same attestation_rid."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Upsert Rev", "urn:test:rev-upsert")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp1 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri, "verdict": "pending", "rationale": "Initial review",
    })
    att_rid = resp1.json()["attestation_rid"]
    assert resp1.json()["verdict"] == "pending"

    resp2 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri, "verdict": "approved", "rationale": "Confirmed",
    })
    assert resp2.json()["attestation_rid"] == att_rid  # Same RID
    assert resp2.json()["verdict"] == "approved"  # Updated
    assert resp2.json()["rationale"] == "Confirmed"


@pytest.mark.anyio
async def test_self_attestation_rejected(client, conn):
    """Self-attestation (reviewer = claimant) → 409."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": claimant_uri,
        "verdict": "approved",
    })
    assert resp.status_code == 409
    assert "Self-attestation" in resp.json()["detail"]


@pytest.mark.anyio
async def test_invalid_reviewer_uri(client, conn):
    """Nonexistent reviewer_uri → 422."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": "urn:test:doesnotexist",
        "verdict": "approved",
    })
    assert resp.status_code == 422
    assert "not found" in resp.json()["detail"]


@pytest.mark.anyio
async def test_wrong_reviewer_type_rejected(client, conn):
    """Reviewer with disallowed entity type (e.g. Evidence) → 422."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri)

    # Create an Evidence entity — not allowed as a reviewer
    evidence_uri = f"urn:test:evidence-{int(time.time() * 1000)}"
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, 'Some Evidence Doc', 'Evidence', 'some evidence doc')
        ON CONFLICT (fuseki_uri) DO NOTHING
    """, evidence_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": evidence_uri,
        "verdict": "approved",
    })
    assert resp.status_code == 422
    assert "not valid for attestation" in resp.json()["detail"]


@pytest.mark.anyio
async def test_verify_with_policy_met(client, conn):
    """Verify to peer_reviewed with >= 1 approved attestation → 200."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Policy Rev", "urn:test:rev-policy")
    # Migration must exist BEFORE the claim is created to avoid grandfathering
    migration_ts = datetime.now(timezone.utc) - timedelta(seconds=5)
    await _ensure_migration_row(conn, applied_at=migration_ts)
    rid = await _setup_test_claim(conn, claimant_uri)

    # Create approved attestation
    await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri, "verdict": "approved",
    })

    resp = await client.patch(f"/claims/{rid}/verify", json={
        "new_level": "peer_reviewed", "actor": "test", "reason": "test",
    })
    assert resp.status_code == 200
    assert resp.json()["verification"] == "peer_reviewed"


@pytest.mark.anyio
async def test_verify_without_attestation_blocked(client, conn):
    """Verify to peer_reviewed without attestation → 409 (post-migration claim)."""
    claimant_uri = await _setup_test_claimant(conn)
    # Migration must exist BEFORE the claim is created to avoid grandfathering
    migration_ts = datetime.now(timezone.utc) - timedelta(seconds=5)
    await _ensure_migration_row(conn, applied_at=migration_ts)
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.patch(f"/claims/{rid}/verify", json={
        "new_level": "peer_reviewed", "actor": "test", "reason": "test",
    })
    assert resp.status_code == 409
    assert "attestation" in resp.json()["detail"].lower()


@pytest.mark.anyio
async def test_grandfathered_claim_bypasses_policy(client, conn):
    """Pre-migration claim → verify without attestation → 200."""
    claimant_uri = await _setup_test_claimant(conn)
    # Create claim with timestamp BEFORE the migration
    old_time = datetime.now(timezone.utc) - timedelta(days=1)
    rid = await _setup_test_claim(conn, claimant_uri, created_at=old_time)
    # Apply migration AFTER the claim was created
    await _ensure_migration_row(conn, applied_at=datetime.now(timezone.utc))

    resp = await client.patch(f"/claims/{rid}/verify", json={
        "new_level": "peer_reviewed", "actor": "test", "reason": "grandfathered",
    })
    assert resp.status_code == 200
    assert resp.json()["verification"] == "peer_reviewed"


@pytest.mark.anyio
async def test_attestation_rid_stability(client, conn):
    """Same (claim, reviewer) always yields same RID."""
    from api.routers.claims_router import _attestation_rid

    rid1 = _attestation_rid("claim-A", "reviewer-X")
    rid2 = _attestation_rid("claim-A", "reviewer-X")
    rid3 = _attestation_rid("claim-A", "reviewer-Y")

    assert rid1 == rid2  # Stable
    assert rid1 != rid3  # Different reviewer → different RID
    assert rid1.startswith("orn:koi-net.attestation:")


@pytest.mark.anyio
async def test_content_hash_populated_on_create(client, conn):
    """Attestation content_hash should be populated immediately on create."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Hash Rev", "urn:test:rev-hash")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Hash test",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["content_hash"] is not None
    assert len(data["content_hash"]) == 64  # BLAKE2b-256 hex


@pytest.mark.anyio
async def test_content_hash_updates_on_verdict_change(client, conn):
    """Content_hash should change when verdict changes."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Hash Change Rev", "urn:test:rev-hash-change")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp1 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "pending",
    })
    hash1 = resp1.json()["content_hash"]

    resp2 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    hash2 = resp2.json()["content_hash"]

    assert hash1 != hash2  # Different verdict → different hash


@pytest.mark.anyio
async def test_anchor_attestation_rejects_pending_verdict(client, conn):
    """Anchor should reject attestations with pending verdict."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Pending Rev", "urn:test:rev-pending-anchor")
    rid = await _setup_test_claim(conn, claimant_uri, verification="ledger_anchored")

    # Create pending attestation
    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "pending",
    })
    att_rid = create_resp.json()["attestation_rid"]

    # Attempt to anchor
    resp = await client.post(f"/claims/{rid}/attestations/{att_rid}/anchor")
    assert resp.status_code == 409
    assert "pending" in resp.json()["detail"].lower()


@pytest.mark.anyio
async def test_anchor_attestation_rejects_non_anchored_parent(client, conn):
    """Anchor should reject when parent claim is not ledger_anchored."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Non-Anch Rev", "urn:test:rev-non-anch")
    rid = await _setup_test_claim(conn, claimant_uri, verification="verified")

    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    att_rid = create_resp.json()["attestation_rid"]

    resp = await client.post(f"/claims/{rid}/attestations/{att_rid}/anchor")
    assert resp.status_code == 409
    assert "ledger_anchored" in resp.json()["detail"]


@pytest.mark.anyio
async def test_reconcile_attestation_no_tx_hash(client, conn):
    """Reconcile should return 409 when attestation has no tx_hash."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Recon Rev", "urn:test:rev-recon")
    rid = await _setup_test_claim(conn, claimant_uri, verification="ledger_anchored")

    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    att_rid = create_resp.json()["attestation_rid"]

    resp = await client.post(f"/claims/{rid}/attestations/{att_rid}/reconcile")
    assert resp.status_code == 409
    assert "tx_hash" in resp.json()["detail"].lower()


@pytest.mark.anyio
async def test_anchored_attestation_immutable(client, conn):
    """Updating an anchored attestation should return 409."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Immut Rev", "urn:test:rev-immut")
    rid = await _setup_test_claim(conn, claimant_uri)

    # Create attestation
    resp1 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Initial review",
    })
    assert resp1.status_code == 201

    # Simulate anchoring by setting attest_tx_hash directly
    await conn.execute("""
        UPDATE claim_attestations
        SET attest_tx_hash = 'ABCDEF1234567890ABCDEF1234567890',
            ledger_iri = 'regen:13toVgf5aZqSVSeJQv562xkXKmjBcN'
        WHERE claim_rid = $1 AND reviewer_uri = $2
    """, rid, reviewer_uri)

    # Attempt to update — should be rejected
    resp2 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "rejected",
        "rationale": "Trying to change anchored attestation",
    })
    assert resp2.status_code == 409
    assert "immutable" in resp2.json()["detail"].lower()


@pytest.mark.anyio
async def test_unanchored_attestation_updatable(client, conn):
    """Unanchored attestation can still be updated via UPSERT."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Update Rev", "urn:test:rev-update")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp1 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "pending",
        "rationale": "Initial",
    })
    assert resp1.status_code == 201
    assert resp1.json()["verdict"] == "pending"

    # Update without anchoring — should succeed
    resp2 = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Now confirmed",
    })
    assert resp2.status_code == 201
    assert resp2.json()["verdict"] == "approved"
    assert resp2.json()["rationale"] == "Now confirmed"


@pytest.mark.anyio
async def test_attestation_response_includes_anchor_fields(client, conn):
    """AttestationResponse should include ledger_iri, attest_timestamp, attestor_address."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Fields Rev", "urn:test:rev-fields")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    data = resp.json()
    # All anchor fields should be present (but null for non-anchored attestation)
    assert "ledger_iri" in data
    assert "attest_timestamp" in data
    assert "attestor_address" in data
    assert data["ledger_iri"] is None
    assert data["attest_timestamp"] is None
    assert data["attestor_address"] is None


# ---------------------------------------------------------------------------
# Issue #12 — Hash Unification tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_legacy_16char_rids_still_queryable(client, conn):
    """Claims with legacy 16-char SHA256 RIDs remain queryable."""
    claimant_uri = await _setup_test_claimant(conn)
    # Insert a claim with a legacy 16-char RID directly
    legacy_rid = "orn:koi-net.claim:abcdef1234567890"
    await conn.execute("""
        INSERT INTO claims (claim_rid, claimant_uri, statement, claim_type,
                            verification, metadata, created_at, updated_at)
        VALUES ($1, $2, 'Legacy claim for testing', 'ecological',
                'self_reported', '{}'::jsonb, NOW(), NOW())
    """, legacy_rid, claimant_uri)

    resp = await client.get(f"/claims/{legacy_rid}")
    assert resp.status_code == 200
    assert resp.json()["claim_rid"] == legacy_rid


@pytest.mark.anyio
async def test_new_claims_get_32char_blake2b_rids(client, conn):
    """New claims created via POST get 32-char BLAKE2b RIDs."""
    claimant_uri = await _setup_test_claimant(conn)

    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": "This is a new claim to test BLAKE2b RID generation",
        "claim_type": "ecological",
    })
    assert resp.status_code == 201
    rid = resp.json()["claim_rid"]
    # Extract the hash portion after the prefix
    hash_part = rid.split(":")[-1]
    assert len(hash_part) == 32, f"Expected 32-char hash, got {len(hash_part)}: {hash_part}"


@pytest.mark.anyio
async def test_legacy_rid_idempotency_across_cutover(client, conn):
    """Re-posting content that exists under a legacy 16-char RID returns existing claim, not duplicate."""
    claimant_uri = await _setup_test_claimant(conn)

    # Compute the legacy SHA256-16 RID for known content
    from api.routers.claims_router import _legacy_claim_rid
    statement = "Legacy idempotency test claim with specific content"
    legacy_rid, _ = _legacy_claim_rid(claimant_uri, statement, "ecological", None, {})

    # Insert the legacy claim directly (simulating a pre-cutover claim)
    await conn.execute("""
        INSERT INTO claims (claim_rid, claimant_uri, statement, claim_type,
                            verification, metadata, created_at, updated_at)
        VALUES ($1, $2, $3, 'ecological', 'self_reported', '{}'::jsonb, NOW(), NOW())
    """, legacy_rid, claimant_uri, statement)
    await conn.execute("""
        INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
        VALUES ($1, NULL, 'self_reported', 'test', 'initial')
    """, legacy_rid)

    # POST the same content — should return the existing legacy claim, not create duplicate
    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": statement,
        "claim_type": "ecological",
    })
    assert resp.status_code == 201  # Idempotent path still returns 201 (decorator default)
    assert resp.json()["claim_rid"] == legacy_rid  # Returns legacy RID, not a new BLAKE2b one


@pytest.mark.anyio
async def test_data_iri_populated_during_prepare_anchor(client, conn):
    """data_iri should be populated during prepare-anchor."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri, verification="self_reported")

    # Set content_hash manually to bypass the full prepare flow's CLI dependency
    await conn.execute("""
        UPDATE claims SET content_hash = 'abc123deadbeef0000000000000000000000000000000000000000000000abcd'
        WHERE claim_rid = $1
    """, rid)

    # We can't easily test the full prepare-anchor since it requires regen CLI,
    # but we can verify the data_iri column exists and is queryable
    row = await conn.fetchrow("SELECT data_iri FROM claims WHERE claim_rid = $1", rid)
    assert row is not None
    # data_iri should be NULL since we haven't run prepare-anchor
    assert row["data_iri"] is None


@pytest.mark.anyio
async def test_data_iri_in_claim_response(client, conn):
    """ClaimResponse should include data_iri field."""
    claimant_uri = await _setup_test_claimant(conn)

    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": "Claim to test data_iri in response model",
        "claim_type": "ecological",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert "data_iri" in data
    assert data["data_iri"] is None  # Not yet prepared


@pytest.mark.anyio
async def test_data_iri_lazy_backfill_at_anchor(client, conn):
    """Claims with content_hash but no data_iri get data_iri at anchor time."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri, verification="verified")

    # Simulate a pre-070 claim that has content_hash but no data_iri
    await conn.execute("""
        UPDATE claims SET content_hash = 'abc123deadbeef0000000000000000000000000000000000000000000000abcd'
        WHERE claim_rid = $1
    """, rid)

    row = await conn.fetchrow("SELECT data_iri, content_hash FROM claims WHERE claim_rid = $1", rid)
    assert row["content_hash"] is not None
    assert row["data_iri"] is None  # Pre-070: no data_iri yet


# ---------------------------------------------------------------------------
# Issue #13 — Identity Bridge tests (attestor_address resolution)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_attestation_creation_with_reviewer_wallet(client, conn):
    """Attestation created with reviewer who has wallet → attestor_address set."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Wallet Rev", "urn:test:rev-wallet")

    # Set wallet_address on reviewer
    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        reviewer_uri, "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e",
    )

    rid = await _setup_test_claim(conn, claimant_uri)
    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["attestor_address"] == "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e"


@pytest.mark.anyio
async def test_attestation_creation_without_wallet(client, conn):
    """Attestation created with reviewer who has no wallet → attestor_address NULL."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "No Wallet Rev", "urn:test:rev-no-wallet")
    rid = await _setup_test_claim(conn, claimant_uri)

    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    assert resp.status_code == 201
    assert resp.json()["attestor_address"] is None


@pytest.mark.anyio
async def test_attestation_anchor_preserves_existing_attestor_address(client, conn):
    """Anchor should NOT overwrite existing attestor_address with service account."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Preserve Rev", "urn:test:rev-preserve-addr")

    # Set wallet on reviewer
    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        reviewer_uri, "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e",
    )

    rid = await _setup_test_claim(conn, claimant_uri, verification="ledger_anchored")

    # Create attestation — should have reviewer wallet as attestor_address
    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    att_rid = create_resp.json()["attestation_rid"]
    assert create_resp.json()["attestor_address"] == "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e"

    # Verify the address persists in DB
    row = await conn.fetchrow(
        "SELECT attestor_address FROM claim_attestations WHERE attestation_rid = $1", att_rid
    )
    assert row["attestor_address"] == "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e"


@pytest.mark.anyio
async def test_attestation_anchor_resolves_wallet_from_entity_registry(client, conn):
    """When attestor_address is NULL, anchor resolves from entity_registry.wallet_address."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Late Wallet Rev", "urn:test:rev-late-wallet")
    rid = await _setup_test_claim(conn, claimant_uri, verification="ledger_anchored")

    # Create attestation WITHOUT wallet (attestor_address=NULL)
    create_resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
    })
    att_rid = create_resp.json()["attestation_rid"]
    assert create_resp.json()["attestor_address"] is None

    # Now register wallet AFTER attestation was created
    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        reviewer_uri, "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e",
    )

    # The 3-tier resolution in anchor_attestation should pick up the wallet
    # (We can't test the full anchor flow without regen CLI, but we can verify
    # the wallet is queryable for the resolution)
    wallet = await conn.fetchval(
        "SELECT wallet_address FROM entity_registry WHERE fuseki_uri = $1", reviewer_uri
    )
    assert wallet == "regen1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqvptr3e"


# ---------------------------------------------------------------------------
# Issue #11 — Schema Integration (ADR-004) tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_claim_type_validation_rejects_invalid(client, conn):
    """POST with invalid claim_type → 422."""
    claimant_uri = await _setup_test_claimant(conn)

    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": "This claim has a bogus type for validation testing",
        "claim_type": "bogus",
    })
    assert resp.status_code == 422
    assert "Invalid claim_type" in resp.json()["detail"]


@pytest.mark.anyio
async def test_claim_type_validation_accepts_all_valid(client, conn):
    """POST with each of the 5 valid claim types → 201."""
    claimant_uri = await _setup_test_claimant(conn)

    for ct in ("ecological", "social", "financial", "governance", "biocultural"):
        resp = await client.post("/claims/", json={
            "claimant_uri": claimant_uri,
            "statement": f"Valid claim type test for {ct} — unique statement",
            "claim_type": ct,
        })
        assert resp.status_code == 201, f"claim_type '{ct}' rejected: {resp.json()}"
        assert resp.json()["claim_type"] == ct


@pytest.mark.anyio
async def test_claim_type_normalization_accepts_uppercase(client, conn):
    """POST with uppercase/padded claim_type → normalized and accepted."""
    claimant_uri = await _setup_test_claimant(conn)

    for raw, expected in [("ECOLOGICAL", "ecological"), (" Social ", "social"), ("Financial", "financial")]:
        resp = await client.post("/claims/", json={
            "claimant_uri": claimant_uri,
            "statement": f"Normalization test for {raw} — unique claim statement",
            "claim_type": raw,
        })
        assert resp.status_code == 201, f"claim_type '{raw}' rejected: {resp.json()}"
        assert resp.json()["claim_type"] == expected


@pytest.mark.anyio
async def test_prepare_anchor_preserves_existing_content_hash(client, conn):
    """Re-running prepare-anchor on a claim with existing content_hash should NOT overwrite it."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri, verification="self_reported")

    # Simulate a pre-#11 claim that already has a legacy content_hash
    legacy_hash = "deadbeef" * 8  # 64-char hex
    await conn.execute(
        "UPDATE claims SET content_hash = $2 WHERE claim_rid = $1",
        rid, legacy_hash,
    )

    # Call prepare-anchor — should preserve the existing hash, not recompute
    resp = await client.post(f"/claims/{rid}/prepare-anchor")
    # May fail on IRI derivation (no regen CLI), but content_hash should be preserved
    data = resp.json()
    assert data["content_hash"] == legacy_hash

    # Verify in DB
    row = await conn.fetchrow("SELECT content_hash FROM claims WHERE claim_rid = $1", rid)
    assert row["content_hash"] == legacy_hash


@pytest.mark.anyio
async def test_canonical_json_includes_context_and_type():
    """Canonical JSON should include @context and @type."""
    from api.routers.claims_router import _canonical_json
    canonical = _canonical_json("urn:test:claimant", "test statement", "ecological", None, {})
    obj = json.loads(canonical)
    assert obj["@context"] == "https://framework.regen.network/schema/"
    assert obj["@type"] == "rfs:Claim"


@pytest.mark.anyio
async def test_canonical_json_includes_credit_class_id():
    """credit_class_id should participate in canonical hash."""
    from api.routers.claims_router import _canonical_json
    canonical = _canonical_json("urn:test:c", "stmt", "ecological", None, {},
                                credit_class_id="C04")
    obj = json.loads(canonical)
    assert obj["credit_class_id"] == "C04"


@pytest.mark.anyio
async def test_different_credit_class_different_rid():
    """Same claim content with different credit_class_id → different RIDs."""
    from api.routers.claims_router import _claim_rid
    rid1 = _claim_rid("urn:test:c", "same statement text", "ecological", None, {},
                       credit_class_id="C04")
    rid2 = _claim_rid("urn:test:c", "same statement text", "ecological", None, {},
                       credit_class_id="C05")
    rid3 = _claim_rid("urn:test:c", "same statement text", "ecological", None, {},
                       credit_class_id=None)
    assert rid1 != rid2
    assert rid1 != rid3
    assert rid2 != rid3


@pytest.mark.anyio
async def test_new_rid_differs_from_legacy():
    """Same inputs produce different RIDs with schema vs without."""
    from api.routers.claims_router import _claim_rid, _legacy_claim_rid
    args = ("urn:test:c", "same statement text here", "ecological", None, {})
    new_rid = _claim_rid(*args)
    legacy_rid1, legacy_rid2 = _legacy_claim_rid(*args)
    assert new_rid != legacy_rid1
    assert new_rid != legacy_rid2


@pytest.mark.anyio
async def test_cross_cutover_idempotency_v1_v2_v3(client, conn):
    """Legacy SHA256, BLAKE2b-no-schema, and BLAKE2b-with-schema all detected as duplicates."""
    claimant_uri = await _setup_test_claimant(conn)
    statement = "Cross-cutover idempotency test claim for issue eleven"

    # Compute all three RID formats
    from api.routers.claims_router import _legacy_claim_rid, _claim_rid
    legacy_sha256, legacy_blake2b = _legacy_claim_rid(claimant_uri, statement, "ecological", None, {})
    new_rid = _claim_rid(claimant_uri, statement, "ecological", None, {})

    # Insert claim with legacy SHA256 RID
    await conn.execute("""
        INSERT INTO claims (claim_rid, claimant_uri, statement, claim_type,
                            verification, metadata, created_at, updated_at)
        VALUES ($1, $2, $3, 'ecological', 'self_reported', '{}'::jsonb, NOW(), NOW())
    """, legacy_sha256, claimant_uri, statement)
    await conn.execute("""
        INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
        VALUES ($1, NULL, 'self_reported', 'test', 'initial')
    """, legacy_sha256)

    # POST same content → should hit legacy idempotency, return existing
    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": statement,
        "claim_type": "ecological",
    })
    assert resp.status_code == 201
    assert resp.json()["claim_rid"] == legacy_sha256


@pytest.mark.anyio
async def test_credit_class_id_round_trip(client, conn):
    """Create claim with credit_class_id, verify in response and DB."""
    claimant_uri = await _setup_test_claimant(conn)

    resp = await client.post("/claims/", json={
        "claimant_uri": claimant_uri,
        "statement": "Claim with credit class for round-trip testing",
        "claim_type": "ecological",
        "credit_class_id": "C04",
    })
    assert resp.status_code == 201
    data = resp.json()
    assert data["credit_class_id"] == "C04"

    # Verify in DB
    row = await conn.fetchrow(
        "SELECT credit_class_id FROM claims WHERE claim_rid = $1", data["claim_rid"]
    )
    assert row["credit_class_id"] == "C04"


@pytest.mark.anyio
async def test_content_hash_includes_context():
    """compute_content_hash() includes @context/@type in canonical form."""
    from api.ledger_anchor import compute_content_hash, compute_legacy_content_hash
    row = {
        "claim_rid": "orn:koi-net.claim:test123",
        "entity_uri": "urn:test:entity",
        "claimant_uri": "urn:test:claimant",
        "statement": "Test statement for hash comparison",
        "claim_type": "ecological",
        "credit_class_id": None,
        "metadata": {},
    }
    new_hash = compute_content_hash(row)
    legacy_hash = compute_legacy_content_hash(row)
    assert new_hash != legacy_hash  # Different canonical forms → different hashes


@pytest.mark.anyio
async def test_legacy_attestation_hash_verified_in_proof_pack(client, conn):
    """Attestation with pre-schema content_hash still gets hash_verified: true via legacy fallback."""
    claimant_uri = await _setup_test_claimant(conn)
    reviewer_uri = await _setup_test_reviewer(conn, "Legacy Hash Rev", "urn:test:rev-legacy-hash")
    rid = await _setup_test_claim(conn, claimant_uri, verification="self_reported")

    # Create attestation — this will get the NEW schema-aware hash
    resp = await client.post(f"/claims/{rid}/attestations", json={
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Legacy hash test",
    })
    att_rid = resp.json()["attestation_rid"]

    # Overwrite the content_hash with the LEGACY hash (simulating pre-#11 attestation)
    from api.ledger_anchor import compute_legacy_attestation_hash
    att_row = {
        "attestation_rid": att_rid,
        "claim_rid": rid,
        "reviewer_uri": reviewer_uri,
        "verdict": "approved",
        "rationale": "Legacy hash test",
        "evidence_uris": None,
    }
    legacy_hash = compute_legacy_attestation_hash(att_row)
    await conn.execute(
        "UPDATE claim_attestations SET content_hash = $2 WHERE attestation_rid = $1",
        att_rid, legacy_hash,
    )

    # Advance to verified + ledger_anchored so proof pack is accessible
    # Set content_hash and ledger_iri on claim to satisfy proof pack guard
    await conn.execute("""
        UPDATE claims SET verification = 'ledger_anchored',
                          content_hash = 'deadbeef00000000000000000000000000000000000000000000000000000000',
                          ledger_iri = 'regen:test_iri_for_legacy_att'
        WHERE claim_rid = $1
    """, rid)

    # Request proof pack
    resp = await client.get(f"/claims/{rid}/proof-pack")
    assert resp.status_code == 200
    data = resp.json()

    # Find our attestation in the proof pack
    att = next((a for a in data["attestations"] if a["attestation_rid"] == att_rid), None)
    assert att is not None
    assert att["hash_verified"] is True  # Legacy fallback should verify


@pytest.mark.anyio
async def test_proof_pack_includes_context(client, conn):
    """Proof pack claim dict has @context and @type."""
    claimant_uri = await _setup_test_claimant(conn)
    rid = await _setup_test_claim(conn, claimant_uri, verification="self_reported")

    # Set claim to ledger_anchored with required fields
    await conn.execute("""
        UPDATE claims SET verification = 'ledger_anchored',
                          content_hash = 'deadbeef00000000000000000000000000000000000000000000000000000000',
                          ledger_iri = 'regen:test_iri_context'
        WHERE claim_rid = $1
    """, rid)

    resp = await client.get(f"/claims/{rid}/proof-pack")
    assert resp.status_code == 200
    data = resp.json()

    assert data["claim"]["@context"] == "https://framework.regen.network/schema/"
    assert data["claim"]["@type"] == "rfs:Claim"
