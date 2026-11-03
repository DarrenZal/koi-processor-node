"""In-process pytest tests for anchor timeout, 202 pending, and reconcile branches.

Monkeypatches broadcast_anchor / query_tx_status / verify_anchor_onchain to
exercise all new code paths deterministically without live network dependency.

Run:  pytest tests/test_claims_reconcile.py -v
Requires: PostgreSQL personal_koi running locally (uses rollback transactions).
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

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


async def _setup_test_claim(conn, *, verification="verified", tx_hash=None, ledger_iri=None):
    """Insert a minimal claim row for testing. Returns claim_rid."""
    import time
    ts = int(time.time() * 1000)
    rid = f"orn:koi-net.claim:test-reconcile-{ts}"

    await conn.execute("""
        INSERT INTO claims (claim_rid, claimant_uri, statement, claim_type,
                            verification, metadata, content_hash, tx_hash, ledger_iri,
                            created_at, updated_at)
        VALUES ($1, 'urn:test:claimant', 'Test claim for reconcile', 'ecological',
                $2, '{}'::jsonb, 'abc123hash', $3, $4, NOW(), NOW())
    """, rid, verification, tx_hash, ledger_iri)

    # Insert initial state log entry
    await conn.execute("""
        INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
        VALUES ($1, NULL, 'self_reported', 'test', 'initial')
    """, rid)

    if verification in ("peer_reviewed", "verified", "ledger_anchored"):
        await conn.execute("""
            INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
            VALUES ($1, 'self_reported', 'peer_reviewed', 'test', 'test')
        """, rid)
    if verification in ("verified", "ledger_anchored"):
        await conn.execute("""
            INSERT INTO claim_state_log (claim_rid, from_state, to_state, actor, reason)
            VALUES ($1, 'peer_reviewed', 'verified', 'test', 'test')
        """, rid)

    return rid


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
# Anchor endpoint tests
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("claims_auth_client")

@pytest.mark.anyio
async def test_anchor_timeout_returns_202(client, conn):
    """When broadcast_anchor returns ready_to_anchor=False with tx_hash, expect 202."""
    rid = await _setup_test_claim(conn, verification="verified")

    mock_result = {
        "claim_rid": rid,
        "content_hash": "abc123hash",
        "ready_to_anchor": False,
        "reason": "Tx broadcast but confirmation timed out.",
        "ledger_iri": "regen:test-iri",
        "ledger_timestamp": None,
        "tx_hash": "AABBCC1234",
    }

    with patch("api.ledger_anchor.broadcast_anchor", return_value=mock_result):
        resp = await client.post(f"/claims/{rid}/anchor")

    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "pending"
    assert data["tx_hash"] == "AABBCC1234"
    assert data["ledger_iri"] == "regen:test-iri"


@pytest.mark.anyio
async def test_anchor_timeout_persists_tx_hash(client, conn):
    """On timeout, tx_hash and ledger_iri should be persisted to the claim row."""
    rid = await _setup_test_claim(conn, verification="verified")

    mock_result = {
        "claim_rid": rid,
        "content_hash": "abc123hash",
        "ready_to_anchor": False,
        "reason": "Timeout",
        "ledger_iri": "regen:persisted-iri",
        "ledger_timestamp": None,
        "tx_hash": "PERSIST_TX_123",
    }

    with patch("api.ledger_anchor.broadcast_anchor", return_value=mock_result):
        await client.post(f"/claims/{rid}/anchor")

    row = await conn.fetchrow("SELECT tx_hash, ledger_iri, verification FROM claims WHERE claim_rid = $1", rid)
    assert row["tx_hash"] == "PERSIST_TX_123"
    assert row["ledger_iri"] == "regen:persisted-iri"
    assert row["verification"] == "verified"  # NOT transitioned


@pytest.mark.anyio
async def test_anchor_prebroadcast_failure_returns_503(client, conn):
    """When broadcast fails before sending (no tx_hash), expect 503."""
    rid = await _setup_test_claim(conn, verification="verified")

    mock_result = {
        "claim_rid": rid,
        "content_hash": "abc123hash",
        "ready_to_anchor": False,
        "reason": "Key 'claims-service' not found in keyring.",
        "ledger_iri": "regen:test-iri",
        "ledger_timestamp": None,
    }

    with patch("api.ledger_anchor.broadcast_anchor", return_value=mock_result):
        resp = await client.post(f"/claims/{rid}/anchor")

    assert resp.status_code == 503


@pytest.mark.anyio
async def test_anchor_verify_fail_still_succeeds(client, conn):
    """When broadcast succeeds, anchor proceeds even if IRI verify fails (REST doesn't support data module)."""
    rid = await _setup_test_claim(conn, verification="verified")

    mock_broadcast = {
        "claim_rid": rid,
        "content_hash": "abc123hash",
        "ready_to_anchor": True,
        "ledger_iri": "regen:verify-fail-iri",
        "ledger_timestamp": "2026-03-09T00:00:00Z",
        "tx_hash": "VERIFY_FAIL_TX",
    }

    with patch("api.ledger_anchor.broadcast_anchor", return_value=mock_broadcast):
        resp = await client.post(f"/claims/{rid}/anchor")

    assert resp.status_code == 200
    data = resp.json()
    assert data["tx_hash"] == "VERIFY_FAIL_TX"

    row = await conn.fetchrow("SELECT verification FROM claims WHERE claim_rid = $1", rid)
    assert row["verification"] == "ledger_anchored"  # Tx confirmation is sufficient


@pytest.mark.anyio
async def test_anchor_full_success(client, conn):
    """When broadcast succeeds and on-chain verify passes, expect 200 + state transition."""
    rid = await _setup_test_claim(conn, verification="verified")

    mock_broadcast = {
        "claim_rid": rid,
        "content_hash": "abc123hash",
        "ready_to_anchor": True,
        "ledger_iri": "regen:success-iri",
        "ledger_timestamp": "2026-03-09T00:00:00Z",
        "tx_hash": "SUCCESS_TX_ABC",
    }

    with patch("api.ledger_anchor.broadcast_anchor", return_value=mock_broadcast), \
         patch("api.ledger_anchor.verify_anchor_onchain", return_value=True):
        resp = await client.post(f"/claims/{rid}/anchor")

    assert resp.status_code == 200
    data = resp.json()
    assert data["ledger_iri"] == "regen:success-iri"
    assert data["tx_hash"] == "SUCCESS_TX_ABC"

    row = await conn.fetchrow("SELECT verification FROM claims WHERE claim_rid = $1", rid)
    assert row["verification"] == "ledger_anchored"


# ---------------------------------------------------------------------------
# Reconcile endpoint tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_reconcile_not_found(client):
    """Reconcile on non-existent claim should return 404."""
    resp = await client.post("/claims/orn:koi-net.claim:doesnotexist/reconcile")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_reconcile_wrong_state(client, conn):
    """Reconcile on a claim not at 'verified' state should return 409."""
    rid = await _setup_test_claim(conn, verification="self_reported", tx_hash="TX123")
    resp = await client.post(f"/claims/{rid}/reconcile")
    assert resp.status_code == 409


@pytest.mark.anyio
async def test_reconcile_no_tx_hash(client, conn):
    """Reconcile on a verified claim without tx_hash should return 409."""
    rid = await _setup_test_claim(conn, verification="verified", tx_hash=None)
    resp = await client.post(f"/claims/{rid}/reconcile")
    assert resp.status_code == 409
    assert "tx_hash" in resp.json()["detail"]


@pytest.mark.anyio
async def test_reconcile_tx_confirmed_anchor_found(client, conn):
    """When tx is confirmed and anchor is on-chain, should transition to ledger_anchored."""
    rid = await _setup_test_claim(conn, verification="verified",
                                  tx_hash="CONFIRMED_TX", ledger_iri="regen:confirmed-iri")

    mock_tx = {"found": True, "code": 0, "raw_log": "", "timestamp": "2026-03-09T01:00:00Z"}

    with patch("api.ledger_anchor.query_tx_status", return_value=mock_tx), \
         patch("api.ledger_anchor.verify_anchor_onchain", return_value=True):
        resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "anchored"
    assert data["ledger_iri"] == "regen:confirmed-iri"

    row = await conn.fetchrow("SELECT verification FROM claims WHERE claim_rid = $1", rid)
    assert row["verification"] == "ledger_anchored"


@pytest.mark.anyio
async def test_reconcile_tx_confirmed_anchor_not_indexed(client, conn):
    """When tx is confirmed but anchor not queryable via REST, should still finalize (tx confirmation sufficient)."""
    rid = await _setup_test_claim(conn, verification="verified",
                                  tx_hash="CONFIRMED_TX_2", ledger_iri="regen:not-indexed-iri")

    mock_tx = {"found": True, "code": 0, "raw_log": "", "timestamp": "2026-03-09T01:00:00Z"}

    with patch("api.ledger_anchor.query_tx_status", return_value=mock_tx), \
         patch("api.ledger_anchor.verify_anchor_onchain", return_value=False):
        resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "anchored"
    assert data["tx_hash"] == "CONFIRMED_TX_2"

    row = await conn.fetchrow("SELECT verification, tx_hash FROM claims WHERE claim_rid = $1", rid)
    assert row["verification"] == "ledger_anchored"  # tx confirmation is sufficient
    assert row["tx_hash"] == "CONFIRMED_TX_2"  # preserved


@pytest.mark.anyio
async def test_reconcile_tx_failed(client, conn):
    """When tx failed on-chain, should clear tx_hash+ledger_iri and return failed."""
    rid = await _setup_test_claim(conn, verification="verified",
                                  tx_hash="FAILED_TX", ledger_iri="regen:failed-iri")

    mock_tx = {"found": True, "code": 5, "raw_log": "out of gas", "timestamp": None}

    with patch("api.ledger_anchor.query_tx_status", return_value=mock_tx):
        resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "failed"

    row = await conn.fetchrow("SELECT tx_hash, ledger_iri, verification FROM claims WHERE claim_rid = $1", rid)
    assert row["tx_hash"] is None  # cleared
    assert row["ledger_iri"] is None  # cleared
    assert row["verification"] == "verified"  # stays at verified


@pytest.mark.anyio
async def test_reconcile_tx_not_found(client, conn):
    """When tx not found (not indexed yet), should return pending with tx_hash preserved."""
    rid = await _setup_test_claim(conn, verification="verified",
                                  tx_hash="NOTFOUND_TX", ledger_iri="regen:notfound-iri")

    mock_tx = {"found": False, "code": None, "raw_log": "tx not found", "timestamp": None}

    with patch("api.ledger_anchor.query_tx_status", return_value=mock_tx):
        resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "pending"

    row = await conn.fetchrow("SELECT tx_hash FROM claims WHERE claim_rid = $1", rid)
    assert row["tx_hash"] == "NOTFOUND_TX"  # preserved


@pytest.mark.anyio
async def test_reconcile_no_regen_cli(client, conn):
    """When regen CLI is missing, query_tx_status returns found=False (never raises)."""
    rid = await _setup_test_claim(conn, verification="verified",
                                  tx_hash="NO_CLI_TX", ledger_iri="regen:no-cli-iri")

    mock_tx = {"found": False, "code": None,
               "raw_log": "regen CLI binary not found in PATH", "timestamp": None}

    with patch("api.ledger_anchor.query_tx_status", return_value=mock_tx):
        resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "pending"
    assert data["tx_hash"] == "NO_CLI_TX"  # preserved, not cleared


# ---------------------------------------------------------------------------
# Response model tests
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_claim_response_has_tx_hash(client, conn):
    """GET /claims/{rid} should include tx_hash field."""
    rid = await _setup_test_claim(conn, verification="verified", tx_hash="RESP_TX")
    resp = await client.get(f"/claims/{rid}")
    assert resp.status_code == 200
    data = resp.json()
    assert "tx_hash" in data
    assert data["tx_hash"] == "RESP_TX"


@pytest.mark.anyio
async def test_anchor_pending_response_model():
    """AnchorPendingResponse Pydantic model has expected fields."""
    from api.routers.claims_router import AnchorPendingResponse
    m = AnchorPendingResponse(
        claim_rid="test", content_hash="abc", tx_hash="TX",
        ledger_iri="regen:iri", status="pending", message="test msg",
    )
    d = m.model_dump()
    assert d["status"] == "pending"
    assert d["tx_hash"] == "TX"
    assert d["message"] == "test msg"


@pytest.mark.anyio
async def test_reconcile_response_model():
    """ReconcileResponse Pydantic model has expected fields."""
    from api.routers.claims_router import ReconcileResponse
    m = ReconcileResponse(
        claim_rid="test", status="anchored", tx_hash="TX",
        ledger_iri="regen:iri", ledger_timestamp="2026-03-09", message="done",
    )
    d = m.model_dump()
    assert d["status"] == "anchored"
    assert d["ledger_timestamp"] == "2026-03-09"


# ---------------------------------------------------------------------------
# Issue #12 — data_iri backfill during reconcile
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_reconcile_backfills_data_iri(client, conn):
    """Reconcile should populate data_iri from ledger_iri when data_iri is NULL."""
    rid = await _setup_test_claim(
        conn, verification="verified",
        tx_hash="RECONCILE_DATA_IRI_TEST_HASH",
        ledger_iri="regen:13toVgf5aZqSVSeJQv562xkXKmjBcNLLLLdata",
    )

    # Verify data_iri is NULL initially
    row = await conn.fetchrow("SELECT data_iri FROM claims WHERE claim_rid = $1", rid)
    assert row["data_iri"] is None

    with patch("api.ledger_anchor.query_tx_status") as mock_tx:
        mock_tx.return_value = {
            "found": True,
            "code": 0,
            "timestamp": "2026-03-12T12:00:00Z",
        }
        with patch("api.ledger_anchor.verify_anchor_onchain", return_value=True):
            resp = await client.post(f"/claims/{rid}/reconcile")

    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "anchored"

    # Verify data_iri was backfilled from ledger_iri
    row = await conn.fetchrow("SELECT data_iri FROM claims WHERE claim_rid = $1", rid)
    assert row["data_iri"] == "regen:13toVgf5aZqSVSeJQv562xkXKmjBcNLLLLdata"
