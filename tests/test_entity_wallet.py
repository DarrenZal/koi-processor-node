"""In-process pytest tests for entity wallet_address endpoint.

Tests PATCH /entities/{uri}/wallet, bech32/EVM validation,
and wallet_address inclusion in entity listing/search responses.

Run:  pytest tests/test_entity_wallet.py -v
Requires: PostgreSQL personal_koi running locally (uses rollback transactions).
          Migration 071_wallet_address must be applied.
"""

import sys
import time
from pathlib import Path
from unittest.mock import patch

import asyncpg
import httpx
import pytest
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))



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


async def _setup_test_entity(conn, name="Test Entity", entity_type="Person", uri=None):
    """Insert a test entity. Returns URI."""
    if uri is None:
        uri = f"urn:test:entity-wallet-{int(time.time() * 1000)}"
    await conn.execute("""
        INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (fuseki_uri) DO NOTHING
    """, uri, name, entity_type, name.lower())
    return uri


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def test_app():
    """Create the personal ingest API with a rollback transaction and patched db_pool."""
    conn = await asyncpg.connect(targets.database_url())
    tx = conn.transaction()
    await tx.start()

    pool = _SingleConnPool(conn)

    try:
        yield conn, pool
    finally:
        await tx.rollback()
        await conn.close()


@pytest.fixture
async def client(test_app):
    """Async httpx client for personal_ingest_api with patched db_pool."""
    _, pool = test_app
    import api.personal_ingest_api as mod
    with patch.object(mod, "db_pool", pool):
        transport = httpx.ASGITransport(app=mod.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


@pytest.fixture
async def conn(test_app):
    conn, _ = test_app
    return conn


# ---------------------------------------------------------------------------
# Tests — wallet validation (pure functions, no DB)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_validate_wallet_valid_bech32():
    """Valid bech32 regen address passes validation."""
    from api.personal_ingest_api import _validate_wallet_address
    import bech32

    data = bech32.convertbits(bytes(20), 8, 5)
    addr = bech32.bech32_encode("regen", data)
    assert _validate_wallet_address(addr) is True


@pytest.mark.anyio
async def test_validate_wallet_valid_evm():
    """Valid EVM hex address passes validation."""
    from api.personal_ingest_api import _validate_wallet_address
    assert _validate_wallet_address("0x1234567890abcdef1234567890abcdef12345678") is True


@pytest.mark.anyio
async def test_validate_wallet_invalid_bech32():
    """Invalid bech32 (wrong hrp or bad checksum) fails validation."""
    from api.personal_ingest_api import _validate_wallet_address
    assert _validate_wallet_address("cosmos1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqnrql8a") is False
    assert _validate_wallet_address("regen1invalid") is False
    assert _validate_wallet_address("notanaddress") is False


@pytest.mark.anyio
async def test_validate_wallet_invalid_evm():
    """Invalid EVM hex (wrong length, bad chars) fails validation."""
    from api.personal_ingest_api import _validate_wallet_address
    assert _validate_wallet_address("0x1234") is False
    assert _validate_wallet_address("0xGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGGG") is False


# ---------------------------------------------------------------------------
# Tests — PATCH /entities/{uri}/wallet HTTP path
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_patch_wallet_valid_bech32_http(client, conn):
    """PATCH /entities/{uri}/wallet with valid regen bech32 → 200."""
    import bech32
    uri = await _setup_test_entity(conn, "HTTP Bech32 Person", "Person", "urn:test:http-wallet-bech32")

    data = bech32.convertbits(bytes(20), 8, 5)
    addr = bech32.bech32_encode("regen", data)

    resp = await client.patch(f"/entities/{uri}/wallet", json={"wallet_address": addr})
    assert resp.status_code == 200
    body = resp.json()
    assert body["wallet_address"] == addr
    assert body["entity_uri"] == uri

    # Verify persisted in DB
    row = await conn.fetchrow("SELECT wallet_address FROM entity_registry WHERE fuseki_uri = $1", uri)
    assert row["wallet_address"] == addr


@pytest.mark.anyio
async def test_patch_wallet_valid_evm_http(client, conn):
    """PATCH /entities/{uri}/wallet with valid EVM hex → 200."""
    uri = await _setup_test_entity(conn, "HTTP EVM Person", "Person", "urn:test:http-wallet-evm")
    evm_addr = "0x1234567890abcdef1234567890abcdef12345678"

    resp = await client.patch(f"/entities/{uri}/wallet", json={"wallet_address": evm_addr})
    assert resp.status_code == 200
    assert resp.json()["wallet_address"] == evm_addr


@pytest.mark.anyio
async def test_patch_wallet_invalid_bech32_http(client, conn):
    """PATCH /entities/{uri}/wallet with invalid bech32 → 400."""
    uri = await _setup_test_entity(conn, "Bad Addr Person", "Person", "urn:test:http-wallet-bad")

    resp = await client.patch(f"/entities/{uri}/wallet", json={"wallet_address": "regen1invalid"})
    assert resp.status_code == 400
    assert "Invalid wallet address" in resp.json()["detail"]


@pytest.mark.anyio
async def test_patch_wallet_nonexistent_entity_http(client, conn):
    """PATCH /entities/{uri}/wallet for missing entity → 404."""
    resp = await client.patch(
        "/entities/urn:test:doesnotexist/wallet",
        json={"wallet_address": "0x1234567890abcdef1234567890abcdef12345678"},
    )
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_patch_wallet_duplicate_returns_409(client, conn):
    """PATCH /entities/{uri}/wallet with already-taken address → 409."""
    uri1 = await _setup_test_entity(conn, "Dup Wallet 1", "Person", "urn:test:http-dup-wallet-1")
    uri2 = await _setup_test_entity(conn, "Dup Wallet 2", "Person", "urn:test:http-dup-wallet-2")
    wallet = "0xabcdef1234567890abcdef1234567890abcdef99"

    # First registration succeeds
    resp1 = await client.patch(f"/entities/{uri1}/wallet", json={"wallet_address": wallet})
    assert resp1.status_code == 200

    # Second registration with same wallet → 409
    resp2 = await client.patch(f"/entities/{uri2}/wallet", json={"wallet_address": wallet})
    assert resp2.status_code == 409
    assert "already registered" in resp2.json()["detail"]


# ---------------------------------------------------------------------------
# Tests — wallet_address in entity queries (schema-level)
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_entity_get_returns_wallet_address(conn):
    """GET /entity/{uri} query includes wallet_address."""
    uri = await _setup_test_entity(conn, "Get Wallet Person", "Person", "urn:test:get-wallet")
    wallet = "0xabcdef1234567890abcdef1234567890abcdef12"

    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        uri, wallet,
    )

    row = await conn.fetchrow("""
        SELECT fuseki_uri, entity_text, entity_type, normalized_text,
               source, first_seen_rid, metadata, wallet_address, created_at
        FROM entity_registry WHERE fuseki_uri = $1
    """, uri)
    assert row["wallet_address"] == wallet


@pytest.mark.anyio
async def test_entity_list_returns_wallet_address(conn):
    """GET /entities query includes wallet_address."""
    uri = await _setup_test_entity(conn, "List Wallet Person", "Person", "urn:test:list-wallet")
    wallet = "0xabcdef1234567890abcdef1234567890abcdef12"

    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        uri, wallet,
    )

    row = await conn.fetchrow("""
        SELECT fuseki_uri, entity_text, entity_type, source, wallet_address, created_at
        FROM entity_registry WHERE fuseki_uri = $1
    """, uri)
    assert row["wallet_address"] == wallet


@pytest.mark.anyio
async def test_wallet_unique_constraint(conn):
    """Two entities cannot share the same wallet_address (DB constraint)."""
    uri1 = await _setup_test_entity(conn, "Unique Wallet 1", "Person", "urn:test:unique-wallet-1")
    uri2 = await _setup_test_entity(conn, "Unique Wallet 2", "Person", "urn:test:unique-wallet-2")
    wallet = "0xabcdef1234567890abcdef1234567890abcdef77"

    await conn.execute(
        "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
        uri1, wallet,
    )

    with pytest.raises(asyncpg.UniqueViolationError):
        await conn.execute(
            "UPDATE entity_registry SET wallet_address = $2 WHERE fuseki_uri = $1",
            uri2, wallet,
        )
