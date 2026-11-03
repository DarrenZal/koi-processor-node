"""Regression tests: the ``session_tokens`` schema contract behind auth_deps.

Why this file exists (2026-08-14):

  ``POST /entities/merge`` returned **HTTP 500** for every caller that presented
  a Bearer token, because ``session_tokens`` did not exist in the ``personal_koi``
  database:

      asyncpg.exceptions.UndefinedTableError: relation "session_tokens" does not exist

  Root cause was migration-ledger drift, not application code. Migrations
  016/017/018 were *stamped* into ``koi_migrations`` by
  ``scripts/stamp_baseline.py`` (which records rows without executing SQL) while
  the SQL had never actually run against this database. The legacy
  ``schema_migrations`` ledger stops at 012, confirming they were never applied.

  Two properties are locked in here so the drift cannot silently return:

  1. The migration *files* 016+017+018, applied in order, produce exactly the
     columns ``api/auth_deps.py::_validate_session_token`` selects.
  2. The dependency's *runtime* contract: an unknown token is a 401, never a 500.

  A deliberate negative control (``test_missing_table_reproduces_the_500``)
  asserts the original failure mode, so these tests are proven to detect the
  defect rather than merely passing alongside it.

  Also pinned: the service-token branch of ``make_service_token_auth`` returns
  **before** any database access. That is why the MCP ``merge_entities`` tool
  kept working throughout the outage — it authenticates with
  ``KOI_CLAIMS_SERVICE_TOKEN``, not a session token. It is NOT an auth bypass.

Convention follows tests/test_entity_retype.py: a throwaway schema whose
search_path deliberately excludes ``public``, so an unqualified reference to a
table missing from the schema errors loudly instead of resolving to real
personal_koi data. DB-gated on KOI_TEST_DATABASE_URL.
"""

from __future__ import annotations

import hashlib
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")

from fastapi import FastAPI  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from tests import koi_test_targets as targets

MIGRATIONS_DIR = ROOT / "migrations"

# The three migrations that together define the table auth_deps.py queries.
# 016 creates it, 017 adds token_hash (+ auth_requests), 018 drops the plaintext
# column. They must be applied in this order: 018 references auth_requests.
SESSION_TOKEN_MIGRATIONS = (
    "016_add_session_tokens.sql",
    "017_secure_auth_flow.sql",
    "018_remove_plain_token_from_session_tokens.sql",
)

_integration_skip = targets.requires_database

SVC_TOKEN = "test-session-auth-service-token"
SVC_AUTH = {"Authorization": f"Bearer {SVC_TOKEN}"}


def _hash_token(token: str) -> str:
    """Mirror api/auth_deps.py::_hash_token (SHA-256 hex)."""
    return hashlib.sha256(token.encode()).hexdigest()


async def _apply_session_token_migrations(conn) -> None:
    """Run 016 -> 017 -> 018 verbatim from disk into the current search_path."""
    for filename in SESSION_TOKEN_MIGRATIONS:
        sql = (MIGRATIONS_DIR / filename).read_text()
        await conn.execute(sql)


async def _create_minimal_entity_registry(conn) -> None:
    """Just enough of entity_registry for /entities/merge to reach its 404.

    The merge body looks up survivor/loser by fuseki_uri; with an empty table it
    returns 404 "survivor not found". That 404 is the signal that auth PASSED.
    """
    await conn.execute("""
        CREATE TABLE entity_registry (
            id           SERIAL PRIMARY KEY,
            fuseki_uri   TEXT UNIQUE NOT NULL,
            merged_into  TEXT
        )
    """)


class _SchemaHarness:
    """A throwaway schema + an ASGI client for the admin router bound to it."""

    def __init__(self, pool, client, schema):
        self.pool = pool
        self.client = client
        self.schema = schema


async def _build_harness(schema: str, *, with_session_tokens: bool):
    """Create the schema, optionally apply the migrations, return a live pool."""
    setup_pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        async with setup_pool.acquire() as conn:
            await conn.execute(f'CREATE SCHEMA "{schema}"')
            await conn.execute(f'SET search_path TO "{schema}"')
            await _create_minimal_entity_registry(conn)
            if with_session_tokens:
                await _apply_session_token_migrations(conn)
    finally:
        await setup_pool.close()

    # search_path = schema ONLY (no public) -> a missing table raises rather
    # than silently resolving to the real public.session_tokens.
    return await asyncpg.create_pool(
        targets.database_url(),
        min_size=1,
        max_size=4,
        server_settings={"search_path": f'"{schema}"'},
    )


async def _drop_schema(schema: str) -> None:
    pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def authed_schema(monkeypatch):
    """Schema WITH session_tokens present (the fixed state)."""
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", SVC_TOKEN)
    schema = f"authtok_{uuid.uuid4().hex[:10]}"
    pool = await _build_harness(schema, with_session_tokens=True)

    from api.routers.admin_router import create_router as create_admin_router

    app = FastAPI()
    app.include_router(create_admin_router(pool), prefix="/entities")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield _SchemaHarness(pool, client, schema)

    await pool.close()
    await _drop_schema(schema)


@pytest_asyncio.fixture
async def unmigrated_schema(monkeypatch):
    """Schema WITHOUT session_tokens — reproduces the reported outage."""
    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", SVC_TOKEN)
    schema = f"nosess_{uuid.uuid4().hex[:10]}"
    pool = await _build_harness(schema, with_session_tokens=False)

    from api.routers.admin_router import create_router as create_admin_router

    app = FastAPI()
    app.include_router(create_admin_router(pool), prefix="/entities")
    # raise_app_exceptions=False so the unhandled asyncpg error surfaces as a
    # 500 response, exactly as it does behind uvicorn in production.
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield _SchemaHarness(pool, client, schema)

    await pool.close()
    await _drop_schema(schema)


MERGE_BODY = {"survivor_uri": "urn:probe:survivor", "loser_uri": "urn:probe:loser"}


# ---------------------------------------------------------------------------
# 1. Migration content: does 016+017+018 produce what auth_deps.py selects?
# ---------------------------------------------------------------------------

@_integration_skip
@pytest.mark.asyncio
async def test_migrations_produce_the_columns_auth_deps_selects(authed_schema):
    """auth_deps selects user_email, expires_at, revoked_at WHERE token_hash=$1."""
    async with authed_schema.pool.acquire() as conn:
        cols = {
            r["column_name"]
            for r in await conn.fetch(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = 'session_tokens'
                """,
                authed_schema.schema,
            )
        }

    # Exactly the columns _validate_session_token depends on.
    assert {"token_hash", "user_email", "expires_at", "revoked_at"} <= cols

    # 018's security fix: the plaintext token column must be gone.
    assert "session_token" not in cols, (
        "migration 018 must drop the plaintext session_token column; "
        "long-lived storage holds hashes only"
    )


@_integration_skip
@pytest.mark.asyncio
async def test_auth_deps_query_executes_against_the_migrated_table(authed_schema):
    """The literal query text from auth_deps.py must run without error."""
    async with authed_schema.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT user_email, expires_at, revoked_at
            FROM session_tokens
            WHERE token_hash = $1
            """,
            _hash_token("nobody"),
        )
    assert row is None  # no such token, but the relation resolves


# ---------------------------------------------------------------------------
# 2. Runtime contract: unknown token is 401, never 500
# ---------------------------------------------------------------------------

@_integration_skip
@pytest.mark.asyncio
async def test_unknown_session_token_returns_401_not_500(authed_schema):
    """THE REGRESSION TEST. Before the fix this was a 500."""
    resp = await authed_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": "Bearer definitely-not-a-real-token"},
    )
    assert resp.status_code == 401, resp.text
    assert resp.json()["detail"] == "Invalid session token"


@_integration_skip
@pytest.mark.asyncio
async def test_missing_table_reproduces_the_500(unmigrated_schema):
    """Negative control: proves these tests actually detect the defect.

    With session_tokens absent, presenting any non-service Bearer token raises
    UndefinedTableError inside the dependency -> 500. This is the exact reported
    outage. If this test ever starts returning 401, the dependency grew its own
    error handling and the assertions above are no longer load-bearing.
    """
    resp = await unmigrated_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": "Bearer definitely-not-a-real-token"},
    )
    assert resp.status_code == 500, (
        f"expected the pre-fix failure mode, got {resp.status_code}: {resp.text}"
    )


@_integration_skip
@pytest.mark.asyncio
async def test_absent_token_401s_without_touching_the_database(unmigrated_schema):
    """No credential at all short-circuits before any query.

    This is why the outage was invisible to unauthenticated probes: they got a
    correct-looking 401 even with the table missing.
    """
    resp = await unmigrated_schema.client.post("/entities/merge", json=MERGE_BODY)
    assert resp.status_code == 401, resp.text


# ---------------------------------------------------------------------------
# 3. Valid / revoked / expired session tokens
# ---------------------------------------------------------------------------

async def _insert_token(pool, token: str, *, expires_in_hours=1, revoked=False):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO session_tokens (token_hash, user_email, expires_at, revoked_at)
            VALUES ($1, $2, $3, $4)
            """,
            _hash_token(token),
            "probe@example.com",
            datetime.now(timezone.utc) + timedelta(hours=expires_in_hours),
            datetime.now(timezone.utc) if revoked else None,
        )


@_integration_skip
@pytest.mark.asyncio
async def test_valid_session_token_passes_auth(authed_schema):
    """A live token authenticates; the 404 proves the endpoint body ran."""
    await _insert_token(authed_schema.pool, "good-token")
    resp = await authed_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": "Bearer good-token"},
    )
    assert resp.status_code == 404, resp.text
    assert "survivor not found" in resp.json()["detail"]


@_integration_skip
@pytest.mark.asyncio
async def test_revoked_session_token_is_401(authed_schema):
    await _insert_token(authed_schema.pool, "revoked-token", revoked=True)
    resp = await authed_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": "Bearer revoked-token"},
    )
    assert resp.status_code == 401, resp.text
    assert "revoked" in resp.json()["detail"].lower()


@_integration_skip
@pytest.mark.asyncio
async def test_expired_session_token_is_401(authed_schema):
    await _insert_token(authed_schema.pool, "expired-token", expires_in_hours=-1)
    resp = await authed_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": "Bearer expired-token"},
    )
    assert resp.status_code == 401, resp.text
    assert "expired" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 4. The service-token branch never touches the database
# ---------------------------------------------------------------------------

@_integration_skip
@pytest.mark.asyncio
async def test_service_token_authenticates_without_session_tokens_table(unmigrated_schema):
    """Explains why the MCP merge_entities tool kept working.

    make_service_token_auth compares the Bearer value against
    KOI_CLAIMS_SERVICE_TOKEN and returns the service identity BEFORE calling
    _validate_session_token. So the MCP path never reads session_tokens and was
    structurally immune to the outage. This is NOT an auth bypass: the request
    is still rejected without the shared secret (asserted above).
    """
    resp = await unmigrated_schema.client.post(
        "/entities/merge", json=MERGE_BODY, headers=SVC_AUTH
    )
    assert resp.status_code == 404, (
        f"service token must pass auth with session_tokens absent, got "
        f"{resp.status_code}: {resp.text}"
    )
    assert "survivor not found" in resp.json()["detail"]


@_integration_skip
@pytest.mark.asyncio
async def test_service_token_is_not_a_bypass(unmigrated_schema):
    """A near-miss secret must not authenticate."""
    resp = await unmigrated_schema.client.post(
        "/entities/merge",
        json=MERGE_BODY,
        headers={"Authorization": f"Bearer {SVC_TOKEN}-wrong"},
    )
    assert resp.status_code == 500  # falls through to the missing-table path
