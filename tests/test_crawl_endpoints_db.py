"""
DB-backed tests for the /web/crawl-agentic and /web/crawl-jobs/{id} endpoints.

Uses httpx.AsyncClient with ASGITransport (keeps pool + app on the same
event loop). Requires ``KOI_TEST_DATABASE_URL``.

Gating ACs covered:
  - AC6, AC15, AC16, AC21, AC30, AC39, AC50, AC52, AC54, AC64, AC72
  - Ownership check on GET /crawl-jobs/{id}
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from tests import koi_test_targets as targets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")
httpx = pytest.importorskip("httpx")

pytestmark = targets.requires_database


@pytest.fixture(autouse=True)
def offline_suffix_list(monkeypatch):
    """tldextract downloads the Public Suffix List on first use; use its bundled copy.

    same_domain() calls tldextract.extract, and without this a fresh environment makes
    a network request to publicsuffix.org during the test.
    """
    try:
        import tldextract
    except ImportError:
        return
    extractor = tldextract.TLDExtract(cache_dir=None, suffix_list_urls=())
    monkeypatch.setattr(tldextract, "extract", extractor)


@pytest.fixture(autouse=True)
def offline_dns(monkeypatch):
    """Resolve the example hosts these tests crawl to a public address, with no network.

    The enqueue path validates every URL, and the SSRF validator resolves its hostname.
    Any other hostname fails to resolve, as offline.
    """
    import socket

    hosts = {"example.org", "www.example.org", "example.com", "www.example.com"}

    def fake_getaddrinfo(host, *args, **kwargs):
        if isinstance(host, str) and host.lower() in hosts:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.215.14", 0))]
        raise socket.gaierror(socket.EAI_NONAME, "offline test resolver")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)


MIGRATION_087 = (ROOT / "migrations" / "087_web_crawl_jobs.sql").read_text()
ENTITY_REGISTRY_DDL = """
CREATE TABLE entity_registry (
    id SERIAL PRIMARY KEY,
    fuseki_uri TEXT UNIQUE NOT NULL,
    entity_text TEXT NOT NULL,
    entity_type TEXT,
    normalized_text TEXT NOT NULL,
    source TEXT,
    first_seen_rid TEXT,
    metadata JSONB,
    embedding TEXT,
    phonetic_code TEXT,
    aliases TEXT[] DEFAULT '{}'::text[],
    node_private BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_entity_registry_normalized ON entity_registry(normalized_text);
CREATE INDEX idx_entity_registry_type ON entity_registry(entity_type);
"""
ENTITY_RELATIONSHIPS_DDL = """
CREATE TABLE entity_relationships (
    id SERIAL PRIMARY KEY,
    subject_uri TEXT NOT NULL,
    predicate TEXT NOT NULL,
    object_uri TEXT NOT NULL,
    source TEXT,
    source_rid TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (subject_uri, predicate, object_uri)
);
"""


class _FakeCaps:
    web_sensor = True
    assertion_history = False
    graph_queries = False


@pytest_asyncio.fixture
async def app_and_pool(monkeypatch):
    from fastapi import FastAPI
    from api import crawl_auth
    from api import personal_ingest_api as pia
    from api.routers.web_router import create_router

    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    schema = f"crawl_ep_{uuid.uuid4().hex[:10]}"
    async with pool0.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}", public')
        await conn.execute(MIGRATION_087)
        await conn.execute(ENTITY_REGISTRY_DDL)
        await conn.execute(ENTITY_RELATIONSHIPS_DDL)
    await pool0.close()

    pool = await asyncpg.create_pool(
        targets.database_url(),
        min_size=1,
        max_size=8,
        server_settings={"search_path": f'"{schema}", public'},
    )

    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "true")
    monkeypatch.setenv("CRAWL_TOKEN__ops__test", "test-token-e")
    crawl_auth.reload_identity_config()

    app = FastAPI()
    app.include_router(create_router(pool, _FakeCaps()))
    app.state.db_pool = pool
    old_embedding_provider = getattr(pia, "embedding_provider", None)
    old_semantic_matching = getattr(pia, "ENABLE_SEMANTIC_MATCHING", None)
    old_enqueue_outbox = getattr(pia, "enqueue_outbox")
    pia.embedding_provider = None
    pia.ENABLE_SEMANTIC_MATCHING = False

    async def _noop_enqueue_outbox(*args, **kwargs):
        return None

    pia.enqueue_outbox = _noop_enqueue_outbox

    try:
        yield app, pool
    finally:
        try:
            async with pool.acquire() as conn:
                await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        except Exception:
            pass
        await pool.close()
        pia.embedding_provider = old_embedding_provider
        pia.ENABLE_SEMANTIC_MATCHING = old_semantic_matching
        pia.enqueue_outbox = old_enqueue_outbox


def _aclient(app):
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def _post(client, body: dict[str, Any], token: str = "test-token-e"):
    return await client.post(
        "/web/crawl-agentic",
        json=body,
        headers={"Authorization": f"Bearer {token}"},
    )


async def _get(client, job_id: int, token: str = "test-token-e"):
    return await client.get(
        f"/web/crawl-jobs/{job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )


async def _commit(client, job_id: int, body: dict[str, Any] | None = None, token: str = "test-token-e"):
    return await client.post(
        f"/web/crawl-jobs/{job_id}/commit",
        json=body or {},
        headers={"Authorization": f"Bearer {token}"},
    )


async def _parse_relate(client, instruction: str, token: str = "test-token-e"):
    return await client.post(
        "/tools/parse-relate-clause",
        json={"instruction": instruction},
        headers={"Authorization": f"Bearer {token}"},
    )


async def _seed_done_job(pool, proposal: dict[str, Any], submitted_by: str = "ops:test") -> int:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs
                (start_url, submitted_by, status, budget_json, result_json, proposal_version, ontology_version)
            VALUES
                ($1, $2, 'done', '{}'::jsonb, $3::jsonb, $4, $5)
            RETURNING id
            """,
            proposal["start_url"],
            submitted_by,
            json.dumps(proposal),
            proposal["proposal_version"],
            proposal["ontology_version"],
        )
    return int(row["id"])


def _sample_proposal() -> dict[str, Any]:
    return {
        "proposal_version": "v1",
        "ontology_version": "1.3.0",
        "start_url": "https://example.org/",
        "root_entity_index": 0,
        "entities": [
            {
                "index": 0,
                "name": "Example Org",
                "type": "Organization",
                "description": "Root org",
                "source_url": "https://example.org/",
                "source_image": None,
                "confidence": 0.95,
                "requires_review": False,
                "metadata": {},
                "existing_rid": None,
            },
            {
                "index": 1,
                "name": "Example Program",
                "type": "Project",
                "description": "Program child",
                "source_url": "https://example.org/program",
                "source_image": None,
                "confidence": 0.91,
                "requires_review": False,
                "metadata": {},
                "existing_rid": None,
            },
        ],
        "relationships": [
            {"subject_index": 0, "predicate": "has_project", "object_index": 1},
        ],
        "recommended_next_crawls": [],
        "stats": {},
    }


def _org_pair_proposal(child_name: str = "District of Highlands") -> dict[str, Any]:
    return {
        "proposal_version": "v1",
        "ontology_version": "1.3.0",
        "start_url": "https://example.org/partners",
        "root_entity_index": 0,
        "entities": [
            {
                "index": 0,
                "name": "Peninsula Streams Society",
                "type": "Organization",
                "description": "Root org",
                "source_url": "https://example.org/partners",
                "source_image": None,
                "confidence": 0.99,
                "requires_review": False,
                "metadata": {},
                "existing_rid": None,
            },
            {
                "index": 1,
                "name": child_name,
                "type": "Organization",
                "description": "Partner org",
                "source_url": "https://example.org/partners",
                "source_image": None,
                "confidence": 0.99,
                "requires_review": False,
                "metadata": {},
                "existing_rid": None,
            },
        ],
        "relationships": [
            {"subject_index": 0, "predicate": "collaborates_with", "object_index": 1},
        ],
        "recommended_next_crawls": [],
        "stats": {},
    }


@pytest.mark.asyncio
async def test_ac39_flag_off_returns_503(app_and_pool, monkeypatch):
    app, _ = app_and_pool
    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "false")
    async with _aclient(app) as c:
        r = await _post(c, {"url": "https://example.org/"})
    assert r.status_code == 503


@pytest.mark.asyncio
async def test_ac50_no_token_returns_401(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r = await c.post("/web/crawl-agentic", json={"url": "https://example.org/"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_enqueue_returns_job_id(app_and_pool):
    app, pool = app_and_pool
    async with _aclient(app) as c:
        r = await _post(c, {"url": "https://example.org/"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert "job_id" in body and isinstance(body["job_id"], int)
    assert body["deduped"] is False
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, submitted_by, start_url, budget_json FROM web_crawl_jobs WHERE id=$1",
            body["job_id"],
        )
    assert row["status"] == "queued"
    assert row["submitted_by"] == "ops:test"
    assert row["start_url"] == "https://example.org/"
    snap = json.loads(row["budget_json"]) if isinstance(row["budget_json"], str) else row["budget_json"]
    for k in ("max_pages", "max_vision_calls", "max_seconds", "max_usd"):
        assert k in snap


@pytest.mark.asyncio
async def test_ac16_same_url_dedup_returns_existing(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r1 = await _post(c, {"url": "https://example.org/"})
        r2 = await _post(c, {"url": "https://example.org/"})
    assert r1.status_code == 200 and r2.status_code == 200
    body1 = r1.json()
    body2 = r2.json()
    assert body1["job_id"] == body2["job_id"]
    assert body2["deduped"] is True


@pytest.mark.asyncio
async def test_ac23_canonicalization_dedupes_variants(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r1 = await _post(c, {"url": "https://Example.ORG:443/"})
        r2 = await _post(c, {"url": "https://example.org/"})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["job_id"] == r2.json()["job_id"]
    assert r2.json()["deduped"] is True


@pytest.mark.asyncio
async def test_ac23_www_not_stripped(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r1 = await _post(c, {"url": "https://www.example.org/"})
        r2 = await _post(c, {"url": "https://example.org/"})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r1.json()["job_id"] != r2.json()["job_id"]


@pytest.mark.asyncio
async def test_ac15_per_user_concurrent_cap(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r1 = await _post(c, {"url": "https://example.org/a"})
        r2 = await _post(c, {"url": "https://example.org/b"})
        r3 = await _post(c, {"url": "https://example.org/c"})
    assert r1.status_code == 200 and r2.status_code == 200
    assert r3.status_code == 429
    assert "concurrent crawl limit" in r3.json()["detail"]["error"]


@pytest.mark.asyncio
async def test_ac21_per_day_cap(app_and_pool):
    app, pool = app_and_pool
    async with pool.acquire() as conn:
        for i in range(10):
            await conn.execute(
                """
                INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json, created_at)
                VALUES ($1, 'ops:test', 'done', '{}'::jsonb, now() - interval '1 hour')
                """,
                f"https://example.org/seed-{i}",
            )
    async with _aclient(app) as c:
        r = await _post(c, {"url": "https://example.org/new"})
    assert r.status_code == 429, r.text
    assert "daily crawl limit" in r.json()["detail"]["error"]


@pytest.mark.asyncio
async def test_ac22_budget_system_ceiling_rejected(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r = await _post(c, {"url": "https://example.org/", "budget": {"max_pages": 100}})
    assert r.status_code == 400
    assert "system ceiling" in r.json()["detail"]["error"]


@pytest.mark.asyncio
async def test_ac70_ssrf_blocks_loopback(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r = await _post(c, {"url": "http://127.0.0.1/"})
    assert r.status_code == 400


@pytest.mark.asyncio
async def test_get_crawl_job_returns_status_and_ownership_enforced(app_and_pool, monkeypatch):
    app, _ = app_and_pool
    monkeypatch.setenv("CRAWL_TOKEN__ops__other", "test-token-f")
    from api import crawl_auth
    try:
        crawl_auth.reload_identity_config()
        async with _aclient(app) as c:
            r1 = await _post(c, {"url": "https://example.org/"})
            job_id = r1.json()["job_id"]
            own = await _get(c, job_id)
            assert own.status_code == 200
            assert own.json()["status"] == "queued"
            foreign = await _get(c, job_id, token="test-token-f")
            assert foreign.status_code == 403
    finally:
        monkeypatch.delenv("CRAWL_TOKEN__ops__other", raising=False)
        crawl_auth.reload_identity_config()


@pytest.mark.asyncio
async def test_ac54_flag_toggle_preserves_queued_jobs(app_and_pool, monkeypatch):
    app, pool = app_and_pool
    async with _aclient(app) as c:
        r = await _post(c, {"url": "https://example.org/"})
    job_id = r.json()["job_id"]
    monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "false")
    async with pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM web_crawl_jobs WHERE id=$1", job_id
        )
    assert status == "queued"


@pytest.mark.asyncio
async def test_ac72_atomic_enqueue_under_concurrency(app_and_pool):
    """N simultaneous enqueue tasks for same (user, url) → exactly 1 DB row."""
    app, pool = app_and_pool

    async def fire_one():
        async with _aclient(app) as c:
            r = await _post(c, {"url": "https://example.org/racey"})
        return r.status_code, r.json()

    results = await asyncio.gather(*(fire_one() for _ in range(8)))
    codes = [c for c, _ in results]
    assert all(c == 200 for c in codes), codes
    job_ids = {body["job_id"] for _, body in results}
    assert len(job_ids) == 1
    deduped = [body["deduped"] for _, body in results]
    assert any(deduped)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM web_crawl_jobs WHERE start_url='https://example.org/racey'"
        )
    assert n == 1


@pytest.mark.asyncio
async def test_commit_endpoint_creates_entities_relationships_and_history(app_and_pool):
    app, pool = app_and_pool
    job_id = await _seed_done_job(pool, _sample_proposal())

    async with _aclient(app) as c:
        r = await _commit(c, job_id)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "committed"
    assert len(body["committed"]) == 2
    assert body["errors"] == []

    async with pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM web_crawl_jobs WHERE id=$1", job_id)
        entity_count = await conn.fetchval("SELECT COUNT(*) FROM entity_registry")
        rel_count = await conn.fetchval("SELECT COUNT(*) FROM entity_relationships")
        history = await conn.fetchval("SELECT commit_history FROM web_crawl_jobs WHERE id=$1", job_id)
    assert status == "committed"
    assert entity_count == 2
    assert rel_count == 1
    history = json.loads(history) if isinstance(history, str) else history
    assert isinstance(history[0]["committed_index_to_rid"], dict)
    assert sorted(history[0]["committed_index_to_rid"].keys()) == ["0", "1"]


@pytest.mark.asyncio
async def test_commit_endpoint_request_local_drop_can_finish_commit(app_and_pool):
    app, pool = app_and_pool
    job_id = await _seed_done_job(pool, _sample_proposal())

    async with _aclient(app) as c:
        first = await _commit(c, job_id, {"proposal_overrides": {"dropped_entity_indices": [1]}})
        second = await _commit(c, job_id)
    assert first.status_code == 200, first.text
    assert second.status_code == 409, second.text
    assert first.json()["status"] == "committed"

    async with pool.acquire() as conn:
        history = await conn.fetchval("SELECT commit_history FROM web_crawl_jobs WHERE id=$1", job_id)
        status = await conn.fetchval("SELECT status FROM web_crawl_jobs WHERE id=$1", job_id)
    history = json.loads(history) if isinstance(history, str) else history
    assert status == "committed"
    assert history[0]["dropped_entity_indices"] == [1]
    assert sorted(history[0]["committed_index_to_rid"].keys()) == ["0"]


@pytest.mark.asyncio
async def test_commit_endpoint_enforces_ownership(app_and_pool, monkeypatch):
    app, pool = app_and_pool
    monkeypatch.setenv("CRAWL_TOKEN__ops__other", "test-token-f")
    from api import crawl_auth
    try:
        crawl_auth.reload_identity_config()
        job_id = await _seed_done_job(pool, _sample_proposal())
        async with _aclient(app) as c:
            r = await _commit(c, job_id, token="test-token-f")
        assert r.status_code == 403
    finally:
        monkeypatch.delenv("CRAWL_TOKEN__ops__other", raising=False)
        crawl_auth.reload_identity_config()


@pytest.mark.asyncio
async def test_commit_endpoint_creates_extra_relationship_to_existing_label(app_and_pool):
    app, pool = app_and_pool
    proposal = _sample_proposal()
    job_id = await _seed_done_job(pool, proposal)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, entity_type, normalized_text, source, metadata)
            VALUES
                ('orn:test.entity:vlh', 'Victoria Landscape Hub', 'Organization', 'victoria landscape hub', 'test', '{}'::jsonb)
            """
        )

    async with _aclient(app) as c:
        r = await _commit(
            c,
            job_id,
            {
                "extra_relationships": [
                    {"from": 0, "predicate": "related_to", "to": "Victoria Landscape Hub"}
                ]
            },
        )
    assert r.status_code == 200, r.text
    assert r.json()["extra_relationships_created"] == 1

    async with pool.acquire() as conn:
        predicate = await conn.fetchval(
            """
            SELECT predicate
              FROM entity_relationships
             WHERE predicate='related_to'
             LIMIT 1
            """
        )
    assert predicate == "related_to"


@pytest.mark.asyncio
async def test_commit_endpoint_captures_relationship_insert_errors_without_aborting(app_and_pool):
    app, pool = app_and_pool
    job_id = await _seed_done_job(pool, _sample_proposal())

    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE OR REPLACE FUNCTION fail_web_crawl_relationship_insert()
            RETURNS trigger AS $$
            BEGIN
                IF NEW.source = 'web-crawl' THEN
                    RAISE EXCEPTION 'synthetic relationship failure';
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
            """
        )
        await conn.execute(
            """
            CREATE TRIGGER fail_web_crawl_relationship_insert
            BEFORE INSERT ON entity_relationships
            FOR EACH ROW
            EXECUTE FUNCTION fail_web_crawl_relationship_insert()
            """
        )

    try:
        async with _aclient(app) as c:
            r = await _commit(c, job_id)
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["status"] == "committed"
        assert any(err["name"] == "relationship:0" for err in body["errors"])

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM web_crawl_jobs WHERE id=$1", job_id)
            rel_count = await conn.fetchval("SELECT COUNT(*) FROM entity_relationships")
            history = await conn.fetchval("SELECT commit_history FROM web_crawl_jobs WHERE id=$1", job_id)
        assert status == "committed"
        assert rel_count == 0
        history = json.loads(history) if isinstance(history, str) else history
        assert any(err["name"] == "relationship:0" for err in history[0]["errors"])
    finally:
        async with pool.acquire() as conn:
            await conn.execute("DROP TRIGGER IF EXISTS fail_web_crawl_relationship_insert ON entity_relationships")
            await conn.execute("DROP FUNCTION IF EXISTS fail_web_crawl_relationship_insert()")


@pytest.mark.asyncio
async def test_commit_endpoint_rename_override_skips_fuzzy_and_creates_new_entity(app_and_pool):
    app, pool = app_and_pool
    proposal = _org_pair_proposal()
    job_id = await _seed_done_job(pool, proposal)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, entity_type, normalized_text, source, metadata)
            VALUES
                (
                    'orn:test.entity:district-of-highlands',
                    'District of Highlands',
                    'Organization',
                    'district of highlands',
                    'seed',
                    '{}'::jsonb
                )
            """
        )

    async with _aclient(app) as c:
        r = await _commit(
            c,
            job_id,
            {
                "proposal_overrides": {
                    "entity_edits": {
                        1: {"name": "District of Saanich"},
                    }
                }
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    created = next(item for item in body["committed"] if item["name"] == "District of Saanich")
    assert created["was_new"] is True
    assert created["rid"] == "orn:personal-koi.entity:organization-district-of-saanich-39db6e8ee64f"

    async with pool.acquire() as conn:
        fresh = await conn.fetchrow(
            """
            SELECT fuseki_uri, entity_text
            FROM entity_registry
            WHERE fuseki_uri = 'orn:personal-koi.entity:organization-district-of-saanich-39db6e8ee64f'
            """
        )
        rel_target = await conn.fetchval(
            """
            SELECT object_uri
            FROM entity_relationships
            WHERE subject_uri = 'orn:personal-koi.entity:organization-peninsula-streams-society-0d25ebb363c9'
              AND predicate = 'collaborates_with'
            """
        )
        reused_highlands = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM entity_relationships
            WHERE subject_uri = 'orn:personal-koi.entity:organization-peninsula-streams-society-0d25ebb363c9'
              AND predicate = 'collaborates_with'
              AND object_uri = 'orn:test.entity:district-of-highlands'
            """
        )

    assert fresh["entity_text"] == "District of Saanich"
    assert rel_target == "orn:personal-koi.entity:organization-district-of-saanich-39db6e8ee64f"
    assert reused_highlands == 0


@pytest.mark.asyncio
async def test_commit_endpoint_rename_override_still_allows_exact_match(app_and_pool):
    app, pool = app_and_pool
    proposal = _org_pair_proposal()
    job_id = await _seed_done_job(pool, proposal)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, entity_type, normalized_text, source, metadata)
            VALUES
                (
                    'orn:test.entity:district-of-saanich-existing',
                    'District of Saanich',
                    'Organization',
                    'district of saanich',
                    'seed',
                    '{}'::jsonb
                )
            """
        )

    async with _aclient(app) as c:
        r = await _commit(
            c,
            job_id,
            {
                "proposal_overrides": {
                    "entity_edits": {
                        1: {"name": "District of Saanich"},
                    }
                }
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    matched = next(item for item in body["committed"] if item["name"] == "District of Saanich")
    assert matched["was_new"] is False
    assert matched["rid"] == "orn:test.entity:district-of-saanich-existing"

    async with pool.acquire() as conn:
        rel_target = await conn.fetchval(
            """
            SELECT object_uri
            FROM entity_relationships
            WHERE subject_uri = 'orn:personal-koi.entity:organization-peninsula-streams-society-0d25ebb363c9'
              AND predicate = 'collaborates_with'
            """
        )
        fresh_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM entity_registry
            WHERE fuseki_uri = 'orn:personal-koi.entity:organization-district-of-saanich-39db6e8ee64f'
            """
        )

    assert rel_target == "orn:test.entity:district-of-saanich-existing"
    assert fresh_count == 0


@pytest.mark.asyncio
async def test_commit_endpoint_non_renamed_entities_still_use_fuzzy_matching(app_and_pool):
    app, pool = app_and_pool
    proposal = _org_pair_proposal(child_name="District of Saanich")
    job_id = await _seed_done_job(pool, proposal)

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entity_registry
                (fuseki_uri, entity_text, entity_type, normalized_text, source, metadata)
            VALUES
                (
                    'orn:test.entity:district-of-highlands',
                    'District of Highlands',
                    'Organization',
                    'district of highlands',
                    'seed',
                    '{}'::jsonb
                )
            """
        )

    async with _aclient(app) as c:
        r = await _commit(c, job_id)

    assert r.status_code == 200, r.text
    body = r.json()
    matched = next(item for item in body["committed"] if item["name"] == "District of Saanich")
    assert matched["was_new"] is False
    assert matched["rid"] == "orn:test.entity:district-of-highlands"

    async with pool.acquire() as conn:
        rel_target = await conn.fetchval(
            """
            SELECT object_uri
            FROM entity_relationships
            WHERE subject_uri = 'orn:personal-koi.entity:organization-peninsula-streams-society-0d25ebb363c9'
              AND predicate = 'collaborates_with'
            """
        )

    assert rel_target == "orn:test.entity:district-of-highlands"


@pytest.mark.asyncio
async def test_parse_relate_clause_endpoint_requires_auth(app_and_pool, monkeypatch):
    app, _ = app_and_pool
    monkeypatch.setattr(
        "api.tools.parse_relate_clause.parse_relate_clause",
        lambda instruction: asyncio.sleep(0, result={"targets": [{"label": "Victoria Landscape Hub", "predicate_hint": "related_to", "type_hint": "Organization"}], "usage": {}, "usd": 0.0}),
    )
    async with _aclient(app) as c:
        r = await c.post("/tools/parse-relate-clause", json={"instruction": "relate to Victoria Landscape Hub"})
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_parse_relate_clause_endpoint_returns_targets_without_touching_job_cost(app_and_pool, monkeypatch):
    app, pool = app_and_pool
    job_id = await _seed_done_job(pool, _sample_proposal())

    async def _fake_parse(instruction: str):
        return {
            "targets": [{"label": "Victoria Landscape Hub", "predicate_hint": "related_to", "type_hint": "Organization"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            "usd": 0.0001,
        }

    monkeypatch.setattr("api.tools.parse_relate_clause.parse_relate_clause", _fake_parse)
    async with pool.acquire() as conn:
        before = await conn.fetchval("SELECT cost_usd FROM web_crawl_jobs WHERE id=$1", job_id)
    async with _aclient(app) as c:
        r = await _parse_relate(c, "ingest https://example.org and relate to Victoria Landscape Hub")
    assert r.status_code == 200, r.text
    assert r.json()["targets"][0]["label"] == "Victoria Landscape Hub"
    async with pool.acquire() as conn:
        after = await conn.fetchval("SELECT cost_usd FROM web_crawl_jobs WHERE id=$1", job_id)
    assert before == after


@pytest.mark.asyncio
async def test_parse_relate_clause_endpoint_rejects_oversize_body(app_and_pool):
    app, _ = app_and_pool
    async with _aclient(app) as c:
        r = await _parse_relate(c, "x" * 5000)
    assert r.status_code == 413


@pytest.mark.asyncio
async def test_parse_relate_clause_endpoint_rate_limited(app_and_pool, monkeypatch):
    app, _ = app_and_pool
    from api.routers import web_router as web_router_module

    async def _fake_parse(instruction: str):
        return {"targets": [], "usage": {"prompt_tokens": 1, "completion_tokens": 1}, "usd": 0.0}

    web_router_module._PARSE_RELATE_CALLS.clear()
    monkeypatch.setattr("api.tools.parse_relate_clause.parse_relate_clause", _fake_parse)
    async with _aclient(app) as c:
        codes = []
        for _ in range(61):
            r = await _parse_relate(c, "relate to test")
            codes.append(r.status_code)
    assert codes.count(200) == 60
    assert codes.count(429) == 1
    web_router_module._PARSE_RELATE_CALLS.clear()


@pytest.mark.asyncio
async def test_diagnostics_config_reports_agentic_crawl_available(app_and_pool):
    app, _ = app_and_pool
    from api import personal_ingest_api as pia

    old_pool = pia.db_pool
    pia.db_pool = app.state.db_pool
    try:
        data = await pia.diagnostics_config()
        assert data['agentic_crawl_available'] is True
    finally:
        pia.db_pool = old_pool
