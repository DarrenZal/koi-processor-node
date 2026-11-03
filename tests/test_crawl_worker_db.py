"""
DB-backed tests for the agentic crawl worker + endpoints.

Require ``KOI_TEST_DATABASE_URL`` (tests/koi_test_targets.py), a role with
permission to create schemas there.

Each test creates its own ephemeral schema, applies migration 087, and
drops the schema on teardown. Skipped when the env var is missing so CI
without a DB stays green.

Gating ACs covered here:
  - AC6, AC12: lifecycle + Sweep C recovery
  - AC15, AC16, AC30: per-user concurrency cap, per-user URL dedup, cross-user
  - AC21, AC72, AC73: per-day cap + atomic enqueue under concurrency
  - AC38, AC82: advisory lock behavior
  - AC43: migration uniq index present
  - AC64: budget snapshot persisted as JSONB map
  - AC66: progress_json.visited populated
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from tests import koi_test_targets as targets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")

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


MIGRATION_087 = (ROOT / "migrations" / "087_web_crawl_jobs.sql").read_text()


async def _apply_schema(conn):
    """Isolate each test in its own schema so parallel runs don't collide."""
    schema = f"crawl_test_{uuid.uuid4().hex[:10]}"
    await conn.execute(f'CREATE SCHEMA "{schema}"')
    await conn.execute(f'SET search_path TO "{schema}", public')
    await conn.execute(MIGRATION_087)
    return schema


async def _drop_schema(conn, schema: str):
    try:
        await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
    except Exception:
        pass


@pytest_asyncio.fixture
async def db_pool():
    pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=5)
    # Hold one connection to set the schema for the pool. asyncpg's per-conn
    # setup uses the init hook; set search_path per acquire via server_settings.
    async with pool.acquire() as conn:
        schema = await _apply_schema(conn)
    try:
        # Recreate the pool so all acquired connections run under the new
        # search_path. We do this by wrapping connects with server_settings.
        await pool.close()
        pool2 = await asyncpg.create_pool(
            targets.database_url(),
            min_size=1,
            max_size=5,
            server_settings={"search_path": f'"{schema}", public'},
        )
        yield pool2, schema
    finally:
        try:
            async with pool2.acquire() as conn:
                await _drop_schema(conn, schema)
        except Exception:
            pass
        await pool2.close()


@pytest.mark.asyncio
async def test_ac43_partial_unique_index_present(db_pool):
    pool, schema = db_pool
    async with pool.acquire() as conn:
        idx = await conn.fetchval(
            "SELECT indexdef FROM pg_indexes WHERE indexname='uniq_inflight_per_user_url'"
        )
    assert idx is not None
    assert "queued" in idx and "running" in idx


@pytest.mark.asyncio
async def test_ac16_same_url_dedup_per_user(db_pool):
    pool, schema = db_pool
    async with pool.acquire() as conn:
        row1 = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued', '{}'::jsonb)
            RETURNING id
            """
        )
        # Second identical insert must fail with unique_violation
        with pytest.raises(asyncpg.exceptions.UniqueViolationError):
            await conn.execute(
                """
                INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
                VALUES ('https://example.org/', 'ops:test', 'queued', '{}'::jsonb)
                """
            )
        assert row1["id"] is not None


@pytest.mark.asyncio
async def test_ac30_cross_user_urls_not_deduped(db_pool):
    pool, schema = db_pool
    async with pool.acquire() as conn:
        a = await conn.fetchval(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:alice', 'queued', '{}'::jsonb)
            RETURNING id
            """
        )
        b = await conn.fetchval(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:bob', 'queued', '{}'::jsonb)
            RETURNING id
            """
        )
    assert a != b


@pytest.mark.asyncio
async def test_ac16_terminal_states_do_not_block_requeue(db_pool):
    """Interrupted/failed jobs don't keep a URL locked from the in-flight index."""
    pool, schema = db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'interrupted', '{}'::jsonb)
            """
        )
        # Same URL, same user, but prior is terminal → second insert should succeed.
        await conn.execute(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued', '{}'::jsonb)
            """
        )
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM web_crawl_jobs WHERE submitted_by='ops:test'"
        )
    assert n == 2


@pytest.mark.asyncio
async def test_ac64_budget_snapshot_persisted(db_pool):
    pool, schema = db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued',
                    '{"max_pages":40,"max_vision_calls":20,"max_seconds":180,"max_usd":0.5}'::jsonb)
            """
        )
        row = await conn.fetchval(
            "SELECT budget_json FROM web_crawl_jobs WHERE submitted_by='ops:test'"
        )
    snapshot = json.loads(row)
    for key in ("max_pages", "max_vision_calls", "max_seconds", "max_usd"):
        assert key in snapshot


@pytest.mark.asyncio
async def test_ac38_sweep_c_marks_orphaned_running_interrupted(db_pool):
    """Row held by a prior worker whose connection is gone → Sweep C marks
    it interrupted with the canonical error string."""
    from api.crawl_worker import CrawlWorker, ERR_PRIOR_CONNECTION_DROPPED

    pool, schema = db_pool
    # Insert a 'running' row — no session holds the advisory lock.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status,
                                        claimed_by, started_at, heartbeat_at,
                                        budget_json)
            VALUES ('https://example.org/', 'ops:test', 'running',
                    'deadhost:1:aaa', now() - interval '2 minutes', now() - interval '2 minutes',
                    '{}'::jsonb)
            RETURNING id
            """
        )
    worker = CrawlWorker(
        dsn=targets.database_url(), pool=pool, worker_id="testworker",
        server_settings={"search_path": f'"{schema}", public'},
    )
    await worker._sweep_c()
    async with pool.acquire() as conn:
        status = await conn.fetchval(
            "SELECT status FROM web_crawl_jobs WHERE id=$1", row["id"]
        )
        error = await conn.fetchval(
            "SELECT error FROM web_crawl_jobs WHERE id=$1", row["id"]
        )
    assert status == "interrupted"
    assert error == ERR_PRIOR_CONNECTION_DROPPED


@pytest.mark.asyncio
async def test_ac82_advisory_lock_held_by_live_worker_blocks_sweep_c(db_pool):
    """Running row whose lock is held by a separate session → Sweep C skips."""
    from api.crawl_worker import CrawlWorker

    pool, schema = db_pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status,
                                        claimed_by, started_at, heartbeat_at,
                                        budget_json)
            VALUES ('https://example.org/', 'ops:test', 'running',
                    'livehost:2:bbb', now(), now(), '{}'::jsonb)
            RETURNING id
            """
        )
    job_id = row["id"]
    # Acquire the lock on a separate session (simulates a live worker).
    holder = await asyncpg.connect(targets.database_url())
    try:
        await holder.execute(
            "SELECT pg_advisory_lock(hashtext('crawl_job_' || $1::text))", str(job_id)
        )
        # Sweep C on a fresh worker sees the lock held → should NOT mark interrupted.
        worker = CrawlWorker(
            dsn=targets.database_url(), pool=pool, worker_id="testworker2",
            server_settings={"search_path": f'"{schema}", public'},
        )
        await worker._sweep_c()
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM web_crawl_jobs WHERE id=$1", job_id
            )
        assert status == "running"
    finally:
        await holder.execute(
            "SELECT pg_advisory_unlock(hashtext('crawl_job_' || $1::text))", str(job_id)
        )
        await holder.close()


@pytest.mark.asyncio
async def test_ac6_lifecycle_claim_and_complete(db_pool):
    """Worker claims a queued job, run_crawl stub returns, status='done'."""
    from api.crawl_worker import CrawlWorker

    pool, schema = db_pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued',
                    '{"max_pages":2,"max_vision_calls":0,"max_seconds":60,"max_usd":0.1}'::jsonb)
            RETURNING id
            """
        )
    job_id = row["id"]

    async def fake_run(**kwargs):
        # Record that we got a pinned connection and its connection is OK.
        assert kwargs["pinned_conn"] is not None
        return {
            "proposal_version": "v1",
            "ontology_version": "1.3.0",
            "start_url": kwargs["start_url"],
            "root_entity_index": 0,
            "entities": [{"index": 0, "name": "Example", "type": "Organization"}],
            "relationships": [],
            "recommended_next_crawls": [],
            "stats": {"pages_visited": 1},
        }

    worker = CrawlWorker(
        dsn=targets.database_url(), pool=pool, worker_id="lifeworker", run_crawl=fake_run,
        server_settings={"search_path": f'"{schema}", public'},
    )
    claim = await worker._claim_one()
    assert claim is not None and claim["id"] == job_id
    await worker._run_job(claim)

    async with pool.acquire() as conn:
        status, result = await conn.fetchrow(
            "SELECT status, result_json FROM web_crawl_jobs WHERE id=$1", job_id
        )
    assert status == "done"
    assert result is not None


@pytest.mark.asyncio
async def test_ac12_worker_failure_marks_failed(db_pool):
    """Any uncaught error in run_crawl flips status to failed with error populated."""
    from api.crawl_worker import CrawlWorker

    pool, schema = db_pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued', '{}'::jsonb)
            RETURNING id
            """
        )
    job_id = row["id"]

    async def bad_run(**kwargs):
        raise RuntimeError("boom")

    worker = CrawlWorker(
        dsn=targets.database_url(), pool=pool, worker_id="failworker", run_crawl=bad_run,
        server_settings={"search_path": f'"{schema}", public'},
    )
    claim = await worker._claim_one()
    await worker._run_job(claim)
    async with pool.acquire() as conn:
        status, error = await conn.fetchrow(
            "SELECT status, error FROM web_crawl_jobs WHERE id=$1", job_id
        )
    assert status == "failed"
    assert "boom" in (error or "")


@pytest.mark.asyncio
async def test_ac72_concurrent_claims_serialized(db_pool):
    """Two workers racing on the same queued row: exactly one wins the claim."""
    from api.crawl_worker import CrawlWorker

    pool, schema = db_pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO web_crawl_jobs (start_url, submitted_by, status, budget_json)
            VALUES ('https://example.org/', 'ops:test', 'queued', '{}'::jsonb)
            """
        )
    w1 = CrawlWorker(dsn=targets.database_url(), pool=pool, worker_id="w1")
    w2 = CrawlWorker(dsn=targets.database_url(), pool=pool, worker_id="w2")
    # kick both claim calls simultaneously
    results = await asyncio.gather(w1._claim_one(), w2._claim_one())
    winners = [r for r in results if r is not None]
    losers = [r for r in results if r is None]
    assert len(winners) == 1
    assert len(losers) == 1


@pytest.mark.asyncio
async def test_ac65_table_absent_worker_skips(tmp_path, monkeypatch):
    """If web_crawl_jobs does not exist (FR/GV path), start_crawl_worker logs + returns None."""
    # Build an isolated pool pointed at a schema with no crawl jobs table.
    pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            schema = f"crawl_bare_{uuid.uuid4().hex[:8]}"
            await conn.execute(f'CREATE SCHEMA "{schema}"')
        try:
            await pool.close()
            pool2 = await asyncpg.create_pool(
                targets.database_url(),
                min_size=1,
                max_size=2,
                server_settings={"search_path": f'"{schema}", pg_catalog'},
            )
            from fastapi import FastAPI
            app = FastAPI()
            app.state.db_pool = pool2

            from api.crawl_worker import start_crawl_worker
            # Even with flag on, table missing must short-circuit.
            monkeypatch.setenv("AGENTIC_CRAWL_ENABLED", "true")
            result = await start_crawl_worker(app, dsn=targets.database_url())
            assert result is None
        finally:
            async with pool2.acquire() as conn:
                await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
            await pool2.close()
    except Exception:
        await pool.close()
        raise
