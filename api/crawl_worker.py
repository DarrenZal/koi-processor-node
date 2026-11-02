"""
Background worker that drives ``web_crawl_jobs`` rows through
queued → running → done/failed/interrupted.

Single-process async worker. Safe under multi-worker uvicorn: the atomic
claim uses ``FOR UPDATE SKIP LOCKED`` and a session-level advisory lock on a
dedicated (non-pooled) asyncpg connection. If the process crashes, the OS
closes the connection and PostgreSQL auto-releases the lock; the next
startup's Sweep C acquires the lock (proving the prior holder is dead) and
marks the row ``interrupted``.

Lifecycle sequence per claim:

  1. Pop one queued job with ``UPDATE … FOR UPDATE SKIP LOCKED RETURNING id``
     (via the shared pool). If none, sleep 2s.
  2. Open a fresh ``asyncpg.Connection`` (NOT from the pool) and
     ``pg_advisory_lock(hashtext('crawl_job_' || id))`` on it. Hold for the
     full run.
  3. Run the crawl. Heartbeat every 30s (``heartbeat_at`` only — never
     ``started_at``). Per-page wall-clock check against ``started_at``.
  4. On completion / failure / cancellation: explicit
     ``pg_advisory_unlock``, close the pinned connection, then
     ``UPDATE … SET status=…`` via the shared pool.

Sweep C (startup, advisory-lock based) runs once before the main loop.
Sweep B (heartbeat timeout) runs every 60s during normal operation as a
safety net against Sweep C misses (pg_advisory_lock is fundamentally
connection-scoped; if a node is still alive but the worker task has
hung, only Sweep B catches it via the 10-min heartbeat threshold).

Inert when either:
- ``AGENTIC_CRAWL_ENABLED`` != ``true`` (feature flag off), OR
- ``web_crawl_jobs`` table does not exist (non-Octo deploy).

AC coverage: 6, 12, 38, 52, 54, 64, 65, 66, 76, 82.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import time
import uuid
from typing import Any, Optional

import asyncpg
from fastapi import FastAPI

from api import crawl_auth, ontology_registry

logger = logging.getLogger(__name__)

# Error strings — canonical, matched by ACs/tests. Do NOT reword ad hoc.
ERR_PRIOR_CONNECTION_DROPPED = "prior worker connection dropped"
ERR_HEARTBEAT_TIMEOUT = "heartbeat timeout"
ERR_WALL_CLOCK_TIMEOUT = "wall-clock timeout"
ERR_COST_BUDGET = "cost budget exhausted"
ERR_START_UNREACHABLE = "start page unreachable"  # prefix; worker adds ': <reason>'
ERR_START_ANALYSIS = "start page analysis failed"  # prefix
ERR_LLM_REPEATED = "LLM analysis failing repeatedly"
ERR_MANUAL_CANCEL = "manual cancel"

_CLAIM_POLL_INTERVAL_S = 2.0
_HANG_GUARD_BUFFER_S = 90  # hard crawl cap = max_seconds + this; recovers the worker from a wedged iteration that blocks between cancel_checks
_PER_PAGE_FETCH_TIMEOUT_S = 75  # per-page hard fetch cap; a hung render raises (crawler skips that page) instead of freezing the loop past the graceful wall-clock stop
_HEARTBEAT_INTERVAL_S = 30.0
_SWEEP_B_INTERVAL_S = 60.0
_HEARTBEAT_TIMEOUT_MINUTES = 10


# Postgres ``hashtext`` returns int4; ``pg_advisory_lock(bigint)`` accepts
# the widened value. Explicit ``::text`` cast keeps asyncpg's type inference
# happy when we bind ``job_id`` as int.
_SQL_LOCK = "SELECT pg_advisory_lock(hashtext('crawl_job_' || $1::text))"
_SQL_TRY_LOCK = "SELECT pg_try_advisory_lock(hashtext('crawl_job_' || $1::text))"
_SQL_UNLOCK = "SELECT pg_advisory_unlock(hashtext('crawl_job_' || $1::text))"


class CrawlWorker:
    """Single async worker loop. Created once per service process."""

    def __init__(
        self,
        *,
        dsn: str,
        pool: asyncpg.Pool,
        worker_id: Optional[str] = None,
        run_crawl: Optional[Any] = None,
        server_settings: Optional[dict[str, str]] = None,
    ) -> None:
        self._dsn = dsn
        self._pool = pool
        self._server_settings = server_settings or {}
        self._worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self._task: Optional[asyncio.Task[Any]] = None
        self._sweep_b_task: Optional[asyncio.Task[Any]] = None
        self._stop_event = asyncio.Event()
        # Injection point: tests pass a stub. In production the
        # web_router registers `_run_crawl_for_job` which drives
        # agentic_crawl with the pinned connection.
        self._run_crawl = run_crawl or _default_run_crawl

    @property
    def worker_id(self) -> str:
        return self._worker_id

    async def start(self) -> None:
        """Run Sweep C then spawn the main loop + Sweep B timer."""
        await self._sweep_c()
        self._task = asyncio.create_task(self._main_loop(), name="crawl_worker.main")
        self._sweep_b_task = asyncio.create_task(self._sweep_b_loop(), name="crawl_worker.sweep_b")
        logger.info("crawl_worker: started worker_id=%s", self._worker_id)

    async def stop(self) -> None:
        self._stop_event.set()
        for t in (self._task, self._sweep_b_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass

    async def _sweep_c(self) -> None:
        """Advisory-lock-based startup recovery.

        For each ``status='running'`` row, open a dedicated connection, try
        the advisory lock. Acquired → prior holder is dead → mark
        ``interrupted``. Not acquired → a live worker (this one, after
        restart, or another node) is handling it; skip.
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM web_crawl_jobs WHERE status='running'"
            )
        if not rows:
            return
        logger.info("crawl_worker: sweep_c scanning %d running rows", len(rows))
        for row in rows:
            job_id = row["id"]
            # Dedicated connection so the lock test isn't affected by the pool.
            sweep_conn = await asyncpg.connect(
                self._dsn, server_settings=self._server_settings
            )
            try:
                acquired = await sweep_conn.fetchval(_SQL_TRY_LOCK, str(job_id))
                if not acquired:
                    continue  # live worker holding it
                try:
                    await sweep_conn.execute(
                        """
                        UPDATE web_crawl_jobs
                           SET status='interrupted',
                               error=$2,
                               finished_at=now()
                         WHERE id=$1 AND status='running'
                        """,
                        job_id,
                        ERR_PRIOR_CONNECTION_DROPPED,
                    )
                    logger.info(
                        "crawl_worker: sweep_c marked job=%d interrupted", job_id
                    )
                finally:
                    await sweep_conn.execute(_SQL_UNLOCK, str(job_id))
            finally:
                await sweep_conn.close()

    async def _sweep_b_loop(self) -> None:
        """Heartbeat-timeout sweep (safety net). Runs every 60s."""
        while not self._stop_event.is_set():
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        f"""
                        UPDATE web_crawl_jobs
                           SET status='interrupted',
                               error=$1,
                               finished_at=now()
                         WHERE status='running'
                           AND heartbeat_at < now() - interval '{_HEARTBEAT_TIMEOUT_MINUTES} minutes'
                        """,
                        ERR_HEARTBEAT_TIMEOUT,
                    )
            except Exception as exc:
                logger.warning("crawl_worker.sweep_b error: %s", exc)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=_SWEEP_B_INTERVAL_S)
            except asyncio.TimeoutError:
                pass

    async def _main_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                claimed = await self._claim_one()
            except Exception as exc:
                logger.exception("crawl_worker claim failed: %s", exc)
                claimed = None
            if claimed is None:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=_CLAIM_POLL_INTERVAL_S
                    )
                except asyncio.TimeoutError:
                    pass
                continue
            # claimed is {id, start_url, goal, submitted_by, budget_json}
            try:
                await self._run_job(claimed)
            except Exception as exc:
                logger.exception("crawl_worker job %s crashed: %s", claimed["id"], exc)

    async def _claim_one(self) -> Optional[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE web_crawl_jobs
                   SET status='running',
                       claimed_by=$1,
                       started_at=now(),
                       heartbeat_at=now()
                 WHERE id = (
                     SELECT id FROM web_crawl_jobs
                      WHERE status='queued'
                      ORDER BY created_at
                      FOR UPDATE SKIP LOCKED
                      LIMIT 1
                 )
                 RETURNING id, start_url, goal, submitted_by, budget_json
                """,
                self._worker_id,
            )
            return dict(row) if row else None

    async def _run_job(self, claimed: dict[str, Any]) -> None:
        job_id: int = claimed["id"]
        pinned = await asyncpg.connect(
            self._dsn, server_settings=self._server_settings
        )
        try:
            await pinned.execute(_SQL_LOCK, str(job_id))
            budget = _coerce_budget(claimed.get("budget_json"))
            try:
                result_dict = await self._run_crawl(
                    job_id=job_id,
                    start_url=claimed["start_url"],
                    goal=claimed.get("goal") or "",
                    budget=budget,
                    pinned_conn=pinned,
                    stop_event=self._stop_event,
                    pool=self._pool,
                )
            except _WorkerCancelled as exc:
                await self._mark_terminal(
                    job_id, status=exc.status, error=exc.message
                )
                return
            except _WorkerFailed as exc:
                await self._mark_terminal(
                    job_id, status="failed", error=exc.message
                )
                return
            except Exception as exc:
                logger.exception("crawl job %d failed: %s", job_id, exc)
                await self._mark_terminal(
                    job_id, status="failed", error=f"uncaught error: {exc}"
                )
                return
            # Success
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE web_crawl_jobs
                       SET status='done',
                           result_json=$2::jsonb,
                           proposal_version=$3,
                           ontology_version=$4,
                           finished_at=now()
                     WHERE id=$1
                    """,
                    job_id,
                    json.dumps(result_dict),
                    result_dict.get("proposal_version", "v1"),
                    result_dict.get("ontology_version", ontology_registry.ONTOLOGY_VERSION),
                )
        finally:
            try:
                await pinned.execute(_SQL_UNLOCK, str(job_id))
            except Exception:
                pass
            try:
                await pinned.close()
            except Exception:
                pass

    async def _mark_terminal(self, job_id: int, *, status: str, error: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE web_crawl_jobs
                   SET status=$2,
                       error=$3,
                       finished_at=now()
                 WHERE id=$1
                """,
                job_id,
                status,
                error,
            )


class _WorkerFailed(Exception):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class _WorkerCancelled(Exception):
    def __init__(self, status: str, message: str) -> None:
        self.status = status
        self.message = message
        super().__init__(message)


def _coerce_budget(budget_json: Any) -> dict[str, Any]:
    if isinstance(budget_json, str):
        try:
            return json.loads(budget_json)
        except (ValueError, TypeError):
            return {}
    if isinstance(budget_json, dict):
        return dict(budget_json)
    return {}


async def _default_run_crawl(
    *,
    job_id: int,
    start_url: str,
    goal: str,
    budget: dict[str, Any],
    pinned_conn: asyncpg.Connection,
    stop_event: asyncio.Event,
    pool: asyncpg.Pool,
) -> dict[str, Any]:
    """Real crawl invocation. Imports kept local to avoid circular imports."""
    from api.agentic_crawler import (
        CrawlBudget,
        CrawlBudgetExceeded,
        agentic_crawl,
    )
    from api.personal_ingest_api import lookup_entity
    from api.vision_ocr import vision_extract_orgs
    from api.web_fetcher import fetch_and_preview

    # Build budget from the job-row snapshot (AC64) — not env defaults.
    budget_obj = CrawlBudget()
    if budget.get("max_pages") is not None:
        budget_obj.max_pages = int(budget["max_pages"])
    if budget.get("max_vision_calls") is not None:
        budget_obj.max_vision_calls = int(budget["max_vision_calls"])
    if budget.get("max_seconds") is not None:
        budget_obj.max_seconds = int(budget["max_seconds"])
    if budget.get("max_usd") is not None:
        budget_obj.max_usd = float(budget["max_usd"])

    async def _fetch(url: str):
        # Per-page hard timeout: fetch_and_preview's internal Playwright/Scrapling
        # timeouts occasionally fail to bound a wedged render (browser launch,
        # redirect loop, never-settling page). Cap each page so a single hung fetch
        # RAISES — agentic_crawl then logs it, counts a skip, and continues to the
        # next candidate — instead of freezing the loop past the graceful wall-clock
        # stop and losing the partial proposal. (2026-07-06: hollyhock wedged on
        # page 6 for ~100s and lost 34 in-flight entities.)
        try:
            return await asyncio.wait_for(
                fetch_and_preview(url, db_pool=pool, _internal_call=True),
                timeout=_PER_PAGE_FETCH_TIMEOUT_S,
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError(
                f"page fetch exceeded {_PER_PAGE_FETCH_TIMEOUT_S}s: {url}"
            ) from exc

    async def _lookup(name: str, entity_type: str):
        async with pool.acquire() as conn:
            return await lookup_entity(conn, name, entity_type)

    async def _vision(image_url: str, role: str, context: str):
        return await vision_extract_orgs(image_url=image_url, role=role, context=context)

    last_heartbeat = time.monotonic()
    started_at = time.monotonic()
    max_seconds = budget_obj.max_seconds

    async def _cancel_check() -> bool:
        # Per-page iteration guardrail — all three budget-breach cases.
        nonlocal last_heartbeat
        # Wall-clock enforced against monotonic start (matches the job-row
        # started_at within a few milliseconds; immutable across heartbeats).
        elapsed = time.monotonic() - started_at
        if elapsed > max_seconds:
            # Graceful stop: signal the crawl loop to break and finalize the partial
            # proposal from entities gathered so far, rather than raising and
            # discarding everything. (Slow multi-page JS crawls were losing 40+
            # in-flight entities on wall-clock timeout — 2026-07-06.)
            logger.info(
                "job %d wall-clock budget reached; stopping to keep partial proposal",
                job_id,
            )
            return True
        # Check for stop / manual cancel.
        if stop_event.is_set():
            raise _WorkerCancelled("interrupted", "service shutdown")
        # Heartbeat every 30s — uses pinned_conn so it proves liveness of the
        # advisory-lock holder.
        now = time.monotonic()
        if now - last_heartbeat >= _HEARTBEAT_INTERVAL_S:
            try:
                await pinned_conn.execute(
                    "UPDATE web_crawl_jobs SET heartbeat_at=now() WHERE id=$1",
                    job_id,
                )
                last_heartbeat = now
            except Exception as exc:
                logger.warning("heartbeat failed on job %d: %s", job_id, exc)
        # Cancel from DB (operator set status='cancelled').
        cancelled = await pinned_conn.fetchval(
            "SELECT status='cancelled' FROM web_crawl_jobs WHERE id=$1", job_id
        )
        if cancelled:
            raise _WorkerCancelled("cancelled", ERR_MANUAL_CANCEL)
        return False

    async def _on_progress(payload: dict[str, Any]) -> None:
        try:
            await pinned_conn.execute(
                """
                UPDATE web_crawl_jobs
                   SET progress_json=$2::jsonb,
                       cost_usd=$3
                 WHERE id=$1
                """,
                job_id,
                json.dumps(payload),
                float(payload.get("cost_usd") or 0.0),
            )
        except Exception as exc:
            logger.warning("progress update failed on job %d: %s", job_id, exc)

    try:
        # Hard hang-guard: a single wedged iteration (e.g. a stuck Playwright fetch)
        # blocks between per-page cancel_checks and would hang the whole worker (job
        # stays "running", heartbeat goes stale, the next job starves). Cap the entire
        # crawl at max_seconds + buffer so the worker always recovers. The graceful
        # wall-clock stop (cancel_check returns True) fires first in the normal case
        # and preserves the partial proposal; this only trips on a true hang.
        proposal = await asyncio.wait_for(
            agentic_crawl(
                start_url=start_url,
                goal=goal,
                budget=budget_obj,
                fetch_fn=_fetch,
                lookup_fn=_lookup,
                vision_fn=_vision,
                progress_callback=_on_progress,
                cancel_check=_cancel_check,
            ),
            timeout=budget_obj.max_seconds + _HANG_GUARD_BUFFER_S,
        )
    except asyncio.TimeoutError as exc:
        raise _WorkerFailed("crawl hard-timeout (hung iteration)") from exc
    except CrawlBudgetExceeded as exc:
        raise _WorkerFailed(str(exc)) from exc

    return {
        "proposal_version": proposal.proposal_version,
        "ontology_version": proposal.ontology_version,
        "start_url": proposal.start_url,
        "root_entity_index": proposal.root_entity_index,
        "entities": proposal.entities,
        "relationships": proposal.relationships,
        "recommended_next_crawls": proposal.recommended_next_crawls,
        "stats": proposal.stats,
    }


async def _table_exists(pool: asyncpg.Pool, name: str) -> bool:
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", name)
    return bool(val)


async def start_crawl_worker(app: FastAPI, *, dsn: Optional[str] = None) -> Optional[CrawlWorker]:
    """FastAPI startup hook. Returns the worker (or None) for test hookup.

    Pre-flights:
      1. ``AGENTIC_CRAWL_ENABLED`` must be true, else skip.
      2. ``web_crawl_jobs`` table must exist (Octo-only migration), else skip.

    The shared codebase deploys to FR/GV/CV as well; those nodes don't have
    migration 087 applied, so step 2 gracefully skips there (see AC65).
    """
    pool = getattr(app.state, "db_pool", None)
    if pool is None:
        logger.info("crawl_worker: db_pool not available on app.state — worker not started")
        return None
    if not await _table_exists(pool, "web_crawl_jobs"):
        logger.info(
            "crawl_worker: web_crawl_jobs table absent — worker not started"
        )
        return None
    if not crawl_auth.is_feature_enabled():
        logger.info("crawl_worker: feature flag disabled — worker not started")
        return None
    resolved_dsn = dsn or os.getenv("POSTGRES_URL")
    if not resolved_dsn:
        logger.warning("crawl_worker: POSTGRES_URL not set — worker not started")
        return None
    search_path_override = os.getenv("CRAWL_WORKER_SEARCH_PATH") or None
    server_settings = {"search_path": search_path_override} if search_path_override else None
    worker = CrawlWorker(dsn=resolved_dsn, pool=pool, server_settings=server_settings)
    await worker.start()
    app.state.crawl_worker = worker
    return worker


async def stop_crawl_worker(app: FastAPI) -> None:
    worker: Optional[CrawlWorker] = getattr(app.state, "crawl_worker", None)
    if worker:
        await worker.stop()
