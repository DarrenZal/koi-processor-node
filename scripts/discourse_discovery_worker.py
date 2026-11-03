#!/usr/bin/env python3
"""Drain ingestion-time discourse discovery jobs. Off by default.

  run     Needs KOI_DISCOURSE_DISCOVERY=1, an explicit --dsn and --once.
          Reclaims jobs whose lease expired, then claims pending jobs one at a
          time (FOR UPDATE SKIP LOCKED, as the terminusdb outbox worker does).
          Each job:
            1. plan, in one READ ONLY, REPEATABLE READ transaction under the
               job's planning deadline, after checking that the source
               revision, the moves, their extraction bindings and the caller
               context are still the job's (otherwise status 'stale');
            2. apply, in one short transaction under the job's apply deadline,
               with the canary's lock timeout, writer lock, write-scope check
               and fresh-connection ledger reconciliation. The candidates, the
               run row (kind ingestion_discovery) and the job's completion
               commit together, so a job is never left half done.
          A plan refused at apply is replanned at most max_replans times. Any
          other failure returns the job to 'pending' with backoff, or 'failed'
          after max_attempts. Nothing is written to the ingested document.
          The per-claim limit is the job's own (profile.per_move_cap, one of
          PER_MOVE_CAPS, else the job fails); the plan, the run's scope and its
          candidate write budget (claims x limit) follow it.
  status  Jobs, their runs, and bound extractions with no job. READ ONLY.

With --state-file, run also writes a JSON summary after each pass: what it did,
the queue by status, and every failed or stale job with its error, so a
scheduled pass that found trouble is visible without querying (the same shape
as embedding-repair's state file). scripts/run_discourse_discovery.sh is the
scheduled entry point.

Embeddings come from the local Ollama provider named in each job's profile.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_discovery as dd  # noqa: E402
from api import discourse_links as dl  # noqa: E402
from scripts import discourse_links_canary as canary  # noqa: E402

ALLOWED_TABLES = canary.CANDIDATE_TABLES + (dd.JOBS_TABLE,)


def ollama_embedder(profile: Dict[str, Any]):
    """Only local Ollama profiles: discovery makes no paid provider call."""
    model = profile["model"]
    if not model.startswith("ollama:"):
        raise ValueError(f"refusing embedding model {model!r}: only local ollama: models")
    from scripts.discourse_links_demo import _Ollama
    return _Ollama(model.split(":", 1)[1])


async def _require_tables(conn) -> None:
    await canary._require_draft_tables(conn)
    if not await dl._has_table(conn, dd.JOBS_TABLE):
        raise SystemExit(f"refusing: {dd.JOBS_TABLE} missing (migration 134 not applied)")


async def reclaim_expired(conn) -> List[Dict[str, Any]]:
    """Running jobs whose lease ran out (the worker died or hung). Their planning
    wrote nothing and their apply either committed together with the job's
    completion or not at all, so they go back to 'pending', or 'failed' when out
    of attempts."""
    rows = await conn.fetch(
        f"""UPDATE {dd.JOBS_TABLE}
               SET status = CASE WHEN attempts >= (limits->>'max_attempts')::int THEN 'failed' ELSE 'pending' END,
                   finished_at = CASE WHEN attempts >= (limits->>'max_attempts')::int THEN clock_timestamp() END,
                   last_error = 'lease expired (claimed by ' || claimed_by || ')',
                   claimed_at = NULL, claimed_by = NULL, next_attempt_at = clock_timestamp(),
                   updated_at = clock_timestamp()
             WHERE status = 'running'
               AND claimed_at < clock_timestamp() - make_interval(secs => (limits->>'lease_s')::float)
         RETURNING id, status""")
    return [dict(r) for r in rows]


async def claim_next(conn, worker_id: str) -> Optional[Dict[str, Any]]:
    row = await conn.fetchrow(
        f"""UPDATE {dd.JOBS_TABLE}
               SET status = 'running', claimed_at = clock_timestamp(), claimed_by = $1,
                   attempts = attempts + 1, updated_at = clock_timestamp()
             WHERE id = (SELECT id FROM {dd.JOBS_TABLE}
                          WHERE status = 'pending' AND next_attempt_at <= clock_timestamp()
                          ORDER BY next_attempt_at, id FOR UPDATE SKIP LOCKED LIMIT 1)
         RETURNING *""", worker_id)
    return dict(row) if row else None


async def _settle_job(dsn: str, job: Dict[str, Any], worker_id: str, status: str, error: Optional[str]) -> str:
    """Leave 'running' for pending (with backoff), failed or stale, if this worker still holds the lease."""
    limits = dd._jsonb(job["limits"])
    if status == "pending" and job["attempts"] >= limits["max_attempts"]:
        status = "failed"
    backoff = limits["retry_backoff_s"][min(job["attempts"], len(limits["retry_backoff_s"])) - 1]
    conn = await asyncpg.connect(dsn)
    try:
        done = await conn.fetchval(
            f"""UPDATE {dd.JOBS_TABLE}
                   SET status = $3, last_error = $4, claimed_at = NULL, claimed_by = NULL,
                       next_attempt_at = CASE WHEN $3 = 'pending' THEN clock_timestamp() + make_interval(secs => $5)
                                              ELSE next_attempt_at END,
                       finished_at = CASE WHEN $3 = 'pending' THEN NULL ELSE clock_timestamp() END,
                       updated_at = clock_timestamp()
                 WHERE id = $1 AND status = 'running' AND claimed_by = $2 RETURNING status""",
            job["id"], worker_id, status, (error or "")[:2000], float(backoff))
    finally:
        await conn.close()
    return done or "lease lost"


async def _plan_job(dsn: str, job: Dict[str, Any], context, embedder, run_id: str) -> Dict[str, Any]:
    profile, limits = dd._jsonb(job["profile"]), dd._jsonb(job["limits"])
    conn = await asyncpg.connect(dsn)

    async def body():
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            drift = await dd.job_drift(conn, job, context)
            if drift:
                raise dd.JobStale("; ".join(drift))
            return await dl.plan_discovery(
                conn, embedder, [m for m, _ in dd._jsonb(job["moves"])], context=context,
                discovered_by=dd.DISCOVERED_BY, run_id=run_id, embedding_model=profile["model"],
                text_prefix=profile["text_prefix"], move_types=profile["move_types"],
                lexical_limit=profile["lexical_limit"], semantic_limit=profile["semantic_limit"],
                per_move_cap=profile["per_move_cap"], per_target_source_cap=profile["per_target_source_cap"],
                batch_size=profile["embed_batch_size"])

    task = asyncio.create_task(body())
    done, _ = await asyncio.wait({task}, timeout=limits["plan_deadline_s"])
    if task in done and task.exception() is None:
        await conn.close()
        return task.result()
    conn.terminate()                                   # read-only: nothing to undo
    if task not in done:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        raise TimeoutError(f"planning deadline of {limits['plan_deadline_s']} s exceeded")
    raise task.exception()


def _scope(job: Dict[str, Any], plan_sha256: str) -> Dict[str, Any]:
    profile = dd._jsonb(job["profile"])
    moves = [m for m, _ in dd._jsonb(job["moves"])]
    return {"kind": dd.RUN_KIND, "job_id": job["id"], "job_key": job["job_key"], "source_rid": job["source_rid"],
            "source_version": job["source_version"], "extraction_run_id": job["extraction_run_id"],
            "seed_move_ids": sorted(moves), "context": "personal", "model": profile["model"],
            "text_prefix": profile["text_prefix"], "per_move_cap": profile["per_move_cap"],
            "per_target_source_cap": profile["per_target_source_cap"],
            "limits": {"max_embedding_writes": len(moves), "max_candidate_inserts": len(moves) * profile["per_move_cap"]},
            "writes_judgments": False, "plan_sha256": plan_sha256}


async def _apply_body(conn, job: Dict[str, Any], plan: Dict[str, Any], scope: Dict[str, Any], context,
                      worker_id: str, started: float) -> Dict[str, Any]:
    run_id = plan["run_id"]
    timings: Dict[str, float] = {}
    async with conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{canary.LOCK_TIMEOUT}'")
        await conn.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{canary.IDLE_IN_TRANSACTION_TIMEOUT}'")
        if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(hashtext($1))", canary.WRITER_LOCK):
            raise canary.CanaryRefused("another discourse-links writer holds the writer lock")
        held = await conn.fetchrow(f"SELECT status, claimed_by, attempts FROM {dd.JOBS_TABLE} WHERE id = $1 FOR UPDATE",
                                   job["id"])
        if (held["status"], held["claimed_by"], held["attempts"]) != ("running", worker_id, job["attempts"]):
            raise canary.CanaryRefused(f"job {job['id']} is no longer leased to {worker_id}")
        await conn.execute(
            """INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status)
               VALUES ($1, $2, $3, $4::jsonb, 'completed')""",
            run_id, dd.RUN_KIND, job["job_key"], json.dumps(scope))
        drift = await dd.job_drift(conn, job, context, lock=True)
        if drift:
            raise dd.JobStale("; ".join(drift))
        recorder = dl.RunRecorder(run_id, **scope["limits"])
        mark = time.monotonic()
        applied = await dl.apply_plan(conn, plan, context=context, recorder=recorder)
        timings["apply_s"] = round(time.monotonic() - mark, 3)
        await conn.execute(
            f"""UPDATE {dd.JOBS_TABLE} SET status = 'completed', run_id = $2, last_error = NULL,
                       claimed_at = NULL, claimed_by = NULL, finished_at = clock_timestamp(),
                       updated_at = clock_timestamp() WHERE id = $1""", job["id"], run_id)
        writes = await canary._transaction_writes(conn)
        outside = {t: w for t, w in writes.items() if t not in {f"public.{x}" for x in ALLOWED_TABLES}}
        if outside:
            raise canary.CanaryRefused(f"the run wrote outside its tables: {outside}")
        locks = await canary._own_locks(conn)
        timings["transaction_s"] = round(time.monotonic() - started, 3)
        result = {"mode": "ingestion", "job_id": job["id"], "plan_sha256": scope["plan_sha256"],
                  "plan": {k: plan[k] for k in ("planned_at", "planned_effects", "timings")},
                  "discovery": plan["summary"], "applied": applied, "effects": dict(recorder.counts),
                  "table_writes": writes, "locks_at_commit": locks, "timings": timings}
        await canary._finish(conn, run_id, result)
    return result


async def run_job(dsn: str, job: Dict[str, Any], *, worker_id: str, embedder_for: Callable = ollama_embedder,
                  plan_dir: Optional[Path] = None) -> Dict[str, Any]:
    """One claimed job to completion, stale, pending (retry) or failed."""
    limits = dd._jsonb(job["limits"])
    moves = dd._jsonb(job["moves"])
    outcome: Dict[str, Any] = {"job_id": job["id"], "attempt": job["attempts"], "plans": []}
    try:
        context = dd.configured_context()
    except ValueError as exc:
        outcome["status"] = await _settle_job(dsn, job, worker_id, "pending", str(exc))
        return outcome
    if len(moves) > limits["max_moves"]:
        outcome["status"] = await _settle_job(dsn, job, worker_id, "failed",
                                              f"{len(moves)} moves > max_moves {limits['max_moves']}")
        return outcome
    cap = dd._jsonb(job["profile"]).get("per_move_cap")
    if type(cap) is not int or cap not in dd.PER_MOVE_CAPS:     # the write budget is claims x this limit
        outcome["status"] = await _settle_job(dsn, job, worker_id, "failed",
                                              f"per_move_cap {cap!r} is not one of {dd.PER_MOVE_CAPS}")
        return outcome
    for replan in range(limits["max_replans"] + 1):
        run_id = f"ingest:{job['id']}:{job['attempts']}" + (f".{replan}" if replan else "")
        try:
            plan = await _plan_job(dsn, job, context, embedder_for(dd._jsonb(job["profile"])), run_id)
        except dd.JobStale as exc:
            outcome.update(status=await _settle_job(dsn, job, worker_id, "stale", f"stale: {exc}"))
            return outcome
        except Exception as exc:  # noqa: BLE001 - provider down, deadline, database error: retry later
            outcome.update(status=await _settle_job(dsn, job, worker_id, "pending",
                                                    f"planning: {type(exc).__name__}: {exc}"))
            return outcome
        data = canary.plan_bytes(plan)
        plan_sha256 = hashlib.sha256(data).hexdigest()
        if plan_dir:
            out = Path(plan_dir) / f"{run_id.replace(':', '_')}.plan.json"
            out.with_name(out.name + ".partial").write_bytes(data)
            os.replace(out.with_name(out.name + ".partial"), out)
        outcome["plans"].append({"run_id": run_id, "sha256": plan_sha256, "planned_effects": plan["planned_effects"]})
        scope = _scope(job, plan_sha256)
        try:
            result = await canary._run_with_deadline(
                dsn, run_id, manifest_sha256=job["job_key"], scope=scope, deadline_s=limits["apply_deadline_s"],
                kind=dd.RUN_KIND,
                body=lambda conn, started: _apply_body(conn, job, plan, scope, context, worker_id, started))
        except canary.RunFailed as exc:
            if exc.committed:                      # the job's completion committed with the run
                outcome.update(status="completed", run_id=run_id, note=str(exc))
                return outcome
            if isinstance(exc.cause, dd.JobStale):
                outcome.update(status=await _settle_job(dsn, job, worker_id, "stale", f"stale: {exc.cause}"))
                return outcome
            if isinstance(exc.cause, dl.PlanRefused) and replan < limits["max_replans"]:
                outcome.setdefault("replanned_after", []).append(str(exc.cause)[:300])
                continue
            outcome.update(status=await _settle_job(dsn, job, worker_id, "pending", str(exc)))
            return outcome
        outcome.update(status="completed", run_id=run_id, effects=result["effects"], timings=result["timings"])
        return outcome
    raise AssertionError("unreachable")


async def queue_summary(dsn: str) -> Dict[str, Any]:
    """Read-only: jobs by status, and the ones needing a person (failed, stale)."""
    view = await canary._readonly(dsn, lambda c: dd.job_status(c))
    counts: Dict[str, int] = {}
    for j in view["jobs"]:
        counts[j["status"]] = counts.get(j["status"], 0) + 1
    attention = [{k: j[k] for k in ("id", "source_rid", "status", "attempts", "last_error", "finished_at")}
                 for j in view["jobs"] if j["status"] in ("failed", "stale")]
    return {"queue": counts, "needs_attention": attention, "unscheduled_extractions": len(view["unscheduled"])}


def write_state(path: Path, state: Dict[str, Any]) -> None:
    partial = path.with_name(path.name + ".partial")
    partial.write_text(json.dumps(state, indent=1, default=str) + "\n")
    os.replace(partial, path)


async def run_once(dsn: str, *, max_jobs: int = 10, worker_id: Optional[str] = None,
                   embedder_for: Callable = ollama_embedder, plan_dir: Optional[Path] = None) -> Dict[str, Any]:
    if os.getenv("KOI_DISCOURSE_DISCOVERY") != "1":
        raise SystemExit("refusing: discovery is off (set KOI_DISCOURSE_DISCOVERY=1 for this process)")
    worker_id = worker_id or f"{socket.gethostname()}:{os.getpid()}"
    conn = await asyncpg.connect(dsn)
    try:
        await _require_tables(conn)
        reclaimed = await reclaim_expired(conn)
        outcomes = []
        while len(outcomes) < max_jobs:
            job = await claim_next(conn, worker_id)
            if job is None:
                break
            outcomes.append(await run_job(dsn, job, worker_id=worker_id, embedder_for=embedder_for,
                                          plan_dir=plan_dir))
    finally:
        await conn.close()
    return {"worker_id": worker_id, "reclaimed": reclaimed, "jobs": outcomes}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--dsn", required=True)
    r.add_argument("--once", action="store_true", help="drain what is due, then exit (required)")
    r.add_argument("--max-jobs", type=int, default=10)
    r.add_argument("--plan-dir", help="keep each plan file here")
    r.add_argument("--state-file", help="write a JSON summary of this pass and the queue here")
    s = sub.add_parser("status")
    s.add_argument("--dsn", default=canary.LIVE_READ_DSN)
    s.add_argument("--source-rid")
    args = ap.parse_args()
    if args.cmd == "status":
        out = asyncio.run(canary._readonly(args.dsn, lambda c: dd.job_status(c, source_rid=args.source_rid)))
    else:
        if not args.once:
            raise SystemExit("refusing: run needs --once (there is no resident worker)")
        out = asyncio.run(run_once(args.dsn, max_jobs=args.max_jobs,
                                   plan_dir=Path(args.plan_dir) if args.plan_dir else None))
        if args.state_file:
            from datetime import datetime, timezone
            write_state(Path(args.state_file), {"at": datetime.now(timezone.utc).isoformat(), "pass": out,
                                                **asyncio.run(queue_summary(args.dsn))})
    print(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
