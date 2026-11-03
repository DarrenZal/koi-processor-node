#!/usr/bin/env python3
"""Candidate-only canary for cross-source discourse links.

  manifest   Seed manifest, eligibility report and approval scope (seed ids,
             model, caps, write limits). One READ ONLY transaction against
             personal_koi by default; writes only --out and prints its sha256.
             That file and hash are what gets approved.
  discover   Needs --manifest and --manifest-sha256 of an approved manifest,
             an explicit --dsn, --apply, KOI_DISCOURSE_DISCOVERY=1 and the draft
             tables. Refuses if any seed move's identity, text, version, binding
             or eligibility differs from the manifest, or if any move was added.
             Runs as ONE transaction: embeddings, candidates, claim texts and an
             effect row per change, under an overall deadline (180 s; at most
             600 s with --deadline-s), a local 2 s lock_timeout and an
             exclusive writer lock. Before commit it checks
             that it wrote only its own tables. A limit breach, timeout or error
             rolls all of it back and leaves only a 'failed' run row; after a
             timeout or lost connection the ledger is read on a fresh connection
             to decide whether the run committed. No retry. Writes no judgment.
  plan       Two-phase discovery, phase 1 (O2). Same approval and drift checks
             as discover, then embeds and ranks in ONE READ ONLY, REPEATABLE
             READ transaction under its own deadline (600 s, at most 1800 s).
             Writes only --out, an inspectable plan, and prints its sha256.
  apply      Phase 2. Needs the plan and its sha256 plus the approved manifest.
             One short transaction (deadline 60 s, at most 180 s) with the same
             lock timeout, writer lock and ledger reconciliation as discover. It
             locks and rechecks every move, vector and candidate the plan names
             and refuses on any difference; it never reruns retrieval. Writes
             only candidate-side tables, never a judgment.
  report     What one run recorded, in the personal context. READ ONLY.
  candidates Read-only look at stored candidates for a text query, a move or a
             run, each labelled as an unreviewed candidate.
  rollback   Undo exactly what one completed run recorded, and only while each
             affected row is still as the run left it; otherwise refuse.
             Unknown, failed or already rolled-back runs are refused.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_links as dl  # noqa: E402

LIVE_READ_DSN = "postgresql://koi:@localhost:5432/personal_koi"
MODEL = "ollama:nomic-embed-text"
TEXT_PREFIX = "search_document: "
PER_MOVE_CAP = 5
# Operational limits for a canary run. Transaction-local only:
# no global database setting changes.
CANARY_DEADLINE_S = 180
MAX_DEADLINE_S = 600      # the most --deadline-s may allow
LOCK_TIMEOUT = "2s"
IDLE_IN_TRANSACTION_TIMEOUT = "60s"
WRITER_LOCK = "discourse-links-writer"
PLAN_DEADLINE_S = 600     # planning only reads; retrieval can take minutes
MAX_PLAN_DEADLINE_S = 1800
APPLY_DEADLINE_S = 60     # the write transaction
MAX_APPLY_DEADLINE_S = 180
DISCOVERED_BY = "system:discourse-links-canary"

# The six focal sources of the bounded sample are the first seed.
SEED_SOURCES = (
    "document:4393761eef740a08b1b97f8f14470f3ca7e0ab61d213c9c44317a170b9da2b8a",  # Levin & Lyons 2026
    "document:262f65fd6a5b0b3cb026cf2e50644d051ab14d8185172bac77e90b3c4c25bd19",  # Garfield essay + talk
    "substack-corpus:michaelgarfield:scenius",
    "substack-corpus:willruddick:toward-a-common-grammar-for-finance",
    "substack-corpus:natehagens:nato-in-ukraine-from-proxy-to-participants",
    "substack-corpus:natehagens:question-everything-towards-cooperation",
)
DRAFT_TABLES = ("discourse_claim_representations", "discourse_move_extraction_bindings",
                "discourse_move_embeddings", "discourse_link_candidates", "discourse_relations",
                "discourse_review_events", "discourse_link_runs", "discourse_link_run_effects")
# What applying a plan may write: no judgment, review event or binding table.
CANDIDATE_TABLES = ("discourse_claim_representations", "discourse_move_embeddings", "discourse_link_candidates",
                    "discourse_link_runs", "discourse_link_run_effects")
CANDIDATE_LABEL = ("CANDIDATES: unreviewed links proposed by text similarity. Not judgments; they establish "
                   "no identity, support, contradiction or corroboration.")


class CanaryRefused(RuntimeError):
    pass


class RunFailed(SystemExit):
    """A run that did not complete. committed=True: the call failed, but the
    ledger read on a fresh connection shows the run committed. `cause` is the
    exception that ended it, or None for a deadline."""

    def __init__(self, message: str, *, committed: bool, cause: Optional[BaseException]):
        super().__init__(message)
        self.committed = committed
        self.cause = cause


def bounded_seconds(limit: float):
    """An argparse type: a finite number of seconds, more than 0 and at most `limit`."""
    def parse(text: str) -> float:
        try:
            value = float(text)
        except ValueError:
            raise argparse.ArgumentTypeError(f"not a number of seconds: {text!r}")
        if not 0 < value <= limit:               # also refuses nan and inf
            raise argparse.ArgumentTypeError(f"must be more than 0 and at most {limit} s, got {text}")
        return value
    return parse


deadline_seconds = bounded_seconds(MAX_DEADLINE_S)          # --deadline-s
plan_deadline_seconds = bounded_seconds(MAX_PLAN_DEADLINE_S)
apply_deadline_seconds = bounded_seconds(MAX_APPLY_DEADLINE_S)


def manifest_bytes(manifest: Dict[str, Any]) -> bytes:
    return (json.dumps(manifest, indent=1, ensure_ascii=False, default=str, sort_keys=True) + "\n").encode()


async def build_manifest(conn, sources: Sequence[str] = SEED_SOURCES, model: str = MODEL,
                         text_prefix: str = TEXT_PREFIX, per_move_cap: int = PER_MOVE_CAP,
                         per_target_source_cap: int = dl.DEFAULT_PER_TARGET_SOURCE_CAP) -> Dict[str, Any]:
    ctx = dl.personal_context()
    manifest = await dl.seed_manifest(conn, sources, context=ctx, model=model)
    seed = sorted(e["move_id"] for e in manifest["entries"] if e["seed_eligible"])
    manifest["approval_scope"] = {
        "kind": "canary", "sources": list(sources), "context": "personal", "model": model,
        "text_prefix": text_prefix, "per_move_cap": per_move_cap, "per_target_source_cap": per_target_source_cap,
        "seed_move_ids": seed,
        "limits": {"max_embedding_writes": len(seed), "max_candidate_inserts": len(seed) * per_move_cap},
        "writes_judgments": False,
    }
    manifest["corpus_coverage"] = await dl.retrieval_coverage(conn, model, context=ctx)
    manifest["database"] = await conn.fetchval("SELECT current_database()")
    manifest["draft_tables_present"] = {t: await dl._has_table(conn, t) for t in DRAFT_TABLES}
    return manifest


def manifest_drift(approved: Dict[str, Any], live: Dict[str, Any]) -> List[str]:
    """Every difference between the approved manifest's pinned fields and now."""
    a = {e["move_id"]: e for e in dl.manifest_pins(approved)}
    b = {e["move_id"]: e for e in dl.manifest_pins(live)}
    problems = []
    for mid in sorted(set(a) | set(b)):
        if mid not in b:
            problems.append(f"{mid}: approved but not present or not visible now")
        elif mid not in a:
            problems.append(f"{mid}: present now but not in the approved manifest")
        elif a[mid] != b[mid]:
            problems.append(f"{mid}: {', '.join(k for k in a[mid] if a[mid][k] != b[mid][k])} changed")
    scope_seed = sorted(approved["approval_scope"]["seed_move_ids"])
    if scope_seed != sorted(e["move_id"] for e in approved["entries"] if e["seed_eligible"]):
        problems.append("approval scope seed ids disagree with the manifest's seed-eligible entries")
    return problems


async def _require_draft_tables(conn) -> None:
    missing = [t for t in DRAFT_TABLES if not await dl._has_table(conn, t)]
    if missing:
        raise SystemExit(f"refusing: draft tables missing in {await conn.fetchval('SELECT current_database()')}: {missing}")


def load_approved_manifest(path: Optional[Path], sha256: Optional[str]) -> Dict[str, Any]:
    if not path or not sha256:
        raise SystemExit("refusing: discovery needs the approved manifest and its sha256 "
                         "(--manifest, --manifest-sha256)")
    data = Path(path).read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise SystemExit(f"refusing: manifest sha256 is {actual}, approved {sha256}")
    manifest = json.loads(data)
    if manifest.get("approval_scope", {}).get("kind") != "canary":
        raise SystemExit("refusing: the manifest has no canary approval scope")
    return manifest


async def _transaction_writes(conn) -> Dict[str, Dict[str, int]]:
    """Rows this transaction has inserted, updated or deleted, per table, so far."""
    rows = await conn.fetch(
        """SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_xact_user_tables
            WHERE n_tup_ins + n_tup_upd + n_tup_del > 0 ORDER BY schemaname, relname""")
    return {f"{r['schemaname']}.{r['relname']}": {"ins": r["n_tup_ins"], "upd": r["n_tup_upd"], "del": r["n_tup_del"]}
            for r in rows}


async def _discover_body(conn, run_id: str, approved: Dict[str, Any], manifest_sha256: str, embedder,
                         embed_batch_size: int, started: float, deadline_s: float) -> Dict[str, Any]:
    """The whole canary run as one transaction. Settings are transaction-local."""
    scope = approved["approval_scope"]
    ctx = dl.personal_context()
    timings: Dict[str, float] = {}
    async with conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        await conn.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{IDLE_IN_TRANSACTION_TIMEOUT}'")
        if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(hashtext($1))", WRITER_LOCK):
            raise CanaryRefused("another discourse-links writer holds the writer lock")
        await conn.execute(
            """INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status)
               VALUES ($1, 'canary', $2, $3::jsonb, 'completed')""",
            run_id, manifest_sha256, json.dumps(scope))
        drift = manifest_drift(approved, await dl.seed_manifest(conn, scope["sources"], context=ctx,
                                                                model=scope["model"]))
        if drift:
            raise CanaryRefused("seed drifted from the approved manifest: " + "; ".join(drift))
        recorder = dl.RunRecorder(run_id, **scope["limits"])
        seed = scope["seed_move_ids"]
        mark = time.monotonic()
        embedded = await dl.embed_moves(conn, embedder, seed, model=scope["model"], text_prefix=scope["text_prefix"],
                                        batch_size=embed_batch_size, recorder=recorder)
        timings["embed_s"] = round(time.monotonic() - mark, 3)
        mark = time.monotonic()
        summary = await dl.discover_candidates(
            conn, seed, context=ctx, discovered_by=DISCOVERED_BY, run_id=run_id,
            embedding_model=scope["model"], per_move_cap=scope["per_move_cap"],
            per_target_source_cap=scope["per_target_source_cap"], recorder=recorder)
        timings["discover_s"] = round(time.monotonic() - mark, 3)
        drift = manifest_drift(approved, await dl.seed_manifest(conn, scope["sources"], context=ctx,
                                                                model=scope["model"]))
        if drift:
            raise CanaryRefused("seed changed during the run: " + "; ".join(drift))
        writes = await _transaction_writes(conn)
        allowed = {f"public.{t}" for t in DRAFT_TABLES}
        outside = {t: w for t, w in writes.items() if t not in allowed}
        if outside:
            raise CanaryRefused(f"the run wrote outside its tables: {outside}")
        timings["transaction_s"] = round(time.monotonic() - started, 3)
        result = {"seed_moves": len(seed), "embedded": embedded, "discovery": summary,
                  "effects": dict(recorder.counts), "table_writes": writes, "timings": timings,
                  "deadline_s": deadline_s}
        await _finish(conn, run_id, result)
    return result


async def _finish(conn, run_id: str, result: Dict[str, Any]) -> None:
    """clock_timestamp(), not now(): now() is the transaction's start, which made
    a run's finished_at equal its started_at."""
    await conn.execute(
        "UPDATE discourse_link_runs SET summary = $2::jsonb, finished_at = clock_timestamp() WHERE run_id = $1",
        run_id, json.dumps(result, default=str))


async def _settle(dsn: str, run_id: str, backend_pid: Optional[int], wait_s: float = 10.0) -> Optional[str]:
    """After a failure: wait (on a fresh connection) for the run's backend to be
    gone, so its transaction is resolved, then read what the run ledger says.
    The answer, not the failed client call, decides whether the run committed."""
    conn = await asyncpg.connect(dsn)
    try:
        for attempt in range(2):
            deadline = time.monotonic() + wait_s
            while backend_pid and await conn.fetchval("SELECT 1 FROM pg_stat_activity WHERE pid = $1", backend_pid):
                if time.monotonic() > deadline:
                    break
                await asyncio.sleep(0.1)
            else:
                break
            if attempt == 0:      # still alive: end our own session's backend
                await conn.fetchval("SELECT pg_terminate_backend($1)", backend_pid)
        return await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1", run_id)
    finally:
        await conn.close()


async def _record_failed(dsn: str, run_id: str, manifest_sha256: str, scope: Dict[str, Any], cause: str,
                         kind: str = "canary") -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            """INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status, error, finished_at)
               VALUES ($1, $5, $2, $3::jsonb, 'failed', $4, clock_timestamp())
               ON CONFLICT (run_id) DO NOTHING""",
            run_id, manifest_sha256, json.dumps(scope), cause, kind)
    finally:
        await conn.close()


def _require_discovery_flag() -> None:
    if os.getenv("KOI_DISCOURSE_DISCOVERY") != "1":
        raise SystemExit("refusing: discovery is off (set KOI_DISCOURSE_DISCOVERY=1 for this run)")


def _require_canary_run_id(run_id: str) -> None:
    if not run_id.startswith("canary:"):
        raise SystemExit("refusing: canary run ids start with 'canary:'")


async def run_discover(dsn: str, run_id: str, *, embedder, manifest_path: Optional[Path] = None,
                       manifest_sha256: Optional[str] = None, model: Optional[str] = None,
                       text_prefix: Optional[str] = None, sources: Optional[Sequence[str]] = None,
                       deadline_s: float = CANARY_DEADLINE_S, embed_batch_size: int = 32) -> Dict[str, Any]:
    """One canary run, embedding and discovery in one transaction, under an overall deadline."""
    _require_discovery_flag()
    _require_canary_run_id(run_id)
    if not 0 < deadline_s <= MAX_DEADLINE_S:
        raise SystemExit(f"refusing: a deadline of {deadline_s!r} s is not more than 0 and at most {MAX_DEADLINE_S} s")
    approved = load_approved_manifest(manifest_path, manifest_sha256)
    scope = approved["approval_scope"]
    for name, given in (("model", model), ("text_prefix", text_prefix), ("sources", list(sources) if sources else None)):
        if given is not None and given != scope[name]:
            raise SystemExit(f"refusing: {name} {given!r} differs from the approved {scope[name]!r}")
    return await _run_with_deadline(
        dsn, run_id, manifest_sha256=manifest_sha256, scope=scope, deadline_s=deadline_s,
        body=lambda conn, started: _discover_body(conn, run_id, approved, manifest_sha256, embedder,
                                                  embed_batch_size, started, deadline_s))


async def _run_with_deadline(dsn: str, run_id: str, *, manifest_sha256: str, scope: Dict[str, Any],
                             deadline_s: float, body, kind: str = "canary") -> Dict[str, Any]:
    """Run body(conn, started), one transaction, under an overall deadline. On
    any failure, timeout or lost connection the session is terminated, so the
    server aborts the transaction and releases its locks. The run ledger is then
    read through a fresh connection. Nothing is retried."""
    conn = await asyncpg.connect(dsn)
    try:
        await _require_draft_tables(conn)
        if await conn.fetchval("SELECT 1 FROM discourse_link_runs WHERE run_id = $1", run_id):
            raise SystemExit(f"refusing: run id {run_id} was already used")
        backend_pid = await conn.fetchval("SELECT pg_backend_pid()")
    except BaseException:
        await conn.close()
        raise
    started = time.monotonic()
    task = asyncio.create_task(body(conn, started))
    done, _ = await asyncio.wait({task}, timeout=deadline_s)
    if task in done and task.exception() is None:
        result = task.result()
        await conn.close()
        return {"database": await _database_name(dsn), "wall_s": round(time.monotonic() - started, 3), **result}
    cause = (f"deadline of {deadline_s:g} s exceeded" if task not in done
             else f"{type(task.exception()).__name__}: {task.exception()}")
    conn.terminate()                       # the server aborts the open transaction and releases its locks
    if task not in done:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
    error = task.exception() if task in done else None
    status = await _settle(dsn, run_id, backend_pid)
    if status == "completed":
        raise RunFailed(f"{kind} {run_id}: {cause}, but the run ledger shows it COMMITTED. "
                        "Inspect it with `report`; do not rerun.", committed=True, cause=error)
    await _record_failed(dsn, run_id, manifest_sha256, scope, cause, kind)
    raise RunFailed(f"{kind} {run_id} failed and wrote nothing: {cause}", committed=False, cause=error)


# ---------------------------------------------------------------------------
# Two-phase discovery (O2): plan, then apply
# ---------------------------------------------------------------------------

def plan_bytes(plan: Dict[str, Any]) -> bytes:
    return (json.dumps(plan, indent=1, ensure_ascii=False, default=str) + "\n").encode()


def load_plan(path: Optional[Path], sha256: Optional[str]) -> Dict[str, Any]:
    if not path or not sha256:
        raise SystemExit("refusing: apply needs the plan and its sha256 (--plan, --plan-sha256)")
    data = Path(path).read_bytes()
    actual = hashlib.sha256(data).hexdigest()
    if actual != sha256:
        raise SystemExit(f"refusing: plan sha256 is {actual}, expected {sha256}")
    return json.loads(data)


def _ollama_digest(model: str) -> Optional[str]:
    """The local model's digest, so a plan names the exact weights. Best effort."""
    import urllib.request
    try:
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=5) as r:
            tags = json.load(r)["models"]
    except Exception:
        return None
    name = model.split(":", 1)[1] if model.startswith("ollama:") else model
    return next((m["digest"] for m in tags if m["name"] in (name, f"{name}:latest")), None)


async def run_plan(dsn: str, run_id: str, *, embedder, manifest_path: Optional[Path], manifest_sha256: Optional[str],
                   out: Path, deadline_s: float = PLAN_DEADLINE_S, embed_batch_size: int = 32,
                   profile: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Phase 1. One READ ONLY, REPEATABLE READ transaction: the approved-seed and
    drift checks of discover, then dl.plan_discovery. Writes only `out`, and only
    when planning finished. On a deadline or error the connection is ended and
    nothing is written anywhere."""
    _require_discovery_flag()
    _require_canary_run_id(run_id)
    if not 0 < deadline_s <= MAX_PLAN_DEADLINE_S:
        raise SystemExit(f"refusing: a planning deadline of {deadline_s!r} s is not more than 0 and at most "
                         f"{MAX_PLAN_DEADLINE_S} s")
    approved = load_approved_manifest(manifest_path, manifest_sha256)
    scope = approved["approval_scope"]
    out = Path(out)
    if out.exists():
        raise SystemExit(f"refusing: {out} exists; a plan is never overwritten")
    ctx = dl.personal_context()
    conn = await asyncpg.connect(dsn)
    started = time.monotonic()

    async def body():
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            await _require_draft_tables(conn)
            if await conn.fetchval("SELECT 1 FROM discourse_link_runs WHERE run_id = $1", run_id):
                raise CanaryRefused(f"run id {run_id} was already used")
            drift = manifest_drift(approved, await dl.seed_manifest(conn, scope["sources"], context=ctx,
                                                                    model=scope["model"]))
            if drift:
                raise CanaryRefused("seed drifted from the approved manifest: " + "; ".join(drift))
            return await dl.plan_discovery(
                conn, embedder, scope["seed_move_ids"], context=ctx, discovered_by=DISCOVERED_BY, run_id=run_id,
                embedding_model=scope["model"], text_prefix=scope["text_prefix"], per_move_cap=scope["per_move_cap"],
                per_target_source_cap=scope["per_target_source_cap"], batch_size=embed_batch_size)

    task = asyncio.create_task(body())
    done, _ = await asyncio.wait({task}, timeout=deadline_s)
    if task not in done or task.exception() is not None:
        cause = (f"deadline of {deadline_s:g} s exceeded" if task not in done
                 else f"{type(task.exception()).__name__}: {task.exception()}")
        conn.terminate()
        if task not in done:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
        raise SystemExit(f"planning {run_id} failed; no plan was written: {cause}")
    plan = task.result()
    await conn.close()
    plan["timings"]["wall_s"] = round(time.monotonic() - started, 3)
    plan["embedding_profile"].update(profile or {})
    plan["manifest_sha256"] = manifest_sha256
    plan["approval_scope"] = scope
    data = plan_bytes(plan)
    partial = out.with_name(out.name + ".partial")
    partial.write_bytes(data)
    os.replace(partial, out)
    return {"out": str(out), "sha256": hashlib.sha256(data).hexdigest(),
            **{k: plan[k] for k in ("run_id", "database", "planned_at", "planned_effects", "timings")},
            "summary": {k: v for k, v in plan["summary"].items() if k != "method_params"}}


async def _own_locks(conn) -> List[Dict[str, Any]]:
    """Relation and advisory locks this transaction holds now. Row locks are not
    in pg_locks; apply_plan reports the rows it locked FOR SHARE."""
    return [dict(r) for r in await conn.fetch(
        """SELECT l.locktype, coalesce(c.relname, l.locktype) AS object, l.mode, count(*) AS n
             FROM pg_locks l LEFT JOIN pg_class c ON c.oid = l.relation
            WHERE l.pid = pg_backend_pid() AND l.locktype IN ('relation', 'advisory')
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""")]


async def _apply_body(conn, plan: Dict[str, Any], plan_sha256: str, approved: Dict[str, Any], manifest_sha256: str,
                      started: float, deadline_s: float) -> Dict[str, Any]:
    """Phase 2 as one short transaction. Settings are transaction-local."""
    scope = approved["approval_scope"]
    ctx = dl.personal_context()
    run_id = plan["run_id"]
    timings: Dict[str, float] = {}
    async with conn.transaction():
        await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        await conn.execute(f"SET LOCAL idle_in_transaction_session_timeout = '{IDLE_IN_TRANSACTION_TIMEOUT}'")
        if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(hashtext($1))", WRITER_LOCK):
            raise CanaryRefused("another discourse-links writer holds the writer lock")
        await conn.execute(
            """INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status)
               VALUES ($1, 'canary', $2, $3::jsonb, 'completed')""",
            run_id, manifest_sha256, json.dumps(dict(scope, plan_sha256=plan_sha256)))
        drift = manifest_drift(approved, await dl.seed_manifest(conn, scope["sources"], context=ctx,
                                                                model=scope["model"]))
        if drift:
            raise CanaryRefused("seed drifted from the approved manifest: " + "; ".join(drift))
        recorder = dl.RunRecorder(run_id, **scope["limits"])
        mark = time.monotonic()
        applied = await dl.apply_plan(conn, plan, context=ctx, recorder=recorder)
        timings["apply_s"] = round(time.monotonic() - mark, 3)
        drift = manifest_drift(approved, await dl.seed_manifest(conn, scope["sources"], context=ctx,
                                                                model=scope["model"]))
        if drift:
            raise CanaryRefused("seed changed during the run: " + "; ".join(drift))
        writes = await _transaction_writes(conn)
        outside = {t: w for t, w in writes.items() if t not in {f"public.{x}" for x in CANDIDATE_TABLES}}
        if outside:
            raise CanaryRefused(f"the run wrote outside its tables: {outside}")
        locks = await _own_locks(conn)
        timings["transaction_s"] = round(time.monotonic() - started, 3)
        result = {"mode": "two-phase", "plan_sha256": plan_sha256, "seed_moves": len(scope["seed_move_ids"]),
                  "plan": {k: plan[k] for k in ("planned_at", "planned_effects", "timings")},
                  "discovery": plan["summary"], "applied": applied, "effects": dict(recorder.counts),
                  "table_writes": writes, "locks_at_commit": locks, "timings": timings, "deadline_s": deadline_s}
        await _finish(conn, run_id, result)
    return result


async def run_apply(dsn: str, *, plan_path: Optional[Path], plan_sha256: Optional[str],
                    manifest_path: Optional[Path], manifest_sha256: Optional[str],
                    deadline_s: float = APPLY_DEADLINE_S) -> Dict[str, Any]:
    """Phase 2: apply one plan made from the approved manifest, under a deadline."""
    _require_discovery_flag()
    if not 0 < deadline_s <= MAX_APPLY_DEADLINE_S:
        raise SystemExit(f"refusing: an apply deadline of {deadline_s!r} s is not more than 0 and at most "
                         f"{MAX_APPLY_DEADLINE_S} s")
    approved = load_approved_manifest(manifest_path, manifest_sha256)
    plan = load_plan(plan_path, plan_sha256)
    scope = approved["approval_scope"]
    if plan.get("manifest_sha256") != manifest_sha256 or plan.get("approval_scope") != scope:
        raise SystemExit("refusing: the plan was not made from this approved manifest")
    if plan.get("query_move_ids") != sorted(scope["seed_move_ids"]):
        raise SystemExit("refusing: the plan's query moves are not the approved seed")
    run_id = plan.get("run_id") or ""
    _require_canary_run_id(run_id)
    return await _run_with_deadline(
        dsn, run_id, manifest_sha256=manifest_sha256, scope=dict(scope, plan_sha256=plan_sha256),
        deadline_s=deadline_s,
        body=lambda conn, started: _apply_body(conn, plan, plan_sha256, approved, manifest_sha256, started,
                                               deadline_s))


async def _database_name(dsn: str) -> str:
    conn = await asyncpg.connect(dsn)
    try:
        return await conn.fetchval("SELECT current_database()")
    finally:
        await conn.close()


async def run_rollback(dsn: str, run_id: str, *, model: Optional[str] = None,
                       sources: Optional[Sequence[str]] = None,
                       accept_legacy_embedding_ownership: bool = False) -> Dict[str, Any]:
    """Undo one completed run from its effect rows: the supported backout.

    Every row the run created or changed must still be exactly as the run left
    it, checked under FOR UPDATE with the writer lock held. A vector counts as
    the run's only if its input hashes, dimension and exact value (the sha256 of
    vector_send) match what the run recorded writing. Any doubt refuses the
    whole rollback, and nothing is changed. The run, its effect rows and any
    job naming it are kept, so the history shows what was undone; the run's
    status becomes rolled_back. Claims, their extraction bindings and the
    ingested document are never touched.

    Effect rows written by earlier versions of this script recorded
    no vector value. Those are refused unless accept_legacy_embedding_ownership
    is passed, and then accepted only when the input hashes match and the row's
    created_at is still the run's own transaction start: the weaker evidence,
    reported in the result. `model` and `sources` are ignored."""
    conn = await asyncpg.connect(dsn)
    try:
        await _require_draft_tables(conn)
        async with conn.transaction():
            await conn.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            if not await conn.fetchval("SELECT pg_try_advisory_xact_lock(hashtext($1))", WRITER_LOCK):
                raise SystemExit("refusing: another discourse-links writer holds the writer lock")
            run = await conn.fetchrow("SELECT * FROM discourse_link_runs WHERE run_id = $1 FOR UPDATE", run_id)
            if run is None:
                raise SystemExit(f"refusing: no run {run_id}")
            if run["status"] != "completed":
                raise SystemExit(f"refusing: run {run_id} is {run['status']}; there is nothing of it to undo")
            effects = await conn.fetch("SELECT * FROM discourse_link_run_effects WHERE run_id = $1 ORDER BY id", run_id)
            by_candidate: Dict[Any, List] = defaultdict(list)
            by_embedding: Dict[Any, List] = defaultdict(list)
            representations: List[str] = []
            for e in effects:
                if e["effect"].startswith("candidate_"):
                    by_candidate[e["candidate_id"]].append(e)
                elif e["effect"].startswith("embedding_"):
                    by_embedding[(e["move_id"], e["model"])].append(e)
                else:
                    representations.append(e["representation_sha256"])
            problems: List[str] = []
            legacy: List[str] = []
            plan_candidates, plan_embeddings = [], []
            for cid, es in sorted(by_candidate.items(), key=lambda kv: str(kv[0])):
                final = json.loads(es[-1]["after_state"])
                c = await conn.fetchrow("SELECT * FROM discourse_link_candidates WHERE id = $1 FOR UPDATE", cid)
                if c is None:
                    problems.append(f"candidate {cid} no longer exists")
                    continue
                touched = (c["status"] != "candidate" or c["proposed_by"] or c["reviewed_by"]
                           or await conn.fetchval("SELECT 1 FROM discourse_relations WHERE candidate_id = $1", cid)
                           or await conn.fetchval("SELECT 1 FROM discourse_review_events WHERE candidate_id = $1", cid))
                if touched:
                    problems.append(f"candidate {cid} was proposed on, reviewed or judged after the run")
                elif (c["times_seen"], dl._iso(c["last_seen_at"])) != (final["times_seen"], final["last_seen_at"]):
                    problems.append(f"candidate {cid} was seen again after the run")
                elif any(e["effect"] == "candidate_inserted" for e in es):
                    plan_candidates.append(("delete", cid, None))
                else:
                    plan_candidates.append(("restore", cid, json.loads(es[0]["prior_state"])))
            for (mid, model_id), es in sorted(by_embedding.items(), key=lambda kv: (str(kv[0][0]), kv[0][1])):
                final = json.loads(es[-1]["after_state"])
                cur = await conn.fetchrow(
                    f"""SELECT e.representation_sha256, e.text_sha256, e.dimension,
                               {dl.VECTOR_SHA_SQL.format(a='e')} AS vector_sha256, e.created_at
                          FROM discourse_move_embeddings e WHERE e.move_id = $1 AND e.model = $2 FOR UPDATE""",
                    mid, model_id)
                if cur is None:
                    problems.append(f"embedding {mid}/{model_id} no longer exists")
                    continue
                if {"dimension", "vector_sha256"} <= set(final):
                    changed = [k for k in ("representation_sha256", "text_sha256", "dimension", "vector_sha256")
                               if cur[k] != final[k]]
                    if changed:
                        problems.append(f"embedding {mid}/{model_id} changed after the run ({', '.join(changed)})")
                        continue
                else:
                    legacy.append(f"{mid}/{model_id}")
                    if not accept_legacy_embedding_ownership:
                        problems.append(f"embedding {mid}/{model_id}: legacy effect row records no vector value, so "
                                        "ownership cannot be shown (see --accept-legacy-embedding-ownership)")
                        continue
                    if ((cur["representation_sha256"], cur["text_sha256"]) != (final["representation_sha256"],
                                                                                final["text_sha256"])
                            or cur["created_at"] != run["started_at"]):
                        problems.append(f"embedding {mid}/{model_id} changed after the run (legacy check)")
                        continue
                if es[0]["effect"] == "embedding_inserted":
                    plan_embeddings.append(("delete", mid, model_id, None))
                else:
                    plan_embeddings.append(("restore", mid, model_id, json.loads(es[0]["prior_state"])))
            if problems:
                raise SystemExit(f"refusing: ownership of {len(problems)} row(s) cannot be established: "
                                 + "; ".join(problems[:10]))
            counts = defaultdict(int)
            for action, cid, prior in plan_candidates:
                if action == "delete":
                    await conn.execute("DELETE FROM discourse_link_candidates WHERE id = $1", cid)
                else:
                    await conn.execute(
                        "UPDATE discourse_link_candidates SET times_seen = $2, last_seen_at = $3 WHERE id = $1",
                        cid, prior["times_seen"], datetime.fromisoformat(prior["last_seen_at"]))
                counts[f"candidates_{action}d"] += 1
            for action, mid, model_id, prior in plan_embeddings:
                if action == "delete":
                    await conn.execute("DELETE FROM discourse_move_embeddings WHERE move_id = $1 AND model = $2",
                                       mid, model_id)
                else:
                    await conn.execute(
                        """UPDATE discourse_move_embeddings SET representation_sha256 = $3, text_sha256 = $4,
                                  dimension = $5, embedding = $6::vector, created_at = $7
                            WHERE move_id = $1 AND model = $2""",
                        mid, model_id, prior["representation_sha256"], prior["text_sha256"], prior["dimension"],
                        prior["embedding"], datetime.fromisoformat(prior["created_at"]))
                counts[f"embeddings_{action}d"] += 1
            for h in representations:
                status = await conn.execute(
                    """DELETE FROM discourse_claim_representations r WHERE r.representation_sha256 = $1
                         AND NOT EXISTS (SELECT 1 FROM discourse_link_candidates c
                                          WHERE r.representation_sha256 IN (c.query_representation_sha256,
                                                                            c.target_representation_sha256))
                         AND NOT EXISTS (SELECT 1 FROM discourse_relations x
                                          WHERE r.representation_sha256 IN (x.from_representation_sha256,
                                                                            x.to_representation_sha256))
                         AND NOT EXISTS (SELECT 1 FROM discourse_move_extraction_bindings b
                                          WHERE b.representation_sha256 = r.representation_sha256)""", h)
                counts["representations_deleted" if status.endswith(" 1") else "representations_kept_in_use"] += 1
            await conn.execute(
                "UPDATE discourse_link_runs SET status = 'rolled_back', finished_at = now() WHERE run_id = $1", run_id)
        return {"database": await conn.fetchval("SELECT current_database()"), "run_id": run_id, **counts,
                "legacy_embedding_effects_accepted": len(legacy), "history_kept": True}
    finally:
        await conn.close()


async def build_report(conn, run_id: str) -> Dict[str, Any]:
    """What one canary run recorded, as the personal context sees it. SELECTs only."""
    await _require_draft_tables(conn)
    run = await conn.fetchrow("SELECT * FROM discourse_link_runs WHERE run_id = $1", run_id)
    if run is None:
        raise SystemExit(f"no run {run_id}")
    scope = json.loads(run["approval_scope"]) if isinstance(run["approval_scope"], str) else run["approval_scope"]
    ctx = dl.personal_context()
    counts = dict(await conn.fetchrow(
        """SELECT count(*) AS candidates, count(*) FILTER (WHERE status <> 'candidate') AS reviewed,
                  count(DISTINCT query_move_id) AS query_moves, count(DISTINCT target_source_rid) AS target_sources,
                  count(*) FILTER (WHERE semantic_rank IS NOT NULL) AS with_semantic_rank
             FROM discourse_link_candidates WHERE run_id = $1""", run_id))
    counts["effects"] = {r["effect"]: r["n"] for r in await conn.fetch(
        "SELECT effect, count(*) AS n FROM discourse_link_run_effects WHERE run_id = $1 GROUP BY 1", run_id)}
    counts["judgments_total"] = await conn.fetchval("SELECT count(*) FROM discourse_relations")
    counts["review_events_total"] = await conn.fetchval("SELECT count(*) FROM discourse_review_events")
    connected = {}
    for mid in scope["seed_move_ids"]:
        view = await dl.connected_material(conn, mid, context=ctx, include_candidates=True)
        connected[mid] = [c for c in view["candidates"] if c["run_id"] == run_id]
    note = None
    if run["status"] == "completed" and run["finished_at"] == run["started_at"]:
        note = ("finished_at equals started_at: the code that ran this canary set it with now(), which is the "
                "transaction's start. The run's duration is summary.timings.transaction_s. Later runs write "
                "clock_timestamp().")
    return {"run": {**{k: dl._iso(run[k]) for k in ("run_id", "status", "manifest_sha256", "started_at",
                                                     "finished_at", "error")}, "timestamp_note": note},
            "counts": counts, "coverage": await dl.retrieval_coverage(conn, scope["model"], context=ctx),
            "same_work_signals": await dl.same_work_signals(conn, context=ctx),
            "candidates_by_seed_move": connected}


def _brief(mv: Dict[str, Any]) -> Dict[str, Any]:
    return {"id": mv["id"], "move_type": mv["move_type"], "title": mv["title"], "detail": mv["detail"],
            "source": {k: mv["source"][k] for k in ("rid", "title", "source_url")},
            "binding_kind": mv["extraction"]["binding_kind"]}


async def inspect_candidates(conn, *, query: Optional[str] = None, move_id: Optional[str] = None,
                             run_id: Optional[str] = None, limit: int = 5) -> Dict[str, Any]:
    """Stored candidates as the personal context sees them, each labelled as an
    unreviewed candidate, with both ends' sources. Current admitted judgments on
    the same moves, if any, are listed apart under admitted_relations. SELECTs only. Without
    --move-id it picks up to `limit` visible moves that have open candidates,
    matching --query (plainto_tsquery) when given."""
    await _require_draft_tables(conn)
    ctx = dl.personal_context()
    if move_id:
        ids = [move_id]
    else:
        vis, vp = dl._visibility(ctx, "m", 4)
        doc = "to_tsvector('english', coalesce(m.title,'')||' '||coalesce(m.detail,''))"
        has = ("EXISTS (SELECT 1 FROM discourse_link_candidates c WHERE m.id IN (c.query_move_id, c.target_move_id)"
               " AND c.status = 'candidate' AND ($2::text IS NULL OR c.run_id = $2))")
        sql = (f"SELECT m.id FROM session_discourse_moves m WHERE {has} AND {vis} AND "
               + (f"{doc} @@ plainto_tsquery('english', $1) ORDER BY ts_rank({doc}, plainto_tsquery('english', $1)) DESC, m.id"
                  if query else "$1::text IS NULL ORDER BY m.id") + " LIMIT $3")
        ids = [str(r["id"]) for r in await conn.fetch(sql, query, run_id, limit, *vp)]
    results = []
    for mid in ids:
        view = await dl.connected_material(conn, mid, context=ctx, include_candidates=True)
        results.append({"move": _brief(view["move"]), "admitted_relations": [
            {"label": "admitted (reviewed judgment)", "relation_id": r["relation_id"], "relation": r["relation"],
             "semantics": r["semantics"], "direction": r["direction"], "asserted_by": r["asserted_by"],
             "authority_basis": r["authority_basis"], "other": _brief(r["other"])} for r in view["relations"]],
            "candidates": [
            {"status": c["status"], "label": "candidate (unreviewed)", "candidate_id": c["candidate_id"],
             "run_id": c["run_id"], "discovered_by": c["discovered_by"], "times_seen": c["times_seen"],
             "found_by": "+".join(leg for leg in ("lexical", "semantic") if c["scores"][f"{leg}_rank"] is not None),
             "scores": c["scores"], "current": c["current"], "admissible_now": c["admissible_now"],
             "admission_blockers": c["admission_blockers"], "other": _brief(c["other"])}
            for c in view["candidates"] if run_id is None or c["run_id"] == run_id]})
    return {"label": CANDIDATE_LABEL, "database": await conn.fetchval("SELECT current_database()"),
            "read_only": await conn.fetchval("SHOW transaction_read_only") == "on",
            "context": "personal", "query": query, "move_id": move_id, "run_id": run_id, "results": results}


async def _readonly(dsn: str, fn):
    conn = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(readonly=True):
            return await fn(conn)
    finally:
        await conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("manifest")
    m.add_argument("--out", required=True)
    m.add_argument("--dsn", default=LIVE_READ_DSN)
    r = sub.add_parser("report")
    r.add_argument("--dsn", required=True)
    r.add_argument("--run-id", required=True)
    r.add_argument("--out", required=True)
    d = sub.add_parser("discover")
    d.add_argument("--manifest", required=True)
    d.add_argument("--manifest-sha256", required=True)
    d.add_argument("--deadline-s", type=deadline_seconds, default=CANARY_DEADLINE_S,
                   help=f"overall deadline in seconds (default {CANARY_DEADLINE_S}, at most {MAX_DEADLINE_S})")
    pl = sub.add_parser("plan")
    pl.add_argument("--dsn", required=True)
    pl.add_argument("--run-id", required=True)
    pl.add_argument("--manifest", required=True)
    pl.add_argument("--manifest-sha256", required=True)
    pl.add_argument("--out", required=True)
    pl.add_argument("--plan-deadline-s", type=plan_deadline_seconds, default=PLAN_DEADLINE_S,
                    help=f"planning deadline in seconds (default {PLAN_DEADLINE_S}, at most {MAX_PLAN_DEADLINE_S})")
    a = sub.add_parser("apply")
    a.add_argument("--dsn", required=True)
    a.add_argument("--plan", required=True)
    a.add_argument("--plan-sha256", required=True)
    a.add_argument("--manifest", required=True)
    a.add_argument("--manifest-sha256", required=True)
    a.add_argument("--apply-deadline-s", type=apply_deadline_seconds, default=APPLY_DEADLINE_S,
                   help=f"write-transaction deadline in seconds (default {APPLY_DEADLINE_S}, at most "
                        f"{MAX_APPLY_DEADLINE_S})")
    a.add_argument("--apply", action="store_true")
    c = sub.add_parser("candidates")
    c.add_argument("--dsn", default=LIVE_READ_DSN)
    c.add_argument("--query")
    c.add_argument("--move-id")
    c.add_argument("--run-id", help="only candidates first discovered by this run (the run stored on the candidate)")
    c.add_argument("--limit", type=int, default=5)
    rb = sub.add_parser("rollback")
    rb.add_argument("--accept-legacy-embedding-ownership", action="store_true",
                    help="for effect rows written before vector values were recorded: accept matching input "
                         "hashes and an unchanged created_at as ownership (weaker; reported)")
    for p in (d, rb):
        p.add_argument("--dsn", required=True)
        p.add_argument("--run-id", required=True)
        p.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    if args.cmd == "manifest":
        manifest = asyncio.run(_readonly(args.dsn, build_manifest))
        data = manifest_bytes(manifest)
        Path(args.out).write_bytes(data)
        print(json.dumps({"out": args.out, "sha256": hashlib.sha256(data).hexdigest(),
                          **{k: manifest[k] for k in ("database", "moves", "seed_eligible",
                                                      "admissible_for_live_judgment", "unavailable_sources",
                                                      "approval_scope", "draft_tables_present")}},
                         indent=1, default=str))
        return
    if args.cmd == "report":
        report = asyncio.run(_readonly(args.dsn, lambda c: build_report(c, args.run_id)))
        Path(args.out).write_text(json.dumps(report, indent=1, ensure_ascii=False, default=str) + "\n")
        print(json.dumps({k: report[k] for k in ("run", "counts", "same_work_signals")}, indent=1, default=str))
        return
    if args.cmd == "candidates":
        view = asyncio.run(_readonly(args.dsn, lambda conn: inspect_candidates(
            conn, query=args.query, move_id=args.move_id, run_id=args.run_id, limit=args.limit)))
        print(json.dumps(view, indent=1, ensure_ascii=False, default=str))
        return
    if args.cmd == "plan":
        from scripts.discourse_links_demo import _Ollama
        model = load_approved_manifest(Path(args.manifest), args.manifest_sha256)["approval_scope"]["model"]
        result = asyncio.run(run_plan(args.dsn, args.run_id, embedder=_Ollama(model.split(":", 1)[1]),
                                      manifest_path=Path(args.manifest), manifest_sha256=args.manifest_sha256,
                                      out=Path(args.out), deadline_s=args.plan_deadline_s,
                                      profile={"ollama_digest": _ollama_digest(model)}))
        print(json.dumps(result, indent=1, default=str))
        return
    if not args.apply:
        raise SystemExit(f"{args.cmd} writes to {args.dsn}; rerun with --apply")
    if args.cmd == "apply":
        result = asyncio.run(run_apply(args.dsn, plan_path=Path(args.plan), plan_sha256=args.plan_sha256,
                                       manifest_path=Path(args.manifest), manifest_sha256=args.manifest_sha256,
                                       deadline_s=args.apply_deadline_s))
    elif args.cmd == "discover":
        from scripts.discourse_links_demo import _Ollama
        approved = load_approved_manifest(Path(args.manifest), args.manifest_sha256)
        embedder = _Ollama(approved["approval_scope"]["model"].split(":", 1)[1])
        result = asyncio.run(run_discover(args.dsn, args.run_id, embedder=embedder,
                                          manifest_path=Path(args.manifest), manifest_sha256=args.manifest_sha256,
                                          deadline_s=args.deadline_s))
    else:
        result = asyncio.run(run_rollback(args.dsn, args.run_id,
                                          accept_legacy_embedding_ownership=args.accept_legacy_embedding_ownership))
    print(json.dumps(result, indent=1, default=str))


if __name__ == "__main__":
    main()
