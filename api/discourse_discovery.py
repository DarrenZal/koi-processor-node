"""Ingestion-time cross-source discourse discovery: the job a newly extracted
source gets, what binds it, and its status (issue #60).

Off by default. The extractor enqueues only with KOI_DISCOURSE_INGEST_DISCOVERY=1,
and only moves whose extraction binding this run recorded with its input
(KOI_DISCOURSE_BINDINGS=1). scripts/discourse_discovery_worker.py drains the
jobs later, so extraction never waits on retrieval: a read-only plan, then a
short candidate-only apply (api.discourse_links.plan_discovery / apply_plan).

A job is bound to the source revision captured with the extraction input, the
extraction run and its input digest, the exact moves and their
representations, the caller context and the embedding and retrieval profile.
When any of these no longer holds the job refuses with status 'stale'; the
newer extraction enqueues its own job. Nothing here writes a binding, so
claims extracted before bindings were recorded never become admissible. A
failed or stale job leaves the ingested document, its moves and its bindings
as they were.

The per-claim limit is PROFILE's (5) unless an ingestion chooses one of
PER_MOVE_CAPS (scripts/ingest_document.py --discourse-move-cap, carried to the
extractor's hook in CAP_ENV). The choice goes into the job's own copy of the
profile, so it is part of the job_key, and the worker reads it from there: the
plan, the run's approved scope and its write budget (claims x limit) all follow
it. The per-target-source limit stays at 2.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, Iterable, List, Optional

from api import discourse_links as dl

INGEST_FLAG = "KOI_DISCOURSE_INGEST_DISCOVERY"
CONTEXT_ENV = "KOI_DISCOURSE_DISCOVERY_CONTEXT"
CAP_ENV = "KOI_DISCOURSE_DISCOVERY_PER_MOVE_CAP"
JOBS_TABLE = "discourse_discovery_jobs"
RUN_KIND = "ingestion_discovery"
DISCOVERED_BY = "system:discourse-ingestion-discovery"

# The profile and limits a new job is bound to. A job keeps its own copy, so a
# later change here does not alter work already queued.
PROFILE: Dict[str, Any] = {
    "model": "ollama:nomic-embed-text", "text_prefix": "search_document: ", "method": dl.METHOD,
    "lexical_limit": 20, "semantic_limit": 20, "per_move_cap": 5,
    "per_target_source_cap": dl.DEFAULT_PER_TARGET_SOURCE_CAP, "move_types": sorted(dl.DEFAULT_MOVE_TYPES),
    "embed_batch_size": 32,
}
# Per-claim limits an ingestion may choose. Anything else is refused, here and in the worker.
PER_MOVE_CAPS = (5, 10)
LIMITS: Dict[str, Any] = {
    "max_moves": 200,             # a larger extraction is refused, not truncated
    "max_attempts": 3,
    "max_replans": 1,             # a plan refused at apply is replanned at most this often per attempt
    "plan_deadline_s": 600, "apply_deadline_s": 60,
    "lease_s": 900,               # a running job not finished by then is reclaimed
    "retry_backoff_s": [60, 300, 1800],
}


class JobStale(RuntimeError):
    """What the job was bound to no longer holds. Not retried."""


def configured_context() -> dl.CallerContext:
    """The context ingestion discovery runs in, built here and never read from a
    job or a caller. Only the personal owner context is in scope."""
    name = os.getenv(CONTEXT_ENV, "personal")
    if name != "personal":
        raise ValueError(f"{CONTEXT_ENV}={name!r}: only the personal owner context is in scope")
    return dl.personal_context()


def job_key(spec: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def job_profile(per_move_cap: Optional[int] = None) -> Dict[str, Any]:
    """The profile a new job is bound to: a copy of PROFILE, with the per-claim
    limit replaced when one is chosen. PROFILE itself is never changed."""
    profile = dict(PROFILE)
    if per_move_cap is None:
        return profile
    if type(per_move_cap) is not int or per_move_cap not in PER_MOVE_CAPS:
        raise ValueError(f"per_move_cap must be one of {PER_MOVE_CAPS}, got {per_move_cap!r}")
    profile["per_move_cap"] = per_move_cap
    return profile


def requested_per_move_cap() -> Optional[int]:
    """The per-claim limit an ingestion chose for its own process (CAP_ENV), or
    None when it chose none. A value that is not one of PER_MOVE_CAPS is refused."""
    raw = os.environ.get(CAP_ENV)
    if raw is None:
        return None
    if raw.strip() not in {str(c) for c in PER_MOVE_CAPS}:
        raise ValueError(f"{CAP_ENV}={raw!r}: the per-claim limit must be one of {PER_MOVE_CAPS}")
    return int(raw)


def _jsonb(v):
    return json.loads(v) if isinstance(v, str) else v


async def enqueue_for_extraction(conn, *, source_rid: str, extraction_run_id: str,
                                 move_ids: Iterable[Any], per_move_cap: Optional[int] = None) -> Dict[str, Any]:
    """Queue discovery for the moves this extraction run wrote and bound with
    its input. Idempotent: the same extraction result, context and profile
    give the same job_key and one job ('exists'). per_move_cap (one of
    PER_MOVE_CAPS, or None for PROFILE's) is bound into the new job's profile, so
    asking for another limit is a separate job ('enqueued', listing the other
    jobs for the extraction). A job already queued is never replaced, cancelled
    or changed. Returns what happened and why."""
    profile = job_profile(per_move_cap)
    if any(source_rid.startswith(p.rstrip("%")) for p in dl.EXCLUDED_SOURCE_PATTERNS):
        return {"status": "skipped", "reason": "source belongs to an excluded corpus"}
    if not await dl._has_table(conn, JOBS_TABLE):
        return {"status": "skipped", "reason": f"{JOBS_TABLE} does not exist (migration 134 not applied)"}
    ids = sorted({str(m) for m in move_ids})
    rows = await conn.fetch(
        f"""SELECT b.move_id, b.association, b.source_version_at_extraction AS version,
                   b.extraction_input_sha256 AS input_sha256, b.representation_sha256 AS bound_repr,
                   {dl.REPR_SQL.format(a='m')} AS repr
              FROM discourse_move_extraction_bindings b JOIN session_discourse_moves m ON m.id = b.move_id
             WHERE b.move_id = ANY($1::uuid[]) AND b.run_id = $2 AND b.source_rid = $3""",
        ids, extraction_run_id, source_rid)
    usable = [r for r in rows if r["association"] == "captured_with_input" and r["version"]
              and r["bound_repr"] == r["repr"]]
    if not usable:
        return {"status": "skipped", "reason": "no move of this run has a binding captured with its input"}
    versions = {r["version"] for r in usable}
    inputs = {r["input_sha256"] for r in usable}
    if len(versions) != 1 or len(inputs) != 1:
        return {"status": "skipped", "reason": f"bindings disagree: {len(versions)} versions, {len(inputs)} inputs"}
    version = versions.pop()
    if await dl.resolve_source_version(conn, source_rid) != version:
        return {"status": "skipped", "reason": "source changed after extraction"}
    context = configured_context()
    spec = {"source_rid": source_rid, "source_version": version, "extraction_run_id": extraction_run_id,
            "extraction_input_sha256": inputs.pop(),
            "moves": sorted([str(r["move_id"]), r["repr"]] for r in usable),
            "context": context.describe(), "profile": profile, "limits": LIMITS}
    key = job_key(spec)
    job_id = await conn.fetchval(
        f"""INSERT INTO {JOBS_TABLE} (job_key, source_rid, source_version, extraction_run_id,
                                      extraction_input_sha256, moves, context, profile, limits)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb)
            ON CONFLICT (job_key) DO NOTHING RETURNING id""",
        key, source_rid, version, extraction_run_id, spec["extraction_input_sha256"],
        json.dumps(spec["moves"]), json.dumps(spec["context"]), json.dumps(profile), json.dumps(LIMITS))
    others = [{"job_id": r["id"], "per_move_cap": _jsonb(r["profile"]).get("per_move_cap"), "status": r["status"]}
              for r in await conn.fetch(
                  f"""SELECT id, profile, status FROM {JOBS_TABLE}
                       WHERE source_rid = $1 AND extraction_run_id = $2 AND job_key <> $3 ORDER BY id""",
                  source_rid, extraction_run_id, key)]
    if job_id is None:
        out = {"status": "exists", "job_id": await conn.fetchval(
                   f"SELECT id FROM {JOBS_TABLE} WHERE job_key = $1", key), "job_key": key,
               "per_move_cap": profile["per_move_cap"],
               "note": "this extraction already has this job (same profile). Nothing was changed."}
    else:
        out = {"status": "enqueued", "job_id": job_id, "job_key": key, "moves": len(usable),
               "skipped_moves": len(ids) - len(usable), "per_move_cap": profile["per_move_cap"]}
        if others:
            out["note"] = ("a separate job for this per-claim limit. The other jobs for this extraction "
                           "were not changed.")
    if others:
        out["other_jobs_for_this_extraction"] = others
    return out


async def job_drift(conn, job: Dict[str, Any], context: dl.CallerContext, *, lock: bool = False) -> List[str]:
    """Everything that no longer matches what the job was bound to. Empty means
    the job may run. lock=True (inside apply) holds the job's binding rows FOR
    SHARE until commit, so a re-extraction cannot rebind them unseen."""
    problems: List[str] = []
    if _jsonb(job["context"]) != context.describe():
        problems.append("caller context changed since the job was bound")
    if await dl.resolve_source_version(conn, job["source_rid"]) != job["source_version"]:
        problems.append("source revision changed since extraction")
    moves = _jsonb(job["moves"])
    ids = [m for m, _ in moves]
    rows = {str(r["move_id"]): r for r in await conn.fetch(
        f"""SELECT move_id, run_id, association, source_version_at_extraction AS version, representation_sha256,
                   extraction_input_sha256
              FROM discourse_move_extraction_bindings WHERE move_id = ANY($1::uuid[])
             ORDER BY move_id{' FOR SHARE' if lock else ''}""", ids)}
    current = await dl.fetch_moves(conn, ids)
    visible = await dl.visible_ids(conn, context, ids)
    for mid, rep in moves:
        mv, b = current.get(mid), rows.get(mid)
        if mv is None:
            problems.append(f"move {mid}: no longer exists")
        elif mv["representation_sha256"] != rep:
            problems.append(f"move {mid}: claim text changed since extraction")
        elif not mv["eligible"] or mid not in visible:
            problems.append(f"move {mid}: no longer eligible or visible")
        elif b is None:
            problems.append(f"move {mid}: extraction binding is gone")
        else:
            want = {"run_id": job["extraction_run_id"], "association": "captured_with_input",
                    "version": job["source_version"], "representation_sha256": rep,
                    "extraction_input_sha256": job["extraction_input_sha256"]}
            changed = [k for k, v in want.items() if b[k] != v]
            if changed:
                problems.append(f"move {mid}: extraction binding changed since the job was bound ({', '.join(changed)})")
    return problems


async def job_status(conn, *, source_rid: Optional[str] = None) -> Dict[str, Any]:
    """Jobs with their own per-claim limit, runs and candidate counts, plus
    extraction runs that bound moves but have no job (not scheduled, or
    scheduling failed). Two jobs for one extraction differ in their limit. A completed job
    whose run was rolled back keeps status 'completed' and shows outputs
    'rolled_back'; candidates_inserted then counts what remains (0). SELECTs only."""
    if not await dl._has_table(conn, JOBS_TABLE):
        return {"jobs_table": False, "jobs": [], "unscheduled": []}
    jobs = [dict(r) for r in await conn.fetch(
        f"""SELECT j.id, j.source_rid, j.source_version, j.extraction_run_id, jsonb_array_length(j.moves) AS moves,
                   (j.profile->>'per_move_cap') AS per_move_cap,
                   j.status, j.attempts, j.next_attempt_at, j.claimed_by, j.last_error, j.run_id,
                   r.status AS run_status,
                   CASE r.status WHEN 'completed' THEN 'present' WHEN 'rolled_back' THEN 'rolled_back' END AS outputs,
                   j.created_at, j.finished_at,
                   (SELECT count(*) FROM discourse_link_candidates c WHERE c.run_id = j.run_id) AS candidates_inserted
              FROM {JOBS_TABLE} j LEFT JOIN discourse_link_runs r ON r.run_id = j.run_id
             WHERE $1::text IS NULL OR j.source_rid = $1 ORDER BY j.id""", source_rid)]
    for j in jobs:                                   # the job's own per-claim limit, as stored
        cap = j["per_move_cap"]
        j["per_move_cap"] = int(cap) if cap is not None and cap.isdigit() else cap
    unscheduled = [dict(r) for r in await conn.fetch(
        f"""SELECT b.source_rid, b.run_id AS extraction_run_id, count(*) AS bound_moves
              FROM discourse_move_extraction_bindings b
             WHERE b.association = 'captured_with_input' AND ($1::text IS NULL OR b.source_rid = $1)
               AND NOT EXISTS (SELECT 1 FROM {JOBS_TABLE} j
                                WHERE j.source_rid = b.source_rid AND j.extraction_run_id = b.run_id)
             GROUP BY 1, 2 ORDER BY 1, 2""", source_rid)]
    return {"jobs_table": True, "jobs": jobs, "unscheduled": unscheduled}
