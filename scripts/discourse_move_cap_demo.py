#!/usr/bin/env python3
"""Offline demonstration of the per-claim limit per ingestion job.

Synthetic, fictional material only, in a throwaway koi_xsrc_scratch_* database
that this script creates and drops. No network: vectors come from an
in-process concept-count embedder (the job profile's model is replaced by it
for this process), so nothing here says anything about retrieval quality. The
extraction output is given; everything after it is the real code path
(extract_deep_documents' discourse hooks, discourse_discovery_worker.run_once,
api.discourse_review, discourse_links_canary rollback).

Shows: a default job (5) and a chosen one (10) with what each binds and
records; that the first five picks at 10 are the picks at 5; the review
queue's per-occurrence tiers, hidden when blind; a re-extraction at 10 that
meets the first five again without changing their recorded first sighting;
a pair met from its other end reviewed from that sighting's own query; and
rollback of that run.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import sys
import tempfile
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_discovery as dd  # noqa: E402
from api import discourse_links as dl  # noqa: E402
from api import discourse_review as dr  # noqa: E402
from scripts import discourse_discovery_demo as ingest_demo  # noqa: E402
from scripts import discourse_discovery_worker as worker  # noqa: E402
from scripts import discourse_links_canary as canary  # noqa: E402
from scripts import discourse_links_demo as demo  # noqa: E402

MODEL = "demo:concept-count"
FLAGS = ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY", "KOI_DISCOURSE_DISCOVERY")
CONCEPTS = {
    "buffer": {"buffer", "buffers", "riparian", "planted", "plantings", "planting", "trees", "shrubs", "bank", "banks"},
    "nitrate": {"nitrate", "nitrogen", "nutrient", "runoff", "fertiliser", "concentrations", "levels"},
    "monitoring": {"volunteer", "volunteers", "testers", "monitoring", "data", "samples", "sampling", "readings"},
    "fell": {"fell", "cut", "reduced", "reduces", "third", "thirty", "percent", "fall"},
}


class ConceptEmbedder:
    """Counts of a few fixed concepts: deterministic, offline, not a model."""
    model_id = MODEL

    async def embed_batch(self, texts):
        out = []
        for t in texts:
            toks = dl._TOKEN_RE.findall(t.lower())
            v = [float(sum(tok in words for tok in toks)) for words in CONCEPTS.values()] + [0.01]
            n = math.sqrt(sum(x * x for x in v))
            out.append([x / n for x in v])
        return out


# Fictional corpus: six sources with two claims each, so one claim's search can fill ten picks at two per source.
CORPUS = [(f"fixture:demo:catchment-{i}", f"Catchment note {i} (fictional)", [
    f"Riparian buffers planted along catchment {i} cut nitrate runoff by about a third.",
    f"Volunteer monitoring in catchment {i} recorded nitrate readings every month."]) for i in range(1, 7)]


def source(n: str) -> Dict[str, Any]:
    src = ingest_demo.new_source(n)
    src["moves"] = src["moves"][:2]                      # two claims keep the report short
    return src


async def load_corpus(conn) -> int:
    ids: List[str] = []
    for rid, title, claims in CORPUS:
        await conn.execute("INSERT INTO document_ingestion_log (document_rid, title, content_hash, deep_extracted_at) "
                           "VALUES ($1, $2, $3, now())", rid, title, ingest_demo.sha(rid))
        for i, text in enumerate(claims):
            ids.append(str(uuid.uuid5(uuid.NAMESPACE_URL, f"{rid}:{i}")))
            await conn.execute("INSERT INTO session_discourse_moves (id, source_type, source_rid, move_type, title) "
                               "VALUES ($1, 'document', $2, 'claim', $3)", ids[-1], rid, text)
    await dl.embed_moves(conn, ConceptEmbedder(), ids, model=MODEL)
    return len(ids)


async def job_view(conn, rid: str) -> Dict[str, Any]:
    j = await conn.fetchrow("SELECT * FROM discourse_discovery_jobs WHERE source_rid = $1 ORDER BY id DESC LIMIT 1", rid)
    return {"job_id": j["id"], "status": j["status"], "job_key": j["job_key"][:16],
            "profile.per_move_cap": json.loads(j["profile"])["per_move_cap"],
            "profile.per_target_source_cap": json.loads(j["profile"])["per_target_source_cap"], "run_id": j["run_id"]}


async def run_view(conn, run_id: str, plan_dir: Path) -> Dict[str, Any]:
    run = await conn.fetchrow("SELECT * FROM discourse_link_runs WHERE run_id = $1", run_id)
    scope = json.loads(run["approval_scope"])
    data = (plan_dir / f"{run_id.replace(':', '_')}.plan.json").read_bytes()
    plan = json.loads(data)
    picks = Counter(s["query_move_id"] for s in plan["sightings"])
    return {"run_id": run_id, "approval_scope.per_move_cap": scope["per_move_cap"],
            "approval_scope.per_target_source_cap": scope["per_target_source_cap"],
            "approval_scope.limits": scope["limits"], "plan.method_params.per_move_cap": plan["method_params"]["per_move_cap"],
            "plan_sha256_matches_scope": scope["plan_sha256"] == hashlib.sha256(data).hexdigest(),
            "picks_per_claim": sorted(picks.values()), "effects": json.loads(run["summary"])["effects"]}


async def queue_rows(dsn: str, *, run_ids: List[str] = None, seen_in_runs: List[str] = None) -> List[Dict[str, Any]]:
    return await canary._readonly(dsn, lambda c: dr.candidate_rows(c, run_ids=run_ids, seen_in_runs=seen_in_runs))


async def run_demo() -> Dict[str, Any]:
    for flag in FLAGS:
        os.environ[flag] = "1"
    os.environ.pop(dd.CAP_ENV, None)
    dd.PROFILE = dict(dd.PROFILE, model=MODEL, text_prefix="")   # this process only: the offline embedder's model
    report: Dict[str, Any] = {"material": "synthetic, fictional", "embedder": "in-process concept counts (offline)",
                              "allowed_per_move_caps": list(dd.PER_MOVE_CAPS), "steps": {}}
    steps = report["steps"]
    name = demo.check_scratch_name(f"{demo.SCRATCH_PREFIX}movecap{os.getpid()}")
    dsn = await demo.create_scratch_db(name)
    plan_dir = Path(tempfile.mkdtemp(prefix="movecap-plans-"))
    work = dict(embedder_for=lambda profile: ConceptEmbedder(), plan_dir=plan_dir)
    conn = await asyncpg.connect(dsn)
    try:
        steps["0_corpus"] = {"sources": len(CORPUS), "claims": await load_corpus(conn)}
        a = await ingest_demo.ingest(conn, source("A"), "extract-A-1")             # no option
        await worker.run_once(dsn, **work)
        os.environ[dd.CAP_ENV] = "10"                                            # --discourse-move-cap 10
        b = await ingest_demo.ingest(conn, source("B"), "extract-B-1")
        os.environ.pop(dd.CAP_ENV)
        await worker.run_once(dsn, **work)
        ja, jb = await job_view(conn, a["source"]), await job_view(conn, b["source"])
        steps["1_jobs"] = {"default (no option)": {"scheduled": a["scheduled"], **ja},
                           "chosen 10": {"scheduled": b["scheduled"], **jb},
                           "module PROFILE.per_move_cap after both": dd.PROFILE["per_move_cap"]}
        steps["2_runs"] = {"default": await run_view(conn, ja["run_id"], plan_dir),
                           "chosen 10": await run_view(conn, jb["run_id"], plan_dir)}

        plans = {}
        for cap in (5, 10):
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                plans[cap] = await dl.plan_discovery(conn, ConceptEmbedder(), b["move_ids"], context=dl.personal_context(),
                                                     discovered_by="demo", run_id=f"demo:{cap}", embedding_model=MODEL,
                                                     per_move_cap=cap)
        by = {cap: {q: [s["target_move_id"] for s in plans[cap]["sightings"] if s["query_move_id"] == q]
                    for q in b["move_ids"]} for cap in plans}
        steps["3_first_five_at_10_equal_picks_at_5"] = {q[:8]: by[10][q][:5] == by[5][q] for q in b["move_ids"]}

        rows = await queue_rows(dsn, seen_in_runs=[jb["run_id"]])
        found, _ = dr.review_items(rows, seen_in_runs=[jb["run_id"]])
        items = dr.queue(found, dr.load_mappings(None), (), "fused")["cross_source"]
        one = next(r for r in items if r["tier"] == "first_five")
        extra = next(r for r in items if r["tier"] == "additional")
        steps["4_review_queue"] = {
            "sightings_by_tier": dr.tier_counts(found),
            "first_discovered_by_this_run": len(await queue_rows(dsn, run_ids=[jb["run_id"]])),
            "operator_view_first_five": dr.render(one, blind=False).splitlines(),
            "operator_view_additional": dr.render(extra, blind=False).splitlines(),
            "blind_view_additional": dr.render(extra, blind=True).splitlines()}

        first = {str(r["id"]): dict(r) for r in await conn.fetch(
            "SELECT * FROM discourse_link_candidates WHERE run_id = $1 ORDER BY id", ja["run_id"])}
        keep = ("run_id", "query_move_id", "method_params", "lexical_rank", "semantic_rank", "fused_score", "discovered_at")
        os.environ[dd.CAP_ENV] = "10"
        await ingest_demo.ingest(conn, source("A"), "extract-A-2")                  # same text, now with 10
        os.environ.pop(dd.CAP_ENV)
        rerun = await worker.run_once(dsn, **work)
        a2 = (await job_view(conn, a["source"]))["run_id"]
        after = {str(r["id"]): dict(r) for r in await conn.fetch(
            "SELECT * FROM discourse_link_candidates WHERE id = ANY($1::uuid[])", list(first))}
        found = {r["candidate_id"]: r for r in await queue_rows(dsn, seen_in_runs=[a2])}
        occ = {c: r["occurrences"] for c, r in found.items()}

        def tiers_in_a2(cids):
            return dict(Counter(o["tier"] for c in cids for o in occ[c] if o["run_id"] == a2))
        by_first_run = Counter(found[c]["run_id"] for c in found)
        from_b = sorted(c for c in found if found[c]["run_id"] == jb["run_id"])
        moved = sorted(c for c in first if tiers_in_a2([c]) == {"additional": 1})
        cid = moved[0] if moved else sorted(first)[0]
        steps["5_rediscovery_at_10"] = {
            "run": a2, "effects": rerun["jobs"][0]["effects"],
            "found_by_this_run_by_first_sighting_run": dict(by_first_run),
            "first_run_candidates": {
                "count": len(first), "first_sightings_unchanged": all(
                    {k: after[c][k] for k in keep} == {k: first[c][k] for k in keep} for c in first),
                "times_seen": sorted(Counter(after[c]["times_seen"] for c in first).items()),
                "tiers_in_this_run": tiers_in_a2(first)},
            "example_first_five_then_additional": {"candidate": cid[:8], "occurrences": occ[cid]},
            "found_first_by_B_and_again_from_A": [{"candidate": c[:8], "occurrences": occ[c]} for c in from_b],
            "first_sighted_by_this_run_by_tier": tiers_in_a2([c for c in found if found[c]["run_id"] == a2]),
            "first_discovered_by_this_run": len(await queue_rows(dsn, run_ids=[a2]))}

        # Reviewing one of B's pairs as A's run met it: A's claim is the anchor, not the stored A (B's claim).
        sightings, _ = dr.review_items(list(found.values()), seen_in_runs=[a2])
        rev = next(it for it in sightings if it["anchor"]["stored_end"] == "b")
        batch = dr.make_batch([rev], database=name, created_at="demo",
                              selection={"run_ids": None, "seen_in_runs": [a2], "tier": None})
        rec = dr.annotation(batch, 1, reviewer="demo:synthetic", answer="useful", created_at="demo")
        steps["5b_review_from_the_sightings_own_query"] = {
            "stored_candidate": {end: {"source": found[rev["candidate_id"]][end]["source"]["rid"],
                                       "move_id": found[rev["candidate_id"]][end]["move_id"]} for end in ("a", "b")}
                                | {"first_discovered_by": found[rev["candidate_id"]]["run_id"]},
            "batch_item": {end: {"source": batch["items"][0][end]["source"]["rid"],
                                 "move_id": batch["items"][0][end]["move_id"]} for end in ("a", "b")}
                          | {"anchor": batch["items"][0]["anchor"], "sighting": batch["items"][0]["sighting"]},
            "annotation": {k: rec[k] for k in ("anchor", "sighting")} | {"orientation": rec["shown"]["orientation"]},
            "blind_view": dr.render(batch["items"][0], position=1, total=1, blind=True).splitlines()}

        rolled = await canary.run_rollback(dsn, a2)
        occ = {r["candidate_id"]: r["occurrences"] for r in await queue_rows(dsn, run_ids=[ja["run_id"]])}
        steps["6_rollback_of_that_run"] = {
            "result": rolled, "still_listed_under_it": len(await queue_rows(dsn, seen_in_runs=[a2])),
            "example_occurrences_now": occ[cid],
            "first_sightings_unchanged": all(
                {k: v for k, v in dict(r).items() if k in keep} == {k: first[str(r["id"])][k] for k in keep}
                for r in await conn.fetch("SELECT * FROM discourse_link_candidates WHERE id = ANY($1::uuid[])",
                                          list(first)))}
        steps["7_no_judgments"] = {t: await conn.fetchval(f"SELECT count(*) FROM {t}")
                                   for t in ("discourse_relations", "discourse_review_events")}
    finally:
        await conn.close()
        await demo.drop_scratch_db(name)
        for p in plan_dir.glob("*"):
            p.unlink()
        plan_dir.rmdir()
        report["scratch_db_dropped"] = name
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    report = asyncio.run(run_demo())
    Path(args.out).write_text(json.dumps(report, indent=1, ensure_ascii=False, default=str) + "\n")
    print(json.dumps(report, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
