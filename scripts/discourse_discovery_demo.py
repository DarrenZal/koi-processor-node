#!/usr/bin/env python3
"""Offline demonstration of ingestion-time discourse discovery.

Synthetic, fictional material only, in a throwaway koi_xsrc_scratch_* database
that this script creates and drops. Embeddings come from the local Ollama
model in the job profile (no paid call). The model's extraction output is
given (the synthetic moves below); everything after it is the real code path:
extract_deep_documents.read_rag_input / write_discourse_moves /
record_discourse_bindings / schedule_discourse_discovery, then
scripts/discourse_discovery_worker.run_once, then the read-only `candidates`
view.

Similarity proposes; it does not establish support, opposition, identity or
independent corroboration, and the demo's corpus is far too small to say
anything about retrieval quality.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_discovery as dd  # noqa: E402
from api import discourse_links as dl  # noqa: E402
from scripts import discourse_discovery_worker as worker  # noqa: E402
from scripts import discourse_links_canary as canary  # noqa: E402
from scripts import discourse_links_demo as demo  # noqa: E402
from scripts import extract_deep_documents as ex  # noqa: E402

FLAGS = ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY", "KOI_DISCOURSE_DISCOVERY")


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


DOC_TEXT = "A fictional county agency note on stream-bank planting."
# Existing material, already in the corpus before the new source arrives. Fictional.
EXISTING = [
    {"rid": "fixture:demo:monitoring-report", "title": "River cooperative monitoring report (fictional)",
     "url": "https://example.org/demo/monitoring-report", "hash": sha("monitoring v1"), "bind": True, "moves": [
         ("claim", "Volunteer testers recorded nitrate levels at twelve river sites every month."),
         ("evidence", "Nitrate readings fell by a third in the two years after riparian buffers were planted."),
         ("definition", "A riparian buffer is a strip of trees and shrubs planted along a stream bank.")]},
    {"rid": f"document:{sha(DOC_TEXT)}", "title": "County agency note (fictional)", "url": None,
     "hash": sha(DOC_TEXT), "bind": False, "moves": [
         ("claim", "Planting vegetation along stream banks reduces nutrient runoff into rivers."),
         ("claim", "Citizen science data can match agency data when sampling protocols are shared.")]},
    {"rid": "fixture:demo:farm-newsletter", "title": "Upstream farm newsletter (fictional)", "url": None,
     "hash": sha("newsletter v1"), "bind": False, "moves": [
         ("counterpoint", "Buffers alone cannot fix nitrate pollution while fertiliser use keeps rising."),
         ("claim", "Most nitrate in the river comes from fields upstream of the town.")]},
    {"rid": "mediawiki:demo:River", "title": "Wiki page held by the pilot (fictional)", "url": None,
     "hash": sha("wiki v1"), "bind": False, "moves": [
         ("claim", "Riparian buffers cut nitrate by a third.")]},
]


def new_source(n: str, version: str = "v1") -> Dict[str, Any]:
    return {"rid": f"fixture:demo:annual-review-{n}", "title": f"Cooperative annual review {n} (fictional)",
            "url": f"https://example.org/demo/annual-review-{n}", "version": f"review {n} {version}",
            "chunks": ["The cooperative's plantings cut nitrate concentrations by about thirty percent.",
                       "Volunteers produced water-quality data that the county agency accepted.",
                       "The review repeats the monitoring report's figure that nitrate fell by a third.",
                       "Fertiliser sales upstream have risen every year since 2020."],
            "moves": [
                {"move_type": "claim", "chunk_range": [0, 0],
                 "title": "The cooperative's buffer plantings cut nitrate concentrations by about thirty percent."},
                {"move_type": "claim", "chunk_range": [1, 1],
                 "title": "Community volunteers can produce water-quality data that an agency accepts."},
                {"move_type": "evidence", "chunk_range": [2, 2],
                 "title": "The monitoring report found that nitrate fell by a third after the plantings."},
                {"move_type": "claim", "chunk_range": [3, 3],
                 "title": "Fertiliser sales upstream have risen every year since 2020."}]}


async def load_existing(conn, embedder) -> None:
    """The corpus before the new source. Some claims have recorded bindings, most do not."""
    ids: List[str] = []
    for s in EXISTING:
        await conn.execute("INSERT INTO document_ingestion_log (document_rid, title, source_url, content_hash, "
                           "deep_extracted_at) VALUES ($1, $2, $3, $4, now())", s["rid"], s["title"], s["url"], s["hash"])
        mids = []
        for i, (mt, title) in enumerate(s["moves"]):
            mid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{s['rid']}:{i}"))
            await conn.execute("INSERT INTO session_discourse_moves (id, source_type, source_rid, move_type, title) "
                               "VALUES ($1, 'document', $2, $3, $4)", mid, s["rid"], mt, title)
            mids.append(mid)
        if s["bind"]:
            async def read():
                return [(0, s["rid"])]
            _, captured = await dl.capture_extraction_input(conn, source_rid=s["rid"], read_chunks=read)
            await dl.record_extraction_bindings(conn, source_rid=s["rid"], move_ids=mids, extraction_input=captured,
                                                run_id="earlier-extraction", recorded_by="demo")
        ids += mids
    # As if an earlier discovery had embedded them.
    await dl.embed_moves(conn, embedder, ids, model=dd.PROFILE["model"], text_prefix=dd.PROFILE["text_prefix"])


async def ingest(conn, src: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    """extract_deep_document's discourse path, the model's output given."""
    rid = src["rid"]
    await conn.execute(
        """INSERT INTO document_ingestion_log (document_rid, title, source_url, content_hash) VALUES ($1, $2, $3, $4)
           ON CONFLICT (document_rid) DO UPDATE SET content_hash = EXCLUDED.content_hash""",
        rid, src["title"], src["url"], sha(src["version"]))
    await conn.execute("INSERT INTO koi_memories (rid, event_type, source_sensor, content) "
                       "VALUES ($1, 'NEW', 'demo', '{}'::jsonb) ON CONFLICT (rid) DO NOTHING", rid)
    for i, text in enumerate(src["chunks"]):
        await conn.execute(
            """INSERT INTO koi_memory_chunks (chunk_rid, document_rid, chunk_index, total_chunks, content)
               VALUES ($1, $2, $3, $4, $5::jsonb) ON CONFLICT (chunk_rid) DO NOTHING""",
            f"{rid}#{i}", rid, i, len(src["chunks"]), json.dumps({"text": text}))
    _, captured = await ex.read_rag_input(conn, rid)
    written: List[Any] = []
    await ex.write_discourse_moves(conn, document_rid=rid, episode_id=None, moves=src["moves"], written_ids=written)
    await ex.record_discourse_bindings(conn, document_rid=rid, extraction_input=captured, move_ids=written,
                                       run_id=run_id)
    await conn.execute("UPDATE document_ingestion_log SET deep_extracted_at = now() WHERE document_rid = $1", rid)
    scheduled = await ex.schedule_discourse_discovery(conn, document_rid=rid, move_ids=written, run_id=run_id)
    moves = await dl.fetch_moves(conn, written)
    return {"source": rid, "moves_written": len(written), "scheduled": scheduled,
            "bindings": sorted({(m["extraction"]["binding_kind"], m["extraction"]["association"])
                                for m in moves.values()}, key=str),
            "admissible_new_claims": sum(m["admissible"] for m in moves.values()), "move_ids": [str(w) for w in written]}


async def counts(conn) -> Dict[str, int]:
    q = {"candidates": "discourse_link_candidates", "judgments": "discourse_relations",
         "review_events": "discourse_review_events", "bindings": "discourse_move_extraction_bindings",
         "jobs": "discourse_discovery_jobs"}
    return {k: await conn.fetchval(f"SELECT count(*) FROM {t}") for k, t in q.items()}


def brief_view(view: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for r in view["results"]:
        out.append({"claim": r["move"]["title"], "source": r["move"]["source"], "candidates": [
            {"label": c["label"], "status": c["status"], "found_by": c["found_by"],
             "fused": round(c["scores"]["fused_score"], 4),
             "cosine": round(c["scores"]["semantic_score"], 3) if c["scores"]["semantic_score"] else None,
             "other_claim": c["other"]["title"], "other_source": c["other"]["source"],
             "other_binding": c["other"]["binding_kind"], "admissible_now": c["admissible_now"],
             "blockers": c["admission_blockers"]} for c in r["candidates"]]})
    return out


async def run_demo() -> Dict[str, Any]:
    for flag in FLAGS:
        os.environ[flag] = "1"
    report: Dict[str, Any] = {"material": "synthetic, fictional", "profile": dd.PROFILE, "steps": {}}
    steps = report["steps"]
    name = demo.check_scratch_name(f"{demo.SCRATCH_PREFIX}ingest{os.getpid()}")
    dsn = await demo.create_scratch_db(name)
    embedder = worker.ollama_embedder(dd.PROFILE)
    conn = await asyncpg.connect(dsn)
    try:
        await load_existing(conn, embedder)
        steps["0_existing_corpus"] = {"sources": len(EXISTING), **await counts(conn)}

        a = await ingest(conn, new_source("A"), "extract-A-1")
        steps["1_extraction_with_binding"] = {k: a[k] for k in ("source", "moves_written", "bindings",
                                                                  "admissible_new_claims", "scheduled")}
        out = await worker.run_once(dsn)
        steps["2_automatic_discovery"] = {"worker": out["jobs"], **await counts(conn)}
        view = await canary._readonly(dsn, lambda c: canary.inspect_candidates(c, query="nitrate", limit=10))
        steps["3_query_output"] = {"label": view["label"], "read_only": view["read_only"],
                                   "results": [r for r in brief_view(view) if r["source"]["rid"] == a["source"]]}

        before = (await counts(conn))["candidates"]
        again = await ex.schedule_discourse_discovery(conn, document_rid=a["source"], move_ids=a["move_ids"],
                                                      run_id="extract-A-1")
        idle = await worker.run_once(dsn)
        await ingest(conn, new_source("A"), "extract-A-2")                 # re-extraction, same text
        rerun = await worker.run_once(dsn)
        steps["4_retry_without_duplication"] = {
            "same_extraction_scheduled_again": again, "worker_with_nothing_due": idle["jobs"],
            "re_extraction_job": rerun["jobs"], "candidates_before": before,
            "candidates_after": (await counts(conn))["candidates"]}

        b = await ingest(conn, new_source("B"), "extract-B-1")
        await conn.execute("UPDATE document_ingestion_log SET content_hash = $2 WHERE document_rid = $1",
                           b["source"], sha("review B v2"))                # a newer revision lands before the worker runs
        stale_source = await worker.run_once(dsn)
        c = await ingest(conn, new_source("C"), "extract-C-1")
        saved = dl.EXCLUDED_SOURCE_PATTERNS
        dl.EXCLUDED_SOURCE_PATTERNS = saved + ("fixture:demo:held:%",)      # the visibility scope changes
        try:
            stale_scope = await worker.run_once(dsn)
        finally:
            dl.EXCLUDED_SOURCE_PATTERNS = saved
        steps["5_drift_refused"] = {"source_revision": stale_source["jobs"], "caller_scope": stale_scope["jobs"],
                                    "status": (await dd.job_status(conn, source_rid=b["source"]))["jobs"]
                                    + (await dd.job_status(conn, source_rid=c["source"]))["jobs"]}

        class Down:
            async def embed_batch(self, texts):
                raise ConnectionError("ollama unreachable (simulated)")
        d = await ingest(conn, new_source("D"), "extract-D-1")
        failed = await worker.run_once(dsn, embedder_for=lambda profile: Down())
        doc = await conn.fetchrow("SELECT deep_extracted_at IS NOT NULL AS extracted FROM document_ingestion_log "
                                  "WHERE document_rid = $1", d["source"])
        steps["6_discovery_failure_keeps_document"] = {
            "worker": failed["jobs"], "document_extracted": doc["extracted"],
            "claims_still_present": len(await dl.fetch_moves(conn, d["move_ids"])),
            "status": (await dd.job_status(conn, source_rid=d["source"]))["jobs"]}

        for flag in ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY"):
            os.environ.pop(flag)
        jobs_before = (await counts(conn))["jobs"]
        e = await ingest(conn, new_source("E"), "extract-E-1")
        steps["7_flags_off"] = {"moves_written": e["moves_written"], "scheduled": e["scheduled"],
                                "bindings": e["bindings"], "jobs_added": (await counts(conn))["jobs"] - jobs_before}
        steps["8_no_judgments"] = await counts(conn)
        steps["mediawiki_candidates"] = await conn.fetchval(
            "SELECT count(*) FROM discourse_link_candidates WHERE target_source_rid LIKE 'mediawiki:%' "
            "OR query_source_rid LIKE 'mediawiki:%'")
    finally:
        await conn.close()
        await demo.drop_scratch_db(name)
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
