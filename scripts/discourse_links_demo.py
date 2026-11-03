#!/usr/bin/env python3
"""Offline demonstration of cross-source discourse link candidates.

Every write goes to a throwaway database this script creates and drops. The
name must start with SCRATCH_PREFIX, so a node's live database and the shared
test database cannot be targeted. The draft migration is applied only there.

Two inputs, run separately:

  ic-fixture   The synthetic intake-graph fixture of intelligence-commons,
               read with `git show` at a pinned commit. Its authored relation
               events act as the review: a discovered candidate is admitted
               only when IC authored a relation for that pair, with IC's label,
               author and rationale. Authored relations discovery missed are
               recorded as authored relations.

  sample       A bounded snapshot of real personal-KOI discourse moves, written
               by `export-sample` (one read-only transaction against
               personal_koi). Nothing is admitted: candidates stay candidates,
               optionally with agent-authored label proposals.

Embeddings come from a local Ollama model or not at all (--embedder none).
The script makes no paid provider call.

  python scripts/discourse_links_demo.py export-sample --out SAMPLE.json
  python scripts/discourse_links_demo.py run --input sample --sample SAMPLE.json --out DIR
  python scripts/discourse_links_demo.py run --input ic-fixture --ic-repo PATH --out DIR
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from api import discourse_links as dl  # noqa: E402

SCRATCH_PREFIX = "koi_xsrc_scratch_"
SNAPSHOT_SQL = REPO_ROOT / "tests" / "fixtures" / "discourse_links" / "existing_schema_snapshot.ddl"
MIGRATION_SQL = REPO_ROOT / "migrations" / "133_discourse_link_candidates.sql"
MIGRATIONS_SQL = (MIGRATION_SQL, REPO_ROOT / "migrations" / "134_discourse_discovery_jobs.sql")
ADMIN_DSN = os.getenv("KOI_SCRATCH_ADMIN_DSN", "postgresql://koi:@localhost:5432/postgres")
LIVE_READ_DSN = "postgresql://koi:@localhost:5432/personal_koi"
MOVE_NS = uuid.UUID("6f1d3a52-6c1b-4b0e-9d43-0a7c0e5b2f10")

IC_COMMIT = "5d85732b79b182c544efa64aac4bf24f4ce5366b"
IC_GRAPH = "docs/learning-field/intake-graph/fixture/reference-graph.json"
IC_ACTIONS = "docs/learning-field/intake-graph/fixture/project-actions/ic.yaml"

# The bounded real sample: six named sources and a deterministic distractor set.
SAMPLE_FOCAL = [
    "document:4393761eef740a08b1b97f8f14470f3ca7e0ab61d213c9c44317a170b9da2b8a",  # Levin & Lyons 2026
    "document:262f65fd6a5b0b3cb026cf2e50644d051ab14d8185172bac77e90b3c4c25bd19",  # Garfield essay + talk
    "substack-corpus:michaelgarfield:scenius",                                     # Garfield essay (Substack)
    "substack-corpus:willruddick:toward-a-common-grammar-for-finance",
]
SAMPLE_DUPLICATE_TITLE = "Are there still peaceful, stable options that maneuver us away from open war?"
SAMPLE_DISTRACTOR_FEEDS = ("willruddick", "michelbauwens", "indyjohar", "natehagens", "michaelgarfield")
SAMPLE_DISTRACTORS_PER_FEED = 10
# Arrival order for the sample run: these are ingested last, one at a time.
SAMPLE_NEW = [
    "document:262f65fd6a5b0b3cb026cf2e50644d051ab14d8185172bac77e90b3c4c25bd19",
    "substack-corpus:michaelgarfield:scenius",
]


# ---------------------------------------------------------------------------
# Throwaway database
# ---------------------------------------------------------------------------

def check_scratch_name(name: str) -> str:
    if not name.startswith(SCRATCH_PREFIX) or not name.replace("_", "").isalnum():
        raise SystemExit(f"refusing database {name!r}: scratch databases must be named {SCRATCH_PREFIX}<alnum>")
    return name


def scratch_dsn(name: str) -> str:
    check_scratch_name(name)
    base, _, _ = ADMIN_DSN.rpartition("/")
    return f"{base}/{name}"


async def create_scratch_db(name: str) -> str:
    check_scratch_name(name)
    admin = await asyncpg.connect(ADMIN_DSN)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    conn = await asyncpg.connect(scratch_dsn(name))
    try:
        assert await conn.fetchval("SELECT current_database()") == name
        await conn.execute(SNAPSHOT_SQL.read_text())
        for migration in MIGRATIONS_SQL:
            await conn.execute(migration.read_text())
    finally:
        await conn.close()
    return scratch_dsn(name)


async def drop_scratch_db(name: str) -> None:
    check_scratch_name(name)
    admin = await asyncpg.connect(ADMIN_DSN)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
    finally:
        await admin.close()


def _ts(value: Optional[str]) -> Optional[datetime]:
    return datetime.fromisoformat(value) if value else None


async def load_sources(conn, sources: List[Dict[str, Any]]) -> None:
    """Copy sources into the scratch database exactly as exported: the log's
    content_hash is NOT rewritten (it is the feed:slug for Substack), and the
    koi_memories row carries only the fields version resolution reads."""
    for s in sources:
        await conn.execute(
            """INSERT INTO document_ingestion_log
                 (document_rid, title, source_url, content_hash, tier, deep_extracted_at, last_ingested_at)
               VALUES ($1, $2, $3, $4, 'thorough', $5, $6)""",
            s["rid"], s["title"], s.get("source_url"), s["content_hash"],
            _ts(s.get("deep_extracted_at")), _ts(s.get("last_ingested_at")) or datetime.now(timezone.utc),
        )
        mem = s.get("memory")
        if mem:
            await conn.execute(
                """INSERT INTO koi_memories (rid, event_type, source_sensor, content, metadata,
                                             content_hash, updated_at)
                   VALUES ($1, 'NEW', $2, '{}'::jsonb, $3::jsonb, $4, $5)""",
                s["rid"], mem["source_sensor"],
                json.dumps({"content_hash": mem["metadata_content_hash"]} if mem["metadata_content_hash"] else {}),
                mem["content_hash"], _ts(mem["updated_at"]),
            )
        for m in s["moves"]:
            await conn.execute(
                """INSERT INTO session_discourse_moves
                     (id, source_type, source_rid, move_type, title, detail, status,
                      turn_range_start, turn_range_end)
                   VALUES ($1, 'document', $2, $3, $4, $5, $6, $7, $8)""",
                m["id"], s["rid"], m["move_type"], m["title"], m.get("detail"), m.get("status"),
                (m.get("chunk_range") or [None, None])[0], (m.get("chunk_range") or [None, None])[1],
            )
        if s.get("record_binding"):
            # Stands in for the extraction run recording its own binding (A2). The
            # IC fixture's claims come from its traces, so their text is the input.
            async def read(moves=s["moves"]):
                return [(i, m["title"]) for i, m in enumerate(moves)]
            _, captured = await dl.capture_extraction_input(conn, source_rid=s["rid"], read_chunks=read)
            await dl.record_extraction_bindings(
                conn, source_rid=s["rid"], move_ids=[m["id"] for m in s["moves"]],
                extraction_input=captured, run_id="fixture-load", recorded_by=s["record_binding"])


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

def _git_show(repo: Path, commit: str, path: str) -> bytes:
    import subprocess
    return subprocess.run(["git", "-C", str(repo), "show", f"{commit}:{path}"],
                          check=True, capture_output=True).stdout


def load_ic_fixture(repo: Path, commit: str = IC_COMMIT) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """IC claims as document moves (current versions only), plus IC's authored relations."""
    graph = json.loads(_git_show(repo, commit, IC_GRAPH))
    actions_sha = hashlib.sha256(_git_show(repo, commit, IC_ACTIONS)).hexdigest()
    nodes = {n["id"]: n for n in graph["nodes"]}
    versions = {n["id"]: n for n in graph["nodes"] if n["type"] == "SourceVersion"}
    current_text: Dict[str, Tuple[int, str]] = {}
    for e in graph["edges"]:
        if e["type"] == "version_of":
            cv = nodes[e["from"]]
            if cv.get("status") == "current":
                current_text[e["to"]] = (int(cv["id"].rsplit("/v", 1)[1]), cv["text"])
    by_source: Dict[str, Dict[str, Any]] = {}
    for c in (n for n in graph["nodes"] if n["type"] == "Claim"):
        if c["source_version"]:
            sv = versions[c["source_version"]]
            rid = f"fixture:ic-intake-graph:{c['source_version'].split(':', 1)[1]}"
            src = by_source.setdefault(rid, {
                "rid": rid, "title": f"{sv['artifact']} (synthetic IC fixture)", "source_url": None,
                "content_hash": sv["version"], "arrival": sv["locators"][0]["retrieved"], "moves": [],
                "record_binding": f"fixture-loader:ic-intake-graph@{commit[:12]}"})
        else:
            rid = "fixture:ic-intake-graph:project.ic"
            src = by_source.setdefault(rid, {
                "rid": rid, "title": "IC project claims (synthetic IC fixture)", "source_url": None,
                "content_hash": actions_sha, "arrival": c["claim_scope"]["date"], "moves": [],
                "record_binding": f"fixture-loader:ic-intake-graph@{commit[:12]}"})
        version, text = current_text[c["id"]]
        src["moves"].append({
            "id": str(uuid.uuid5(MOVE_NS, c["id"])), "fixture_claim": c["id"], "claim_version": version,
            "move_type": "claim", "title": text, "detail": None, "status": "asserted"})
    relations = []
    for e in graph["edges"]:
        if e["type"] in ("supports", "challenges", "aligns_with"):
            relations.append({
                "from": str(uuid.uuid5(MOVE_NS, e["from"])), "to": str(uuid.uuid5(MOVE_NS, e["to"])),
                "from_claim": e["from"], "to_claim": e["to"], "relation": e["type"],
                "semantics": e["semantics"], "event": e["provenance"]["event"], "by": e["provenance"]["by"],
                "rationale": e["rationale"], "asserted_against": e.get("asserted_against"),
                "current_versions": {"from": current_text[e["from"]][0], "to": current_text[e["to"]][0]}})
    sources = sorted(by_source.values(), key=lambda s: (s["arrival"], s["rid"]))
    return sources, relations


EXPORT_SQL = """
    SELECT m.id, m.source_rid, m.move_type, m.title, m.detail, m.status,
           m.turn_range_start, m.turn_range_end,
           l.title AS source_title, l.source_url, l.content_hash, l.created_at,
           l.deep_extracted_at, l.last_ingested_at,
           km.rid IS NOT NULL AS has_memory, km.source_sensor,
           km.metadata->>'content_hash' AS km_metadata_hash, km.content_hash AS km_column_hash,
           km.updated_at AS km_updated_at
      FROM session_discourse_moves m
      JOIN document_ingestion_log l ON l.document_rid = m.source_rid
      LEFT JOIN koi_memories km ON km.rid = m.source_rid
     WHERE m.source_type = 'document' AND m.superseded_at IS NULL
       AND m.source_rid = ANY($1::text[])
     ORDER BY l.created_at, m.source_rid, m.turn_range_start NULLS FIRST, m.id
"""


async def export_sample(out: Path) -> Dict[str, Any]:
    """One READ ONLY transaction against personal_koi. Writes only `out`."""
    conn = await asyncpg.connect(LIVE_READ_DSN)
    try:
        async with conn.transaction(readonly=True):
            dup = await conn.fetch(
                "SELECT DISTINCT source_rid FROM session_discourse_moves "
                "WHERE source_type='document' AND superseded_at IS NULL AND title = $1 ORDER BY 1",
                SAMPLE_DUPLICATE_TITLE)
            focal = SAMPLE_FOCAL + [r["source_rid"] for r in dup]
            distractors: List[str] = []
            for feed in SAMPLE_DISTRACTOR_FEEDS:
                rows = await conn.fetch(
                    """SELECT source_rid FROM (
                           SELECT DISTINCT m.source_rid FROM session_discourse_moves m
                            WHERE m.source_type='document' AND m.superseded_at IS NULL
                              AND m.source_rid LIKE $1 AND NOT (m.source_rid = ANY($2::text[]))) d
                        ORDER BY md5(source_rid) LIMIT $3""",
                    f"substack-corpus:{feed}:%", focal, SAMPLE_DISTRACTORS_PER_FEED)
                distractors += [r["source_rid"] for r in rows]
            rows = await conn.fetch(EXPORT_SQL, focal + distractors)
    finally:
        await conn.close()
    by_rid: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        s = by_rid.setdefault(r["source_rid"], {
            "rid": r["source_rid"], "title": r["source_title"], "source_url": r["source_url"],
            "content_hash": r["content_hash"], "arrival": r["created_at"].isoformat(),
            "deep_extracted_at": r["deep_extracted_at"] and r["deep_extracted_at"].isoformat(),
            "last_ingested_at": r["last_ingested_at"] and r["last_ingested_at"].isoformat(),
            "memory": ({"source_sensor": r["source_sensor"], "metadata_content_hash": r["km_metadata_hash"],
                        "content_hash": r["km_column_hash"], "updated_at": r["km_updated_at"].isoformat()}
                       if r["has_memory"] else None),
            "role": "focal" if r["source_rid"] in focal else "distractor", "moves": []})
        s["moves"].append({"id": str(r["id"]), "move_type": r["move_type"], "title": r["title"],
                           "detail": r["detail"], "status": r["status"],
                           "chunk_range": [r["turn_range_start"], r["turn_range_end"]]})
    snapshot = {
        "exported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "exported_from": "personal_koi (one READ ONLY transaction)",
        "selection": {"focal": focal, "distractor_feeds": list(SAMPLE_DISTRACTOR_FEEDS),
                      "distractors_per_feed": SAMPLE_DISTRACTORS_PER_FEED,
                      "distractor_order": "md5(source_rid)"},
        "sources": sorted(by_rid.values(), key=lambda s: (s["arrival"], s["rid"])),
    }
    out.write_text(json.dumps(snapshot, indent=1, ensure_ascii=False) + "\n")
    return {"sources": len(by_rid), "moves": len(rows), "focal": len(focal), "distractors": len(distractors)}


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

class _Ollama:
    """Thin wrapper over the repo's provider that records the model id."""

    def __init__(self, model: str):
        from api.embedding_provider import OllamaEmbeddingProvider
        self._p = OllamaEmbeddingProvider(model=model)
        self.model_id = f"ollama:{model}"

    async def embed_batch(self, texts):
        return await self._p.embed_batch(texts)


async def run(args) -> Dict[str, Any]:
    if args.input == "ic-fixture":
        sources, authored = load_ic_fixture(Path(args.ic_repo).expanduser(), args.ic_commit)
        new_rids = [s["rid"] for s in sources[1:]]   # every source after the first arrives in date order
    else:
        snap = json.loads(Path(args.sample).read_text())
        sources, authored = snap["sources"], []
        new_rids = [r for r in SAMPLE_NEW if any(s["rid"] == r for s in sources)]
        dup_newer = [s["rid"] for s in sources if s["role"] == "focal"
                     and any(m["title"] == SAMPLE_DUPLICATE_TITLE for m in s["moves"])][1:]
        new_rids += dup_newer
    embedder = None if args.embedder == "none" else _Ollama(args.embedder.split(":", 1)[1])
    model = embedder.model_id if embedder else None
    prefix = args.text_prefix if embedder else ""
    name = check_scratch_name(f"{SCRATCH_PREFIX}{os.getpid()}_{uuid.uuid4().hex[:8]}")
    dsn = await create_scratch_db(name)
    result: Dict[str, Any] = {"input": args.input, "scratch_db": name, "embedding_model": model,
                              "text_prefix": prefix, "runs": [], "reviews": [], "authored_recorded": []}
    try:
        conn = await asyncpg.connect(dsn)
        try:
            assert (await conn.fetchval("SELECT current_database()")).startswith(SCRATCH_PREFIX)
            existing = [s for s in sources if s["rid"] not in new_rids]
            arriving = [next(s for s in sources if s["rid"] == r) for r in new_rids]
            await load_sources(conn, existing)
            # --embed-scope new models enabling embeddings for new ingestion only: the
            # existing corpus stays reachable lexically and is invisible to the semantic leg.
            if embedder and args.embed_scope == "all":
                await dl.embed_moves(conn, embedder, [m["id"] for s in existing for m in s["moves"]],
                                     model=model, text_prefix=prefix)
            ctx = dl.personal_context()
            # The expanded (uncapped) view needs authority; offline it is fixture-replay, scratch-only.
            expanded = (dl.mint_fixture_replay_authority("demo:expanded-view")
                        if not args.per_target_source_cap else None)
            discover = dict(discovered_by="agent:discourse-links-demo", embedding_model=model,
                            per_move_cap=args.per_move_cap, context=ctx, expanded_by=expanded,
                            per_target_source_cap=args.per_target_source_cap or None)
            # Each new source arrives, is embedded, and is matched against everything already
            # present. The same discovery then runs again on the unchanged corpus: it must
            # insert nothing.
            count_sql = "SELECT count(*) FROM discourse_link_candidates"
            rerun_inserted = 0
            for s in arriving:
                await load_sources(conn, [s])
                ids = [m["id"] for m in s["moves"]]
                if embedder:
                    await dl.embed_moves(conn, embedder, ids, model=model, text_prefix=prefix)
                for run_id in (f"arrival:{s['rid']}", f"rerun:{s['rid']}"):
                    summary = await dl.discover_candidates(conn, ids, run_id=run_id, **discover)
                    result["runs"].append({"run": run_id, **summary})
                rerun_inserted += summary["inserted"]
            after_arrivals = await conn.fetchval(count_sql)
            result["coverage"] = await dl.retrieval_coverage(conn, model, context=ctx)
            firsts = await dl.fetch_moves(conn, [s["moves"][0]["id"] for s in sources])
            result["source_versions"] = sorted(
                ({"rid": m["source"]["rid"], "status": m["source"]["version_status"],
                  "basis": m["source"]["version_basis"], "version": m["source"]["version"]}
                 for m in firsts.values()), key=lambda v: v["rid"])
            # The older focal sources then query too, so pairs can be met from the other side.
            # New pairs may appear (the per-move cap is not symmetric); a duplicate pair must not.
            # Repeating that pass on the unchanged corpus must again insert nothing.
            reverse_ids = [m["id"] for s in sources if s["rid"] not in new_rids
                           and s.get("role", "focal") == "focal" for m in s["moves"]]
            reverse = []
            for run_id in ("reverse:focal", "reverse:focal:repeat"):
                summary = await dl.discover_candidates(conn, reverse_ids, run_id=run_id, **discover)
                result["runs"].append({"run": run_id, **summary})
                reverse.append(summary)
            result["rerun_check"] = {
                "after_arrivals": after_arrivals, "identical_rerun_inserted": rerun_inserted,
                "reverse_pass_new_pairs": reverse[0]["inserted"],
                "reverse_pass_rediscovered": reverse[0]["rediscovered"],
                "reverse_repeat_inserted": reverse[1]["inserted"],
                "after_reverse_pass": await conn.fetchval(count_sql),
                "duplicate_pair_rows": await conn.fetchval(
                    """SELECT count(*) FROM (SELECT least(query_move_id::text, target_move_id::text) a,
                                                    greatest(query_move_id::text, target_move_id::text) b
                                               FROM discourse_link_candidates GROUP BY 1, 2
                                              HAVING count(*) > 1) d""")}
            result["same_work_signals"] = await dl.same_work_signals(conn, context=ctx)
            if args.input == "ic-fixture":
                await _replay_ic_reviews(conn, authored, result)
            elif args.proposals:
                await _apply_proposals(conn, json.loads(Path(args.proposals).read_text()), result)
            result["candidates"] = await _dump_candidates(conn)
            result["relations"] = [dict(r) for r in await conn.fetch(
                "SELECT id::text, from_move_id::text, to_move_id::text, relation, semantics, origin, "
                "asserted_by, authority_basis, rationale, candidate_id::text, from_binding_kind, from_bound_version, "
                "to_binding_kind, to_bound_version FROM discourse_relations ORDER BY 1")]
            focus = [m["id"] for s in sources for m in s["moves"]
                     if s.get("role", "focal") == "focal"]
            result["connected"] = {mid: await dl.connected_material(conn, mid, context=ctx, include_candidates=True)
                                   for mid in focus}
            result["corroboration"] = {mid: await dl.corroboration_report(conn, mid, context=ctx) for mid in focus}
            result["searches"] = [await dl.search_connected(conn, q, context=ctx, limit=3, include_candidates=True)
                                  for q in args.search]
            if args.input == "ic-fixture":
                result["authored_pair_recall"] = _authored_recall(result, authored)
        finally:
            await conn.close()
    finally:
        if not args.keep_db:
            await drop_scratch_db(name)
            result["scratch_db_dropped"] = True
    return result


async def _replay_ic_reviews(conn, authored, result) -> None:
    """IC's authored relation events are the review decisions for this input."""
    for rel in authored:
        stale = rel["asserted_against"] and (
            rel["asserted_against"]["from"] != rel["current_versions"]["from"]
            or rel["asserted_against"]["to"] != rel["current_versions"]["to"])
        if stale:
            result["reviews"].append({"event": rel["event"], "action": "not_replayed",
                                      "why": "IC asserted it against a claim version that is now superseded (IC flags it for recheck)"})
            continue
        cand = await conn.fetchrow(
            """SELECT id, query_move_id::text AS q FROM discourse_link_candidates
                WHERE status = 'candidate' AND ((query_move_id = $1 AND target_move_id = $2)
                   OR (query_move_id = $2 AND target_move_id = $1))""", rel["from"], rel["to"])
        authority = dl.mint_fixture_replay_authority(rel["by"])
        if cand:
            rel_id = await dl.review_candidate(
                conn, str(cand["id"]), decision="admit", authority=authority,
                rationale=f"{rel['event']}: {rel['rationale']}", relation=rel["relation"],
                semantics=rel["semantics"], reverse=(cand["q"] != rel["from"]))
            result["reviews"].append({"event": rel["event"], "action": "admitted_candidate",
                                      "candidate_id": str(cand["id"]), "relation_id": rel_id})
        else:
            rel_id = await dl.record_authored_relation(
                conn, rel["from"], rel["to"], relation=rel["relation"], semantics=rel["semantics"],
                authority=authority, rationale=f"{rel['event']}: {rel['rationale']}")
            result["reviews"].append({"event": rel["event"], "action": "recorded_authored_not_discovered",
                                      "relation_id": rel_id})


async def _apply_proposals(conn, proposals, result) -> None:
    for p in proposals:
        cid = await conn.fetchval(
            """SELECT id FROM discourse_link_candidates
                WHERE (query_move_id = $1 AND target_move_id = $2) OR (query_move_id = $2 AND target_move_id = $1)""",
            p["move_a"], p["move_b"])
        if cid is None:
            result["reviews"].append({"proposal": p, "action": "no_such_candidate"})
            continue
        await dl.propose_relation(conn, str(cid), relation=p["relation"], semantics=p["semantics"],
                                  proposed_by=p["proposed_by"], rationale=p["rationale"])
        result["reviews"].append({"candidate_id": str(cid), "action": "proposed",
                                  "relation": p["relation"], "semantics": p["semantics"]})


async def _dump_candidates(conn) -> List[Dict[str, Any]]:
    rows = await conn.fetch("SELECT id FROM discourse_link_candidates ORDER BY fused_score DESC, id")
    out = []
    for r in rows:
        c = await conn.fetchrow("SELECT * FROM discourse_link_candidates WHERE id = $1", r["id"])
        moves = await dl.fetch_moves(conn, [c["query_move_id"], c["target_move_id"]])
        out.append({
            "id": str(c["id"]), "status": c["status"], "run_id": c["run_id"], "times_seen": c["times_seen"],
            "fused_score": c["fused_score"], "lexical_rank": c["lexical_rank"], "lexical_score": c["lexical_score"],
            "semantic_rank": c["semantic_rank"], "semantic_score": c["semantic_score"],
            "query": moves[str(c["query_move_id"])], "target": moves[str(c["target_move_id"])],
            "proposal": c["proposed_relation"] and {"relation": c["proposed_relation"],
                                                    "semantics": c["proposed_semantics"], "by": c["proposed_by"]},
            "reviewed_by": c["reviewed_by"], "review_authority": c["review_authority"],
            "recorded_bindings": {"query": [c["query_binding_kind"], c["query_bound_version"]],
                                  "target": [c["target_binding_kind"], c["target_bound_version"]]},
        })
    return out


def _authored_recall(result, authored) -> List[Dict[str, Any]]:
    pairs = {frozenset((c["query"]["id"], c["target"]["id"])): c for c in result["candidates"]}
    return [{"event": a["event"], "relation": a["relation"], "from_claim": a["from_claim"], "to_claim": a["to_claim"],
             "discovered": frozenset((a["from"], a["to"])) in pairs,
             "candidate_status": (pairs.get(frozenset((a["from"], a["to"]))) or {}).get("status")}
            for a in authored]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    ex = sub.add_parser("export-sample")
    ex.add_argument("--out", required=True)
    rn = sub.add_parser("run")
    rn.add_argument("--input", choices=["ic-fixture", "sample"], required=True)
    rn.add_argument("--ic-repo", default="~/projects/intelligence-commons-wt-intake-graph")
    rn.add_argument("--ic-commit", default=IC_COMMIT)
    rn.add_argument("--sample")
    rn.add_argument("--proposals")
    rn.add_argument("--embedder", default="ollama:nomic-embed-text", help="ollama:<model> or none")
    rn.add_argument("--text-prefix", default="search_document: ")
    rn.add_argument("--per-move-cap", type=int, default=5)
    rn.add_argument("--per-target-source-cap", type=int, default=2, help="0 disables the cap")
    rn.add_argument("--embed-scope", choices=["all", "new"], default="all",
                    help="new: embed only arriving sources, as enabling embeddings for new ingestion alone would")
    rn.add_argument("--tag", default="")
    rn.add_argument("--search", action="append", default=[])
    rn.add_argument("--keep-db", action="store_true")
    rn.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.cmd == "export-sample":
        print(json.dumps(asyncio.run(export_sample(Path(args.out))), indent=1))
        return
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    result = asyncio.run(run(args))
    result["args"] = {k: v for k, v in vars(args).items() if k != "cmd"}
    tag = args.tag or f"{args.input}-{'lexical' if args.embedder == 'none' else 'hybrid'}"
    (out / f"{tag}.json").write_text(json.dumps(result, indent=1, ensure_ascii=False, default=str) + "\n")
    runs = [{k: r[k] for k in ("run", "query_moves", "inserted", "rediscovered", "no_embedding",
                               "unknown_binding_queries", "capped_by_target_source")}
            for r in result["runs"]]
    print(json.dumps({"out": str(out / f"{tag}.json"), "runs": runs, "coverage": result["coverage"],
                      "rerun_check": result["rerun_check"], "reviews": len(result["reviews"]),
                      "same_work_signals": result["same_work_signals"],
                      "candidates": len(result["candidates"]), "relations": len(result["relations"])},
                     indent=1, default=str))


if __name__ == "__main__":
    main()
