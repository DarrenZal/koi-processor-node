#!/usr/bin/env python3
"""
Session-recall benchmark harness.

Runs benchmark queries against a configurable backend (B0/B1/B2/B3) and produces
a CSV row per query for later aggregation.

The queries file is YAML: a top-level `queries:` list whose items carry an id, the
query text, a shape and the ground-truth session ids (see load_queries()).

Usage:
    # B0: claude-mem via claude-cli
    python3 scripts/session_recall_bench.py \\
        --baseline b0 \\
        --queries tmp/session-recall-bench-queries.yaml \\
        --output tmp/session-recall-bench-results-b0.csv

    # B1/B2: KOI HTTP (B1 == before hybrid session search; B2 == after)
    python3 scripts/session_recall_bench.py \\
        --baseline b1 \\
        --queries tmp/session-recall-bench-queries.yaml \\
        --output tmp/session-recall-bench-results-b1.csv \\
        --koi-url http://localhost:8351

    # B0 SQLite fallback (when claude-cli unavailable)
    python3 scripts/session_recall_bench.py \\
        --baseline b0-sqlite \\
        --queries tmp/session-recall-bench-queries.yaml \\
        --output tmp/session-recall-bench-results-b0-sqlite.csv \\
        --claude-mem-db <path to the claude-mem plugin's db.sqlite>

    # B3: Graphiti (optional comparison backend)
    python3 scripts/session_recall_bench.py \\
        --baseline b3 \\
        --queries tmp/session-recall-bench-queries.yaml \\
        --output tmp/session-recall-bench-results-b3.csv \\
        --graphiti-url http://localhost:8000

CSV row schema:
    query_id, baseline, transport, query_text, shape,
    ground_truth_count, top_5_session_ids, matched_count,
    recall_at_5, precision_at_5, mrr, latency_p50_ms, latency_p95_ms,
    subjective_score, notes
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sqlite3
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import urllib.error
import urllib.request


# ─────────────────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Query:
    id: str
    query: str
    shape: str
    ground_truth: list[str]
    notes: str = ""

    @property
    def is_null_answer(self) -> bool:
        """True for queries where the correct answer is empty/'no match'."""
        return len(self.ground_truth) == 0

    @property
    def is_multi_answer(self) -> bool:
        return len(self.ground_truth) > 1


@dataclass
class Result:
    session_ids: list[str]   # top-N session IDs from the backend, in rank order
    latencies_ms: list[float]   # per-call latency for repeat-runs (default 1)
    raw_response: str = ""   # for debugging


# ─────────────────────────────────────────────────────────────────────────────
# Query loader
# ─────────────────────────────────────────────────────────────────────────────


def load_queries(yaml_path: Path) -> list[Query]:
    """Load benchmark queries from YAML. Supports a thin yaml fallback if PyYAML missing."""
    text = yaml_path.read_text()
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(text)
    except ImportError:
        # Minimal fallback parser for our specific format. Tolerates simple cases.
        # Production use should pip install pyyaml.
        sys.stderr.write(
            "PyYAML not installed; using minimal fallback. Install via: pip install pyyaml\n"
        )
        data = _minimal_yaml_parse(text)

    queries: list[Query] = []
    for q in data.get("queries", []):
        if q.get("query") in (None, "", "TBD-OPERATOR"):
            sys.stderr.write(f"  skipping {q.get('id')} — TBD-OPERATOR placeholder\n")
            continue
        gt = q.get("ground_truth") or []
        if not isinstance(gt, list):
            gt = [gt]
        queries.append(Query(
            id=q["id"],
            query=q["query"],
            shape=q.get("shape", "unknown"),
            ground_truth=[str(x) for x in gt],
            notes=q.get("notes", "") or "",
        ))
    return queries


def _minimal_yaml_parse(text: str) -> dict:
    """Hand-rolled YAML parser for our specific bench-queries format. Brittle by design."""
    # The proper path is `pip install pyyaml`. This only handles flat top-level lists.
    raise NotImplementedError(
        "Minimal YAML parser not implemented; install pyyaml: pip install pyyaml"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Backend transports
# ─────────────────────────────────────────────────────────────────────────────


def query_koi_http(query_text: str, koi_url: str, top_k: int = 5,
                   timeout: float = 10.0) -> Result:
    """B1/B2: HTTP GET to KOI unified-search.

    Endpoint shape (per knowledge_router.py 2026-04-28):
      GET /knowledge/unified-search?query=...&include=sessions&limit=20
      Server hard-caps limit at 20. Response shape:
        {"results": [{"session_id", "score", "source"="session", ...}], ...}

    Returns top_k DISTINCT session IDs (deduped, rank-preserving). Fetches
    full 20-chunk window then collapses to unique sessions — important
    because high-relevance queries can return many chunks from one session.
    """
    from urllib.parse import urlencode
    qs = urlencode({"query": query_text, "include": "sessions", "limit": 20})
    url = f"{koi_url.rstrip('/')}/knowledge/unified-search?{qs}"
    req = urllib.request.Request(url, method="GET")
    t0 = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
    except urllib.error.URLError as e:
        return Result(session_ids=[], latencies_ms=[(time.monotonic() - t0) * 1000],
                      raw_response=f"<error: {e}>")
    latency_ms = (time.monotonic() - t0) * 1000

    # Extract distinct session IDs in rank order; cap at top_k.
    session_ids: list[str] = []
    seen: set[str] = set()
    for r in payload.get("results", []):
        sid = r.get("session_id")
        if sid and sid not in seen:
            seen.add(sid)
            session_ids.append(sid)
            if len(session_ids) >= top_k:
                break

    return Result(session_ids=session_ids, latencies_ms=[latency_ms],
                  raw_response=json.dumps(payload)[:2000])


def query_claude_mem_cli(query_text: str, top_k: int = 5,
                          timeout: float = 30.0) -> Result:
    """B0: invoke `claude -p --print --output-format=json` to trigger claude-mem search MCP tool."""
    # We craft a prompt that prompts the model to call the search tool with this query.
    # The model should return tool-use blocks; we extract result content.
    prompt = (
        f"Use the claude-mem search tool to find sessions matching the query "
        f"'{query_text}'. Return the top {top_k} matches as a JSON list of "
        f"objects with session_id, score, and excerpt fields. Output ONLY the JSON list."
    )
    cmd = ["claude", "-p", "--print", "--output-format=json", prompt]
    t0 = time.monotonic()
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return Result(session_ids=[], latencies_ms=[timeout * 1000],
                      raw_response="<claude-cli timeout>")
    latency_ms = (time.monotonic() - t0) * 1000

    if res.returncode != 0:
        return Result(session_ids=[], latencies_ms=[latency_ms],
                      raw_response=f"<claude-cli exit {res.returncode}: {res.stderr[:500]}>")

    # Parse claude --output-format=json — wraps content + tool_use.
    # KNOWN ISSUE (2026-04-27 PM smoke test): claude-mem returns observation IDs in addition to
    # source_session_id fields. Naive UUID-regex grabs both, causing recall@5 mismatch.
    # Fix: prefer "session_id" or "source_session_id" field-tagged values; fall back to
    # UUID regex only if no field-tagged matches found.
    session_ids: list[str] = []
    try:
        obj = json.loads(res.stdout)
        text_blob = json.dumps(obj)
    except json.JSONDecodeError:
        text_blob = res.stdout

    # Pass 1: prefer field-tagged values (claude-mem observations have source_session_id)
    field_pattern = re.compile(
        r'"(?:session_id|source_session_id)"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"',
        re.IGNORECASE,
    )
    field_matches = field_pattern.findall(text_blob)
    if field_matches:
        # Deduplicate while preserving order (Python 3.7+ dict ordering)
        seen: set[str] = set()
        for sid in field_matches:
            if sid not in seen:
                seen.add(sid)
                session_ids.append(sid)
                if len(session_ids) >= top_k:
                    break

    # Pass 2: fallback to all-UUIDs if no field-tagged matches (less precise)
    if not session_ids:
        uuid_pattern = re.compile(
            r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
            re.IGNORECASE,
        )
        all_uuids = uuid_pattern.findall(text_blob)
        # Deduplicate
        seen2: set[str] = set()
        for sid in all_uuids:
            if sid not in seen2:
                seen2.add(sid)
                session_ids.append(sid)
                if len(session_ids) >= top_k:
                    break

    return Result(session_ids=session_ids, latencies_ms=[latency_ms],
                  raw_response=res.stdout[:2000])


def query_claude_mem_sqlite(query_text: str, db_path: Path, top_k: int = 5) -> Result:
    """B0 fallback: direct SQLite FTS5 query into claude-mem's observation table."""
    if not db_path.exists():
        return Result(session_ids=[], latencies_ms=[0.0],
                      raw_response=f"<sqlite db not found: {db_path}>")
    t0 = time.monotonic()
    conn = sqlite3.connect(str(db_path))
    try:
        # claude-mem stores observations in FTS5-indexed tables. Schema varies by version.
        # Probe for likely table names.
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [r[0] for r in cur.fetchall()]
        fts_table = None
        for cand in ("observations_fts", "observations", "memories_fts"):
            if cand in tables:
                fts_table = cand
                break
        if fts_table is None:
            return Result(session_ids=[], latencies_ms=[0.0],
                          raw_response=f"<no fts table; tables: {tables}>")

        # FTS5 keyword search; scrub query for special chars.
        safe_q = re.sub(r'[^\w\s]', ' ', query_text)
        try:
            cur = conn.execute(
                f"SELECT * FROM {fts_table} WHERE {fts_table} MATCH ? LIMIT ?",
                (safe_q, top_k),
            )
            rows = cur.fetchall()
        except sqlite3.OperationalError as e:
            return Result(session_ids=[], latencies_ms=[(time.monotonic() - t0) * 1000],
                          raw_response=f"<sqlite error: {e}>")

        # Best-effort extract: look for session_id column or UUID in row content.
        session_ids: list[str] = []
        col_names = [d[0] for d in cur.description]
        for row in rows:
            row_dict = dict(zip(col_names, row))
            sid = row_dict.get("session_id") or row_dict.get("source_session_id")
            if sid:
                session_ids.append(str(sid))
            else:
                # Regex fallback on row content
                blob = json.dumps(row_dict, default=str)
                m = re.search(
                    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
                    blob, re.IGNORECASE,
                )
                if m:
                    session_ids.append(m.group(0))

        latency_ms = (time.monotonic() - t0) * 1000
        return Result(session_ids=session_ids[:top_k], latencies_ms=[latency_ms],
                      raw_response=str(rows)[:2000])
    finally:
        conn.close()


def query_graphiti_http(query_text: str, graphiti_url: str, top_k: int = 5,
                        timeout: float = 10.0) -> Result:
    """B3 (deprecated stub): HTTP query against Graphiti FastAPI server.

    Pivoted 2026-04-28: Graphiti runs as Python library, not HTTP server, in the
    POC (operator finding via FalkorDB docs `agentic-memory/graphiti.html`). Use
    `query_graphiti_python` instead. Stub kept for the --baseline b3-http
    invocation in case a server-mode benchmark is wanted later.
    """
    return Result(session_ids=[], latencies_ms=[0.0],
                  raw_response="<b3-http deprecated; use --baseline b3 (python lib)>")


# Module-level Graphiti instance — initialized once per bench run, reused per query.
_GRAPHITI_INSTANCE: Optional[Any] = None
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)


def _get_graphiti(group_id: str = "graphiti_poc_20260428"):
    """Lazy-init Graphiti+FalkorDB.

    NOTE (graphiti-core 0.29.0 quirk): FalkorDriver writes to graph keyed by
    `group_id` during add_episode but reads from graph keyed by `database`
    constructor arg. To read what we wrote, configure `database=group_id`.

    Tier-2 override (additive): GRAPHITI_GROUP_ID env var overrides the
    POC default; Tier-2 sets it to "koi_canon_v1".
    """
    group_id = os.environ.get("GRAPHITI_GROUP_ID", group_id)
    global _GRAPHITI_INSTANCE
    if _GRAPHITI_INSTANCE is not None:
        return _GRAPHITI_INSTANCE
    if "EMBEDDING_DIM" not in os.environ:
        os.environ["EMBEDDING_DIM"] = "3072"
    from graphiti_core import Graphiti
    from graphiti_core.driver.falkordb_driver import FalkorDriver
    from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig

    driver = FalkorDriver(host="localhost", port=6380, database=group_id)
    embedder = OpenAIEmbedder(
        config=OpenAIEmbedderConfig(embedding_model="text-embedding-3-large", embedding_dim=3072)
    )
    _GRAPHITI_INSTANCE = Graphiti(graph_driver=driver, embedder=embedder)
    return _GRAPHITI_INSTANCE


def query_graphiti_python(query_text: str, top_k: int = 5,
                          group_id: Optional[str] = None) -> Result:
    """B3: Direct Python library call into Graphiti.

    Each ADR was ingested with a session-attribution footer. The LLM extracts
    the session UUID as a named Entity, with `(:Episodic)-[:MENTIONS]->(:Entity)`
    edges from each ADR-episode to its session-UUID entity. (The LLM did NOT
    create direct entity-to-entity RELATES_TO links to sessions, so the bench
    must walk one extra hop.)

    Algorithm:
      1. Hybrid search → top 20 edges (FactResult / EntityEdge), each carrying
         an `episodes` list of Episodic UUIDs.
      2. Collect the Episodic UUIDs across all top edges.
      3. Single Cypher query: walk each Episodic via MENTIONS → Entity, return
         entity names whose text contains a UUID-shaped substring.
      4. Regex-extract UUIDs in edge-rank order; dedupe; return top_k.

    Tier-2 (Strand-D fix) variant: with explicit RELATES_TO {AUTHORED_WITHIN}
    edges between ADR-Entity and session-UUID-Entity, the regex over edge.fact
    surfaces session UUIDs without needing the episode-walk fallback.
    """
    if group_id is None:
        group_id = os.environ.get("GRAPHITI_GROUP_ID", "graphiti_poc_20260428")
    import asyncio

    async def _run():
        g = _get_graphiti(group_id=group_id)
        edges = await g.search(
            query=query_text, group_ids=[group_id], num_results=20
        )
        # For each edge, the `episodes` field lists the episodic UUIDs that
        # produced this fact. We use those to walk MENTIONS → session-UUID nodes.
        episodes_by_edge: list[list[str]] = []
        all_episodes: list[str] = []
        for e in edges:
            eps = getattr(e, "episodes", None) or []
            episodes_by_edge.append(list(eps))
            for ep in eps:
                if ep not in all_episodes:
                    all_episodes.append(ep)
        # Walk Episodic -> MENTIONS -> Entity in one query
        ep_to_session_names: dict[str, list[str]] = {}
        if all_episodes:
            recs, _, _ = await g.driver.execute_query(
                "MATCH (ep:Episodic)-[:MENTIONS]->(n:Entity) "
                "WHERE ep.uuid IN $uuids "
                "RETURN ep.uuid AS ep_uuid, n.name AS name",
                uuids=all_episodes,
            )
            for r in recs:
                ep_to_session_names.setdefault(r.get("ep_uuid"), []).append(r.get("name") or "")
        return edges, episodes_by_edge, ep_to_session_names

    t0 = time.monotonic()
    try:
        edges, episodes_by_edge, ep_to_names = asyncio.run(_run())
    except Exception as e:
        return Result(session_ids=[], latencies_ms=[(time.monotonic() - t0) * 1000],
                      raw_response=f"<graphiti error: {e}>")
    latency_ms = (time.monotonic() - t0) * 1000

    session_ids: list[str] = []
    seen: set[str] = set()
    debug_lines: list[str] = []
    for e, eps in zip(edges, episodes_by_edge):
        fact_text = getattr(e, "fact", "") or ""
        edge_name = getattr(e, "name", "") or ""
        # walk to mentioned entities for these episodes
        names: list[str] = []
        for ep in eps:
            names.extend(ep_to_names.get(ep, []))
        haystack = " | ".join([fact_text] + names)
        debug_lines.append(f"{edge_name[:30]} | eps={len(eps)} | mentions=[{','.join(set(n[:25] for n in names))[:120]}] | {fact_text[:50]}")
        for m in _UUID_RE.finditer(haystack):
            sid = m.group(0).lower()
            if sid not in seen:
                seen.add(sid)
                session_ids.append(sid)
                if len(session_ids) >= top_k:
                    break
        if len(session_ids) >= top_k:
            break

    return Result(session_ids=session_ids, latencies_ms=[latency_ms],
                  raw_response="\n".join(debug_lines)[:2000])


# ─────────────────────────────────────────────────────────────────────────────
# Scoring
# ─────────────────────────────────────────────────────────────────────────────


def score_query(query: Query, result: Result, top_k: int = 5) -> dict[str, Any]:
    """Compute recall@k, precision@k, MRR per multi-answer policy in §3.0."""
    top_n = result.session_ids[:top_k]
    matched = [sid for sid in top_n if sid in query.ground_truth]
    matched_count = len(matched)

    # Multi-answer policy:
    #  - null-answer (q09): correct = empty top-N
    #  - single-answer: recall ∈ {0, 1}; precision ∈ {0, 0.2}
    #  - multi-answer: recall = matched / min(N_truth, k); precision = matched / k
    if query.is_null_answer:
        recall_at_k = 1.0 if not top_n else 0.0
        precision_at_k = 1.0 if not top_n else 0.0
    elif query.is_multi_answer:
        n_truth = len(query.ground_truth)
        recall_at_k = matched_count / min(n_truth, top_k)
        precision_at_k = matched_count / top_k
    else:
        # single-answer
        recall_at_k = 1.0 if matched_count >= 1 else 0.0
        precision_at_k = matched_count / top_k

    # MRR: rank position of first ground-truth match
    mrr = 0.0
    for rank, sid in enumerate(top_n, start=1):
        if sid in query.ground_truth:
            mrr = 1.0 / rank
            break

    p50 = statistics.median(result.latencies_ms) if result.latencies_ms else 0.0
    sorted_lat = sorted(result.latencies_ms)
    p95_idx = max(0, int(len(sorted_lat) * 0.95) - 1) if sorted_lat else 0
    p95 = sorted_lat[p95_idx] if sorted_lat else 0.0

    return {
        "matched_count": matched_count,
        "recall_at_5": round(recall_at_k, 3),
        "precision_at_5": round(precision_at_k, 3),
        "mrr": round(mrr, 3),
        "latency_p50_ms": round(p50, 1),
        "latency_p95_ms": round(p95, 1),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration
# ─────────────────────────────────────────────────────────────────────────────


def run_bench(baseline: str, queries: list[Query], args) -> list[dict[str, Any]]:
    """Run all queries against the given baseline; return list of result rows."""
    rows: list[dict[str, Any]] = []
    for q in queries:
        sys.stderr.write(f"  [{baseline}] {q.id}: {q.query[:60]}...\n")

        # Dispatch to backend transport
        if baseline in ("b1", "b2"):
            result = query_koi_http(q.query, args.koi_url, top_k=5)
            transport = "http"
        elif baseline == "b0":
            result = query_claude_mem_cli(q.query, top_k=5)
            transport = "claude-cli"
        elif baseline == "b0-sqlite":
            result = query_claude_mem_sqlite(q.query, Path(args.claude_mem_db), top_k=5)
            transport = "sqlite"
        elif baseline == "b3":
            result = query_graphiti_python(q.query, top_k=5)
            transport = "python-lib"
        elif baseline == "b3-http":
            result = query_graphiti_http(q.query, args.graphiti_url, top_k=5)
            transport = "http"
        else:
            sys.stderr.write(f"    unknown baseline: {baseline}\n")
            continue

        scores = score_query(q, result, top_k=5)
        rows.append({
            "query_id": q.id,
            "baseline": baseline,
            "transport": transport,
            "query_text": q.query,
            "shape": q.shape,
            "ground_truth_count": len(q.ground_truth),
            "top_5_session_ids": ",".join(result.session_ids),
            "matched_count": scores["matched_count"],
            "recall_at_5": scores["recall_at_5"],
            "precision_at_5": scores["precision_at_5"],
            "mrr": scores["mrr"],
            "latency_p50_ms": scores["latency_p50_ms"],
            "latency_p95_ms": scores["latency_p95_ms"],
            "subjective_score": "",   # operator fills later
            "notes": q.notes[:200].replace("\n", " "),
        })
    return rows


def write_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    if not rows:
        sys.stderr.write("no rows to write\n")
        return
    fieldnames = list(rows[0].keys())
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    sys.stderr.write(f"wrote {len(rows)} rows to {output_path}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--baseline", required=True,
                        choices=["b0", "b0-sqlite", "b1", "b2", "b3", "b3-http"])
    parser.add_argument("--queries", required=True, type=Path,
                        help="path to bench queries YAML")
    parser.add_argument("--output", required=True, type=Path,
                        help="path for CSV output")
    parser.add_argument("--koi-url", default="http://localhost:8351",
                        help="KOI base URL for B1/B2")
    parser.add_argument("--graphiti-url", default="http://localhost:8000",
                        help="Graphiti server URL for B3")
    parser.add_argument("--claude-mem-db",
                        help="path to claude-mem SQLite for B0 fallback")
    args = parser.parse_args()

    if not args.queries.exists():
        sys.stderr.write(f"queries file not found: {args.queries}\n")
        return 2

    queries = load_queries(args.queries)
    sys.stderr.write(f"loaded {len(queries)} queries from {args.queries}\n")

    if not queries:
        sys.stderr.write("no queries to run (all TBD-OPERATOR placeholders?)\n")
        return 1

    rows = run_bench(args.baseline, queries, args)
    write_csv(rows, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
