#!/usr/bin/env python3
"""Self-healing repair for NULL embeddings across every semantic-read surface.

Why this exists
---------------
A NULL embedding is not degraded retrieval — it is INVISIBILITY. Every vector
read path hard-filters `... IS NOT NULL` with no text fallback:
  facts     api/routers/knowledge_router.py:944, 1343, 1818, 2321
  entities  api/retrieval_executors.py:73-77
  chunks    api/personal_ingest_api.py:5392, 5456, 6062
  sessions  api/personal_ingest_api.py:4976, 4996, 5011, 5038, 5050

Without a repair job, rows written while the embedding provider is down stay
invisible until someone runs a script by hand.

Two failure modes shaped the design; keep these properties:

1. A scheduled job whose script path disappears (for example after a branch
   switch in the checkout it runs from) fails on every tick and rows pile up
   with no vector. Hence: the wrapper self-locates, and distinct exit codes
   name the missing piece.

2. A provider outage that is not transient (for example exhausted credit). A
   scheduler that restarts a job on every non-zero exit turns each failure into
   a tight retry loop, every run making a failed provider call while the pending
   queue grows. Hence: exit 0 on external failure, a one-token canary, and an
   exponential circuit breaker, so an outage costs a handful of canary calls.

Exit codes
----------
    0  Normal, INCLUDING "provider is down" and "backoff active". A down
       provider is not a job failure; treating it as one is what caused the
       storm. Observability comes from the log line + state file, never from a
       nonzero exit.
    2  Config error (missing POSTGRES_URL / unusable embedding provider).
    3  Cost guard aborted the run.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import asyncpg  # noqa: E402

MODEL = os.getenv("EMBEDDING_MODEL") or "text-embedding-3-large"
DIM = int(os.getenv("EMBEDDING_DIMENSION") or "3072")
RATE_PER_MILLION = 0.13
DEFAULT_COST_ABORT_USD = 1.00
DEFAULT_LIMIT = 500
BATCH_SIZE = 100
MAX_TOKENS = 8000              # 8192 model cap minus safety margin
MAX_CHARS_FALLBACK = 24000     # ~6k tokens, used only if tiktoken is unavailable
QUARANTINE_AFTER = 5
BACKOFF_SECONDS = [300, 600, 1200, 2400, 3600]
APPLICATION_NAME = "koi-embedding-repair"   # MUST NOT start with 'deep-extract:layers_only:'
                                            # (tr_layers_only_guard_facts raises on that prefix)
ALERT_AFTER_SECONDS = 3600
TASK_API = os.getenv("KOI_TASK_API", "http://localhost:8351")


@dataclass(frozen=True)
class Surface:
    key: str
    table: str
    id_col: str
    vec_col: str
    text_sql: str
    extra_where: str
    order_sql: str


SURFACES: Dict[str, Surface] = {
    # Chunk selection is deliberately byte-equivalent to the proven
    # backfill_chunks_by_sensor.py path, minus its INNER JOIN to koi_memories:
    # that join makes an orphaned chunk permanently unreachable by repair while
    # still counted by the /health gauge (0 orphans today; latent).
    "chunks": Surface(
        key="chunks", table="koi_memory_chunks", id_col="id",
        vec_col="embedding_3072", text_sql="content->>'text'",
        extra_where="content->>'text' IS NOT NULL AND length(content->>'text') > 0",
        order_sql="id ASC",
    ),
    # Superseded facts (valid_to IS NOT NULL) ARE repaired. They are still rows,
    # they still bypass cosine dedup until embedded, and excluding them
    # under-reported the backlog 8.4x on 2026-08-01 (see reembed-null-facts.py:120-127).
    "facts": Surface(
        key="facts", table="knowledge_facts", id_col="id",
        vec_col="fact_embedding_3072", text_sql="fact_text",
        extra_where="fact_text IS NOT NULL AND length(fact_text) > 0",
        order_sql="created_at ASC, id ASC",
    ),
    # Text composition mirrors backfill_entity_embeddings.py:113-124 exactly:
    #   name                       when neither context nor description
    #   "name: ctx. desc"          otherwise (concat_ws skips NULLs)
    # merged_into IS NOT NULL rows are tombstones — not read paths.
    "entities": Surface(
        key="entities", table="entity_registry", id_col="fuseki_uri",
        vec_col="embedding_3072",
        text_sql=(
            "CASE WHEN COALESCE("
            "  NULLIF(btrim(COALESCE(metadata->>'context','')),''),"
            "  NULLIF(btrim(COALESCE(description,'')),'')) IS NULL "
            "THEN entity_text "
            "ELSE entity_text || ': ' || concat_ws('. ',"
            "  NULLIF(btrim(COALESCE(metadata->>'context','')),''),"
            "  NULLIF(btrim(COALESCE(description,'')),'')) END"
        ),
        extra_where="entity_text IS NOT NULL AND length(btrim(entity_text)) > 0 "
                    "AND merged_into IS NULL",
        order_sql="id ASC",
    ),
    # Fourth surface. Read with the same hard IS NOT NULL filter, absent from
    # /health's gauge block, and its ONLY writer lives in a different repo
    # (RegenAI/koi-sensors/sensors/claude_sessions/claude_session_sensor.py:640).
    # 0/442,890 today only because that sensor DELETEs+reinserts per session.
    "sessions": Surface(
        key="sessions", table="session_chunks", id_col="id",
        vec_col="embedding_3072", text_sql="chunk_text",
        extra_where="chunk_text IS NOT NULL AND length(chunk_text) > 0",
        order_sql="id ASC",
    ),
}


# ---------------------------------------------------------------- truncation
_TOK = None
_TOK_TRIED = False


def truncate(text: str) -> str:
    """Token-accurate truncation, but NEVER at import time.

    backfill_chunks_by_sensor.py:55 builds the tokenizer at MODULE scope, and
    tiktoken downloads cl100k_base.tiktoken over the network when uncached. That
    produced 184 crashes that emit NOTHING to stdout (they precede every print),
    so they were invisible to any monitoring reading the run log. Lazy + a
    char-based fallback means a tiktoken failure degrades, it does not crash.
    """
    global _TOK, _TOK_TRIED
    if not _TOK_TRIED:
        _TOK_TRIED = True
        try:
            import tiktoken
            _TOK = tiktoken.encoding_for_model(MODEL)
        except Exception as e:
            print(f"WARN: tiktoken unavailable ({e}); char-based truncation", file=sys.stderr)
            _TOK = None
    if _TOK is None:
        return text[:MAX_CHARS_FALLBACK]
    toks = _TOK.encode(text)
    return text if len(toks) <= MAX_TOKENS else _TOK.decode(toks[:MAX_TOKENS])


# ------------------------------------------------------------------ provider
def build_provider() -> Tuple[Optional[object], Optional[str]]:
    """Return the OpenAI PRIMARY provider, or (None, reason).

    create_embedding_provider() returns a FallbackChainEmbeddingProvider in
    production (EMBEDDING_FALLBACK=ollama:nomic-embed-text is set in
    config/personal.env — verified 2026-08-14). That class owns _pad_to_dim
    (api/embedding_provider.py:479-490), which turns a 768-dim Ollama vector
    into a schema-valid, index-insertable, information-poor 3072-dim vector.
    Today it is reachable only from embed_query; one future edit moving it to
    the write path would silently poison every row this job repairs, and neither
    the schema nor the index would notice. So: unwrap, and refuse anything that
    is not the OpenAI primary.
    """
    from api.embedding_provider import (  # noqa: WPS433
        create_embedding_provider, FallbackChainEmbeddingProvider, OpenAIEmbeddingProvider)
    p = create_embedding_provider()
    if p is None:
        return None, "create_embedding_provider() returned None (check EMBEDDING_PROVIDER / OPENAI_API_KEY)"
    while isinstance(p, FallbackChainEmbeddingProvider):
        p = p._primary
    if not isinstance(p, OpenAIEmbeddingProvider):
        return None, (f"refusing to write vectors produced by {type(p).__name__}; "
                      f"the WRITE path requires OpenAIEmbeddingProvider")
    if p.dimension != DIM:
        return None, f"provider dimension {p.dimension} != expected {DIM}"
    return p, None


def vector_is_sane(v) -> Tuple[bool, str]:
    """Gate every vector before it can reach an UPDATE.

    An all-zero or zero-padded 3072-vector is SCHEMA-VALID and indexes fine, so
    'a vector was written' proves nothing. The v[768:] check is the exact
    signature of _pad_to_dim over nomic-embed-text (768 real dims + 2304 zeros).
    """
    if v is None:
        return False, "None"
    if len(v) != DIM:
        return False, f"dimension {len(v)} != {DIM}"
    if not any(v):
        return False, "all-zero vector"
    if not any(v[768:]):
        return False, "zero-padded tail (fallback-provider signature)"
    return True, ""


# --------------------------------------------------------------------- state
def load_state(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_state(path: Path, st: dict) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(st, indent=2, sort_keys=True))
        tmp.replace(path)
    except Exception as e:
        print(f"WARN: could not persist state to {path}: {e}", file=sys.stderr)


def enter_backoff(st: dict, reason: str) -> None:
    n = int(st.get("consecutive_failed_runs", 0))
    delay = BACKOFF_SECONDS[min(n, len(BACKOFF_SECONDS) - 1)]
    st["consecutive_failed_runs"] = n + 1
    st["backoff_until"] = (datetime.now(timezone.utc) + timedelta(seconds=delay)).isoformat()
    st["backoff_reason"] = reason
    print(f"PROVIDER UNAVAILABLE ({reason}). Backing off {delay}s "
          f"(failure #{n + 1}); next attempt not before {st['backoff_until']}. Exiting 0.")


def backoff_active(st: dict) -> Optional[str]:
    raw = st.get("backoff_until")
    if not raw:
        return None
    try:
        until = datetime.fromisoformat(raw)
    except Exception:
        return None
    return raw if datetime.now(timezone.utc) < until else None


# --------------------------------------------------------------------- alert
def maybe_alert(st: dict, counts: Dict[str, int], canary_ok: bool) -> None:
    """Best-effort task alert. NEVER raises, never affects the repair.

    Fires only when the provider is UP and a surface has still been non-zero for
    an hour — i.e. 'we can reach OpenAI and still cannot clear this', which is
    the genuinely alarming state. Nothing polls the /health null_embed gauges
    (repo-wide grep: definitions and prose only, no scheduled consumer), so
    without this the gauge is decoration. Disable with EMBED_REPAIR_TASK_ALERT=0.
    """
    if os.getenv("EMBED_REPAIR_TASK_ALERT", "1") == "0" or not canary_ok:
        return
    seen = st.setdefault("nonzero_since", {})
    now = datetime.now(timezone.utc)
    for key, n in counts.items():
        task_key = f"koi-null-embed-{key}"
        if n == 0:
            if seen.pop(key, None):
                _post(f"{TASK_API}/tasks/{task_key}", {"status": "done"}, method="PATCH")
            continue
        since = seen.setdefault(key, now.isoformat())
        try:
            age = (now - datetime.fromisoformat(since)).total_seconds()
        except Exception:
            age = 0
        if age >= ALERT_AFTER_SECONDS:
            _post(f"{TASK_API}/tasks/ingest", {
                "taskKey": task_key,
                "title": f"NULL embeddings not clearing: {key} ({n} rows)",
                "sourceType": "embedding-repair",
                "status": "open",
                "description": (f"{n} rows in surface '{key}' have had no embedding for "
                                f"{int(age // 60)} min while the embedding provider is "
                                f"reachable. These rows are invisible to semantic search."),
            })


def _post(url: str, payload: dict, method: str = "POST") -> None:
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(), method=method,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).read()
    except Exception:
        pass  # alerting must never block or fail the repair


# ---------------------------------------------------------------------- work
async def count_pending(conn, s: Surface) -> int:
    return await conn.fetchval(
        f"SELECT COUNT(*) FROM {s.table} WHERE {s.vec_col} IS NULL AND {s.extra_where}") or 0


async def fetch_pending(conn, s: Surface, limit: int):
    rows = await conn.fetch(f"""
        SELECT {s.id_col} AS row_id, ({s.text_sql}) AS text
        FROM {s.table}
        WHERE {s.vec_col} IS NULL AND {s.extra_where}
        ORDER BY {s.order_sql}
        LIMIT {int(limit)}
    """)
    return [(r["row_id"], r["text"]) for r in rows]


async def chunk_metadata_prehook(conn) -> None:
    """Preserved verbatim from backfill_chunks_by_sensor.py:177-199.

    Without metadata.repo, unified_search's docs surface filters these chunks out
    (knowledge_router.py:1070, 1305). Idempotent: only touches NULL repo.
    """
    for sensor, (repo_val, source_type_val) in {
        "email-sensor": ("email-newsletter", "email_message"),
        "ics-event": ("calendar-ics", "calendar_event"),
    }.items():
        result = await conn.execute("""
            UPDATE koi_memory_chunks SET metadata =
                COALESCE(metadata, '{}'::jsonb)
                || jsonb_build_object('repo', $1::text, 'source_type', $2::text)
            WHERE (metadata->>'repo' IS NULL)
              AND document_rid IN (SELECT rid FROM koi_memories WHERE source_sensor = $3::text)
        """, repo_val, source_type_val, sensor)
        if result and not result.endswith("0"):
            print(f"  metadata backfill ({sensor}): {result}")


async def repair_surface(conn, provider, s: Surface, args, st: dict,
                         spent: List[float], run_log) -> str:
    """Return 'ok' | 'provider_down' | 'cost_abort'."""
    fails: Dict[str, int] = st.setdefault("row_failures", {})
    pending = await fetch_pending(conn, s, args.limit)
    quarantined = [rid for rid, _ in pending
                   if fails.get(f"{s.key}:{rid}", 0) >= QUARANTINE_AFTER]
    work = [(rid, txt) for rid, txt in pending
            if fails.get(f"{s.key}:{rid}", 0) < QUARANTINE_AFTER]
    if quarantined:
        print(f"  [{s.key}] QUARANTINED {len(quarantined)} row(s) after "
              f"{QUARANTINE_AFTER} consecutive failures: {quarantined[:5]}")
    if not work:
        return "ok"

    est_tokens = max(1, sum(len(t or "") for _, t in work) // 4)
    est_cost = est_tokens / 1_000_000 * RATE_PER_MILLION
    if spent[0] + est_cost > args.cost_abort_usd:
        print(f"  [{s.key}] COST ABORT: ${spent[0] + est_cost:.4f} would exceed "
              f"--cost-abort-usd ${args.cost_abort_usd:.2f}; skipping. "
              f"Lower --limit or raise the ceiling.")
        return "cost_abort"

    print(f"  [{s.key}] repairing {len(work)} row(s), est ${est_cost:.4f}")
    if args.dry_run:
        return "ok"

    ok = 0
    for i in range(0, len(work), args.batch_size):
        batch = work[i:i + args.batch_size]
        texts = [truncate(t or "") for _, t in batch]
        spent[0] += sum(max(1, len(t) // 4) for t in texts) / 1_000_000 * RATE_PER_MILLION
        if spent[0] > args.cost_abort_usd:
            print(f"  [{s.key}] COST ABORT mid-run at ${spent[0]:.4f}; stopped after {ok} rows")
            return "cost_abort"

        embs = await provider.embed_batch_or_none(texts, prompt_type="extraction")
        if embs is None:
            # Whole-batch provider failure MID-RUN. Do not grind through the rest
            # of the backlog making one failed call per batch — that is exactly
            # failure mode 2 in the module docstring. Stop everything and back off.
            for rid, _ in batch:
                fails[f"{s.key}:{rid}"] = fails.get(f"{s.key}:{rid}", 0) + 1
            return "provider_down"

        good = []
        for (rid, _), emb in zip(batch, embs):
            sane, why = vector_is_sane(emb)
            if not sane:
                fails[f"{s.key}:{rid}"] = fails.get(f"{s.key}:{rid}", 0) + 1
                print(f"  [{s.key}] REJECTED vector for {rid}: {why} (row left NULL)",
                      file=sys.stderr)
                continue
            good.append((rid, emb))

        if good:
            async with conn.transaction():
                for rid, emb in good:
                    await conn.execute(
                        f"UPDATE {s.table} SET {s.vec_col} = $1::vector "
                        f"WHERE {s.id_col} = $2 AND {s.vec_col} IS NULL",
                        str(emb), rid)
                    fails.pop(f"{s.key}:{rid}", None)
                    run_log.write(f"{s.key}\t{rid}\n")
                    ok += 1
        print(f"  [{s.key}] batch {i // args.batch_size + 1}: {ok}/{len(work)}", flush=True)
    return "ok"


async def run(args) -> int:
    state_path = Path(args.state_file)
    st = load_state(state_path)

    if not args.ignore_backoff:
        until = backoff_active(st)
        if until:
            print(f"Backoff active until {until} "
                  f"(reason: {st.get('backoff_reason')}). Zero API calls. Exiting 0.")
            return 0

    db_url = os.environ.get("POSTGRES_URL")
    if not db_url:
        print("ERROR: POSTGRES_URL not set", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(db_url, server_settings={"application_name": APPLICATION_NAME})
    try:
        counts = {k: await count_pending(conn, s) for k, s in SURFACES.items()}
        print(f"[{datetime.now(timezone.utc).isoformat()}] pending: " +
              "  ".join(f"{k}={v}" for k, v in counts.items()))

        selected = [SURFACES[k] for k in (args.surface or list(SURFACES))]
        nothing_to_do = not any(counts[s.key] for s in selected)

        provider, err = build_provider()
        if provider is None:
            print(f"ERROR: {err}", file=sys.stderr)
            return 2

        if not args.dry_run:
            # ONE-TOKEN CANARY, RUN ON EVERY PASS — including when there is nothing to
            # repair. It used to be skipped on the empty-queue path, which then called
            # maybe_alert(canary_ok=True) and cleared consecutive_failed_runs having
            # never touched the provider. During a quiet period the provider could be
            # dead for hours while this file recorded a clean bill of health.
            #
            # That matters beyond this job now: /health derives `embedding_available`
            # from this state file, so an unprobed "fine" propagates to the endpoint
            # everything else trusts. One token per 300s is ~288 calls a day, which is
            # far below the cost of not knowing. It is also the ONLY thing on the
            # machine that probes the provider on a schedule.
            try:
                await provider.embed("koi embedding repair canary")
            except Exception as e:
                enter_backoff(st, f"canary embed failed: {type(e).__name__}: {e}")
                save_state(state_path, st)
                return 0
            st["consecutive_failed_runs"] = 0
            st.pop("backoff_until", None)
            st.pop("backoff_reason", None)
            # Positive evidence, with a timestamp. A reader can then distinguish
            # "verified working 2 minutes ago" from "nobody has checked since Friday",
            # which `consecutive_failed_runs: 0` alone cannot express.
            st["last_success_at"] = datetime.now(timezone.utc).isoformat()

        if nothing_to_do:
            maybe_alert(st, counts, canary_ok=True)
            save_state(state_path, st)
            print("Nothing to do.")
            return 0

        run_dir = Path(args.run_log_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        run_path = run_dir / f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.ids"

        spent = [0.0]
        t0 = time.time()
        rc = 0
        with run_path.open("a") as run_log:
            if any(s.key == "chunks" for s in selected) and not args.dry_run:
                await chunk_metadata_prehook(conn)
            for s in selected:
                if counts[s.key] == 0:
                    continue
                outcome = await repair_surface(conn, provider, s, args, st, spent, run_log)
                if outcome == "provider_down":
                    enter_backoff(st, "batch embed returned None mid-run")
                    break
                if outcome == "cost_abort":
                    rc = 3

        after = {k: await count_pending(conn, s) for k, s in SURFACES.items()}
        maybe_alert(st, after, canary_ok=backoff_active(st) is None)
        save_state(state_path, st)
        print(f"Done in {time.time() - t0:.1f}s  cost ${spent[0]:.4f}  run log {run_path}")
        print("remaining: " + "  ".join(f"{k}={v}" for k, v in after.items()))
        return rc
    finally:
        await conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--surface", action="append", choices=sorted(SURFACES),
                    help="Repeatable. Default: all surfaces.")
    ap.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Rows per surface per run.")
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--cost-abort-usd", type=float, default=DEFAULT_COST_ABORT_USD)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--ignore-backoff", action="store_true",
                    help="Manual override: run even if the circuit breaker is open.")
    ap.add_argument("--state-file",
                    default=str(REPO_ROOT / "logs" / "embedding-repair-state.json"))
    ap.add_argument("--run-log-dir", default="/tmp/embedding-repair-runs")
    args = ap.parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
