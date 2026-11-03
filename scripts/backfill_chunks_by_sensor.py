#!/usr/bin/env python3
"""Backfill koi_memory_chunks.embedding_3072 for chunks under selected sensors.

Sister script to backfill_3072_embeddings_from_manifest.py. That one is
manifest-driven (doc-scanner reconciliation). This one is sensor-driven:
finds every chunk whose embedding_3072 IS NULL for the given source_sensor(s)
and fills it via OpenAI text-embedding-3-large@3072.

Use case: email-sensor and ics-event insert chunks without populating
embedding_3072; they used to be backfilled by ad-hoc one-shot runs but
nothing was scheduled. This script is meant to run as a scheduled job that
catches them up on a recurring basis.

Usage:
    # Single sensor, blocking until caught up:
    python3 scripts/backfill_chunks_by_sensor.py --source-sensor email-sensor

    # Multiple sensors, rate-limited per scheduled run:
    python3 scripts/backfill_chunks_by_sensor.py \\
        --source-sensor email-sensor --source-sensor ics-event --limit 500

    # All pending regardless of sensor (scheduled-job default):
    python3 scripts/backfill_chunks_by_sensor.py --all-pending --limit 500

    # Dry-run (count + cost; no API calls; no writes):
    python3 scripts/backfill_chunks_by_sensor.py --source-sensor email-sensor --dry-run

Tail item: `--all-pending` is safe by construction — it filters by
embedding_3072 IS NULL so it's idempotent.
"""

import argparse
import asyncio
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import asyncpg
import tiktoken

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # api.provider_http import

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
OPENAI_MODEL = "text-embedding-3-large"
OPENAI_DIMENSIONS = 3072
BATCH_SIZE = 100
RATE_PER_MILLION = 0.13
COST_ABORT_USD = 5.0
# OpenAI text-embedding-3-large cap is 8192 tokens. Use tiktoken to
# truncate by actual token boundary (char-count truncation under-counts
# on dense content like URLs and base64).
MAX_TOKENS_PER_CHUNK = 8000  # 192-token safety margin under the 8192 cap

_TOKENIZER = tiktoken.encoding_for_model(OPENAI_MODEL)


def _truncate_to_token_limit(text: str) -> str:
    tokens = _TOKENIZER.encode(text)
    if len(tokens) <= MAX_TOKENS_PER_CHUNK:
        return text
    return _TOKENIZER.decode(tokens[:MAX_TOKENS_PER_CHUNK])
DEFAULT_RUN_LOG_DIR = Path("/tmp/backfill-chunks-runs")


async def fetch_pending_chunks(
    conn,
    source_sensors: Optional[List[str]],
    since: Optional[str],
    limit: Optional[int],
):
    """Return list of (chunk_id, text) for chunks missing embedding_3072.

    If source_sensors is None, matches all sensors (--all-pending mode).
    If `since` is provided (YYYY-MM-DD), only chunks whose parent doc was
    created on/after that date are returned.
    Ordered oldest-first so recurring runs converge predictably.
    """
    clauses = ["c.embedding_3072 IS NULL", "c.content->>'text' IS NOT NULL"]
    params: list = []
    if source_sensors:
        params.append(source_sensors)
        clauses.append(f"m.source_sensor = ANY(${len(params)}::text[])")
    if since:
        params.append(since)
        clauses.append(f"m.created_at >= ${len(params)}::timestamp")
    where = " AND ".join(clauses)
    sql = f"""
        SELECT c.id, c.content->>'text' AS text
        FROM koi_memory_chunks c
        JOIN koi_memories m ON c.document_rid = m.rid
        WHERE {where}
        ORDER BY m.created_at ASC, c.id ASC
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    rows = await conn.fetch(sql, *params)
    return [(r["id"], r["text"]) for r in rows]


async def embed_batch(client, texts: List[str]) -> List[List[float]]:
    safe_texts = [_truncate_to_token_limit(t) for t in texts]
    resp = await asyncio.to_thread(
        client.embeddings.create,
        model=OPENAI_MODEL,
        input=safe_texts,
        dimensions=OPENAI_DIMENSIONS,
    )
    return [d.embedding for d in resp.data]


async def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source-sensor", action="append", default=[],
                    help="source_sensor to filter by (repeatable). Required unless --all-pending.")
    ap.add_argument("--all-pending", action="store_true",
                    help="Process all chunks with embedding_3072 IS NULL regardless of sensor.")
    ap.add_argument("--since", default=None,
                    help="Only consider chunks whose parent doc was created on/after YYYY-MM-DD.")
    ap.add_argument("--limit", type=int, default=None,
                    help="Cap chunks-per-run (for LaunchAgent rate-throttling).")
    ap.add_argument("--db-url", default=POSTGRES_URL)
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--cost-abort-usd", type=float, default=COST_ABORT_USD)
    ap.add_argument("--run-log-dir", default=str(DEFAULT_RUN_LOG_DIR),
                    help="Append updated chunk ids to <dir>/<timestamp>.ids for rollback.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Count matching chunks; do not call OpenAI or write.")
    args = ap.parse_args()

    if not args.source_sensor and not args.all_pending:
        print("ERROR: must specify at least one --source-sensor or --all-pending", file=sys.stderr)
        return 2

    sensors = args.source_sensor if not args.all_pending else None

    conn = await asyncpg.connect(args.db_url)
    try:
        pending = await fetch_pending_chunks(conn, sensors, args.since, args.limit)
        scope = "all sensors" if args.all_pending else f"sensors={args.source_sensor}"
        print(f"Scope: {scope}, since={args.since or 'beginning'}, limit={args.limit or 'none'}")
        print(f"Pending chunks (embedding_3072 IS NULL): {len(pending)}")

        if not pending:
            print("Nothing to do.")
            return 0

        # Cost estimate up-front (chars/4 → tokens)
        total_chars = sum(len(t) for _, t in pending)
        est_tokens = max(1, total_chars // 4)
        est_cost = (est_tokens / 1_000_000) * RATE_PER_MILLION
        print(f"Estimated tokens: {est_tokens:,}  cost: ${est_cost:.4f} "
              f"(model={OPENAI_MODEL}, dim={OPENAI_DIMENSIONS})")

        if est_cost > args.cost_abort_usd:
            print(f"ABORT: estimated cost ${est_cost:.4f} exceeds --cost-abort-usd ${args.cost_abort_usd}")
            print("       Re-run with --limit to throttle, or raise --cost-abort-usd.")
            return 3

        if args.dry_run:
            print("Dry-run: skipping API calls + writes.")
            return 0

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set", file=sys.stderr)
            return 2
        from openai import OpenAI
        # SDK defaults are Timeout(read=600, write=600, pool=600) with
        # max_retries=2 = up to 30 MINUTES on one embed call, long enough to
        # wedge the API's event loop. api/provider_http.py exists to bound
        # this, and every scheduled caller goes through it.
        from api.provider_http import (
            provider_sync_client, provider_timeout, PROVIDER_MAX_RETRIES)
        client = OpenAI(
            api_key=api_key,
            timeout=provider_timeout(),
            max_retries=PROVIDER_MAX_RETRIES,
            http_client=provider_sync_client(),
        )

        run_log_dir = Path(args.run_log_dir)
        run_log_dir.mkdir(parents=True, exist_ok=True)
        run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_log_path = run_log_dir / f"{run_ts}.ids"

        # Also backfill metadata.repo for sensors that don't set it (email-sensor,
        # ics-event). Without it, unified_search's docs surface filters them out
        # via WHERE mc.metadata->>'repo' IS NOT NULL (knowledge_router.py:1070,1305).
        # Idempotent: only touches chunks where repo is currently NULL.
        sensor_repo_defaults = {
            "email-sensor": ("email-newsletter", "email_message"),
            "ics-event": ("calendar-ics", "calendar_event"),
        }
        for sensor, (repo_val, source_type_val) in sensor_repo_defaults.items():
            result = await conn.execute(
                """
                UPDATE koi_memory_chunks SET metadata =
                    COALESCE(metadata, '{}'::jsonb)
                    || jsonb_build_object('repo', $1::text, 'source_type', $2::text)
                WHERE (metadata->>'repo' IS NULL)
                  AND document_rid IN (
                      SELECT rid FROM koi_memories WHERE source_sensor = $3::text
                  )
                """,
                repo_val, source_type_val, sensor,
            )
            if result and not result.endswith("0"):
                print(f"  metadata backfill ({sensor}): {result}")

        updated = 0
        total_tokens_actual = 0
        t0 = time.time()
        with run_log_path.open("a") as log_fp:
            for i in range(0, len(pending), args.batch_size):
                batch = pending[i : i + args.batch_size]
                ids = [r[0] for r in batch]
                texts = [r[1] for r in batch]
                total_tokens_actual += sum(max(1, len(t) // 4) for t in texts)
                running_cost = (total_tokens_actual / 1_000_000) * RATE_PER_MILLION
                if running_cost > args.cost_abort_usd:
                    print(f"ABORT mid-run: cost ${running_cost:.4f} > ${args.cost_abort_usd}; "
                          f"stopped at chunk {updated}/{len(pending)}")
                    return 3
                embeddings = await embed_batch(client, texts)
                async with conn.transaction():
                    for cid, emb in zip(ids, embeddings):
                        await conn.execute(
                            "UPDATE koi_memory_chunks SET embedding_3072 = $1::vector WHERE id = $2",
                            str(emb),
                            cid,
                        )
                        log_fp.write(f"{cid}\n")
                        updated += 1
                print(f"  batch {i // args.batch_size + 1}: {len(batch)} chunks "
                      f"(running total {updated}/{len(pending)})", flush=True)

        elapsed = time.time() - t0
        final_cost = (total_tokens_actual / 1_000_000) * RATE_PER_MILLION
        print(f"\nDone: updated {updated}/{len(pending)} chunks in {elapsed:.1f}s")
        print(f"Cost: ${final_cost:.4f} ({total_tokens_actual:,} tokens @ ${RATE_PER_MILLION}/1M)")
        print(f"Run log: {run_log_path}")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
