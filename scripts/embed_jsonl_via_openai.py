#!/usr/bin/env python3
"""Embed a JSONL of {"id": ..., "text": ...} records via OpenAI text-embedding-3-large.

Usage:
  # Dry run (local tiktoken estimate, no API calls)
  python3 scripts/embed_jsonl_via_openai.py --input /tmp/x.jsonl --dry-run

  # Apply (calls OpenAI, writes vectors + stamps metadata.embedding_model/embedded_at)
  python3 scripts/embed_jsonl_via_openai.py --input /tmp/x.jsonl --output /tmp/x.vectors.jsonl \\
      --apply --table entity_registry --db-url postgresql://...

The apply path:
  1. Reads input JSONL
  2. Resumes from output file if present (skips ids already in output)
  3. Batches OpenAI embeddings.create(model=text-embedding-3-large, dimensions=1024)
  4. Writes {"id": ..., "embedding": [...]} lines to output (append mode)
  5. For each id, UPDATEs table.metadata to merge {embedding_model, embedded_at}
     (uses COALESCE to handle NULL metadata on entity_registry)
  6. Aborts if cumulative cost > $5 or 3 consecutive batches fail with 429/5xx

Output is compatible with scripts/import_reembeddings.py: filename is detected
from the table (entity_registry → entity_registry_embeddings.jsonl, etc) when
using --output-auto.
"""

import argparse
import asyncio
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# text-embedding-3-large published rate (April 2026): $0.13 per 1M input tokens
OPENAI_RATE_PER_MILLION = 0.13
OPENAI_MODEL = "text-embedding-3-large"
OPENAI_DIMENSIONS = 1024
BATCH_SIZE = 200
MAX_CONSECUTIVE_FAILS = 3
COST_ABORT_USD = 5.0


def count_tokens(encoding, text: str) -> int:
    if not text:
        return 0
    try:
        return len(encoding.encode(text, disallowed_special=()))
    except Exception:
        # Fallback: ~4 chars/token
        return max(1, len(text) // 4)


def read_jsonl(path: str) -> List[Dict]:
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def read_existing_ids(output_path: Optional[str]) -> set:
    if not output_path or not os.path.exists(output_path):
        return set()
    ids = set()
    with open(output_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ids.add(str(rec["id"]))
            except Exception:
                continue
    return ids


def dry_run(records: List[Dict]) -> Tuple[int, float]:
    import tiktoken
    encoding = tiktoken.get_encoding("cl100k_base")
    total_tokens = 0
    for rec in records:
        total_tokens += count_tokens(encoding, rec.get("text", ""))
    cost = total_tokens / 1_000_000 * OPENAI_RATE_PER_MILLION
    return total_tokens, cost


async def update_provenance(db_url: str, table: str, ids: List[str], model_name: str, ts_iso: str):
    """UPDATE metadata JSONB to merge {embedding_model, embedded_at}.
    Uses COALESCE so entity_registry (nullable metadata, no default) works safely.
    """
    if not ids:
        return
    import asyncpg
    stamp = json.dumps({"embedding_model": model_name, "embedded_at": ts_iso})
    conn = await asyncpg.connect(db_url)
    try:
        int_ids = [int(x) for x in ids]
        await conn.execute(
            f"""
            UPDATE {table}
               SET metadata = COALESCE(metadata, '{{}}'::jsonb) || $1::jsonb
             WHERE id = ANY($2::int[])
            """,
            stamp,
            int_ids,
        )
    finally:
        await conn.close()


async def apply_embed(
    records: List[Dict],
    output_path: str,
    api_key: str,
    db_url: Optional[str],
    table: Optional[str],
    dimensions: int = OPENAI_DIMENSIONS,
) -> Tuple[int, int, float]:
    """Returns (processed_count, total_tokens, total_cost)."""
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    existing = read_existing_ids(output_path)
    if existing:
        print(f"  Resume: skipping {len(existing)} already-embedded ids", flush=True)

    pending = [r for r in records if str(r["id"]) not in existing]
    total = len(pending)
    print(f"  Processing {total} records in batches of {BATCH_SIZE}", flush=True)

    processed = 0
    total_tokens = 0
    total_cost = 0.0
    consecutive_fails = 0
    ts_iso = dt.datetime.now(dt.timezone.utc).isoformat()

    # Ensure output dir exists
    Path(os.path.dirname(output_path) or ".").mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    with open(output_path, "a") as out_f:
        for i in range(0, total, BATCH_SIZE):
            batch = pending[i:i + BATCH_SIZE]
            texts = [r.get("text", "") for r in batch]
            ids = [str(r["id"]) for r in batch]

            try:
                resp = await asyncio.to_thread(
                    client.embeddings.create,
                    model=OPENAI_MODEL,
                    input=texts,
                    dimensions=dimensions,
                )
                consecutive_fails = 0
            except Exception as e:
                consecutive_fails += 1
                print(f"  Batch {i // BATCH_SIZE} FAILED ({consecutive_fails}/{MAX_CONSECUTIVE_FAILS}): {e}", flush=True)
                if consecutive_fails >= MAX_CONSECUTIVE_FAILS:
                    print(f"  ABORT: {MAX_CONSECUTIVE_FAILS} consecutive failures", flush=True)
                    break
                await asyncio.sleep(2 ** consecutive_fails)
                continue

            # Write vectors
            for rec_id, data in zip(ids, resp.data):
                out_f.write(json.dumps({"id": rec_id, "embedding": data.embedding}) + "\n")
            out_f.flush()

            # Cost tracking (usage is provided by the API)
            batch_tokens = getattr(resp.usage, "total_tokens", 0) or getattr(resp.usage, "prompt_tokens", 0) or 0
            total_tokens += batch_tokens
            batch_cost = batch_tokens / 1_000_000 * OPENAI_RATE_PER_MILLION
            total_cost += batch_cost
            processed += len(batch)

            # Provenance stamps (best-effort; failure here doesn't invalidate vectors)
            if db_url and table:
                try:
                    await update_provenance(db_url, table, ids, OPENAI_MODEL, ts_iso)
                except Exception as e:
                    print(f"  WARN: provenance stamp failed for batch {i // BATCH_SIZE}: {e}", flush=True)

            elapsed = time.time() - t0
            rate = processed / elapsed * 60 if elapsed > 0 else 0
            print(
                f"  batch {i // BATCH_SIZE + 1}/{(total + BATCH_SIZE - 1) // BATCH_SIZE} "
                f"({processed}/{total}, {rate:.0f}/min) "
                f"tokens={total_tokens} cost=${total_cost:.4f}",
                flush=True,
            )

            if total_cost > COST_ABORT_USD:
                print(f"  ABORT: cumulative cost ${total_cost:.4f} > ${COST_ABORT_USD}", flush=True)
                break

    return processed, total_tokens, total_cost


def main():
    p = argparse.ArgumentParser(description="Embed JSONL via OpenAI text-embedding-3-large")
    p.add_argument("--input", required=True, help="Input JSONL with {id, text}")
    p.add_argument("--output", help="Output JSONL with {id, embedding} (append mode, resume-safe)")
    p.add_argument("--dry-run", action="store_true", help="Local tiktoken estimate only, no API calls")
    p.add_argument("--apply", action="store_true", help="Call OpenAI and write embeddings")
    p.add_argument("--table", help="DB table name for provenance stamps (e.g. entity_registry)")
    p.add_argument("--db-url", default=os.getenv("POSTGRES_URL"), help="DB URL for provenance stamps")
    p.add_argument("--dimensions", type=int, default=OPENAI_DIMENSIONS,
                   help=f"Target embedding dimension (default {OPENAI_DIMENSIONS}). text-embedding-3-large supports any ≤ 3072.")
    args = p.parse_args()

    if not (args.dry_run or args.apply):
        print("Must specify --dry-run or --apply", file=sys.stderr)
        sys.exit(2)

    records = read_jsonl(args.input)
    print(f"Input: {args.input} ({len(records)} records)", flush=True)

    if args.dry_run:
        total_tokens, cost = dry_run(records)
        print(f"  tokens={total_tokens} estimated_cost=${cost:.4f}")
        return

    if not args.output:
        print("--apply requires --output", file=sys.stderr)
        sys.exit(2)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OPENAI_API_KEY not set", file=sys.stderr)
        sys.exit(2)

    processed, tokens, cost = asyncio.run(
        apply_embed(records, args.output, api_key, args.db_url, args.table,
                    dimensions=args.dimensions)
    )
    print(f"\nDone: processed={processed} tokens={tokens} cost=${cost:.4f} dim={args.dimensions}")
    print(f"Output: {args.output}")


if __name__ == "__main__":
    main()
