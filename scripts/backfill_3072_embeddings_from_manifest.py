#!/usr/bin/env python3
"""Backfill koi_memory_chunks.embedding_3072 for docs listed in a manifest.

For each doc_id in the manifest, finds chunks whose embedding_3072 IS NULL,
embeds the existing chunk text via OpenAI text-embedding-3-large at 3072-dim,
and UPDATEs the column in place. Does NOT re-chunk or re-index — assumes
chunks already exist and only the 3072-dim column needs population.

Use case: after a move to OpenAI 3072-dim embeddings, chunks written by an
older 1024-dim embedding service have no 3072 embedding. doc-scanner re-runs don't
backfill embedding_3072 in place. This utility is durable-by-template:
future "manifest of doc_ids needing 3072 backfill" work reuses it.

Manifest format: one doc_id per line; lines starting with '#' or blank lines
ignored. Pass multiple --manifest args to combine.

Usage:
    python3 scripts/backfill_3072_embeddings_from_manifest.py \
        --manifest manifests/first-batch.txt \
        --manifest manifests/second-batch.txt

    # Dry-run (count only; no API calls; no writes)
    python3 scripts/backfill_3072_embeddings_from_manifest.py \
        --manifest <path> --dry-run
"""

import argparse
import asyncio
import os
import sys
import time
from typing import List

import asyncpg

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
OPENAI_MODEL = "text-embedding-3-large"
OPENAI_DIMENSIONS = 3072
BATCH_SIZE = 100  # OpenAI accepts up to 2048; 100 is conservative for memory + retry granularity
RATE_PER_MILLION = 0.13  # USD per 1M input tokens (text-embedding-3-large, 2026-04 published rate)
COST_ABORT_USD = 5.0


def read_manifest(path: str) -> List[str]:
    doc_ids = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            doc_ids.append(s)
    return doc_ids


async def fetch_pending_chunks(conn, doc_ids: List[str]):
    """Return list of (chunk_id, text) for chunks under doc_ids missing embedding_3072."""
    rows = await conn.fetch(
        """
        SELECT c.id, c.content->>'text' AS text
        FROM koi_memory_chunks c
        JOIN koi_memories m ON c.document_rid = m.rid
        WHERE m.source_sensor = 'doc-scanner'
          AND jsonb_extract_path_text(m.metadata, 'doc_id') = ANY($1::text[])
          AND c.embedding_3072 IS NULL
          AND c.content->>'text' IS NOT NULL
        ORDER BY c.id
        """,
        doc_ids,
    )
    return [(r["id"], r["text"]) for r in rows]


async def embed_batch(client, texts: List[str]) -> List[List[float]]:
    """One OpenAI call returning len(texts) embeddings of OPENAI_DIMENSIONS each."""
    resp = await asyncio.to_thread(
        client.embeddings.create,
        model=OPENAI_MODEL,
        input=texts,
        dimensions=OPENAI_DIMENSIONS,
    )
    return [d.embedding for d in resp.data]


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manifest", action="append", required=True,
                    help="Path to a doc_id manifest (one id per line; '#' comments). Repeatable.")
    ap.add_argument("--db-url", default=POSTGRES_URL)
    ap.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    ap.add_argument("--dry-run", action="store_true",
                    help="Count matching chunks; do not call OpenAI or write.")
    args = ap.parse_args()

    doc_ids: List[str] = []
    for m in args.manifest:
        ids = read_manifest(m)
        print(f"  manifest {m}: {len(ids)} doc_ids")
        doc_ids.extend(ids)
    doc_ids = sorted(set(doc_ids))
    print(f"Total unique doc_ids: {len(doc_ids)}")

    conn = await asyncpg.connect(args.db_url)
    try:
        pending = await fetch_pending_chunks(conn, doc_ids)
        print(f"Pending chunks (embedding_3072 IS NULL): {len(pending)}")

        if not pending:
            print("Nothing to do.")
            return 0

        if args.dry_run:
            print("Dry-run: skipping API calls + writes.")
            return 0

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set", file=sys.stderr)
            return 2
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        updated = 0
        total_tokens_estimate = 0
        t0 = time.time()
        for i in range(0, len(pending), args.batch_size):
            batch = pending[i : i + args.batch_size]
            ids = [r[0] for r in batch]
            texts = [r[1] for r in batch]
            # Rough token estimate (~4 chars/token) for cost-abort guard
            total_tokens_estimate += sum(max(1, len(t) // 4) for t in texts)
            est_cost = (total_tokens_estimate / 1_000_000) * RATE_PER_MILLION
            if est_cost > COST_ABORT_USD:
                print(f"ABORT: estimated cost ${est_cost:.4f} exceeds ${COST_ABORT_USD}")
                return 3
            embeddings = await embed_batch(client, texts)
            async with conn.transaction():
                for cid, emb in zip(ids, embeddings):
                    await conn.execute(
                        "UPDATE koi_memory_chunks SET embedding_3072 = $1::vector WHERE id = $2",
                        str(emb),
                        cid,
                    )
                    updated += 1
            print(f"  batch {i // args.batch_size + 1}: {len(batch)} chunks (running total {updated}/{len(pending)})")
        elapsed = time.time() - t0
        cost = (total_tokens_estimate / 1_000_000) * RATE_PER_MILLION
        print(f"\nDone: updated {updated}/{len(pending)} chunks in {elapsed:.1f}s")
        print(f"Estimated cost: ${cost:.4f} ({total_tokens_estimate} tokens @ ${RATE_PER_MILLION}/1M)")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
