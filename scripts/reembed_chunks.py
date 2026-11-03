#!/usr/bin/env python3
"""
Re-embed all koi_memory_chunks using the configured embedding provider.

Used when switching embedding providers (e.g., OpenAI -> Ollama) and the
vector dimension changes, requiring a full re-embed of all chunks.

Usage:
    python3 scripts/reembed_chunks.py                            # dry run: count only (default)
    python3 scripts/reembed_chunks.py --apply                    # re-embed the first 500 chunks
    python3 scripts/reembed_chunks.py --apply --limit 0          # re-embed all chunks
    python3 scripts/reembed_chunks.py --apply --batch-size 50    # smaller batches
    python3 scripts/reembed_chunks.py --apply --limit 100        # first 100 only

Without --apply (or with --dry-run, which wins) it counts the chunks in a read-only
session, creates no embedding provider and makes no embedding calls. --apply makes
embedding calls (paid with the OpenAI provider) and UPDATEs koi_memory_chunks; --limit
caps the chunks per run at 500 unless given (0 = no limit).
"""

import argparse
import asyncio
import logging
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from api.embedding_provider import create_embedding_provider

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 50
DEFAULT_LIMIT = 500
READ_ONLY = {"default_transaction_read_only": "on"}


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


async def main(args):
    dry_run = args.dry_run or not getattr(args, "apply", False)
    provider = None
    if not dry_run:
        provider = create_embedding_provider()
        if provider is None:
            logger.error("No embedding provider configured.")
            sys.exit(1)

        logger.info(f"Embedding provider: {provider.model_name} (dim={provider.dimension})")

    db_url = os.environ.get("POSTGRES_URL")
    if not db_url:
        logger.error("POSTGRES_URL not set")
        sys.exit(1)
    logger.info(f"Database: {mask_url(db_url)}")

    if dry_run:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2, server_settings=READ_ONLY)
    else:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=2)
    batch_size = args.batch_size or BATCH_SIZE

    # Count chunks
    async with pool.acquire() as conn:
        if args.only_null:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM koi_memory_chunks WHERE embedding IS NULL"
            )
        else:
            total = await conn.fetchval("SELECT COUNT(*) FROM koi_memory_chunks")

    logger.info(f"Chunks to re-embed: {total}")

    if dry_run:
        async with pool.acquire() as conn:
            by_sensor = await conn.fetch("""
                SELECT m.source_sensor, COUNT(*) as cnt
                FROM koi_memory_chunks c
                JOIN koi_memories m ON m.rid = c.document_rid
                GROUP BY m.source_sensor ORDER BY cnt DESC
            """)
            for r in by_sensor:
                logger.info(f"  {r['source_sensor']}: {r['cnt']}")
        cap = min(total, args.limit) if args.limit else total
        logger.info(f"DRY RUN: --apply would re-embed {cap} chunks "
                    f"(--limit {args.limit or 0}; 0 = no limit) and UPDATE koi_memory_chunks.embedding")
        await pool.close()
        return

    # Fetch all chunk IDs and text
    where = "WHERE c.embedding IS NULL" if args.only_null else ""
    limit = f"LIMIT {args.limit}" if args.limit else ""

    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT c.chunk_rid,
                   c.content->>'text' AS text,
                   c.content->>'title' AS title
            FROM koi_memory_chunks c
            {where}
            ORDER BY c.chunk_rid
            {limit}
        """)

    logger.info(f"Processing {len(rows)} chunks in batches of {batch_size}")

    embedded = 0
    failures = 0
    start_time = time.time()

    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start:batch_start + batch_size]
        texts = []
        rids = []

        for row in batch:
            title = row["title"] or ""
            text = row["text"] or ""
            embed_text = f"Page: {title}\n\n{text}" if title else text
            texts.append(embed_text[:8000])
            rids.append(row["chunk_rid"])

        # embed_batch_or_none emits the embedding token-tracking
        # metrics. Returns None on
        # whole-batch failure (matches embed_or_none semantics); same
        # back-off/continue behavior as the prior try/except.
        embeddings = await provider.embed_batch_or_none(
            texts, prompt_type="extraction"
        )
        if embeddings is None:
            logger.error(
                f"Batch {batch_start // batch_size + 1} failed "
                f"(embed_batch_or_none returned None)"
            )
            failures += len(batch)
            await asyncio.sleep(5)
            continue

        async with pool.acquire() as conn:
            async with conn.transaction():
                for rid, emb in zip(rids, embeddings):
                    emb_str = '[' + ','.join(str(x) for x in emb) + ']'
                    await conn.execute(
                        "UPDATE koi_memory_chunks SET embedding = $1::vector WHERE chunk_rid = $2",
                        emb_str, rid
                    )

        embedded += len(batch)
        elapsed = time.time() - start_time
        rate = embedded / elapsed * 60 if elapsed > 0 else 0
        logger.info(
            f"  Batch {batch_start // batch_size + 1}: "
            f"{embedded}/{len(rows)} done ({rate:.0f}/min), "
            f"failures: {failures}"
        )

        await asyncio.sleep(0.2)

    elapsed = time.time() - start_time
    logger.info(f"\nDone in {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    logger.info(f"  Embedded: {embedded}")
    logger.info(f"  Failures: {failures}")
    logger.info(f"  Rate: {embedded / elapsed * 60:.0f}/min")

    async with pool.acquire() as conn:
        remaining = await conn.fetchval(
            "SELECT COUNT(*) FROM koi_memory_chunks WHERE embedding IS NULL"
        )
    logger.info(f"  Remaining without embedding: {remaining}")

    await pool.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Re-embed koi_memory_chunks",
        epilog="Without --apply this is a dry run: it counts the chunks in a read-only session "
               "and makes no embedding calls.")
    parser.add_argument("--apply", action="store_true",
                        help="embed the chunks (paid calls with the OpenAI provider) and UPDATE them")
    parser.add_argument("--batch-size", type=int, help=f"Batch size (default: {BATCH_SIZE})")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max chunks to process (default {DEFAULT_LIMIT}; 0 = no limit)")
    parser.add_argument("--only-null", action="store_true", help="Only embed chunks with NULL embedding")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count only, no embedding (the default; wins over --apply)")
    return parser.parse_args(argv)


def cli(argv=None):
    asyncio.run(main(parse_args(argv)))


if __name__ == "__main__":
    cli()
