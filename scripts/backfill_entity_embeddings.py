#!/usr/bin/env python3
"""
Backfill missing entity embeddings in entity_registry.

Usage:
  python3 scripts/backfill_entity_embeddings.py                       # dry run: count only (default)
  python3 scripts/backfill_entity_embeddings.py --apply               # embed the first 500 missing
  python3 scripts/backfill_entity_embeddings.py --apply --limit 0     # embed all missing
  python3 scripts/backfill_entity_embeddings.py --apply --limit 100   # first 100 only
  python3 scripts/backfill_entity_embeddings.py --apply --entity-type Concept
  python3 scripts/backfill_entity_embeddings.py --apply --re-embed-enriched  # re-embed B8a-enriched entities

Without --apply (or with --dry-run, which wins) it counts the entities in a read-only
session, creates no embedding provider and makes no embedding calls. --apply makes
embedding calls (paid with the OpenAI provider) and UPDATEs entity_registry; --limit
caps the entities per run at 500 unless given (0 = no limit).
"""

import argparse
import asyncio
import logging
import os
import re
import sys
import time

# Add parent dir so api/ imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg
from api.embedding_provider import create_embedding_provider

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BATCH_SIZE = 100
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
            logger.error("No embedding provider configured. Set OPENAI_API_KEY.")
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

    # Build query — entity_registry uses fuseki_uri and entity_text (not uri/name)
    if args.re_embed_enriched:
        where = "WHERE metadata->>'context' IS NOT NULL"
        logger.info("Mode: re-embed B8a-enriched entities (metadata->>'context' IS NOT NULL)")
    else:
        where = "WHERE embedding_3072 IS NULL"
    params = []
    if args.entity_type:
        where += f" AND entity_type = ${1}"
        params.append(args.entity_type)

    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM entity_registry {where}", *params)

    label = "enriched entities to re-embed" if args.re_embed_enriched else "entities missing embeddings"
    logger.info(f"{label}: {total}")
    if args.entity_type:
        logger.info(f"  (filtered to type: {args.entity_type})")

    if dry_run:
        # Show breakdown
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT entity_type, COUNT(*) as cnt FROM entity_registry "
                f"{where} GROUP BY entity_type ORDER BY cnt DESC",
                *params
            )
            for r in rows:
                logger.info(f"  {r['entity_type']}: {r['cnt']}")
            if args.re_embed_enriched:
                with_ctx = await conn.fetchval(
                    f"SELECT COUNT(*) FROM entity_registry {where} "
                    "AND LENGTH(metadata->>'context') > 10", *params
                )
                logger.info(f"  With context > 10 chars: {with_ctx}")
            else:
                with_desc = await conn.fetchval(
                    "SELECT COUNT(*) FROM entity_registry WHERE embedding_3072 IS NULL "
                    "AND description IS NOT NULL AND description != ''"
                )
                logger.info(f"  With description: {with_desc}, Name-only: {total - with_desc}")
        cap = min(total, args.limit) if args.limit else total
        logger.info(f"DRY RUN: --apply would embed {cap} entities "
                    f"(--limit {args.limit or 0}; 0 = no limit) and UPDATE entity_registry.embedding_3072")
        await pool.close()
        return

    limit_clause = f"LIMIT {args.limit}" if args.limit else ""
    order = "ORDER BY entity_type, entity_text"

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT fuseki_uri, entity_text, description, metadata->>'context' AS context FROM entity_registry {where} {order} {limit_clause}",
            *params
        )

    logger.info(f"Processing {len(rows)} entities in batches of {BATCH_SIZE}")

    embedded = 0
    name_only = 0
    with_desc = 0
    failures = 0
    start_time = time.time()

    for batch_start in range(0, len(rows), BATCH_SIZE):
        batch = rows[batch_start:batch_start + BATCH_SIZE]
        written = 0
        texts = []
        uris = []

        for row in batch:
            name = row["entity_text"] or ""
            ctx = (row["context"] or "").strip()
            desc = (row["description"] or "").strip()
            # Combine: context (B8a LLM-generated from doc links) + description (original metadata)
            combined = ". ".join(filter(None, [ctx, desc]))
            if combined:
                text = f"{name}: {combined}"
                with_desc += 1
            else:
                text = name
                name_only += 1
            texts.append(text[:8000])  # truncate to avoid token limits
            uris.append(row["fuseki_uri"])

        # Uses embed_batch_or_none for
        # B2/C4 token-tracking metric emission. Returns None on
        # whole-batch failure (matches embed_or_none semantics); same
        # back-off/continue behavior as the prior try/except.
        embeddings = await provider.embed_batch_or_none(
            texts, prompt_type="extraction"
        )
        if embeddings is None:
            logger.error(
                f"Batch {batch_start//BATCH_SIZE + 1} failed "
                f"(embed_batch_or_none returned None)"
            )
            failures += len(batch)
            await asyncio.sleep(5)  # back off on failure
            continue

        # Write to DB
        async with pool.acquire() as conn:
            async with conn.transaction():
                for uri, emb in zip(uris, embeddings):
                    # AND embedding_3072 IS NULL: the fact and chunk repair paths
                    # both re-check under row lock; this one did not, making it
                    # "idempotent by luck" rather than by construction.
                    status = await conn.execute(
                        "UPDATE entity_registry SET embedding_3072 = $1 "
                        "WHERE fuseki_uri = $2 AND embedding_3072 IS NULL",
                        str(emb), uri
                    )
                    if status.endswith(" 1"):
                        written += 1

        embedded += written  # was len(batch) — counted rows, not writes
        elapsed = time.time() - start_time
        rate = embedded / elapsed * 60 if elapsed > 0 else 0
        logger.info(
            f"  Batch {batch_start//BATCH_SIZE + 1}: "
            f"{embedded}/{len(rows)} done ({rate:.0f}/min), "
            f"failures: {failures}"
        )

        # Small delay between batches to avoid rate limits
        await asyncio.sleep(0.5)

    elapsed = time.time() - start_time
    logger.info(f"\nDone in {elapsed:.1f}s")
    logger.info(f"  Embedded: {embedded}")
    logger.info(f"  With description: {with_desc}")
    logger.info(f"  Name-only: {name_only}")
    logger.info(f"  Failures: {failures}")
    logger.info(f"  Rate: {embedded / elapsed * 60:.0f}/min")

    # Final check
    async with pool.acquire() as conn:
        remaining = await conn.fetchval(
            "SELECT COUNT(*) FROM entity_registry WHERE embedding_3072 IS NULL"
        )
    logger.info(f"  Remaining without embedding: {remaining}")
    if args.re_embed_enriched:
        logger.info("  (re-embed mode: entities already had embeddings, they were updated)")

    await pool.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Backfill entity embeddings",
        epilog="Without --apply this is a dry run: it counts the entities in a read-only session "
               "and makes no embedding calls.")
    parser.add_argument("--apply", action="store_true",
                        help="embed the entities (paid calls with the OpenAI provider) and UPDATE them")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Max entities to process (default {DEFAULT_LIMIT}; 0 = no limit)")
    parser.add_argument("--entity-type", help="Filter to specific entity type")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count only, no embedding (the default; wins over --apply)")
    parser.add_argument("--re-embed-enriched", action="store_true",
                        help="Re-embed entities with B8a-enriched metadata context (instead of only missing embeddings)")
    return parser.parse_args(argv)


def cli(argv=None):
    asyncio.run(main(parse_args(argv)))


if __name__ == "__main__":
    cli()
