#!/usr/bin/env python3
"""
B8a Pilot: Enrich entity descriptions from linked document text.

For entities with no description/context, pulls text from their top linked
documents and generates a 1-2 sentence description via GPT-4o-mini.

Usage:
    # Dry run (the default) — show entities that would be enriched
    python3 scripts/b8a_enrich_entities.py

    # Enrich top 50 entities by doc-link count (--limit defaults to 50)
    python3 scripts/b8a_enrich_entities.py --apply

    # Enrich only entities matching a keyword
    python3 scripts/b8a_enrich_entities.py --apply --filter "eelgrass" --limit 10

    # Full enrichment of all linkable entities
    python3 scripts/b8a_enrich_entities.py --apply --limit 0

Without --apply (or with --dry-run, which wins) it lists the entities in a read-only
session and makes no LLM calls. --apply makes paid LLM calls and UPDATEs
entity_registry.metadata; --limit caps the entities per run at 50 unless given
(0 = all).

Requires:
    - OPENAI_API_KEY set
    - POSTGRES_URL set (source config/personal.env)
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
CONTEXT_MODEL = os.getenv("B8_CONTEXT_MODEL", "gpt-4o-mini")
DEFAULT_LIMIT = 50
READ_ONLY = {"default_transaction_read_only": "on"}

_openai_client = None


def _get_client():
    global _openai_client
    if _openai_client is None:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    return _openai_client


ENTITY_PROMPT = """\
You are generating a concise description for an entity in a bioregional knowledge commons.

Entity name: {entity_name}
Entity type: {entity_type}

Here are excerpts from documents that mention this entity:

{doc_excerpts}

Write a 1-2 sentence factual description of this entity based on the document excerpts above. \
Focus on what it is, where it is relevant, and why it matters for bioregional ecology or governance. \
Answer only with the description."""


async def generate_entity_description(
    entity_name: str,
    entity_type: str,
    doc_excerpts: str,
) -> str:
    """Generate a description for an entity from its document mentions."""
    client = _get_client()
    prompt = ENTITY_PROMPT.format(
        entity_name=entity_name,
        entity_type=entity_type,
        doc_excerpts=doc_excerpts[:8000],  # safety truncation
    )
    try:
        response = await client.chat.completions.create(
            model=CONTEXT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=150,
            temperature=0.0,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"Description generation failed for {entity_name}: {e}")
        return ""


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


async def run_enrichment(args):
    dry_run = args.dry_run or not getattr(args, "apply", False)
    db_url = os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL") or ""
    if not db_url:
        logger.error("No POSTGRES_URL or DATABASE_URL set")
        sys.exit(1)
    logger.info(f"Database: {mask_url(db_url)}")

    if dry_run:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3, server_settings=READ_ONLY)
    else:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)

    # Find entities with doc links but no description
    filter_clause = ""
    params = []
    if args.filter:
        filter_clause = "AND e.entity_text ILIKE $1"
        params.append(f"%{args.filter}%")

    async with pool.acquire() as conn:
        entities = await conn.fetch(f"""
            SELECT e.fuseki_uri, e.entity_text, e.entity_type, e.metadata,
                   COUNT(del.document_rid) as doc_links
            FROM entity_registry e
            JOIN document_entity_links del ON del.entity_uri = e.fuseki_uri
            WHERE NOT e.node_private
              AND (e.metadata->>'context' IS NULL OR LENGTH(e.metadata->>'context') < 10)
              AND (e.metadata->>'description' IS NULL OR LENGTH(e.metadata->>'description') < 10)
              {filter_clause}
            GROUP BY e.fuseki_uri, e.entity_text, e.entity_type, e.metadata
            ORDER BY COUNT(del.document_rid) DESC
        """, *params)

    total = len(entities)
    if args.limit:
        entities = entities[:args.limit]

    logger.info(f"Found {total} enrichable entities, processing {len(entities)}")

    if dry_run:
        for e in entities[:30]:
            logger.info(f"  {e['entity_type']:15} {e['entity_text'][:50]:52} links={e['doc_links']}")
        est_cost = len(entities) * 2000 * 0.15 / 1_000_000 + len(entities) * 50 * 0.60 / 1_000_000
        logger.info(f"\nDRY RUN — estimated cost: ~${est_cost:.2f} for {len(entities)} entities "
                    f"(--limit {args.limit or 0}; 0 = all). Pass --apply to enrich them.")
        await pool.close()
        return

    enriched = 0
    errors = 0
    sem = asyncio.Semaphore(args.concurrency)
    t0 = time.time()

    async def enrich_one(entity):
        nonlocal enriched, errors
        async with sem:
            uri = entity['fuseki_uri']
            name = entity['entity_text']
            etype = entity['entity_type']

            # Get document excerpts for this entity
            async with pool.acquire() as conn:
                doc_rows = await conn.fetch("""
                    SELECT LEFT(c.content->>'text', 500) as excerpt,
                           m.content->>'title' as title
                    FROM document_entity_links del
                    JOIN koi_memory_chunks c ON c.document_rid = del.document_rid
                    JOIN koi_memories m ON m.rid = c.document_rid
                    WHERE del.entity_uri = $1
                    ORDER BY random()
                    LIMIT 5
                """, uri)

            if not doc_rows:
                return

            excerpts = "\n\n---\n\n".join(
                f"From \"{r['title'] or 'untitled'}\":\n{r['excerpt']}"
                for r in doc_rows
            )

            description = await generate_entity_description(name, etype, excerpts)
            if not description:
                errors += 1
                return

            # Update entity metadata
            async with pool.acquire() as conn:
                meta = entity['metadata'] or {}
                if isinstance(meta, str):
                    meta = json.loads(meta)
                meta['context'] = description
                await conn.execute("""
                    UPDATE entity_registry
                    SET metadata = $1::jsonb
                    WHERE fuseki_uri = $2
                """, json.dumps(meta), uri)

            enriched += 1
            if enriched % 20 == 0:
                elapsed = time.time() - t0
                logger.info(f"  [{enriched}/{len(entities)}] {enriched/elapsed:.1f}/s — {name[:50]}")

    await asyncio.gather(*[enrich_one(e) for e in entities])

    elapsed = time.time() - t0
    logger.info(f"\nEnrichment complete:")
    logger.info(f"  Enriched: {enriched}/{len(entities)}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Time: {elapsed:.1f}s")

    # Show a few samples
    if enriched > 0:
        async with pool.acquire() as conn:
            samples = await conn.fetch("""
                SELECT entity_text, entity_type, LEFT(metadata->>'context', 120) as ctx
                FROM entity_registry
                WHERE metadata->>'context' IS NOT NULL AND LENGTH(metadata->>'context') > 10
                ORDER BY random() LIMIT 5
            """)
        logger.info("\nSample enriched descriptions:")
        for s in samples:
            logger.info(f"  {s['entity_type']:12} {s['entity_text'][:30]:32} {s['ctx']}")

    await pool.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="B8a Entity Description Enrichment",
        epilog="Without --apply this is a dry run: it lists the entities in a read-only session "
               "and makes no LLM calls.")
    parser.add_argument("--apply", action="store_true",
                        help="generate descriptions (paid LLM calls) and UPDATE entity_registry.metadata")
    parser.add_argument("--dry-run", action="store_true",
                        help="list only (the default); wins over --apply")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"max entities to process (default {DEFAULT_LIMIT}; 0 = all)")
    parser.add_argument("--filter", type=str, default="")
    parser.add_argument("--concurrency", type=int, default=5)
    args = parser.parse_args(argv)
    asyncio.run(run_enrichment(args))


if __name__ == "__main__":
    main()
