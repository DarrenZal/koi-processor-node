#!/usr/bin/env python3
"""
B8 Backfill: Generate contextual retrieval snippets for all existing chunks
and optionally re-embed them with the contextualized text.

Usage:
    # Dry run (the default) — count chunks, estimate cost
    python3 scripts/backfill_contextual_retrieval.py

    # Test on 5 docs, context only (no re-embedding)
    python3 scripts/backfill_contextual_retrieval.py --apply --limit 5 --skip-embed

    # MediaWiki spot-check
    python3 scripts/backfill_contextual_retrieval.py --apply --limit 10 --skip-embed --document-rid mediawiki:

    # First 10 documents with re-embedding (--limit defaults to 10)
    python3 scripts/backfill_contextual_retrieval.py --apply

    # Full backfill with re-embedding
    python3 scripts/backfill_contextual_retrieval.py --apply --limit 0

    # Full backfill, custom concurrency
    python3 scripts/backfill_contextual_retrieval.py --apply --limit 0 --concurrency 3

Without --apply (or with --dry-run, which wins) it reads the chunks in a read-only
session, estimates the cost and makes no LLM or embedding calls. --apply generates
contexts (B8_TRANSPORT: `claude -p` by default, or paid OpenAI calls), re-embeds unless
--skip-embed (paid with the OpenAI provider) and UPDATEs koi_memory_chunks; --limit caps
the documents per run at 10 unless given (0 = all). A chunk whose context generation
returns nothing is left unchanged and counted as an error.

Requires:
    - OPENAI_API_KEY set (for context generation + embedding)
    - POSTGRES_URL or DATABASE_URL set (for DB connection)
    - source config/personal.env for local runs
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
from itertools import groupby
from operator import itemgetter

# Add parent dir so we can import api modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncpg

from api.contextual_retriever import generate_contexts_for_document
from api.embedding_provider import create_embedding_provider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 10
READ_ONLY = {"default_transaction_read_only": "on"}


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def _get_db_url() -> str:
    return os.getenv("POSTGRES_URL") or os.getenv("DATABASE_URL") or ""


def _build_embed_text(context: str, chunk: dict, source_sensor: str) -> str:
    """Reconstruct source-specific embed text with context prepended.

    Preserves the original embedding format for each source type.
    """
    chunk_text = chunk.get("text", "")
    title = chunk.get("title", "")
    section_title = chunk.get("section_title", "")

    if source_sensor == "mediawiki-sensor":
        # MediaWiki: "Page: {title} | Section: {section}\n\n{text}"
        base = f"Page: {title} | Section: {section_title}\n\n{chunk_text}"
    else:
        # Default: just chunk text
        base = chunk_text

    if context:
        return f"{context}\n\n{base}"
    return base


async def run_backfill(args):
    dry_run = args.dry_run or not getattr(args, "apply", False)
    db_url = _get_db_url()
    if not db_url:
        logger.error("No POSTGRES_URL or DATABASE_URL set")
        sys.exit(1)
    logger.info(f"Database: {mask_url(db_url)}")

    if dry_run:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3, server_settings=READ_ONLY)
        embedder = None
    else:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
        embedder = create_embedding_provider() if not args.skip_embed else None

        if not args.skip_embed and embedder is None:
            logger.warning("No embedding provider available — running with --skip-embed")
            args.skip_embed = True

    # Query all chunks grouped by document
    rid_filter = ""
    params = []
    if args.document_rid:
        rid_filter = "WHERE c.document_rid LIKE $1"
        params.append(f"{args.document_rid}%")

    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT
                c.chunk_rid,
                c.document_rid,
                c.chunk_index,
                c.content AS chunk_content,
                m.content->>'text' AS doc_text,
                m.content->>'title' AS doc_title,
                m.source_sensor
            FROM koi_memory_chunks c
            JOIN koi_memories m ON m.rid = c.document_rid
            {rid_filter}
            ORDER BY c.document_rid, c.chunk_index
        """, *params)

    total_chunks = len(rows)
    logger.info(f"Found {total_chunks} chunks across documents")

    if total_chunks == 0:
        logger.info("Nothing to process")
        return

    # Group by document
    docs = []
    for doc_rid, group in groupby(rows, key=itemgetter("document_rid")):
        chunks = list(group)
        docs.append((doc_rid, chunks))

    if args.limit:
        docs = docs[:args.limit]
        total_chunks = sum(len(c) for _, c in docs)

    logger.info(f"Processing {len(docs)} documents, {total_chunks} chunks")

    # Dry run: estimate cost
    if dry_run:
        total_input_tokens = 0
        for doc_rid, chunks in docs:
            doc_text = chunks[0]["doc_text"] or ""
            doc_tokens = len(doc_text) // 4  # rough estimate
            for c in chunks:
                chunk_tokens = len(json.loads(c["chunk_content"]).get("text", "")) // 4
                total_input_tokens += doc_tokens + chunk_tokens + 100  # prompt overhead
        total_output_tokens = total_chunks * 50
        input_cost = total_input_tokens * 0.15 / 1_000_000
        output_cost = total_output_tokens * 0.60 / 1_000_000
        embed_cost = total_chunks * 500 * 0.02 / 1_000_000 if not args.skip_embed else 0
        logger.info(f"DRY RUN — estimated cost:")
        logger.info(f"  Context generation: ~${input_cost + output_cost:.2f} "
                     f"({total_input_tokens:,} input + {total_output_tokens:,} output tokens)")
        logger.info(f"  Re-embedding: ~${embed_cost:.2f}")
        logger.info(f"  Total: ~${input_cost + output_cost + embed_cost:.2f}")
        logger.info(f"DRY RUN: --apply would process {len(docs)} documents "
                    f"(--limit {args.limit or 0}; 0 = all) and UPDATE koi_memory_chunks")
        await pool.close()
        return

    # Process documents
    processed = 0
    skipped_code = 0
    errors = 0
    t0 = time.time()

    for doc_idx, (doc_rid, chunks) in enumerate(docs):
        doc_text = chunks[0]["doc_text"] or ""
        doc_title = chunks[0]["doc_title"] or ""
        source_sensor = chunks[0]["source_sensor"] or ""

        # Skip code entity chunks if --skip-code
        if args.skip_code and "github" in (source_sensor or ""):
            # Check if these are code entity chunks (they have entity-style chunk_rids)
            # For now, skip all github sensor chunks when --skip-code is set
            # A more precise check would look at content structure
            chunk_contents = [json.loads(c["chunk_content"]) for c in chunks]
            if any(cc.get("entity_name") for cc in chunk_contents):
                skipped_code += len(chunks)
                continue

        # Build chunk dicts for context generation
        chunk_dicts = []
        for c in chunks:
            content = json.loads(c["chunk_content"])
            chunk_dicts.append(content)

        # Generate contexts
        try:
            contexts = await generate_contexts_for_document(
                document_text=doc_text,
                chunks=chunk_dicts,
                document_title=doc_title,
                concurrency=args.concurrency,
            )
        except Exception as e:
            logger.error(f"Context generation failed for {doc_rid}: {e}")
            errors += len(chunks)
            continue

        # Prepare updates
        embed_texts = []
        updates = []
        no_context = 0
        for i, (c, ctx, chunk_dict) in enumerate(zip(chunks, contexts, chunk_dicts)):
            if not ctx:
                # Context generation failed or returned nothing for this chunk. Leave the
                # row unchanged rather than writing "context": "" and re-embedding text
                # that did not change.
                no_context += 1
                continue

            # Update content JSONB with context
            chunk_dict["context"] = ctx
            updated_content = json.dumps(chunk_dict)

            # Build source-specific embed text
            embed_text = _build_embed_text(ctx, chunk_dict, source_sensor)
            embed_texts.append(embed_text)
            updates.append((c["chunk_rid"], updated_content, embed_text))

        if no_context:
            errors += no_context
            logger.warning(f"No context generated for {no_context}/{len(chunks)} chunks of "
                           f"{doc_rid}; those chunks were left unchanged")
        if not updates:
            continue

        # Re-embed if needed
        embeddings = [None] * len(updates)
        if not args.skip_embed and embedder:
            # Uses embed_batch_or_none for
            # B2/C4 token-tracking metric emission (one aggregate JSONL
            # record per batch with is_batch=true). Returns None on
            # whole-batch failure; per-batch handling keeps partial progress
            # across batches in the same doc.
            for batch_start in range(0, len(embed_texts), 100):
                batch = embed_texts[batch_start:batch_start + 100]
                batch_embs = await embedder.embed_batch_or_none(
                    batch, prompt_type="extraction"
                )
                if batch_embs is None:
                    logger.warning(
                        f"Embedding failed for {doc_rid} (batch_start={batch_start}, "
                        f"size={len(batch)}); leaving these chunks with None embedding."
                    )
                    continue
                for j, emb in enumerate(batch_embs):
                    embeddings[batch_start + j] = emb

        # Write to DB
        async with pool.acquire() as conn:
            async with conn.transaction():
                for (chunk_rid, updated_content, _), embedding in zip(updates, embeddings):
                    if embedding is not None:
                        emb_str = '[' + ','.join(str(x) for x in embedding) + ']'
                        await conn.execute("""
                            UPDATE koi_memory_chunks
                            SET content = $1::jsonb, embedding = $2::vector
                            WHERE chunk_rid = $3
                        """, updated_content, emb_str, chunk_rid)
                    else:
                        await conn.execute("""
                            UPDATE koi_memory_chunks
                            SET content = $1::jsonb
                            WHERE chunk_rid = $2
                        """, updated_content, chunk_rid)

        processed += len(updates)
        if processed % 50 < len(updates):
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed > 0 else 0
            logger.info(f"  [{processed}/{total_chunks}] {rate:.1f} chunks/s — "
                        f"doc {doc_idx+1}/{len(docs)}: {doc_rid[:60]}")

    elapsed = time.time() - t0
    logger.info(f"\nBackfill complete:")
    logger.info(f"  Processed: {processed} chunks in {len(docs)} documents")
    logger.info(f"  Skipped (code): {skipped_code}")
    logger.info(f"  Errors: {errors}")
    logger.info(f"  Time: {elapsed:.1f}s ({processed/elapsed:.1f} chunks/s)" if elapsed > 0 else "")
    logger.info(f"  Embed: {'enabled' if not args.skip_embed else 'skipped'}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="B8 Contextual Retrieval Backfill",
        epilog="Without --apply this is a dry run: it estimates the cost in a read-only session "
               "and makes no LLM or embedding calls.")
    parser.add_argument("--apply", action="store_true",
                        help="generate contexts and re-embed (LLM and paid embedding calls), "
                             "then UPDATE koi_memory_chunks")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count chunks and estimate cost without modifying "
                             "(the default; wins over --apply)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help=f"Process only first N documents (default {DEFAULT_LIMIT}; 0 = all)")
    parser.add_argument("--skip-embed", action="store_true",
                        help="Generate context only, don't re-embed")
    parser.add_argument("--concurrency", type=int, default=5,
                        help="Max concurrent LLM calls per document")
    parser.add_argument("--skip-code", action="store_true", default=True,
                        help="Skip GitHub code entity chunks (default: true)")
    parser.add_argument("--no-skip-code", action="store_false", dest="skip_code",
                        help="Process all chunks including code entities")
    parser.add_argument("--document-rid", type=str, default="",
                        help="Only process chunks with this document_rid prefix")
    args = parser.parse_args(argv)

    asyncio.run(run_backfill(args))


if __name__ == "__main__":
    main()
