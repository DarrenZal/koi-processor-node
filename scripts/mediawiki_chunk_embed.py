#!/usr/bin/env python3
"""
MediaWiki chunk + embed: reads parsed page JSONs, chunks plain_text,
embeds each chunk, stores in koi_memories + koi_memory_chunks for RAG.

Usage:
    python scripts/mediawiki_chunk_embed.py \
        --parsed-dir data/mediawiki_parsed/ \
        --manifest data/mediawiki_manifest.jsonl \
        --wiki-url https://salishsearestoration.org \
        --min-words 30 --batch-size 50 [--dry-run]

Environment variables:
    POSTGRES_URL  (preferred on production servers)
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (fallback)
    OPENAI_API_KEY (required for embeddings)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import time
import unicodedata
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg

# Ensure koi-processor root is on sys.path
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.chunker import SentenceAwareChunker
from api.embedding_provider import create_embedding_provider

logger = logging.getLogger("mediawiki_chunk_embed")

# ---------------------------------------------------------------------------
# Slug helper (matches mediawiki_parse_dump.py / mediawiki_bulk_import.py)
# ---------------------------------------------------------------------------

_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9\s-]")
_SLUG_SPACE_RE = re.compile(r"[\s]+")


def _title_to_slug(title: str, max_len: int = 100) -> str:
    s = unicodedata.normalize("NFC", title.strip().lower())
    s = _SLUG_UNSAFE_RE.sub("", s)
    s = _SLUG_SPACE_RE.sub("-", s).strip("-")
    return s[:max_len] if s else "untitled"


# ---------------------------------------------------------------------------
# Manifest + page JSON readers (from mediawiki_bulk_import.py)
# ---------------------------------------------------------------------------

def read_manifest(manifest_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def load_page_json(parsed_dir: str, title: str, slug_seen: Dict[str, int]) -> Optional[Dict[str, Any]]:
    slug = _title_to_slug(title)
    if slug in slug_seen:
        slug_seen[slug] += 1
        slug = f"{slug}-{slug_seen[slug]}"
    else:
        slug_seen[slug] = 1

    page_path = os.path.join(parsed_dir, f"{slug}.json")
    if not os.path.exists(page_path):
        return None
    with open(page_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------

async def get_db_connection() -> asyncpg.Connection:
    postgres_url = os.environ.get("POSTGRES_URL")
    if postgres_url:
        return await asyncpg.connect(postgres_url)

    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = int(os.environ.get("DB_PORT", "5432"))
    db_name = os.environ.get("DB_NAME")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "")

    if not db_name:
        raise RuntimeError("DB_NAME or POSTGRES_URL environment variable is required")

    return await asyncpg.connect(
        host=db_host, port=db_port, database=db_name,
        user=db_user, password=db_password,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def run(args: argparse.Namespace) -> None:
    # --- Read manifest, filter to chunkable pages ---
    manifest = read_manifest(args.manifest)
    pages = [
        p for p in manifest
        if p.get("page_class") != "alias_only"
        and not p.get("is_redirect")
        and p.get("word_count", 0) >= args.min_words
    ]
    logger.info(f"Manifest: {len(manifest)} total, {len(pages)} eligible (>= {args.min_words} words, non-alias)")

    if args.dry_run:
        logger.info(f"[DRY RUN] Would process {len(pages)} pages")
        return

    # --- Init embedding provider ---
    embedder = create_embedding_provider()
    if embedder is None:
        logger.error("No embedding provider configured. Set OPENAI_API_KEY.")
        sys.exit(1)
    logger.info(f"Embedding provider: {embedder.model_name} (dim={embedder.dimension})")

    chunker = SentenceAwareChunker(chunk_size=500, chunk_overlap=50, min_chunk_size=100)

    # --- Connect to DB ---
    conn = await get_db_connection()
    logger.info("Connected to database")

    # --- Check for existing mediawiki-sensor docs (idempotency) ---
    existing_rids = set()
    rows = await conn.fetch(
        "SELECT rid FROM koi_memories WHERE source_sensor = 'mediawiki-sensor'"
    )
    for r in rows:
        existing_rids.add(r['rid'])
    logger.info(f"Already embedded: {len(existing_rids)} documents")

    wiki_base = args.wiki_url.rstrip("/")

    doc_count = 0
    chunk_count = 0
    skip_count = 0
    error_count = 0
    slug_seen: Dict[str, int] = {}
    batch_start = time.time()

    for i, entry in enumerate(pages):
        title = entry["title"]
        source_rid = entry.get("source_rid", "")
        doc_rid = f"mediawiki:{source_rid.split(':',1)[-1]}" if source_rid else f"mediawiki:{_title_to_slug(title)}"

        # Skip if already embedded
        if doc_rid in existing_rids:
            skip_count += 1
            continue

        # Load page JSON
        page_data = load_page_json(args.parsed_dir, title, slug_seen)
        if page_data is None:
            logger.warning(f"Page JSON not found for: {title}")
            error_count += 1
            continue

        plain_text = page_data.get("plain_text", "")
        if not plain_text or len(plain_text.split()) < args.min_words:
            skip_count += 1
            continue

        # Build wiki URL
        page_slug = title.replace(" ", "_")
        wiki_url = f"{wiki_base}/wiki/{page_slug}"

        try:
            # Build chunk list: either section-aware or flat
            if args.section_aware:
                sections_list = page_data.get("sections", [])
                section_texts = page_data.get("section_texts", {})
                # Build section_id -> section_title lookup
                section_title_map = {s["id"]: s["title"] for s in sections_list}

                chunk_entries = []  # list of (text, embed_text, section_id, section_title, wiki_url_with_anchor)
                for sec in sections_list:
                    sec_id = sec["id"]
                    sec_title = sec["title"]
                    sec_text = section_texts.get(sec_id, "")
                    if not sec_text or len(sec_text.split()) < args.min_words:
                        continue
                    sec_anchor = f"{wiki_url}#{sec_id}"
                    if len(sec_text.split()) < 500:
                        chunk_entries.append((sec_text, f"Page: {title} | Section: {sec_title}\n\n{sec_text}", sec_id, sec_title, sec_anchor))
                    else:
                        sub_chunks = chunker.chunk_text(sec_text)
                        for sc in sub_chunks:
                            chunk_entries.append((sc["text"], f"Page: {title} | Section: {sec_title}\n\n{sc['text']}", sec_id, sec_title, sec_anchor))

                if not chunk_entries:
                    skip_count += 1
                    continue

                embed_texts = [ce[1] for ce in chunk_entries]
            else:
                # Flat chunking (original behavior)
                chunks = chunker.chunk_text(plain_text)
                if not chunks:
                    skip_count += 1
                    continue
                embed_texts = [f"Page: {title}\n\n{c['text']}" for c in chunks]

            # Batch embed (up to 100 per API call)
            # Pack 2 (2026-04-28): migrated to embed_batch_or_none for
            # B2/C4 token-tracking metric emission. Returns None on
            # whole-batch failure; raise to preserve outer per-doc
            # fail-loud semantics (outer try/except catches & increments
            # error_count, then continues to next doc).
            embeddings = []
            for batch_start_idx in range(0, len(embed_texts), 100):
                batch = embed_texts[batch_start_idx:batch_start_idx + 100]
                batch_embs = await embedder.embed_batch_or_none(
                    batch, prompt_type="extraction"
                )
                if batch_embs is None:
                    raise RuntimeError(
                        f"embed_batch_or_none returned None for "
                        f"batch_start_idx={batch_start_idx}, size={len(batch)}"
                    )
                embeddings.extend(batch_embs)

            # Store in DB within a transaction
            async with conn.transaction():
                # Upsert koi_memories
                doc_content = json.dumps({
                    "title": title,
                    "text": plain_text,
                    "wiki_url": wiki_url,
                    "template_type": entry.get("template_type"),
                    "page_class": entry.get("page_class"),
                })
                doc_metadata = json.dumps({
                    "source_rid": source_rid,
                    "page_id": entry.get("page_id"),
                    "word_count": entry.get("word_count", 0),
                })

                await conn.execute("""
                    INSERT INTO koi_memories (id, rid, event_type, source_sensor, content, metadata)
                    VALUES ($1, $2, 'NEW', 'mediawiki-sensor', $3::jsonb, $4::jsonb)
                    ON CONFLICT (rid) DO UPDATE SET
                        content = EXCLUDED.content,
                        metadata = EXCLUDED.metadata,
                        updated_at = NOW()
                """, uuid.uuid4(), doc_rid, doc_content, doc_metadata)

                # Delete any existing chunks for this doc (clean re-embed)
                await conn.execute(
                    "DELETE FROM koi_memory_chunks WHERE document_rid = $1", doc_rid
                )

                # Insert chunks with embeddings
                if args.section_aware:
                    for idx, (text, _embed_text, sec_id, sec_title, sec_url) in enumerate(chunk_entries):
                        chunk_rid = f"{doc_rid}#section:{sec_id}#chunk{idx}"
                        chunk_content = json.dumps({
                            "text": text,
                            "title": title,
                            "chunk_index": idx,
                            "section_id": sec_id,
                            "section_title": sec_title,
                            "wiki_url": sec_url,
                        })
                        embedding_str = '[' + ','.join(str(x) for x in embeddings[idx]) + ']'

                        await conn.execute("""
                            INSERT INTO koi_memory_chunks
                                (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding)
                            VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector)
                            ON CONFLICT (chunk_rid) DO UPDATE SET
                                content = EXCLUDED.content,
                                embedding = EXCLUDED.embedding
                        """, chunk_rid, doc_rid, idx, len(chunk_entries),
                            chunk_content, embedding_str)

                        chunk_count += 1
                else:
                    for idx, chunk in enumerate(chunks):
                        chunk_rid = f"{doc_rid}#chunk{idx}"
                        chunk_content = json.dumps({
                            "text": chunk["text"],
                            "title": title,
                            "chunk_index": chunk["index"],
                        })
                        embedding_str = '[' + ','.join(str(x) for x in embeddings[idx]) + ']'

                        await conn.execute("""
                            INSERT INTO koi_memory_chunks
                                (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding)
                            VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector)
                            ON CONFLICT (chunk_rid) DO UPDATE SET
                                content = EXCLUDED.content,
                                embedding = EXCLUDED.embedding
                        """, chunk_rid, doc_rid, chunk["index"], chunk["total_chunks"],
                            chunk_content, embedding_str)

                        chunk_count += 1

            doc_count += 1
            existing_rids.add(doc_rid)

            # Progress logging
            if doc_count % args.batch_size == 0:
                elapsed = time.time() - batch_start
                logger.info(
                    f"Progress: {doc_count} docs, {chunk_count} chunks, "
                    f"{skip_count} skipped, {error_count} errors "
                    f"({elapsed:.1f}s elapsed)"
                )

        except Exception as e:
            logger.error(f"Error processing '{title}': {e}")
            error_count += 1
            continue

    # Reindex IVFFlat after bulk insert
    logger.info("Reindexing chunk embedding index...")
    try:
        await conn.execute("REINDEX INDEX idx_chunks_embedding")
        logger.info("Index rebuilt.")
    except Exception as e:
        logger.warning(f"REINDEX failed (may not exist or different name): {e}")

    await conn.close()

    elapsed = time.time() - batch_start
    logger.info(
        f"Done: {doc_count} docs embedded, {chunk_count} chunks stored, "
        f"{skip_count} skipped, {error_count} errors ({elapsed:.1f}s)"
    )


def main():
    parser = argparse.ArgumentParser(description="Chunk + embed MediaWiki pages for RAG")
    parser.add_argument("--parsed-dir", required=True, help="Directory of per-page JSON files")
    parser.add_argument("--manifest", required=True, help="Path to manifest JSONL")
    parser.add_argument("--wiki-url", required=True, help="Base wiki URL (e.g. https://salishsearestoration.org)")
    parser.add_argument("--min-words", type=int, default=30, help="Minimum word count to chunk (default: 30)")
    parser.add_argument("--batch-size", type=int, default=50, help="Log progress every N docs (default: 50)")
    parser.add_argument("--section-aware", action="store_true", help="Chunk per-section instead of flat page text")
    parser.add_argument("--dry-run", action="store_true", help="Count eligible pages without processing")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
