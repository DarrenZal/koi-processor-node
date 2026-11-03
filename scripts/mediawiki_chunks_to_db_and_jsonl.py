#!/usr/bin/env python3
"""Build MediaWiki chunks in personal_koi with NULL embeddings + emit reembed JSONL.

For a bulk bootstrap on a separate GPU machine when the remote embedding service is
too slow. Writes koi_memories + koi_memory_chunks rows (embedding=NULL) and a JSONL
`{"id": chunk_rid, "text": embed_text}` that reembed_on_h200.py (local GPU) or
reembed_via_poly.py (remote service) can consume. Load the vectors back with
import_reembeddings.py once re-embedded.

Mirrors section-aware chunking from mediawiki_chunk_embed.py. Idempotent via
koi_memory_chunks.chunk_rid UNIQUE + ON CONFLICT DO UPDATE.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.chunker import SentenceAwareChunker  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def read_manifest(manifest_path: str) -> List[Dict[str, Any]]:
    out = []
    with open(manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _slugify_title(title: str, max_len: int = 100) -> str:
    import re, unicodedata
    s = unicodedata.normalize("NFC", title.strip().lower())
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s[:max_len] if s else "untitled"


def load_page_json(parsed_dir: str, title: str, slug_seen: Dict[str, int]) -> Optional[Dict[str, Any]]:
    slug = _slugify_title(title)
    base = slug
    if slug in slug_seen:
        slug_seen[slug] += 1
        slug = f"{base}-{slug_seen[base]}"
    else:
        slug_seen[slug] = 0
    path = os.path.join(parsed_dir, f"{slug}.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


async def run(args):
    manifest = read_manifest(args.manifest)
    pages = [p for p in manifest if p.get("page_class") != "alias_only" and p.get("word_count", 0) >= args.min_words]
    logger.info(f"Manifest: {len(manifest)} total, {len(pages)} eligible (>= {args.min_words} words, non-alias)")

    chunker = SentenceAwareChunker(chunk_size=500, chunk_overlap=50, min_chunk_size=100)

    conn = await asyncpg.connect(args.db_url)
    logger.info("Connected to database")

    wiki_base = args.wiki_url.rstrip("/")
    slug_seen: Dict[str, int] = {}

    jsonl_out = open(args.jsonl, "w", encoding="utf-8")
    doc_count = chunk_count = skip_count = error_count = 0
    t_start = time.time()

    for i, entry in enumerate(pages):
        title = entry["title"]
        source_rid = entry.get("source_rid", "")
        doc_rid = f"mediawiki:{source_rid.split(':',1)[-1]}" if source_rid else f"mediawiki:{_slugify_title(title)}"

        page_data = load_page_json(args.parsed_dir, title, slug_seen)
        if page_data is None:
            error_count += 1
            continue

        plain_text = page_data.get("plain_text", "")
        if not plain_text or len(plain_text.split()) < args.min_words:
            skip_count += 1
            continue

        page_slug = title.replace(" ", "_")
        wiki_url = f"{wiki_base}/wiki/{page_slug}"

        try:
            # Section-aware chunking (same logic as mediawiki_chunk_embed.py)
            sections_list = page_data.get("sections", [])
            section_texts = page_data.get("section_texts", {})
            chunk_entries = []
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

            async with conn.transaction():
                doc_content = json.dumps({
                    "title": title, "text": plain_text, "wiki_url": wiki_url,
                    "template_type": entry.get("template_type"),
                    "page_class": entry.get("page_class"),
                })
                doc_metadata = json.dumps({
                    "source_rid": source_rid, "page_id": entry.get("page_id"),
                    "word_count": entry.get("word_count", 0),
                })
                await conn.execute("""
                    INSERT INTO koi_memories (id, rid, event_type, source_sensor, content, metadata)
                    VALUES ($1, $2, 'NEW', 'mediawiki-sensor', $3::jsonb, $4::jsonb)
                    ON CONFLICT (rid) DO UPDATE SET content = EXCLUDED.content,
                        metadata = EXCLUDED.metadata, updated_at = NOW()
                """, uuid.uuid4(), doc_rid, doc_content, doc_metadata)

                # Clean re-chunk
                await conn.execute("DELETE FROM koi_memory_chunks WHERE document_rid = $1", doc_rid)

                for idx, (text, embed_text, sec_id, sec_title, sec_url) in enumerate(chunk_entries):
                    chunk_rid = f"{doc_rid}#section:{sec_id}#chunk{idx}"
                    chunk_content = json.dumps({
                        "text": text, "title": title, "chunk_index": idx,
                        "section_id": sec_id, "section_title": sec_title, "wiki_url": sec_url,
                    })
                    await conn.execute("""
                        INSERT INTO koi_memory_chunks
                            (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding)
                        VALUES ($1, $2, $3, $4, $5::jsonb, NULL)
                        ON CONFLICT (chunk_rid) DO UPDATE SET content = EXCLUDED.content
                    """, chunk_rid, doc_rid, idx, len(chunk_entries), chunk_content)

                    jsonl_out.write(json.dumps({"id": chunk_rid, "text": embed_text[:2000]}) + "\n")
                    chunk_count += 1

            doc_count += 1
            if doc_count % 500 == 0:
                elapsed = time.time() - t_start
                logger.info(f"Progress: {doc_count} docs, {chunk_count} chunks, {skip_count} skipped, {error_count} err ({elapsed:.1f}s)")

        except Exception as e:
            logger.warning(f"Error on '{title}': {e}")
            error_count += 1

    jsonl_out.close()
    await conn.close()
    elapsed = time.time() - t_start
    logger.info(f"Done: {doc_count} docs, {chunk_count} chunks, {skip_count} skipped, {error_count} errors in {elapsed:.1f}s")
    logger.info(f"JSONL for H200: {args.jsonl}")


def main():
    ap = argparse.ArgumentParser(description="Chunk mediawiki pages into DB (null embeddings) + emit reembed JSONL")
    ap.add_argument("--parsed-dir", required=True)
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--wiki-url", required=True)
    ap.add_argument("--jsonl", required=True, help="Output JSONL path for H200 reembed")
    ap.add_argument("--db-url", default=os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi"))
    ap.add_argument("--min-words", type=int, default=30)
    args = ap.parse_args()
    Path(os.path.dirname(os.path.abspath(args.jsonl))).mkdir(parents=True, exist_ok=True)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
