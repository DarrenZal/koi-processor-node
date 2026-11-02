#!/usr/bin/env python3
"""Substack ingestion sensor for personal-koi.

Ingests a Substack publication's posts into personal-koi under the
`substack-corpus:<feed_slug>:<post_slug>` RID scheme (source_sensor
'substack-corpus-backfill').

Drives off Substack's public JSON API (no Playwright). Free posts need no auth;
set SUBSTACK_SID (a paid subscriber's session cookie) to also ingest `only_paid`
posts in full instead of skipping them.
  - archive list:  GET /api/v1/archive?sort=new&offset=N&limit=12  (slug, post_date, title, audience)
  - per-post body: GET /api/v1/posts/<slug>                        (body_html, subtitle, audience)

For each archive post whose RID is not already in koi_memories (or --force):
  body_html -> text -> chunk (TextChunker) -> embed (OpenAI 3072) ->
  upsert koi_memories + koi_memory_chunks(embedding_3072) -> POST /ingest
  (links the publication's author Person entity to the document).

Idempotent: only NEW rids are ingested by default, so the already-repaired
141 backfill rows are never re-embedded. `--force` re-ingests everything.

Config: which publications to ingest is personal config in
config/substack_publications.yaml (gitignored; see the .example file) — not
hardcoded here, so forks configure their own.
Schedule: daily, with your platform's scheduler.

Usage (first `set -a; source config/personal.env; set +a`, which exports OPENAI_API_KEY and
POSTGRES_URL; without set -a the script sees neither):
    python scripts/substack_sensor.py            # ingest new posts for all publications
    python scripts/substack_sensor.py --dry-run  # list what would be ingested
    python scripts/substack_sensor.py --max-posts 3   # smoke test
"""
import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg
import httpx
from bs4 import BeautifulSoup

# Resolve api.* imports (mirror ingest_document.py / doc_scanner.py).
sys.path.insert(0, str(Path(__file__).parent.parent))
from api.embedding_provider import OpenAIEmbeddingProvider  # noqa: E402
from api.chunker import TextChunker  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("substack-sensor")

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
KOI_BASE_URL = os.getenv("PERSONAL_KOI_API_URL", os.getenv("DOC_INGEST_KOI_URL", "http://localhost:8351"))
EMBEDDING_DIMENSION = int(os.getenv("EMBEDDING_DIMENSION", "3072"))
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-large")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
CHUNK_SIZE, CHUNK_OVERLAP = 500, 50
SOURCE_SENSOR = "substack-corpus-backfill"  # continuity with the 2026-05-18 indyjohar backfill
ACCESS_SOURCE = "substack-public"
UA = {"User-Agent": "Mozilla/5.0 (personal-koi substack sensor)"}

# Optional Substack session cookie (paid-subscriber auth). When set, the sensor
# fetches full body_html for `only_paid` posts instead of skipping them. The
# cookie rotates periodically — an expired one yields a short/empty body, which
# the <200-char gate in run() skips, so stale creds degrade gracefully.
SUBSTACK_SID = os.getenv("SUBSTACK_SID", "").strip()
SUBSTACK_COOKIES = {"substack.sid": SUBSTACK_SID} if SUBSTACK_SID else None

# WHICH publications to ingest is personal config, loaded at runtime from
# config/substack_publications.yaml (see substack_config / the .example file).
# Deliberately NOT hardcoded here so forks configure their own publications
# without editing code or committing personal choices to the repo.
from substack_config import load_publications  # noqa: E402


def rid_for(feed_slug: str, post_slug: str) -> str:
    return f"substack-corpus:{feed_slug}:{post_slug}"


def html_to_text(body_html: Optional[str]) -> str:
    soup = BeautifulSoup(body_html or "", "html.parser")
    text = soup.get_text("\n")
    return re.sub(r"\n{3,}", "\n\n", text).strip()


async def fetch_archive(http: httpx.AsyncClient, base: str) -> Dict[str, Dict[str, Any]]:
    """Paginate /api/v1/archive -> {slug: {title, post_date, audience}}."""
    posts: Dict[str, Dict[str, Any]] = {}
    for off in range(0, 2000, 12):
        try:
            r = await http.get(f"{base}/api/v1/archive",
                               params={"sort": "new", "offset": off, "limit": 12}, headers=UA)
        except Exception as e:
            logger.warning("archive fetch error at offset %d: %s", off, e)
            break
        if r.status_code != 200:
            logger.warning("archive HTTP %d at offset %d", r.status_code, off)
            break
        page = r.json()
        if not page:
            break
        for p in page:
            posts[p["slug"]] = {"title": p.get("title"), "post_date": p.get("post_date"),
                                "audience": p.get("audience"), "subtitle": p.get("subtitle")}
        await asyncio.sleep(0.3)
    return posts


async def fetch_body(http: httpx.AsyncClient, base: str, slug: str) -> Dict[str, Any]:
    r = await http.get(f"{base}/api/v1/posts/{slug}", headers=UA)
    if r.status_code != 200:
        return {}
    return r.json()


async def upsert_post(pool: asyncpg.Pool, embedder: OpenAIEmbeddingProvider, chunker: TextChunker,
                      http: httpx.AsyncClient, pub: Dict[str, Any], slug: str, meta: Dict[str, Any],
                      body_text: str, dry_run: bool) -> Dict[str, Any]:
    rid = rid_for(pub["feed_slug"], slug)
    url = f"{pub['base']}/p/{slug}"
    content_hash = hashlib.sha256(body_text.encode("utf-8")).hexdigest()[:16]
    chunks = chunker.chunk_text(body_text)
    if dry_run:
        return {"rid": rid, "chunks": len(chunks), "chars": len(body_text), "written": False}

    embeddings: List[Optional[List[float]]] = []
    null_embeds = 0
    for c in chunks:
        try:
            embeddings.append(await embedder.embed(c["text"]))
        except Exception as e:  # null-on-fail (mirror doc_scanner / ingest_document)
            logger.warning("embed failed %s: %s", rid, e)
            embeddings.append(None)
            null_embeds += 1

    doc_content = {"title": meta.get("title"), "subtitle": meta.get("subtitle"),
                   "text": body_text, "url": url}
    doc_meta = {
        "url": url, "repo": "substack-backfill", "tags": pub["tags"],
        "title": meta.get("title"), "author": pub["author"], "domain": pub["domain"],
        "feed_slug": pub["feed_slug"], "source_type": "substack_corpus",
        "canonical_slug": slug, "published_at": meta.get("post_date"),
        "content_hash": content_hash,
    }
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchrow("SELECT id FROM koi_memories WHERE rid=$1", rid)
            event_type = "NEW" if existing is None else "UPDATE"
            await conn.execute(
                """
                INSERT INTO koi_memories
                    (id, rid, event_type, source_sensor, content, metadata, is_private, access_source)
                VALUES (gen_random_uuid(), $1, $2, $3, $4::jsonb, $5::jsonb, FALSE, $6)
                ON CONFLICT (rid) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    updated_at = NOW()
                """,
                rid, event_type, SOURCE_SENSOR,
                json.dumps(doc_content), json.dumps(doc_meta), ACCESS_SOURCE,
            )
            await conn.execute("DELETE FROM koi_memory_chunks WHERE document_rid=$1", rid)
            total = len(chunks)
            for i, (c, emb) in enumerate(zip(chunks, embeddings)):
                chunk_meta = {"slug": slug, "feed_slug": pub["feed_slug"], "source_sensor": SOURCE_SENSOR}
                if emb is None:
                    chunk_meta["embedding_failed"] = True
                await conn.execute(
                    """
                    INSERT INTO koi_memory_chunks
                        (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding_3072, metadata)
                    VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector(3072), $7::jsonb)
                    ON CONFLICT (chunk_rid) DO UPDATE SET
                        chunk_index = EXCLUDED.chunk_index, total_chunks = EXCLUDED.total_chunks,
                        content = EXCLUDED.content, embedding_3072 = EXCLUDED.embedding_3072,
                        metadata = EXCLUDED.metadata
                    """,
                    f"{rid}#chunk{i}", rid, i, total,
                    json.dumps({"text": c["text"], "context": meta.get("title") or slug}),
                    json.dumps(emb) if emb is not None else None, json.dumps(chunk_meta),
                )

    # Link the author Person entity to the document (document_entity_links).
    try:
        resp = await http.post(f"{KOI_BASE_URL}/ingest", json={
            "document_rid": rid, "content": body_text[:2000],
            "entities": [{**pub["author_entity"], "confidence": 0.99,
                          "context": f"Author of Substack post: {meta.get('title')}"}],
            "source": SOURCE_SENSOR,
        }, timeout=30.0)
        if resp.status_code != 200:
            logger.warning("/ingest %d for %s", resp.status_code, rid)
    except Exception as e:
        logger.warning("/ingest error %s: %s", rid, e)

    return {"rid": rid, "chunks": total, "chars": len(body_text), "null_embeds": null_embeds, "written": True}


async def run(args) -> None:
    embedder = None
    if not args.dry_run:
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY not set — source config/personal.env first.")
        embedder = OpenAIEmbeddingProvider(api_key=OPENAI_API_KEY, model=EMBEDDING_MODEL, dimension=EMBEDDING_DIMENSION)
    chunker = TextChunker(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    totals = {"considered": 0, "ingested": 0, "skipped": 0, "errors": 0}
    try:
        publications = load_publications()
        async with httpx.AsyncClient(timeout=60.0, cookies=SUBSTACK_COOKIES) as http:
            for pub in publications:
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT rid FROM koi_memories WHERE rid LIKE $1",
                        f"substack-corpus:{pub['feed_slug']}:%")
                existing = {r["rid"] for r in rows}
                archive = await fetch_archive(http, pub["base"])
                logger.info("%s: archive=%d posts, already in DB=%d", pub["feed_slug"], len(archive), len(existing))
                todo = [s for s in archive if args.force or rid_for(pub["feed_slug"], s) not in existing]
                # newest first; cap for smoke tests
                todo.sort(key=lambda s: archive[s].get("post_date") or "", reverse=True)
                if args.max_posts:
                    todo = todo[: args.max_posts]
                logger.info("%s: %d posts to ingest", pub["feed_slug"], len(todo))
                for slug in todo:
                    totals["considered"] += 1
                    meta = dict(archive[slug])
                    try:
                        detail = await fetch_body(http, pub["base"], slug)
                        # Paid posts: skip only when we have no subscriber cookie.
                        # With SUBSTACK_SID set the API returns full body_html for
                        # paid posts; the <200-char gate below still catches any
                        # that come back paywalled (e.g. an expired cookie).
                        if detail.get("audience") not in (None, "everyone") and not SUBSTACK_COOKIES:
                            logger.info("skip non-free %s (audience=%s)", slug, detail.get("audience"))
                            totals["skipped"] += 1
                            continue
                        body_text = html_to_text(detail.get("body_html"))
                        meta["subtitle"] = detail.get("subtitle") or meta.get("subtitle")
                        if len(body_text) < 200:
                            logger.info("skip short/empty body %s (%d chars)", slug, len(body_text))
                            totals["skipped"] += 1
                            continue
                        res = await upsert_post(pool, embedder, chunker, http, pub, slug, meta, body_text, args.dry_run)
                        totals["ingested"] += 1
                        logger.info("%s %s (%d chunks)", "DRY" if args.dry_run else "ingested", res["rid"], res["chunks"])
                        await asyncio.sleep(0.4)
                    except Exception as e:
                        totals["errors"] += 1
                        logger.warning("error ingesting %s: %s", slug, e)
    finally:
        await pool.close()
    logger.info("DONE %s", totals)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true", help="List what would be ingested, no writes")
    ap.add_argument("--force", action="store_true", help="Re-ingest posts already present (re-embeds)")
    ap.add_argument("--max-posts", type=int, default=None, help="Cap posts per publication (smoke test)")
    asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    main()
