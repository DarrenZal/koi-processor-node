#!/usr/bin/env python3
"""Ingest a scraped Substack author corpus into personal-koi (generalized).

Generalized from an earlier single-author script so any author's scraped
FULL-content JSON can be backfilled the same way. Reads a corpus JSON
(list of posts, or {posts:[...]}), dedups against existing rows by canonical
slug, chunks + embeds (OpenAI text-embedding-3-large, 3072-dim), and inserts
into koi_memories + koi_memory_chunks. Idempotent (ON CONFLICT DO NOTHING).

RID namespace: substack-corpus:<feed-slug>:<slug>.

Handles both `full_content` (snake) and `fullContent` (camel) content fields.

Usage:
    python3 scripts/ingest_substack_corpus.py \
      --corpus-path ~/path/to/author_full_posts.json \
      --feed-slug exampleauthor --substack-domain exampleauthor.substack.com \
      --author "Example Author" --domain commons \
      --tags substack,example-author --dry-run
"""

import argparse
import asyncio
import base64
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import asyncpg
import tiktoken

POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")

OPENAI_MODEL = "text-embedding-3-large"
OPENAI_DIMENSIONS = 3072
MAX_TOKENS_PER_CHUNK = 8000
MAX_CHUNKS_PER_POST = 32
CHUNK_TARGET_TOKENS = 1500

SOURCE_SENSOR = "substack-corpus-backfill"
REPO_NAME = "substack-backfill"

RATE_PER_MILLION = 0.13
COST_ABORT_USD = 5.0
BATCH_SIZE = 50

_TOKENIZER = tiktoken.encoding_for_model(OPENAI_MODEL)

# Set in main() from --substack-domain
SUBSTACK_DOMAIN = ""       # e.g. exampleauthor.substack.com
DOMAIN_TOKEN = ""          # e.g. exampleauthor (subdomain label; currently unused, kept
                           # in case a future caller needs the bare subdomain token)


def canonical_slug(url_or_text: str) -> Optional[str]:
    """Extract the Substack canonical post slug for the configured author domain.

    Prefer a direct `<domain>/p/<slug>` hit; fall back to decoding a
    substack.com/redirect/2/<token> and re-testing against the same domain.
    """
    if not url_or_text:
        return None
    direct = re.compile(re.escape(SUBSTACK_DOMAIN) + r"/p/([a-z0-9\-]+)")
    m = direct.search(url_or_text)
    if m:
        return m.group(1).rstrip("-")
    m = re.search(r"substack\.com/redirect/2/([A-Za-z0-9_-]+)", url_or_text)
    if m:
        try:
            token = m.group(1)
            decoded = json.loads(base64.urlsafe_b64decode(token + "==").decode())
            target = decoded.get("e", "")
            m2 = direct.search(target)
            if m2:
                return m2.group(1).rstrip("-")
        except Exception:
            pass
    return None


def chunk_text(text: str) -> List[Dict[str, str]]:
    tokens = _TOKENIZER.encode(text)
    if not tokens:
        return []
    chunks = []
    for i, start in enumerate(range(0, len(tokens), CHUNK_TARGET_TOKENS)):
        if i >= MAX_CHUNKS_PER_POST:
            break
        end = min(start + CHUNK_TARGET_TOKENS, len(tokens))
        chunk_tokens = tokens[start:end][:MAX_TOKENS_PER_CHUNK]
        chunks.append({"index": i, "text": _TOKENIZER.decode(chunk_tokens)})
    return chunks


async def fetch_existing_slugs(conn, feed_slug: str) -> Set[str]:
    """Slugs already present in the substack-corpus for this feed.

    Must check the corpus itself (source_sensor == SOURCE_SENSOR), not whether
    some unrelated sensor has ever seen a copy of the post. This previously
    also matched any 'email-sensor' row whose raw text mentioned the domain —
    the generic Gmail sensor independently mirrors every inbound Substack
    notification email, so it always had a copy, which made every paid-only
    post look "already covered" and permanently blocked it from ever being
    promoted into the corpus. Corpus rows store a clean canonical_slug in
    metadata, so no text-regex needed.
    """
    rows = await conn.fetch(
        """
        SELECT metadata->>'canonical_slug' AS slug
        FROM koi_memories
        WHERE source_sensor = $1 AND metadata->>'feed_slug' = $2
        """,
        SOURCE_SENSOR,
        feed_slug,
    )
    return {r["slug"] for r in rows if r["slug"]}


async def embed_batch(client, texts: List[str]) -> List[List[float]]:
    safe = []
    for t in texts:
        toks = _TOKENIZER.encode(t)
        if len(toks) > MAX_TOKENS_PER_CHUNK:
            t = _TOKENIZER.decode(toks[:MAX_TOKENS_PER_CHUNK])
        safe.append(t)
    resp = await asyncio.to_thread(
        client.embeddings.create,
        model=OPENAI_MODEL, input=safe, dimensions=OPENAI_DIMENSIONS,
    )
    return [d.embedding for d in resp.data]


def parse_corpus(path: str) -> List[Dict]:
    """Read corpus JSON (list or {posts:[...]}); normalize content field to full_content."""
    with open(path) as f:
        d = json.load(f)
    posts = d.get("posts", []) if isinstance(d, dict) else d
    out = []
    for p in posts:
        content = p.get("full_content") or p.get("fullContent")
        if p.get("url") and content:
            p = dict(p)
            p["full_content"] = content
            out.append(p)
    return out


async def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--corpus-path", required=True)
    ap.add_argument("--feed-slug", required=True, help="RID namespace, e.g. exampleauthor")
    ap.add_argument("--substack-domain", required=True, help="e.g. exampleauthor.substack.com")
    ap.add_argument("--author", required=True)
    ap.add_argument("--domain", default="commons")
    ap.add_argument("--tags", default="", help="comma-separated")
    ap.add_argument("--db-url", default=POSTGRES_URL)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    global SUBSTACK_DOMAIN, DOMAIN_TOKEN
    SUBSTACK_DOMAIN = args.substack_domain.strip().lower()
    DOMAIN_TOKEN = SUBSTACK_DOMAIN.split(".")[0]

    feed_slug = args.feed_slug
    author = args.author
    domain = args.domain
    tags = [t.strip() for t in args.tags.split(",") if t.strip()] or ["substack", feed_slug]

    posts = parse_corpus(args.corpus_path)
    print(f"[{feed_slug}] Corpus posts (url+content): {len(posts)}")

    conn = await asyncpg.connect(args.db_url)
    skip_log_path = Path(f"/tmp/{feed_slug}-ingest-skipped-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.log")
    try:
        existing_slugs = await fetch_existing_slugs(conn, feed_slug)
        print(f"[{feed_slug}] Existing slugs already in corpus: {len(existing_slugs)}")

        by_slug: Dict[str, Dict] = {}
        skipped_no_slug = []
        for p in posts:
            slug = canonical_slug(p["url"])
            if not slug:
                skipped_no_slug.append(p["url"])
                continue
            if slug in by_slug:
                if len(p.get("full_content", "")) > len(by_slug[slug].get("full_content", "")):
                    by_slug[slug] = p
            else:
                by_slug[slug] = p

        to_ingest = [(slug, post) for slug, post in by_slug.items() if slug not in existing_slugs]
        print(f"[{feed_slug}] Distinct slugs: {len(by_slug)} (skipped {len(skipped_no_slug)} no-slug); to ingest after dedup: {len(to_ingest)}")

        if args.limit:
            to_ingest = to_ingest[: args.limit]
            print(f"[{feed_slug}] --limit applied: {len(to_ingest)}")
        if not to_ingest:
            print(f"[{feed_slug}] Nothing to ingest.")
            return 0

        planned: List[Tuple[str, Dict, List[Dict]]] = []
        total_chunks = 0
        total_chars = 0
        for slug, post in to_ingest:
            chunks = chunk_text(post.get("full_content", ""))
            if not chunks:
                continue
            planned.append((slug, post, chunks))
            total_chunks += len(chunks)
            total_chars += sum(len(c["text"]) for c in chunks)

        est_tokens = total_chars // 4
        est_cost = (est_tokens / 1_000_000) * RATE_PER_MILLION
        print(f"[{feed_slug}] Planned: {len(planned)} posts → {total_chunks} chunks; ~{est_tokens:,} tokens, ~${est_cost:.4f}")
        if est_cost > COST_ABORT_USD:
            print(f"[{feed_slug}] ABORT: est ${est_cost:.4f} > ${COST_ABORT_USD}")
            return 3
        if args.dry_run:
            print(f"[{feed_slug}] Dry-run: no API/DB writes.  ({len(skipped_no_slug)} no-slug)")
            return 0

        if skipped_no_slug:
            with skip_log_path.open("w") as f:
                for u in skipped_no_slug:
                    f.write(f"NO_SLUG\t{u}\n")
            print(f"[{feed_slug}] Skipped-log: {skip_log_path}")

        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("ERROR: OPENAI_API_KEY not set", file=sys.stderr)
            return 2
        from openai import OpenAI
        client = OpenAI(api_key=api_key)

        inserted_posts = inserted_chunks = actual_tokens = 0
        t0 = time.time()
        for slug, post, chunks in planned:
            url = post["url"]
            title = post.get("title", "(untitled)")
            subtitle = post.get("subtitle", "")
            full_content = post.get("full_content", "")
            date = post.get("date", "") or None
            document_rid = f"substack-corpus:{feed_slug}:{slug}"
            parent_content = {"text": full_content[:1000], "title": title, "subtitle": subtitle, "url": url}
            parent_metadata = {
                "repo": REPO_NAME, "source_type": "substack_corpus", "feed_slug": feed_slug,
                "canonical_slug": slug, "url": url, "title": title, "author": author,
                "domain": domain, "tags": tags, "published_at": date,
            }
            embeddings: List[List[float]] = []
            for i in range(0, len(chunks), BATCH_SIZE):
                batch_chunks = chunks[i : i + BATCH_SIZE]
                texts = [c["text"] for c in batch_chunks]
                actual_tokens += sum(len(_TOKENIZER.encode(t)) for t in texts)
                if (actual_tokens / 1_000_000) * RATE_PER_MILLION > COST_ABORT_USD:
                    print(f"[{feed_slug}] ABORT mid-run: cost cap; stopped at {inserted_posts} posts")
                    return 3
                embeddings.extend(await embed_batch(client, texts))
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO koi_memories (rid, event_type, source_sensor, content, metadata)
                    VALUES ($1::text, 'NEW', $2::text, $3::jsonb, $4::jsonb)
                    ON CONFLICT (rid) DO NOTHING
                    """,
                    document_rid, SOURCE_SENSOR, json.dumps(parent_content), json.dumps(parent_metadata),
                )
                for chunk, emb in zip(chunks, embeddings):
                    chunk_rid = f"{document_rid}#chunk{chunk['index']}"
                    chunk_content = {"text": chunk["text"], "context": f"substack:{feed_slug}:{slug}"}
                    chunk_metadata = {
                        "repo": REPO_NAME, "source_type": "substack_corpus", "feed_slug": feed_slug,
                        "canonical_slug": slug, "url": url, "title": title, "author": author,
                        "domain": domain, "tags": tags,
                    }
                    await conn.execute(
                        """
                        INSERT INTO koi_memory_chunks
                            (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding_3072, metadata)
                        VALUES ($1::text, $2::text, $3::int, $4::int, $5::jsonb, $6::vector, $7::jsonb)
                        ON CONFLICT (chunk_rid) DO NOTHING
                        """,
                        chunk_rid, document_rid, chunk["index"], len(chunks),
                        json.dumps(chunk_content), str(emb), json.dumps(chunk_metadata),
                    )
                    inserted_chunks += 1
            inserted_posts += 1
            if inserted_posts % 20 == 0:
                print(f"[{feed_slug}]   {inserted_posts}/{len(planned)} posts, {inserted_chunks} chunks")

        final_cost = (actual_tokens / 1_000_000) * RATE_PER_MILLION
        print(f"[{feed_slug}] Done: {inserted_posts}/{len(planned)} posts, {inserted_chunks} chunks in {time.time()-t0:.1f}s; ${final_cost:.4f} ({actual_tokens:,} tok)")
        return 0
    finally:
        await conn.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
