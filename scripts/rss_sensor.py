#!/usr/bin/env python3
"""RSS discourse sensor for personal-koi.

Polls configured RSS/Atom feeds, embeds title+summary text, and stores items in
koi_memories + koi_memory_chunks under metadata.repo='discourse-rss'.
"""

import argparse
import asyncio
import hashlib
import html
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import asyncpg
import feedparser

# Cloudflare and similar gateways block default Python user-agents.
# Use a browser-like UA on every feedparser request + the urlopen fallback.
# 2026-04-30: failed feeds with default UA: docs.anthropic.com claude-code rss,
# blog.langchain.com, www.cognition.ai/blog. All worked after this change.
_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
feedparser.USER_AGENT = _BROWSER_UA
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from api.chunker import TextChunker
from api.embedding_provider import create_embedding_provider

POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://koi:@localhost:5432/personal_koi",
)
STATE_DIR = Path(os.getenv("RSS_SENSOR_STATE_DIR", "/tmp/rss-sensor"))
SOURCE_SENSOR = "discourse-rss"
REPO_NAME = "discourse-rss"
MAX_CHUNKS_PER_ITEM = 4

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("rss_sensor")
INVALID_XML_CHARS = re.compile(
    "[\x00-\x08\x0b\x0c\x0e-\x1f]"
)


def _strip_html(value: str) -> str:
    value = re.sub(r"<[^>]+>", " ", value or "")
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def _published_at(entry: Any) -> Optional[datetime]:
    for attr in ("published_parsed", "updated_parsed", "created_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:6], tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
    for attr in ("published", "updated", "created"):
        s = getattr(entry, attr, None)
        if s:
            try:
                dt = parsedate_to_datetime(s)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except (TypeError, ValueError):
                continue
    return None


def _entry_value(entry: Any, *names: str) -> str:
    for name in names:
        value = getattr(entry, name, None)
        if value:
            return str(value)
    return ""


def _stable_document_rid(feed_url: str, feed_slug: str, entry: Any, title: str, published: Optional[datetime]) -> Optional[str]:
    # Prefer canonical links where present. Some feeds emit repeated guid/id
    # values across entries; link avoids collapsing distinct posts.
    raw_id = _entry_value(entry, "link", "id", "guid")
    if not raw_id:
        published_key = published.isoformat() if published else _entry_value(entry, "published", "updated")
        if title and published_key:
            raw_id = f"{title}|{published_key}"
        elif title:
            raw_id = f"{title}|{_entry_value(entry, 'link') or _strip_html(_entry_value(entry, 'summary'))[:100]}"
    if not raw_id:
        return None
    digest = hashlib.sha256(f"{feed_url}|{raw_id}".encode("utf-8")).hexdigest()[:16]
    return f"{SOURCE_SENSOR}:{feed_slug}:{digest}"


def _parse_feed(url: str) -> Any:
    parsed = feedparser.parse(url)
    if parsed.entries or not getattr(parsed, "bozo", False):
        return parsed

    req = Request(url, headers={"User-Agent": _BROWSER_UA})
    with urlopen(req, timeout=30) as resp:
        body = resp.read()
    text = body.decode("utf-8", errors="replace")
    text = INVALID_XML_CHARS.sub("", text)
    reparsed = feedparser.parse(text)
    if reparsed.entries or not getattr(reparsed, "bozo", False):
        logger.info("Recovered feed after XML sanitization: %s", url)
        return reparsed
    return parsed


def _load_feeds(path: Path, feed_url: Optional[str]) -> List[Dict[str, Any]]:
    # Which feeds to poll is personal config (config/rss_feeds.yaml, gitignored).
    # Fall back to the committed template so a fresh fork still runs.
    if not path.exists():
        example = path.with_name(path.stem + ".example" + path.suffix)
        if example.exists():
            logger.warning("No personal %s — using %s. Copy it to %s and add your feeds.",
                           path.name, example.name, path.name)
            path = example
    with path.open() as f:
        data = yaml.safe_load(f) or {}
    feeds = [feed for feed in data.get("feeds", []) if feed.get("enabled", True)]
    if feed_url:
        feeds = [feed for feed in feeds if feed.get("url") == feed_url]
    for feed in feeds:
        if not feed.get("url") or not feed.get("slug"):
            raise ValueError(f"Invalid feed config: {feed}")
    return feeds


async def _insert_item(
    conn: asyncpg.Connection,
    provider: Any,
    chunker: TextChunker,
    feed: Dict[str, Any],
    entry: Any,
    dry_run: bool,
) -> str:
    feed_url = feed["url"]
    feed_slug = feed["slug"]
    title = _strip_html(_entry_value(entry, "title")) or "(untitled)"
    summary = _strip_html(_entry_value(entry, "summary", "description"))
    link = _entry_value(entry, "link")
    author = _entry_value(entry, "author")
    published = _published_at(entry)
    document_rid = _stable_document_rid(feed_url, feed_slug, entry, title, published)
    if not document_rid:
        return "skipped_no_id"

    exists = await conn.fetchval("SELECT 1 FROM koi_memories WHERE rid = $1", document_rid)
    if exists:
        return "skipped_idempotent"

    item_text = f"{title}\n\n{summary}".strip()
    if not item_text:
        return "skipped_no_id"

    chunks = chunker.chunk_text(item_text)[:MAX_CHUNKS_PER_ITEM]
    if not chunks:
        return "skipped_no_id"

    if dry_run:
        logger.info("DRY-RUN would insert %s (%s chunks): %s", document_rid, len(chunks), title)
        return "inserted"

    embeddings: List[List[float]] = []
    for chunk in chunks:
        embedding = await provider.embed(chunk["text"])
        if not embedding:
            raise RuntimeError("embedding provider returned empty vector")
        if len(embedding) != 3072:
            raise RuntimeError(f"expected 3072-dim embedding, got {len(embedding)}")
        embeddings.append(embedding)

    published_iso = published.isoformat() if published else None
    tags = list(feed.get("tags") or [])
    for tag in getattr(entry, "tags", []) or []:
        term = getattr(tag, "term", None)
        if term and term not in tags:
            tags.append(term)

    parent_content = {
        "text": item_text,
        "title": title,
        "summary": summary,
        "link": link,
    }
    parent_metadata = {
        "feed_url": feed_url,
        "feed_slug": feed_slug,
        "link": link,
        "domain": feed.get("domain", "other"),
        "tags": tags,
        "author": author,
        "published_at": published_iso,
    }
    async with conn.transaction():
        await conn.execute(
            """
            INSERT INTO koi_memories
                (rid, event_type, source_sensor, content, metadata)
            VALUES ($1, 'NEW', $2, $3::jsonb, $4::jsonb)
            ON CONFLICT (rid) DO NOTHING
            """,
            document_rid,
            SOURCE_SENSOR,
            json.dumps(parent_content),
            json.dumps(parent_metadata),
        )

        for chunk, embedding in zip(chunks, embeddings):
            chunk_index = int(chunk["index"])
            chunk_rid = f"{document_rid}:{chunk_index}"
            chunk_content = {
                "text": chunk["text"],
                "context": f"discourse:{feed_slug}",
            }
            chunk_metadata = {
                "repo": REPO_NAME,
                "source_type": "discourse_rss",
                "feed_url": feed_url,
                "feed_slug": feed_slug,
                "title": title,
                "link": link,
                "author": author,
                "published_at": published_iso,
                "domain": feed.get("domain", "other"),
                "tags": tags,
            }
            await conn.execute(
                """
                INSERT INTO koi_memory_chunks
                    (chunk_rid, document_rid, chunk_index, total_chunks, content, embedding_3072, metadata)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::vector, $7::jsonb)
                ON CONFLICT (chunk_rid) DO NOTHING
                """,
                chunk_rid,
                document_rid,
                chunk_index,
                len(chunks),
                json.dumps(chunk_content),
                json.dumps(embedding),
                json.dumps(chunk_metadata),
            )
    return "inserted"


def _write_last_run(summary: Dict[str, Any]) -> None:
    if summary.get("feeds_succeeded", 0) < 1:
        return
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = STATE_DIR / ".last-run"
    now = datetime.now(timezone.utc).isoformat()
    path.write_text(f"{now}\n{json.dumps(summary, sort_keys=True)}\n")


async def run(args: argparse.Namespace) -> Dict[str, Any]:
    feeds = _load_feeds(Path(args.feeds_file), args.feed_url)
    provider = None
    if not args.dry_run:
        provider = create_embedding_provider()
        if provider is None:
            logger.error("Embedding provider not configured")
            raise SystemExit(4)

    summary: Dict[str, Any] = {
        "partial": False,
        "feeds_total": len(feeds),
        "feeds_succeeded": 0,
        "feeds_failed": 0,
        "items_inserted": 0,
        "items_skipped_idempotent": 0,
        "items_skipped_no_id": 0,
        "items_failed": 0,
        "failed_feeds": [],
    }
    chunker = TextChunker(chunk_size=500, chunk_overlap=50, min_chunk_size=20)

    try:
        conn = await asyncpg.connect(POSTGRES_URL)
    except Exception as e:
        logger.error("DB unreachable: %s", e)
        raise SystemExit(3)

    try:
        for feed in feeds:
            url = feed["url"]
            try:
                parsed = _parse_feed(url)
                if getattr(parsed, "bozo", False) and not parsed.entries:
                    raise RuntimeError(getattr(parsed, "bozo_exception", "feed parse failed"))
                summary["feeds_succeeded"] += 1
                for entry in parsed.entries:
                    try:
                        status = await _insert_item(conn, provider, chunker, feed, entry, args.dry_run)
                        if status == "inserted":
                            summary["items_inserted"] += 1
                        elif status == "skipped_idempotent":
                            summary["items_skipped_idempotent"] += 1
                        elif status == "skipped_no_id":
                            summary["items_skipped_no_id"] += 1
                    except Exception as e:
                        logger.warning("Item failed feed=%s title=%r: %s", url, _entry_value(entry, "title"), e)
                        summary["items_failed"] += 1
            except Exception as e:
                logger.warning("Feed failed %s: %s", url, e)
                summary["feeds_failed"] += 1
                summary["failed_feeds"].append(url)
    finally:
        await conn.close()

    summary["partial"] = summary["feeds_failed"] > 0 or summary["items_failed"] > 0
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll RSS feeds into personal-koi")
    parser.add_argument("--once", action="store_true", default=True)
    parser.add_argument("--feeds-file", default=str(Path(__file__).parent.parent / "config" / "rss_feeds.yaml"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--feed-url", help="Poll only one configured feed URL")
    args = parser.parse_args()

    try:
        summary = asyncio.run(run(args))
    except SystemExit as e:
        return int(e.code)
    except Exception as e:
        logger.error("Configuration/load error: %s", e)
        return 4

    print(json.dumps(summary, indent=2, sort_keys=True))
    if not args.dry_run and summary["feeds_succeeded"] >= 1:
        _write_last_run(summary)
    if summary["feeds_succeeded"] == 0 and summary["feeds_total"] > 0:
        return 2
    return 2 if summary["partial"] else 0


if __name__ == "__main__":
    sys.exit(main())
