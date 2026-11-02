#!/usr/bin/env python3
"""Walk a live MediaWiki's API to produce a complete JSON export of a namespace.

Output shape (consumed by `api.mediawiki_parser.parse_json_export`):
    { "<title>": { "content": "<wikitext>", "pageid": <int>, "timestamp": "<iso>" }, ... }

Used when XML dumps are partial/unavailable. Walks `list=allpages` with continuation,
fetches page content in batches of 50. Respects 1 req/s rate limit.

No database connection. Idempotent resume: if --output exists, loads it, only fetches
page IDs not already present.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import aiohttp

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.mediawiki_api import MediaWikiClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def list_all_pageids(session: aiohttp.ClientSession, api_url: str,
                           namespace: int, request_delay: float) -> List[Dict]:
    """Walk action=query&list=allpages via apcontinue. Returns [{pageid, title, ns}, ...]."""
    out: List[Dict] = []
    apcontinue = None
    while True:
        params = {
            "action": "query",
            "list": "allpages",
            "apnamespace": str(namespace),
            "aplimit": "500",
            "format": "json",
        }
        if apcontinue:
            params["apcontinue"] = apcontinue
        async with session.get(api_url, params=params) as resp:
            resp.raise_for_status()
            data = await resp.json()
        pages = data.get("query", {}).get("allpages", [])
        out.extend(pages)
        cont = data.get("continue", {})
        apcontinue = cont.get("apcontinue")
        logger.info(f"allpages: {len(out)} titles enumerated (latest={pages[-1]['title'] if pages else '<none>'})")
        if not apcontinue:
            break
        await asyncio.sleep(request_delay)
    return out


async def export(api_url: str, output_path: str, namespace: int,
                 batch_size: int, request_delay: float, limit: int | None):
    # Resume support
    existing: Dict[str, dict] = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
            logger.info(f"Resume: {len(existing)} titles already in {output_path}")
        except Exception as e:
            logger.warning(f"Could not load existing output ({e}) — starting fresh")
            existing = {}

    async with aiohttp.ClientSession(
        headers={"User-Agent": "Mozilla/5.0 (compatible; p2pfoundation-koi/1.0; +mailto:maintainer@example.com)"}
    ) as session:
        all_pages = await list_all_pageids(session, api_url, namespace, request_delay)
        logger.info(f"Total {len(all_pages)} pages in namespace {namespace}")

        if limit:
            all_pages = all_pages[:limit]
            logger.info(f"--limit applied: fetching only {len(all_pages)}")

        # Drop already-fetched titles
        to_fetch_ids = [p["pageid"] for p in all_pages if p["title"] not in existing]
        logger.info(f"{len(to_fetch_ids)} pages need content (resume skipped {len(all_pages) - len(to_fetch_ids)})")

        client = MediaWikiClient(api_url=api_url, request_delay=request_delay)
        try:
            for i in range(0, len(to_fetch_ids), batch_size):
                batch = to_fetch_ids[i:i + batch_size]
                pages = await client.fetch_page_batch(batch)
                for pc in pages:
                    existing[pc.title] = {
                        "content": pc.wikitext,
                        "pageid": pc.pageid,
                        "timestamp": pc.timestamp,
                    }
                if (i // batch_size) % 5 == 0 or i + batch_size >= len(to_fetch_ids):
                    # Persist every 5 batches + final
                    tmp = output_path + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(existing, f, ensure_ascii=False)
                    os.replace(tmp, output_path)
                    logger.info(f"Progress: {len(existing)} titles written "
                                f"(batch {i//batch_size + 1}/{(len(to_fetch_ids)+batch_size-1)//batch_size})")
        finally:
            await client.close()

    logger.info(f"Done: {len(existing)} titles in {output_path}")


def main():
    ap = argparse.ArgumentParser(description="Live-API full export of a MediaWiki namespace to JSON.")
    ap.add_argument("--wiki-url", required=True, help="Wiki base URL (e.g. https://wiki.p2pfoundation.net)")
    ap.add_argument("--output", required=True, help="Path to output JSON file (idempotent resume)")
    ap.add_argument("--namespace", type=int, default=0, help="MediaWiki namespace (default: 0 = Main)")
    ap.add_argument("--batch-size", type=int, default=50, help="Pageids per fetch_page_batch call")
    ap.add_argument("--request-delay", type=float, default=1.0, help="Seconds between API calls")
    ap.add_argument("--limit", type=int, default=None, help="Stop after N pages (smoke-test)")
    args = ap.parse_args()

    api_url = args.wiki_url.rstrip("/") + "/api.php"
    Path(os.path.dirname(os.path.abspath(args.output))).mkdir(parents=True, exist_ok=True)

    asyncio.run(export(api_url, args.output, args.namespace, args.batch_size,
                       args.request_delay, args.limit))


if __name__ == "__main__":
    main()
