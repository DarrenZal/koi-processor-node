#!/usr/bin/env python3
"""
MediaWiki bulk importer: reads parsed page JSON + manifest, resolves entities
against the KOI knowledge graph, and promotes edges into entity_relationships.

Usage:
    # Dry run
    python scripts/mediawiki_bulk_import.py \
        --parsed-dir data/mediawiki_parsed/ \
        --manifest data/mediawiki_manifest.jsonl \
        --wiki-url https://salishsearestoration.org \
        --dry-run

    # Pilot (top 50 by promotion priority)
    python scripts/mediawiki_bulk_import.py \
        --parsed-dir data/mediawiki_parsed/ \
        --manifest data/mediawiki_manifest.jsonl \
        --wiki-url https://salishsearestoration.org \
        --limit 50 \
        --run-id pilot-2026-03-06

    # Full import
    python scripts/mediawiki_bulk_import.py \
        --parsed-dir data/mediawiki_parsed/ \
        --manifest data/mediawiki_manifest.jsonl \
        --wiki-url https://salishsearestoration.org \
        --run-id full-2026-03-08

Environment variables:
    DB_HOST     (default: localhost)
    DB_PORT     (default: 5432)
    DB_NAME     (required)
    DB_USER     (default: postgres)
    DB_PASSWORD (default: empty string)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import asyncpg

# Ensure the koi-processor root is on sys.path so `api.*` is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.mediawiki_parser import PARSER_VERSION
from api.mediawiki_ingest import (
    upsert_page_state,
    store_page_links,
    register_redirect_alias,
    process_entity_bearing_page,
    _write_log,
    _title_to_slug,
    BATCH_SIZE,
    HIGH_DEGREE_THRESHOLD,
)

logger = logging.getLogger("mediawiki_bulk_import")


# ---------------------------------------------------------------------------
# Manifest reader
# ---------------------------------------------------------------------------

def read_manifest(manifest_path: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Read the manifest JSONL (already sorted by promotion_priority desc)."""
    rows: List[Dict[str, Any]] = []
    with open(manifest_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if limit is not None and limit > 0:
        rows = rows[:limit]
    return rows


def load_page_json(parsed_dir: str, title: str, slug_seen: Dict[str, int]) -> Optional[Dict[str, Any]]:
    """Load the per-page JSON file matching a title. Handles slug collisions."""
    slug = _title_to_slug(title)
    if slug in slug_seen:
        slug_seen[slug] += 1
        slug = f"{slug}-{slug_seen[slug]}"
    else:
        slug_seen[slug] = 1

    page_path = os.path.join(parsed_dir, f"{slug}.json")
    if not os.path.exists(page_path):
        logger.warning(f"Page JSON not found: {page_path}")
        return None
    with open(page_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

async def get_db_connection(args: argparse.Namespace) -> asyncpg.Connection:
    # Prefer POSTGRES_URL (used on production servers)
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
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
    )


async def verify_schema(conn: asyncpg.Connection) -> None:
    """Verify mediawiki_page_state table exists."""
    exists = await conn.fetchval("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = 'mediawiki_page_state'
        )
    """)
    if not exists:
        raise RuntimeError(
            "mediawiki_page_state table does not exist. "
            "Run migration 063_mediawiki_import.sql first."
        )


async def upsert_wiki(conn: asyncpg.Connection, wiki_url: str) -> int:
    """Upsert wiki in mediawiki_wikis, return wiki_id."""
    # Derive api_url from wiki_url
    api_url = wiki_url.rstrip("/") + "/api.php"
    row = await conn.fetchrow("""
        INSERT INTO mediawiki_wikis (base_url, api_url, wiki_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (base_url) DO UPDATE SET api_url = EXCLUDED.api_url
        RETURNING id
    """, wiki_url.rstrip("/"), api_url, wiki_url.rstrip("/").split("//")[-1])
    return row["id"]


async def create_import_run(
    conn: asyncpg.Connection,
    run_id: str,
    wiki_id: int,
    mode: str,
    page_limit: Optional[int],
) -> int:
    """Create an import run record, return its DB id."""
    row = await conn.fetchrow("""
        INSERT INTO mediawiki_import_runs (run_id, wiki_id, parse_version, mode, page_limit)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (run_id) DO UPDATE SET
            status = 'running',
            started_at = NOW(),
            pages_processed = 0,
            entities_created = 0,
            entities_matched = 0,
            edges_promoted = 0,
            finished_at = NULL
        RETURNING id
    """, run_id, wiki_id, PARSER_VERSION, mode, page_limit)
    return row["id"]


async def finalize_import_run(
    conn: asyncpg.Connection,
    run_id: str,
    pages_processed: int,
    entities_created: int,
    entities_matched: int,
    edges_promoted: int,
    status: str = "completed",
) -> None:
    await conn.execute("""
        UPDATE mediawiki_import_runs
        SET pages_processed = $2,
            entities_created = $3,
            entities_matched = $4,
            edges_promoted = $5,
            status = $6,
            finished_at = NOW()
        WHERE run_id = $1
    """, run_id, pages_processed, entities_created, entities_matched, edges_promoted, status)


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def dry_run(manifest: List[Dict[str, Any]], parsed_dir: str) -> None:
    """Print what the import would do without touching the DB."""
    entity_bearing = [m for m in manifest if m.get("page_class") == "entity_bearing"
                      and m.get("ingest_confidence", 0) >= 0.6]
    source_only = [m for m in manifest if m.get("page_class") == "source_only"]
    alias_only = [m for m in manifest if m.get("page_class") == "alias_only"]
    below_threshold = [m for m in manifest if m.get("page_class") == "entity_bearing"
                       and m.get("ingest_confidence", 0) < 0.6]

    print(f"=== DRY RUN ===")
    print(f"Parse version: {PARSER_VERSION}")
    print(f"Total pages in manifest: {len(manifest)}")
    print()
    print(f"Will process:")
    print(f"  entity_bearing (conf >= 0.6):  {len(entity_bearing)}")
    print(f"  entity_bearing (conf < 0.6):   {len(below_threshold)}  (skipped)")
    print(f"  source_only:                   {len(source_only)}  (staged)")
    print(f"  alias_only:                    {len(alias_only)}  (redirect aliases)")
    print()

    # Count edges
    slug_seen: Dict[str, int] = {}
    total_structural = 0
    total_editorial = 0
    quarantine_candidates = 0

    for m in manifest:
        page_data = load_page_json(parsed_dir, m["title"], slug_seen)
        if page_data is None:
            continue
        structural = page_data.get("structural_edges", [])
        editorial = page_data.get("editorial_edges", [])
        total_structural += len(structural)
        total_editorial += len(editorial)

        weak = sum(1 for s in structural if s["confidence"] < 0.85) + len(editorial)
        if weak > HIGH_DEGREE_THRESHOLD:
            quarantine_candidates += 1

    print(f"Candidate edges:")
    print(f"  structural:  {total_structural}")
    print(f"  editorial:   {total_editorial}")
    print(f"  total:       {total_structural + total_editorial}")
    print()
    print(f"High-degree quarantine candidates (>{HIGH_DEGREE_THRESHOLD} weak edges): {quarantine_candidates}")
    print()

    if entity_bearing:
        print(f"Top 10 by promotion priority:")
        for m in entity_bearing[:10]:
            print(f"  {m['promotion_priority']:.2f}  {m['title']}")


# ---------------------------------------------------------------------------
# Main async flow
# ---------------------------------------------------------------------------

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk import parsed MediaWiki pages into KOI knowledge graph."
    )
    parser.add_argument(
        "--parsed-dir", required=True,
        help="Directory containing per-page JSON files.",
    )
    parser.add_argument(
        "--manifest", required=True,
        help="Path to the manifest JSONL file (sorted by promotion_priority).",
    )
    parser.add_argument(
        "--wiki-url", required=True,
        help="Base URL of the wiki (e.g. https://salishsearestoration.org).",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Process only the top N pages from manifest.",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Import run identifier. Required for non-dry-run.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview what would happen without touching the DB.",
    )
    parser.add_argument(
        "--log-dir", default="data",
        help="Directory for import log JSONL (default: data/).",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable debug logging.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    )

    # Read manifest
    manifest = read_manifest(args.manifest, args.limit)
    logger.info(f"Loaded {len(manifest)} pages from manifest (limit={args.limit})")

    if args.dry_run:
        dry_run(manifest, args.parsed_dir)
        return

    if not args.run_id:
        logger.error("--run-id is required for non-dry-run imports")
        sys.exit(1)

    # Connect to DB
    conn = await get_db_connection(args)
    try:
        await verify_schema(conn)
        logger.info("Schema verified: mediawiki_page_state table exists")

        # Upsert wiki
        wiki_id = await upsert_wiki(conn, args.wiki_url)
        logger.info(f"Wiki registered: id={wiki_id}, url={args.wiki_url}")

        # Create import run
        mode = f"limit-{args.limit}" if args.limit else "full"
        run_db_id = await create_import_run(conn, args.run_id, wiki_id, mode, args.limit)
        logger.info(f"Import run created: run_id={args.run_id}, db_id={run_db_id}")

        # Open log file
        os.makedirs(args.log_dir, exist_ok=True)
        log_path = os.path.join(args.log_dir, "mediawiki_import_log.jsonl")
        log_file = open(log_path, "a", encoding="utf-8")

        # Process in batches
        totals = {
            "pages_processed": 0,
            "pages_skipped": 0,
            "entities_created": 0,
            "entities_matched": 0,
            "edges_promoted": 0,
            "errors": 0,
        }

        slug_seen: Dict[str, int] = {}
        batches = [manifest[i:i + BATCH_SIZE] for i in range(0, len(manifest), BATCH_SIZE)]

        for batch_idx, batch in enumerate(batches):
            logger.info(f"Batch {batch_idx + 1}/{len(batches)} ({len(batch)} pages)")

            tx = conn.transaction()
            await tx.start()
            try:
                for manifest_row in batch:
                    title = manifest_row["title"]
                    page_data = load_page_json(args.parsed_dir, title, slug_seen)
                    if page_data is None:
                        logger.warning(f"Skipping '{title}': page JSON not found")
                        totals["errors"] += 1
                        continue

                    page_class = page_data.get("page_class", "source_only")
                    ingest_confidence = page_data.get("ingest_confidence", 0.0)

                    # Upsert page state
                    page_state_id, was_skipped = await upsert_page_state(
                        conn, wiki_id, page_data
                    )

                    if was_skipped:
                        totals["pages_skipped"] += 1
                        logger.debug(f"Skipped (unchanged): {title}")
                        continue

                    # Store source-native links
                    link_count = await store_page_links(conn, wiki_id, page_state_id, page_data)
                    logger.debug(f"Stored {link_count} page links for '{title}'")

                    # Route by page class
                    if page_class == "entity_bearing" and ingest_confidence >= 0.6:
                        page_counters = await process_entity_bearing_page(
                            conn, page_data, page_state_id, wiki_id, args.run_id, log_file,
                        )
                        totals["entities_created"] += page_counters["entities_created"]
                        totals["entities_matched"] += page_counters["entities_matched"]
                        totals["edges_promoted"] += page_counters["edges_promoted"]

                    elif page_class == "source_only":
                        await conn.execute("""
                            UPDATE mediawiki_page_state
                            SET status = 'staged', last_run_id = $2
                            WHERE id = $1
                        """, page_state_id, args.run_id)
                        _write_log(log_file, page_data.get("source_rid", ""),
                                   title, None, None, None, "staged", None, args.run_id)

                    elif page_class == "alias_only":
                        redirect_target = page_data.get("redirect_target")
                        if redirect_target:
                            await register_redirect_alias(conn, title, redirect_target)
                        await conn.execute("""
                            UPDATE mediawiki_page_state
                            SET status = 'skipped', last_run_id = $2
                            WHERE id = $1
                        """, page_state_id, args.run_id)
                        _write_log(log_file, page_data.get("source_rid", ""),
                                   title, None, None, None, "alias_registered", None, args.run_id)

                    else:
                        # entity_bearing but below threshold
                        await conn.execute("""
                            UPDATE mediawiki_page_state
                            SET status = 'staged', last_run_id = $2
                            WHERE id = $1
                        """, page_state_id, args.run_id)

                    totals["pages_processed"] += 1

                await tx.commit()
                logger.info(
                    f"  Batch {batch_idx + 1} committed: "
                    f"+{sum(1 for _ in batch)} pages"
                )

            except Exception as e:
                await tx.rollback()
                totals["errors"] += len(batch)
                logger.error(f"  Batch {batch_idx + 1} ROLLED BACK: {e}")

        log_file.close()

        # Finalize run
        status = "completed" if totals["errors"] == 0 else "completed_with_errors"
        await finalize_import_run(
            conn, args.run_id,
            totals["pages_processed"],
            totals["entities_created"],
            totals["entities_matched"],
            totals["edges_promoted"],
            status,
        )

        # Print summary
        print()
        print(f"=== Import Complete ===")
        print(f"Run ID:            {args.run_id}")
        print(f"Status:            {status}")
        print(f"Pages processed:   {totals['pages_processed']}")
        print(f"Pages skipped:     {totals['pages_skipped']}  (content_hash unchanged)")
        print(f"Entities created:  {totals['entities_created']}")
        print(f"Entities matched:  {totals['entities_matched']}")
        print(f"Edges promoted:    {totals['edges_promoted']}")
        print(f"Errors:            {totals['errors']}")
        print(f"Import log:        {log_path}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
