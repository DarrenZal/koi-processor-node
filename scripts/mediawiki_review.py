#!/usr/bin/env python3
"""
MediaWiki review/promotion CLI: triage staged and quarantined pages,
inspect individual pages, and promote them into the KOI knowledge graph.

Subcommands:
    list           Show staged/quarantined pages with filters
    inspect        Full details for a single page
    promote        Promote a single page to entity
    bulk-promote   Promote pages matching a filter

Usage:
    # List staged source_only pages sorted by promotion priority
    python scripts/mediawiki_review.py list --class source_only --sort promotion_priority --limit 50

    # List quarantined pages
    python scripts/mediawiki_review.py list --class quarantined

    # Inspect a specific page
    python scripts/mediawiki_review.py inspect --title "Chehalis Basin" --parsed-dir data/mediawiki_parsed/

    # Promote a single page
    python scripts/mediawiki_review.py promote --title "Chehalis Basin" --type Location \
        --parsed-dir data/mediawiki_parsed/ --run-id review-2026-03-07

    # Bulk promote with dry-run preview
    python scripts/mediawiki_review.py bulk-promote --type Concept \
        --min-words 200 --parsed-dir data/mediawiki_parsed/ --run-id review-2026-03-07 --dry-run

Environment variables:
    DB_HOST     (default: localhost)
    DB_PORT     (default: 5432)
    DB_NAME     (required, or set POSTGRES_URL)
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
from typing import Any, Dict, List, Optional

import asyncpg

# Ensure the koi-processor root is on sys.path so `api.*` is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from scripts.mediawiki_bulk_import import (
    get_db_connection,
    load_page_json,
    read_manifest,
    upsert_wiki,
    create_import_run,
    finalize_import_run,
)
from api.mediawiki_ingest import (
    process_entity_bearing_page,
    upsert_page_state,
    store_page_links,
    _title_to_slug,
)

logger = logging.getLogger("mediawiki_review")


# ---------------------------------------------------------------------------
# Startup guard — verify migration 083 is applied
# ---------------------------------------------------------------------------

async def _check_resolver_present(conn: asyncpg.Connection) -> None:
    fn = await conn.fetchval(
        "SELECT 1 FROM pg_proc WHERE proname = 'mediawiki_resolve_redirect'")
    vw = await conn.fetchval(
        "SELECT 1 FROM pg_views WHERE viewname = 'v_mediawiki_page_resolved'")
    if not (fn and vw):
        print("ERROR: scripts/mediawiki_review.py requires migration "
              "083_mediawiki_redirect_resolver.sql to be applied.\n"
              "Run: psql -v ON_ERROR_STOP=1 -d personal_koi -f "
              "migrations/083_mediawiki_redirect_resolver.sql",
              file=sys.stderr)
        sys.exit(2)


# ---------------------------------------------------------------------------
# Wiki-id resolution — auto-detect for single-wiki installs
# ---------------------------------------------------------------------------

async def _resolve_wiki_id(conn: asyncpg.Connection, explicit_wiki_id: Optional[int]) -> int:
    if explicit_wiki_id is not None:
        return explicit_wiki_id
    rows = await conn.fetch("SELECT id FROM mediawiki_wikis LIMIT 2")
    if len(rows) == 1:
        return rows[0]["id"]
    print(f"ERROR: --wiki-id is required (found {len(rows)} wikis)",
          file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# list subcommand
# ---------------------------------------------------------------------------

async def cmd_list(args: argparse.Namespace) -> None:
    conn = await get_db_connection(args)
    try:
        conditions = ["1=1"]
        params: list = []
        idx = 1

        if args.page_class == "quarantined":
            conditions.append(f"status = ${idx}")
            params.append("quarantined")
            idx += 1
        elif args.page_class:
            conditions.append(f"page_class = ${idx}")
            params.append(args.page_class)
            idx += 1
            # Default: show staged pages (not already ingested)
            conditions.append(f"status IN ('staged', 'pending')")

        if args.status:
            conditions.append(f"status = ${idx}")
            params.append(args.status)
            idx += 1

        if args.template_type:
            conditions.append(f"template_type = ${idx}")
            params.append(args.template_type)
            idx += 1

        if args.min_words:
            conditions.append(f"word_count >= ${idx}")
            params.append(args.min_words)
            idx += 1

        sort_col = "promotion_priority"
        sort_dir = "DESC"
        if args.sort:
            if args.sort in ("title", "word_count", "wikilink_count",
                             "promotion_priority", "ingest_confidence",
                             "template_type"):
                sort_col = args.sort
                sort_dir = "ASC" if args.sort == "title" else "DESC"

        limit_clause = f"LIMIT ${idx}" if args.limit else ""
        if args.limit:
            params.append(args.limit)

        where = " AND ".join(conditions)
        query = f"""
            SELECT title, page_class, bkc_entity_type, template_type,
                   word_count, wikilink_count, promotion_priority, status
            FROM mediawiki_page_state
            WHERE {where}
            ORDER BY {sort_col} {sort_dir}
            {limit_clause}
        """

        rows = await conn.fetch(query, *params)

        if not rows:
            print("No matching pages found.")
            return

        # Print header
        fmt = "{:<50s} {:>6s} {:>5s} {:>8s} {:>12s} {:>8s} {:>10s}"
        print(fmt.format("TITLE", "WORDS", "LINKS", "CLASS", "TEMPLATE", "PRIORITY", "STATUS"))
        print("-" * 105)
        for r in rows:
            title = r["title"][:49]
            print(fmt.format(
                title,
                str(r["word_count"] or 0),
                str(r["wikilink_count"] or 0),
                (r["page_class"] or "")[:8],
                (r["template_type"] or "")[:12],
                f"{r['promotion_priority'] or 0:.2f}",
                (r["status"] or "")[:10],
            ))

        print(f"\n{len(rows)} pages shown.")

    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# inspect subcommand
# ---------------------------------------------------------------------------

async def cmd_inspect(args: argparse.Namespace) -> None:
    # Load page JSON
    slug_seen: Dict[str, int] = {}
    page_data = load_page_json(args.parsed_dir, args.title, slug_seen)
    if page_data is None:
        print(f"Page JSON not found for '{args.title}' in {args.parsed_dir}")
        sys.exit(1)

    print(f"=== {page_data['title']} ===")
    print(f"Page ID:          {page_data.get('page_id', 'N/A')}")
    print(f"Page class:       {page_data.get('page_class', 'N/A')}")
    print(f"BKC entity type:  {page_data.get('bkc_entity_type', 'N/A')}")
    print(f"Template type:    {page_data.get('template_type', 'N/A')}")
    print(f"Word count:       {page_data.get('word_count', 0)}")
    print(f"Ingest confidence:{page_data.get('ingest_confidence', 0):.3f}")
    print(f"Promo priority:   {page_data.get('promotion_priority', 0):.3f}")
    print(f"Source RID:       {page_data.get('source_rid', 'N/A')}")

    # Sections
    sections = page_data.get("sections", [])
    if sections:
        print(f"\nSections ({len(sections)}):")
        for s in sections:
            if isinstance(s, dict):
                print(f"  - {s.get('heading', '(untitled)')} (level {s.get('level', '?')})")
            else:
                print(f"  - {s}")

    # Edges
    structural = page_data.get("structural_edges", [])
    editorial = page_data.get("editorial_edges", [])
    print(f"\nStructural edges: {len(structural)}")
    print(f"Editorial edges:  {len(editorial)}")

    # Top wikilinks
    wikilinks = page_data.get("wikilinks", [])
    if wikilinks:
        print(f"\nTop {min(10, len(wikilinks))} wikilinks:")
        for wl in wikilinks[:10]:
            if isinstance(wl, dict):
                print(f"  - {wl.get('target', wl)}")
            else:
                print(f"  - {wl}")

    # Aliases
    aliases = page_data.get("aliases", [])
    if aliases:
        print(f"\nAliases: {', '.join(aliases)}")

    # DB state (if connected) — resolver-aware
    try:
        conn = await get_db_connection(args)
        try:
            wiki_id = await _resolve_wiki_id(conn, getattr(args, 'wiki_id', None))

            resolved = await conn.fetchrow("""
                SELECT id, title, is_redirect, redirect_target,
                       canonical_id, canonical_title, hops, resolution_status
                FROM v_mediawiki_page_resolved
                WHERE title = $1 AND wiki_id = $2
                ORDER BY id DESC LIMIT 1
            """, args.title, wiki_id)

            if not resolved:
                print(f"\n(No DB state found for this title)")
            else:
                # Print redirect resolution info
                if resolved["is_redirect"]:
                    if resolved["canonical_id"] is not None:
                        hop_s = "hop" if resolved["hops"] == 1 else "hops"
                        print(f"\nResolved '{args.title}' → '{resolved['canonical_title']}' "
                              f"({resolved['hops']} {hop_s})")
                    else:
                        print(f"\nWarning: redirect target '{resolved['redirect_target']}' "
                              f"not found ({resolved['resolution_status']}) "
                              f"— showing redirect row only")

                # Fetch full detail from the canonical row (or self if non-redirect)
                detail_id = resolved["canonical_id"] if resolved["canonical_id"] else resolved["id"]
                row = await conn.fetchrow("""
                    SELECT status, review_status, entity_uri, last_run_id, ingested_at
                    FROM mediawiki_page_state
                    WHERE id = $1
                """, detail_id)
                if row:
                    print(f"\n--- DB State ---")
                    print(f"Status:           {row['status']}")
                    print(f"Review status:    {row['review_status']}")
                    print(f"Entity URI:       {row['entity_uri'] or '(none)'}")
                    print(f"Last run:         {row['last_run_id'] or '(none)'}")
                    print(f"Ingested at:      {row['ingested_at'] or '(never)'}")
        finally:
            await conn.close()
    except SystemExit:
        raise
    except Exception:
        print(f"\n(Could not connect to DB for state lookup)")


# ---------------------------------------------------------------------------
# promote subcommand
# ---------------------------------------------------------------------------

async def cmd_promote(args: argparse.Namespace) -> None:
    slug_seen: Dict[str, int] = {}
    page_data = load_page_json(args.parsed_dir, args.title, slug_seen)
    if page_data is None:
        print(f"Page JSON not found for '{args.title}' in {args.parsed_dir}")
        sys.exit(1)

    # Override type and class
    page_data["bkc_entity_type"] = args.type
    page_data["page_class"] = "entity_bearing"

    conn = await get_db_connection(args)
    try:
        wiki_id = await _resolve_wiki_id(conn, getattr(args, 'wiki_id', None))

        # Resolve through redirects — promote always targets the canonical page
        resolved = await conn.fetchrow("""
            SELECT canonical_id, resolution_status
            FROM v_mediawiki_page_resolved
            WHERE title = $1 AND wiki_id = $2
            ORDER BY id DESC LIMIT 1
        """, args.title, wiki_id)

        if not resolved:
            print(f"Page '{args.title}' not found in mediawiki_page_state.")
            sys.exit(1)

        if resolved["canonical_id"] is None:
            print(f"Cannot promote '{args.title}' — resolution_status="
                  f"{resolved['resolution_status']}", file=sys.stderr)
            sys.exit(1)

        # Get or create page_state using the canonical id
        ps_row = await conn.fetchrow(
            "SELECT id FROM mediawiki_page_state WHERE id = $1", resolved["canonical_id"]
        )
        if ps_row:
            page_state_id = ps_row["id"]
        else:
            # Upsert page state from the JSON
            page_state_id, _ = await upsert_page_state(conn, wiki_id, page_data)
            await store_page_links(conn, wiki_id, page_state_id, page_data)

        # Create/reuse import run
        run_db_id = await create_import_run(
            conn, args.run_id, wiki_id, "review-promote", None
        )

        # Open log file
        os.makedirs(args.log_dir, exist_ok=True)
        log_path = os.path.join(args.log_dir, "mediawiki_review_log.jsonl")
        log_file = open(log_path, "a", encoding="utf-8")

        try:
            counters = await process_entity_bearing_page(
                conn, page_data, page_state_id, wiki_id, args.run_id, log_file
            )
        finally:
            log_file.close()

        await finalize_import_run(
            conn, args.run_id, 1,
            counters["entities_created"],
            counters["entities_matched"],
            counters["edges_promoted"],
        )

        print(f"\n=== Promoted: {args.title} ===")
        print(f"Type:              {args.type}")
        print(f"Run ID:            {args.run_id}")
        print(f"Entities created:  {counters['entities_created']}")
        print(f"Entities matched:  {counters['entities_matched']}")
        print(f"Edges promoted:    {counters['edges_promoted']}")
        print(f"Log:               {log_path}")

    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# bulk-promote subcommand
# ---------------------------------------------------------------------------

async def cmd_bulk_promote(args: argparse.Namespace) -> None:
    conn = await get_db_connection(args)
    try:
        # Query matching pages
        conditions = ["page_class = 'source_only'", "status IN ('staged', 'pending')"]
        params: list = []
        idx = 1

        if args.min_words:
            conditions.append(f"word_count >= ${idx}")
            params.append(args.min_words)
            idx += 1

        if args.template_type:
            conditions.append(f"template_type = ${idx}")
            params.append(args.template_type)
            idx += 1

        if args.min_priority:
            conditions.append(f"promotion_priority >= ${idx}")
            params.append(args.min_priority)
            idx += 1

        where = " AND ".join(conditions)
        limit_clause = ""
        if args.limit:
            limit_clause = f"LIMIT ${idx}"
            params.append(args.limit)

        rows = await conn.fetch(f"""
            SELECT id, title, page_id, word_count, promotion_priority
            FROM mediawiki_page_state
            WHERE {where}
            ORDER BY promotion_priority DESC
            {limit_clause}
        """, *params)

        if not rows:
            print("No matching pages found.")
            return

        print(f"Found {len(rows)} pages matching filter.")
        if args.dry_run:
            print(f"\n=== DRY RUN (--type {args.type}) ===")
            for r in rows:
                print(f"  {r['promotion_priority']:.2f}  {r['word_count']:>6d}w  {r['title']}")
            print(f"\n{len(rows)} pages would be promoted as {args.type}.")
            return

        # Get wiki_id
        wiki_row = await conn.fetchrow("SELECT id FROM mediawiki_wikis LIMIT 1")
        if not wiki_row:
            print("No wiki registered in mediawiki_wikis. Run a bulk import first.")
            sys.exit(1)
        wiki_id = wiki_row["id"]

        # Create import run
        run_db_id = await create_import_run(
            conn, args.run_id, wiki_id, "review-bulk-promote", len(rows)
        )

        # Open log file
        os.makedirs(args.log_dir, exist_ok=True)
        log_path = os.path.join(args.log_dir, "mediawiki_review_log.jsonl")
        log_file = open(log_path, "a", encoding="utf-8")

        totals = {"pages": 0, "entities_created": 0, "entities_matched": 0,
                  "edges_promoted": 0, "errors": 0}
        slug_seen: Dict[str, int] = {}

        for r in rows:
            title = r["title"]
            page_data = load_page_json(args.parsed_dir, title, slug_seen)
            if page_data is None:
                logger.warning(f"Skipping '{title}': page JSON not found")
                totals["errors"] += 1
                continue

            # Override type and class
            page_data["bkc_entity_type"] = args.type
            page_data["page_class"] = "entity_bearing"

            page_state_id = r["id"]

            tx = conn.transaction()
            await tx.start()
            try:
                # Ensure page links are stored
                await store_page_links(conn, wiki_id, page_state_id, page_data)

                counters = await process_entity_bearing_page(
                    conn, page_data, page_state_id, wiki_id, args.run_id, log_file
                )
                totals["entities_created"] += counters["entities_created"]
                totals["entities_matched"] += counters["entities_matched"]
                totals["edges_promoted"] += counters["edges_promoted"]
                totals["pages"] += 1
                await tx.commit()
                logger.info(f"Promoted: {title} (+{counters['entities_created']} entities, +{counters['edges_promoted']} edges)")
            except Exception as e:
                await tx.rollback()
                totals["errors"] += 1
                logger.error(f"Failed to promote '{title}': {e}")

        log_file.close()

        status = "completed" if totals["errors"] == 0 else "completed_with_errors"
        await finalize_import_run(
            conn, args.run_id, totals["pages"],
            totals["entities_created"], totals["entities_matched"],
            totals["edges_promoted"], status,
        )

        print(f"\n=== Bulk Promote Complete ===")
        print(f"Run ID:            {args.run_id}")
        print(f"Type:              {args.type}")
        print(f"Status:            {status}")
        print(f"Pages promoted:    {totals['pages']}")
        print(f"Entities created:  {totals['entities_created']}")
        print(f"Entities matched:  {totals['entities_matched']}")
        print(f"Edges promoted:    {totals['edges_promoted']}")
        print(f"Errors:            {totals['errors']}")
        print(f"Log:               {log_path}")

    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Review and promote staged/quarantined MediaWiki pages."
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- list ---
    p_list = subparsers.add_parser("list", help="Show staged/quarantined pages")
    p_list.add_argument("--class", dest="page_class", default=None,
                        help="Filter by page_class (source_only, entity_bearing, quarantined)")
    p_list.add_argument("--status", default=None,
                        help="Filter by status (staged, pending, quarantined, ingested)")
    p_list.add_argument("--template-type", default=None,
                        help="Filter by template_type")
    p_list.add_argument("--min-words", type=int, default=None,
                        help="Minimum word count")
    p_list.add_argument("--sort", default="promotion_priority",
                        help="Sort column (promotion_priority, word_count, title, wikilink_count)")
    p_list.add_argument("--limit", type=int, default=50,
                        help="Max rows to show (default: 50)")

    # --- inspect ---
    p_inspect = subparsers.add_parser("inspect", help="Full details for one page")
    p_inspect.add_argument("--title", required=True, help="Page title")
    p_inspect.add_argument("--parsed-dir", required=True,
                           help="Directory with per-page JSON files")
    p_inspect.add_argument("--wiki-id", type=int, default=None,
                           help="Wiki ID (auto-detected if only one wiki exists)")

    # --- promote ---
    p_promote = subparsers.add_parser("promote", help="Promote a single page to entity")
    p_promote.add_argument("--title", required=True, help="Page title")
    p_promote.add_argument("--type", required=True,
                           help="BKC entity type (e.g. Location, Concept, Project)")
    p_promote.add_argument("--parsed-dir", required=True,
                           help="Directory with per-page JSON files")
    p_promote.add_argument("--run-id", required=True,
                           help="Import run identifier")
    p_promote.add_argument("--log-dir", default="data",
                           help="Directory for review log JSONL (default: data/)")
    p_promote.add_argument("--wiki-id", type=int, default=None,
                           help="Wiki ID (auto-detected if only one wiki exists)")

    # --- bulk-promote ---
    p_bulk = subparsers.add_parser("bulk-promote",
                                   help="Promote pages matching a filter")
    p_bulk.add_argument("--type", required=True,
                        help="BKC entity type to assign")
    p_bulk.add_argument("--min-words", type=int, default=None,
                        help="Minimum word count filter")
    p_bulk.add_argument("--template-type", default=None,
                        help="Filter by template_type")
    p_bulk.add_argument("--min-priority", type=float, default=None,
                        help="Minimum promotion_priority filter")
    p_bulk.add_argument("--limit", type=int, default=None,
                        help="Max pages to promote")
    p_bulk.add_argument("--parsed-dir", required=True,
                        help="Directory with per-page JSON files")
    p_bulk.add_argument("--run-id", required=True,
                        help="Import run identifier")
    p_bulk.add_argument("--log-dir", default="data",
                        help="Directory for review log JSONL (default: data/)")
    p_bulk.add_argument("--dry-run", action="store_true",
                        help="Preview without DB changes")

    return parser


async def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s  %(message)s",
    )

    # Startup guard — verify migration 083 before any command runs.
    # Commands that use the resolver (inspect, promote) need this;
    # run it for all commands so the error is caught early.
    if args.command in ("inspect", "promote"):
        conn = await get_db_connection(args)
        try:
            await _check_resolver_present(conn)
        finally:
            await conn.close()

    dispatch = {
        "list": cmd_list,
        "inspect": cmd_inspect,
        "promote": cmd_promote,
        "bulk-promote": cmd_bulk_promote,
    }
    await dispatch[args.command](args)


if __name__ == "__main__":
    asyncio.run(main())
