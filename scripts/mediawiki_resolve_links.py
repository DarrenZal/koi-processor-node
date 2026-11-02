#!/usr/bin/env python3
"""
Post-import cross-reference resolver: resolves mediawiki_page_links targets
against imported entities and creates missing entity_relationships edges.

Run this AFTER mediawiki_bulk_import.py to connect page links that now have
imported target entities on both sides.

Usage:
    python scripts/mediawiki_resolve_links.py --wiki-url https://salishsearestoration.org

Environment variables:
    POSTGRES_URL  (preferred)
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD  (fallback)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

import asyncpg

logger = logging.getLogger("mediawiki_resolve_links")


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


async def resolve_links(conn: asyncpg.Connection, wiki_id: int, dry_run: bool) -> dict:
    """Resolve unresolved page links against imported page entities.

    For each unresolved link:
    1. Look up normalized_target_title in mediawiki_page_state
    2. If target page was imported (has entity_uri) -> resolved
    3. If target page exists but not imported -> source_only_target
    4. If target is a redirect -> follow redirect -> redirect_resolved
    5. Otherwise -> leave as unresolved
    """
    counters = {
        "resolved": 0,
        "redirect_resolved": 0,
        "source_only_target": 0,
        "edges_created": 0,
        "already_resolved": 0,
        "no_target": 0,
        "errors": 0,
    }

    # Get all unresolved links for this wiki
    unresolved = await conn.fetch("""
        SELECT pl.id, pl.source_page_id, pl.target_title, pl.normalized_target_title,
               pl.predicate, pl.confidence, pl.edge_class,
               ps.entity_uri AS source_entity_uri, ps.source_rid AS source_page_rid
        FROM mediawiki_page_links pl
        JOIN mediawiki_page_state ps ON ps.id = pl.source_page_id
        WHERE pl.wiki_id = $1
        AND pl.resolution_status = 'unresolved'
        AND ps.entity_uri IS NOT NULL
    """, wiki_id)

    logger.info(f"Found {len(unresolved)} unresolved links with source entities")

    for link in unresolved:
        norm_target = link["normalized_target_title"]
        if not norm_target:
            counters["no_target"] += 1
            continue

        # Try direct match on normalized_title in page_state
        target_page = await conn.fetchrow("""
            SELECT id, entity_uri, status, is_redirect, redirect_target
            FROM mediawiki_page_state
            WHERE wiki_id = $1
            AND lower(normalized_title) = $2
            LIMIT 1
        """, wiki_id, norm_target)

        if target_page is None:
            # Try matching against title directly (case-insensitive)
            target_page = await conn.fetchrow("""
                SELECT id, entity_uri, status, is_redirect, redirect_target
                FROM mediawiki_page_state
                WHERE wiki_id = $1
                AND lower(title) = $2
                LIMIT 1
            """, wiki_id, norm_target)

        if target_page is None:
            counters["no_target"] += 1
            continue

        resolved_uri = None
        resolution_status = None

        if target_page["is_redirect"] and target_page["redirect_target"]:
            # Follow redirect chain (one level)
            redirect_norm = target_page["redirect_target"].lower().strip()
            redirect_page = await conn.fetchrow("""
                SELECT entity_uri FROM mediawiki_page_state
                WHERE wiki_id = $1
                AND (lower(normalized_title) = $2 OR lower(title) = $2)
                AND entity_uri IS NOT NULL
                LIMIT 1
            """, wiki_id, redirect_norm)

            if redirect_page:
                resolved_uri = redirect_page["entity_uri"]
                resolution_status = "redirect_resolved"
                counters["redirect_resolved"] += 1
            else:
                resolution_status = "source_only_target"
                counters["source_only_target"] += 1
        elif target_page["entity_uri"]:
            resolved_uri = target_page["entity_uri"]
            resolution_status = "resolved"
            counters["resolved"] += 1
        else:
            resolution_status = "source_only_target"
            counters["source_only_target"] += 1

        if dry_run:
            if resolved_uri:
                logger.debug(
                    f"Would resolve: {link['target_title']} -> {resolved_uri} "
                    f"({resolution_status})"
                )
            continue

        # Update link resolution status
        await conn.execute("""
            UPDATE mediawiki_page_links
            SET resolution_status = $2,
                resolved_target_uri = $3,
                target_match_confidence = $4
            WHERE id = $1
        """, link["id"], resolution_status, resolved_uri,
            1.0 if resolved_uri else None)

        # Create entity_relationship edge if both endpoints exist
        if resolved_uri and link["source_entity_uri"]:
            subject_uri = link["source_entity_uri"]
            if subject_uri == resolved_uri:
                continue  # Skip self-referential

            predicate = link["predicate"] or "related_to"

            # Verify predicate is allowed
            predicate_ok = await conn.fetchval("""
                SELECT EXISTS (SELECT 1 FROM allowed_predicates WHERE predicate = $1)
            """, predicate)
            if not predicate_ok:
                predicate = "related_to"
                predicate_ok = await conn.fetchval("""
                    SELECT EXISTS (SELECT 1 FROM allowed_predicates WHERE predicate = $1)
                """, predicate)
                if not predicate_ok:
                    continue

            # Check if edge already exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM entity_relationships
                    WHERE subject_uri = $1 AND predicate = $2 AND object_uri = $3
                )
            """, subject_uri, predicate, resolved_uri)

            if not exists:
                try:
                    await conn.execute("""
                        INSERT INTO entity_relationships
                            (subject_uri, predicate, object_uri, confidence, source, source_rid)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT (subject_uri, predicate, object_uri) DO NOTHING
                    """,
                        subject_uri,
                        predicate,
                        resolved_uri,
                        link["confidence"],
                        "mediawiki_import",
                        link["source_page_rid"],
                    )
                    counters["edges_created"] += 1
                except Exception as e:
                    logger.debug(f"Edge creation failed: {e}")
                    counters["errors"] += 1

    return counters


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Resolve cross-references in mediawiki_page_links after bulk import."
    )
    parser.add_argument(
        "--wiki-url", required=True,
        help="Base URL of the wiki (e.g. https://salishsearestoration.org).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview resolutions without writing to DB.",
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

    conn = await get_db_connection()
    try:
        # Look up wiki_id
        wiki_id = await conn.fetchval("""
            SELECT id FROM mediawiki_wikis WHERE base_url = $1
        """, args.wiki_url.rstrip("/"))

        if wiki_id is None:
            logger.error(f"Wiki not found: {args.wiki_url}")
            sys.exit(1)

        logger.info(f"Resolving links for wiki_id={wiki_id} ({args.wiki_url})")

        # Get pre-resolution stats
        pre_stats = await conn.fetch("""
            SELECT resolution_status, COUNT(*) AS cnt
            FROM mediawiki_page_links
            WHERE wiki_id = $1
            GROUP BY resolution_status
            ORDER BY cnt DESC
        """, wiki_id)
        logger.info("Pre-resolution link stats:")
        for row in pre_stats:
            logger.info(f"  {row['resolution_status']}: {row['cnt']}")

        counters = await resolve_links(conn, wiki_id, args.dry_run)

        # Get post-resolution stats
        post_stats = await conn.fetch("""
            SELECT resolution_status, COUNT(*) AS cnt
            FROM mediawiki_page_links
            WHERE wiki_id = $1
            GROUP BY resolution_status
            ORDER BY cnt DESC
        """, wiki_id)

        prefix = "[DRY RUN] " if args.dry_run else ""
        print()
        print(f"=== {prefix}Cross-Reference Resolution ===")
        print(f"Resolved (direct):        {counters['resolved']}")
        print(f"Resolved (redirect):      {counters['redirect_resolved']}")
        print(f"Source-only target:       {counters['source_only_target']}")
        print(f"No target page found:     {counters['no_target']}")
        print(f"New edges created:        {counters['edges_created']}")
        print(f"Errors:                   {counters['errors']}")
        print()
        print("Post-resolution link status:")
        for row in post_stats:
            print(f"  {row['resolution_status']}: {row['cnt']}")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
