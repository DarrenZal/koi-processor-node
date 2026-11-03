#!/usr/bin/env python3
"""Backfill vault notes for existing web_ingest entities that lack .md files.

Usage:
    cd <koi-processor checkout>
    venv/bin/python -m api.scripts.backfill_vault_notes            # dry run (the default)
    venv/bin/python -m api.scripts.backfill_vault_notes --apply    # write notes and RID mappings

Without --apply (or with --dry-run, which wins) it lists the notes it would create,
reads the database in a read-only session and writes nothing. --apply writes the
missing notes under VAULT_PATH (or OBSIDIAN_VAULT_PATH) and upserts
entity_rid_mappings. The database is POSTGRES_URL, or DB_HOST/DB_PORT/DB_NAME/DB_USER/
DB_PASSWORD (default octo_koi on 127.0.0.1:5432).
"""

import argparse
import asyncio
import logging
import os
import re
import sys
from datetime import date

import asyncpg

from api.entity_schema import type_to_folder
from api.vault_note_utils import (
    build_frontmatter,
    sanitize_filename,
    vault_note_path,
    vault_slug,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


async def backfill(dry_run: bool = False):
    # Support both POSTGRES_URL (production) and individual DB_* vars (dev)
    postgres_url = os.environ.get("POSTGRES_URL")
    if not postgres_url:
        db_name = os.environ.get("DB_NAME", "octo_koi")
        db_user = os.environ.get("DB_USER", "postgres")
        db_pass = os.environ.get("DB_PASSWORD", "postgres")
        db_host = os.environ.get("DB_HOST", "127.0.0.1")
        db_port = int(os.environ.get("DB_PORT", "5432"))
        postgres_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    vault_root = os.path.expanduser(
        os.environ.get("VAULT_PATH") or os.environ.get("OBSIDIAN_VAULT_PATH", "")
    )
    if not vault_root or not os.path.isdir(vault_root):
        logger.error(f"Vault root not found: {vault_root!r}")
        sys.exit(1)

    logger.info(f"Database: {mask_url(postgres_url)}  Vault: {vault_root}")
    if dry_run:
        conn = await asyncpg.connect(
            postgres_url, server_settings={"default_transaction_read_only": "on"})
    else:
        conn = await asyncpg.connect(postgres_url)

    try:
        rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry
            WHERE source = 'web_ingest'
            ORDER BY entity_text
        """)
        logger.info(f"Found {len(rows)} web_ingest entities in DB")

        created = 0
        skipped = 0
        errors = 0
        rid_mapped = 0

        for row in rows:
            name = row["entity_text"]
            etype = row["entity_type"] or "Concept"
            uri = row["fuseki_uri"]

            folder = type_to_folder(etype)
            safe_name = sanitize_filename(name)
            if safe_name is None:
                logger.warning(f"  SKIP (bad name): {name!r}")
                errors += 1
                continue

            note_path = vault_note_path(vault_root, folder, safe_name)
            if note_path is None:
                logger.warning(f"  SKIP (path traversal): {name}")
                errors += 1
                continue

            vault_rel = os.path.relpath(note_path, vault_root)
            v_rid = f"{folder.lower()}/{vault_slug(name)}"

            if os.path.exists(note_path):
                logger.info(f"  EXISTS: {vault_rel}")
                skipped += 1
            else:
                if dry_run:
                    logger.info(f"  WOULD CREATE: {vault_rel}")
                else:
                    fm = {
                        "@type": f"bkc:{etype}",
                        "name": name,
                        "uri": uri,
                        "source": "web_ingest",
                        "dateAccessed": str(date.today()),
                    }
                    content = build_frontmatter(fm)
                    content += f"\n# {name}\n"

                    os.makedirs(os.path.dirname(note_path), exist_ok=True)
                    with open(note_path, "w", encoding="utf-8") as f:
                        f.write(content)
                    logger.info(f"  CREATED: {vault_rel}")
                created += 1

            # Upsert RID mapping
            if not dry_run:
                tag = await conn.execute("""
                    INSERT INTO entity_rid_mappings (
                        vault_rid, vault_path, canonical_uri, entity_type,
                        name, sync_status, last_synced
                    ) VALUES ($1, $2, $3, $4, $5, 'linked', NOW())
                    ON CONFLICT (vault_rid) DO UPDATE SET
                        vault_path = EXCLUDED.vault_path,
                        canonical_uri = EXCLUDED.canonical_uri,
                        entity_type = EXCLUDED.entity_type,
                        name = EXCLUDED.name,
                        sync_status = 'linked',
                        last_synced = NOW()
                    WHERE entity_rid_mappings.canonical_uri = EXCLUDED.canonical_uri
                """, v_rid, vault_rel, uri, etype, name)
                if tag.endswith(" 0"):
                    logger.warning(f"  RID collision: {v_rid} maps to different URI, skipped")
                else:
                    rid_mapped += 1

        logger.info(
            f"Done: {created} created, {skipped} skipped (exist), "
            f"{rid_mapped} RID mappings, {errors} errors"
        )
    finally:
        await conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Backfill vault notes for web_ingest entities",
        epilog="Without --apply this is a dry run: it lists the notes it would create and "
               "writes nothing.")
    parser.add_argument("--apply", action="store_true",
                        help="write the missing vault notes and upsert entity_rid_mappings")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing files (the default; wins over --apply)")
    args = parser.parse_args(argv)
    asyncio.run(backfill(dry_run=args.dry_run or not args.apply))


if __name__ == "__main__":
    main()
