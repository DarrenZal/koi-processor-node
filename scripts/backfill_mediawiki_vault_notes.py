#!/usr/bin/env python3
"""Backfill vault notes for MediaWiki-imported entities that lack .md files.

Usage:
    cd /path/to/koi-processor
    set -a && source config/personal.env && set +a
    venv/bin/python scripts/backfill_mediawiki_vault_notes.py \
        --wiki-url https://salishsearestoration.org \
        --parsed-dir data/mediawiki_parsed/ \
        --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import asyncpg

# Ensure the koi-processor root is on sys.path so `api.*` is importable.
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from api.entity_schema import type_to_folder, FOLDER_FALLBACKS
from api.vault_note_utils import (
    build_frontmatter,
    sanitize_filename,
    vault_note_path,
    vault_slug,
)
from api.vault_parser import PREDICATE_TO_FIELD, FIELD_TO_PREDICATE

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Slug helper (matches mediawiki_bulk_import.py / mediawiki_parse_dump.py)
# ---------------------------------------------------------------------------

_SLUG_UNSAFE_RE = re.compile(r"[^a-z0-9\s-]")
_SLUG_SPACE_RE = re.compile(r"[\s]+")


def _page_slug(title: str) -> str:
    s = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode().lower()
    s = _SLUG_UNSAFE_RE.sub("", s)
    s = _SLUG_SPACE_RE.sub("-", s.strip())
    return s or "untitled"


# ---------------------------------------------------------------------------
# DB connection (same pattern as mediawiki_bulk_import.py)
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
# Relationship → frontmatter wikilink builder
# ---------------------------------------------------------------------------

def _build_rel_fields(
    uri: str,
    rels: List[asyncpg.Record],
    uri_to_entity: Dict[str, Tuple[str, str]],
) -> Dict[str, List[str]]:
    """Build frontmatter relationship fields from entity_relationships rows.

    Returns a dict mapping field names to lists of wikilink strings, e.g.:
        {"broader": ["[[Concepts/Marine Ecology]]"], "related_to": ["[[Projects/Foo]]"]}
    """
    fields: Dict[str, List[str]] = defaultdict(list)

    for rel in rels:
        predicate = rel["predicate"]
        subject_uri = rel["subject_uri"]
        object_uri = rel["object_uri"]

        field_name = PREDICATE_TO_FIELD.get(predicate)
        if not field_name:
            continue

        # Determine the target entity (the "other" side of the relationship)
        if subject_uri == uri:
            target_uri = object_uri
        elif object_uri == uri:
            target_uri = subject_uri
        else:
            continue

        target_info = uri_to_entity.get(target_uri)
        if not target_info:
            continue

        target_name, target_type = target_info
        target_folder = type_to_folder(target_type) if target_type else "Concepts"
        safe_target = sanitize_filename(target_name)
        if not safe_target:
            continue

        wikilink = f"[[{target_folder}/{safe_target}]]"
        if wikilink not in fields[field_name]:
            fields[field_name].append(wikilink)

    return dict(fields)


# ---------------------------------------------------------------------------
# Main backfill
# ---------------------------------------------------------------------------

async def backfill(
    wiki_url: str,
    parsed_dir: Optional[str] = None,
    dry_run: bool = False,
):
    conn = await get_db_connection()

    vault_root = os.path.expanduser(
        os.environ.get("VAULT_PATH") or os.environ.get("OBSIDIAN_VAULT_PATH", "")
    )
    if not vault_root or not os.path.isdir(vault_root):
        logger.error(f"Vault root not found: {vault_root!r}")
        sys.exit(1)

    wiki_base = wiki_url.rstrip("/")

    # Load parsed page JSONs if provided (for lead section body text)
    parsed_pages: Dict[str, dict] = {}
    if parsed_dir and os.path.isdir(parsed_dir):
        for fname in os.listdir(parsed_dir):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(parsed_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                slug = _page_slug(data.get("title", fname[:-5]))
                parsed_pages[slug] = data
            except (json.JSONDecodeError, KeyError):
                continue
        logger.info(f"Loaded {len(parsed_pages)} parsed page JSONs from {parsed_dir}")

    try:
        # 1. Fetch all mediawiki_import entities
        rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry
            WHERE source = 'mediawiki_import'
            ORDER BY entity_text
        """)
        logger.info(f"Found {len(rows)} mediawiki_import entities in DB")

        if not rows:
            logger.info("Nothing to do.")
            return

        # Build URI → (name, type) lookup for wikilink generation
        uri_to_entity: Dict[str, Tuple[str, str]] = {}
        all_uris: List[str] = []
        for row in rows:
            uri = row["fuseki_uri"]
            uri_to_entity[uri] = (row["entity_text"], row["entity_type"] or "Concept")
            all_uris.append(uri)

        # Also include non-mediawiki entities so cross-source wikilinks work
        other_rows = await conn.fetch("""
            SELECT fuseki_uri, entity_text, entity_type
            FROM entity_registry
            WHERE source != 'mediawiki_import'
        """)
        for row in other_rows:
            uri_to_entity[row["fuseki_uri"]] = (
                row["entity_text"], row["entity_type"] or "Concept"
            )

        # 2. Batch-fetch relationships for all mediawiki entities (single query)
        rel_rows = await conn.fetch("""
            SELECT subject_uri, predicate, object_uri
            FROM entity_relationships
            WHERE subject_uri = ANY($1) OR object_uri = ANY($1)
        """, all_uris)
        logger.info(f"Fetched {len(rel_rows)} relationships")

        # Build per-entity relationship index
        rels_by_uri: Dict[str, List[asyncpg.Record]] = defaultdict(list)
        for rel in rel_rows:
            if rel["subject_uri"] in all_uris:
                rels_by_uri[rel["subject_uri"]].append(rel)
            if rel["object_uri"] in all_uris and rel["object_uri"] != rel["subject_uri"]:
                rels_by_uri[rel["object_uri"]].append(rel)

        # 3. Generate vault notes
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

            note_path_abs = vault_note_path(vault_root, folder, safe_name)
            if note_path_abs is None:
                logger.warning(f"  SKIP (path traversal): {name}")
                errors += 1
                continue

            vault_rel = os.path.relpath(note_path_abs, vault_root)
            v_rid = f"{folder.lower()}/{vault_slug(name)}"

            if os.path.exists(note_path_abs):
                skipped += 1
                continue

            # Build frontmatter
            wiki_title = name.replace(" ", "_")
            fm: Dict[str, any] = {
                "@type": f"bkc:{etype}",
                "name": name,
                "uri": uri,
                "source": "mediawiki_import",
                "wiki_url": f"{wiki_base}/wiki/{wiki_title}",
                "dateAccessed": str(date.today()),
            }

            # Add relationship wikilinks
            entity_rels = rels_by_uri.get(uri, [])
            if entity_rels:
                rel_fields = _build_rel_fields(uri, entity_rels, uri_to_entity)
                fm.update(rel_fields)

            content = build_frontmatter(fm)
            content += f"\n# {name}\n"

            # Append lead section text from parsed JSON if available
            lead_text = ""
            if parsed_pages:
                slug = _page_slug(name)
                page_data = parsed_pages.get(slug)
                if page_data:
                    # Try multiple possible locations for lead text
                    lead = (
                        page_data.get("lead_text")
                        or page_data.get("lead")
                        or (page_data.get("section_texts") or {}).get("lead")
                        or ""
                    )
                    if isinstance(lead, str) and lead.strip():
                        # Clean up wiki markup artifacts
                        lead_text = lead.strip()
                        # Remove image markup like "[[File:...]]" and "[[Image:...]]"
                        lead_text = re.sub(r'\[\[(?:File|Image):[^\]]+\]\]', '', lead_text)
                        # Remove image alignment/size prefixes like "left|400px|caption..."
                        # These appear as: alignment|sizepx|caption text followed by actual content
                        lead_text = re.sub(r'(?:left|right|center)\|\d+px\|[^)]*\)', '', lead_text)
                        # Remove Category:... tags
                        lead_text = re.sub(r'Category:[^\s]+', '', lead_text)
                        # Remove leftover wiki formatting
                        lead_text = re.sub(r"'{2,3}", '', lead_text)  # bold/italic wiki markup
                        lead_text = lead_text.strip()
                        if lead_text:
                            content += f"\n{lead_text}\n"

            # Render relationships as body content (visible in Quartz)
            entity_rels_for_body = rels_by_uri.get(uri, [])
            if entity_rels_for_body:
                rel_lines = []
                for rel in entity_rels_for_body:
                    predicate = rel["predicate"]
                    if rel["subject_uri"] == uri:
                        target_uri = rel["object_uri"]
                    elif rel["object_uri"] == uri:
                        target_uri = rel["subject_uri"]
                    else:
                        continue
                    target_info = uri_to_entity.get(target_uri)
                    if not target_info:
                        continue
                    target_name, target_type = target_info
                    target_folder = type_to_folder(target_type) if target_type else "Concepts"
                    safe_target = sanitize_filename(target_name)
                    if not safe_target:
                        continue
                    display_pred = predicate.replace("_", " ")
                    rel_lines.append(f"- {display_pred}: [[{target_folder}/{safe_target}]]")

                if rel_lines:
                    content += "\n## Relationships\n\n"
                    # Deduplicate and sort
                    seen = set()
                    for line in sorted(rel_lines):
                        if line not in seen:
                            content += line + "\n"
                            seen.add(line)

            # Add wiki source link
            content += f"\n---\n*Source: [{name} on Salish Sea Wiki]({wiki_base}/wiki/{wiki_title})*\n"

            if dry_run:
                logger.info(f"  WOULD CREATE: {vault_rel}")
                created += 1
                continue

            os.makedirs(os.path.dirname(note_path_abs), exist_ok=True)
            with open(note_path_abs, "w", encoding="utf-8") as f:
                f.write(content)
            logger.info(f"  CREATED: {vault_rel}")
            created += 1

            # Upsert RID mapping
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


def main():
    parser = argparse.ArgumentParser(
        description="Backfill vault notes for MediaWiki-imported entities"
    )
    parser.add_argument(
        "--wiki-url", required=True,
        help="Base wiki URL (e.g. https://salishsearestoration.org)",
    )
    parser.add_argument(
        "--parsed-dir",
        help="Directory of per-page JSON files for lead section text",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without writing files or DB changes",
    )
    args = parser.parse_args()
    asyncio.run(backfill(
        wiki_url=args.wiki_url,
        parsed_dir=args.parsed_dir,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
