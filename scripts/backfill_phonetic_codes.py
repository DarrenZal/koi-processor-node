#!/usr/bin/env python3
"""
Backfill Phonetic Codes for Entity Registry

This script backfills phonetic_code for entity types that have
phonetic_matching=true in their schema configuration.

Key features:
- Only backfills for schema-enabled types (opt-in)
- Uses per-type stopwords from schema
- Clears stale codes for types that toggled to phonetic=false
- Safe to run multiple times (idempotent)

Usage:
    python scripts/backfill_phonetic_codes.py [--apply] [--dry-run] [--clear-stale] [--stats]

Options:
    --apply       Write the codes (UPDATE entity_registry; CREATE INDEX if missing)
    --dry-run     Show what would be done without making changes (the default;
                  wins over --apply)
    --clear-stale Clear phonetic_code for types with phonetic_matching=false
                  (with --apply; otherwise only reported)
    --stats       Show current statistics only (read-only)

Without --apply it reads in a read-only session and changes nothing. The database is
POSTGRES_URL (default postgresql://localhost:5432/personal_koi, connecting as the
current OS user).
"""

import os
import re
import sys
import argparse
import asyncio
import asyncpg
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from metaphone import doublemetaphone
from api.entity_schema import (
    get_entity_schemas,
    get_schema_for_type,
    get_first_significant_token,
    get_phonetic_enabled_types,
    reload_entity_schemas,
)

# Database URL (no user in the default: the driver then uses the current OS user)
DB_URL = os.getenv('POSTGRES_URL', 'postgresql://localhost:5432/personal_koi')
READ_ONLY = {'default_transaction_read_only': 'on'}


def mask_url(url):
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def get_phonetic_code(text: str) -> str | None:
    """Get Double Metaphone code for text."""
    if not text:
        return None
    codes = doublemetaphone(text)
    return codes[0] if codes[0] else codes[1]


async def backfill_phonetic_codes(dry_run: bool = False, clear_stale: bool = False):
    """
    Backfill phonetic_code for entities with phonetic_matching enabled.

    Args:
        dry_run: If True, show what would be done without making changes
        clear_stale: If True, clear phonetic_code for types with phonetic=false
    """
    # Reload schemas to get latest from vault
    vault_path = os.environ.get('VAULT_PATH', os.path.expanduser('~/Documents/Notes'))
    schemas = reload_entity_schemas(vault_path)

    phonetic_types = get_phonetic_enabled_types()
    all_types = list(schemas.keys())
    non_phonetic_types = [t for t in all_types if t not in phonetic_types]

    print(f"Loaded {len(schemas)} entity schemas from vault")
    print(f"Phonetic-enabled types: {phonetic_types}")
    print(f"Non-phonetic types: {non_phonetic_types}")
    print(f"Database: {mask_url(DB_URL)}{' (dry run, read-only)' if dry_run else ''}")
    print()

    if dry_run:
        conn = await asyncpg.connect(DB_URL, server_settings=READ_ONLY)
    else:
        conn = await asyncpg.connect(DB_URL)

    try:
        # Step 1: Clear stale codes for non-phonetic types (if requested)
        if clear_stale and non_phonetic_types:
            print("=== Step 1: Clearing stale phonetic codes ===")
            for entity_type in non_phonetic_types:
                count = await conn.fetchval("""
                    SELECT COUNT(*) FROM entity_registry
                    WHERE entity_type = $1 AND phonetic_code IS NOT NULL
                """, entity_type)

                if count > 0:
                    print(f"  {entity_type}: {count} entities with stale phonetic codes")
                    if not dry_run:
                        await conn.execute("""
                            UPDATE entity_registry
                            SET phonetic_code = NULL
                            WHERE entity_type = $1 AND phonetic_code IS NOT NULL
                        """, entity_type)
                        print(f"    -> Cleared")
                    else:
                        print(f"    -> Would clear (dry-run)")
            print()

        # Step 2: Backfill for phonetic-enabled types
        print("=== Step 2: Backfilling phonetic codes ===")
        total_updated = 0

        for entity_type in phonetic_types:
            schema = get_schema_for_type(entity_type)
            stopwords = schema.phonetic_stopwords

            # Find entities missing phonetic_code
            rows = await conn.fetch("""
                SELECT id, normalized_text FROM entity_registry
                WHERE entity_type = $1 AND phonetic_code IS NULL
            """, entity_type)

            if not rows:
                print(f"  {entity_type}: No entities need backfill")
                continue

            print(f"  {entity_type}: {len(rows)} entities to backfill")
            print(f"    Stopwords: {stopwords or 'none'}")

            updated = 0
            for row in rows:
                # Get first significant token (skip stopwords)
                first_token = get_first_significant_token(row['normalized_text'], stopwords)
                phonetic_code = get_phonetic_code(first_token)

                if phonetic_code:
                    if not dry_run:
                        await conn.execute("""
                            UPDATE entity_registry
                            SET phonetic_code = $1
                            WHERE id = $2
                        """, phonetic_code, row['id'])
                    updated += 1

                    # Show first few examples
                    if updated <= 3:
                        print(f"      {row['normalized_text'][:40]} -> {first_token} -> {phonetic_code}")

            if updated > 3:
                print(f"      ... and {updated - 3} more")

            print(f"    -> {'Would update' if dry_run else 'Updated'} {updated} entities")
            total_updated += updated

        print()
        print(f"=== Summary ===")
        print(f"Total entities {'would be ' if dry_run else ''}updated: {total_updated}")

        # Step 3: Verify index exists
        print()
        print("=== Step 3: Verifying index ===")
        index_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE indexname = 'idx_entity_phonetic_code'
            )
        """)

        if index_exists:
            print("  Index idx_entity_phonetic_code: EXISTS")
        else:
            print("  Index idx_entity_phonetic_code: MISSING")
            if not dry_run:
                print("  Creating index...")
                await conn.execute("""
                    CREATE INDEX idx_entity_phonetic_code ON entity_registry(phonetic_code)
                    WHERE phonetic_code IS NOT NULL
                """)
                print("  -> Index created")
            else:
                print("  -> Would create index (dry-run)")

    finally:
        await conn.close()


async def show_stats():
    """Show current phonetic code statistics."""
    print(f"Database: {mask_url(DB_URL)}")
    conn = await asyncpg.connect(DB_URL, server_settings=READ_ONLY)

    try:
        print("=== Current Phonetic Code Statistics ===")
        print()

        # Get counts by type
        rows = await conn.fetch("""
            SELECT
                entity_type,
                COUNT(*) as total,
                COUNT(phonetic_code) as with_phonetic,
                COUNT(*) - COUNT(phonetic_code) as without_phonetic
            FROM entity_registry
            GROUP BY entity_type
            ORDER BY total DESC
        """)

        print(f"{'Type':<20} {'Total':<10} {'With Code':<12} {'Without':<10}")
        print("-" * 52)
        for row in rows:
            print(f"{row['entity_type'] or 'NULL':<20} {row['total']:<10} {row['with_phonetic']:<12} {row['without_phonetic']:<10}")

        print()

        # Show sample phonetic codes
        print("=== Sample Phonetic Codes ===")
        samples = await conn.fetch("""
            SELECT entity_type, entity_text, normalized_text, phonetic_code
            FROM entity_registry
            WHERE phonetic_code IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 10
        """)

        for s in samples:
            print(f"  {s['entity_type']}: {s['entity_text'][:30]} -> {s['phonetic_code']}")

    finally:
        await conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Backfill phonetic codes for entity registry',
        epilog='Without --apply this is a dry run: it reads in a read-only session and '
               'changes nothing.')
    parser.add_argument('--apply', action='store_true',
                        help='Write the codes (and clear stale ones with --clear-stale)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be done without making changes (the default; wins over --apply)')
    parser.add_argument('--clear-stale', action='store_true', help='Clear phonetic codes for types with phonetic=false')
    parser.add_argument('--stats', action='store_true', help='Show current statistics only')
    args = parser.parse_args(argv)

    if args.stats:
        asyncio.run(show_stats())
    else:
        asyncio.run(backfill_phonetic_codes(dry_run=args.dry_run or not args.apply,
                                            clear_stale=args.clear_stale))


if __name__ == '__main__':
    main()
