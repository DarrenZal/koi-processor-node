#!/usr/bin/env python3
"""
Stamp a baseline manifest into the koi_migrations registry table.

Reads a baseline JSON manifest, verifies sha256 checksums against the actual
migration files, and upserts entries into the koi_migrations table.

A manifest is JSON with a "migrations" list; each entry has "id", "file" and
"checksum" (sha256 of the file), and an optional "note".

Usage:
    python scripts/stamp_baseline.py \
        --manifest <baseline manifest>.json \
        --db-url postgresql://localhost:5432/personal_koi

    python scripts/stamp_baseline.py \
        --manifest <baseline manifest>.json \
        --db-url postgresql://localhost:5432/<another node database> \
        --dry-run
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path


def compute_sha256(filepath: Path) -> str:
    return hashlib.sha256(filepath.read_bytes()).hexdigest()


def load_manifest(manifest_path: Path) -> dict:
    with open(manifest_path) as f:
        return json.load(f)


def verify_checksums(
    manifest: dict, migrations_dirs: list[Path]
) -> list[dict]:
    """Verify each migration file's checksum. Returns list of verified entries.

    Searches through multiple migrations directories in order, so a node-specific
    manifest can find files in that node's own migrations dir while core files come
    from the canonical one.
    """
    entries = []
    errors = []

    for entry in manifest["migrations"]:
        migration_id = entry["id"]
        filename = entry["file"]
        expected_checksum = entry["checksum"]

        # Search across all provided migration directories
        filepath = None
        for d in migrations_dirs:
            candidate = d / filename
            if candidate.exists():
                filepath = candidate
                break

        if filepath is None:
            # For core invariants sourced from another repo, skip file verification
            if entry.get("note") and "sourced from" in entry.get("note", ""):
                print(f"  SKIP  {migration_id} -- file not local ({entry['note']})")
                entries.append({
                    "id": migration_id,
                    "checksum": expected_checksum,
                    "verified": False,
                })
                continue
            searched = ", ".join(str(d) for d in migrations_dirs)
            errors.append(f"  MISSING  {migration_id} -- {filename} not found in: {searched}")
            continue

        actual_checksum = compute_sha256(filepath)
        if actual_checksum != expected_checksum:
            errors.append(
                f"  MISMATCH  {migration_id}\n"
                f"    expected: {expected_checksum}\n"
                f"    actual:   {actual_checksum}"
            )
            continue

        entries.append({
            "id": migration_id,
            "checksum": expected_checksum,
            "verified": True,
        })
        print(f"  OK  {migration_id}")

    if errors:
        print("\nChecksum verification errors:")
        for err in errors:
            print(err)
        sys.exit(1)

    return entries


def stamp_database(entries: list[dict], db_url: str, dry_run: bool):
    """Upsert verified entries into koi_migrations table."""
    if dry_run:
        print(f"\n[DRY RUN] Would stamp {len(entries)} migrations into {db_url}:")
        for e in entries:
            status = "verified" if e["verified"] else "unverified (remote file)"
            print(f"  {e['id']}  ({status})")
        return

    try:
        import psycopg2
    except ImportError:
        print("Error: psycopg2 not installed. Install with: pip install psycopg2-binary")
        sys.exit(1)

    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()

        # Ensure the table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS koi_migrations (
                migration_id TEXT PRIMARY KEY,
                applied_at TIMESTAMPTZ DEFAULT NOW(),
                checksum TEXT NOT NULL
            )
        """)

        stamped = 0
        updated = 0
        unchanged = 0

        for entry in entries:
            cur.execute(
                """
                INSERT INTO koi_migrations (migration_id, checksum)
                VALUES (%s, %s)
                ON CONFLICT (migration_id) DO UPDATE
                    SET checksum = EXCLUDED.checksum,
                        applied_at = NOW()
                RETURNING (xmax = 0) AS inserted
                """,
                (entry["id"], entry["checksum"]),
            )
            row = cur.fetchone()
            if row[0]:
                stamped += 1
            else:
                updated += 1

        conn.commit()
        print(f"\nStamped: {stamped} new, {updated} updated")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Stamp a baseline migration manifest into koi_migrations"
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Path to baseline manifest JSON file",
    )
    parser.add_argument(
        "--db-url",
        required=True,
        help="PostgreSQL connection string (e.g. postgresql://localhost:5432/personal_koi)",
    )
    parser.add_argument(
        "--migrations-dir",
        action="append",
        default=None,
        help="Directory containing migration SQL files (repeatable; default: manifest's grandparent). "
        "For BKC manifests, pass both canonical and Octo dirs: "
        "--migrations-dir /path/to/canonical/migrations --migrations-dir /path/to/octo/migrations",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be stamped without writing to database",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest).resolve()
    if not manifest_path.exists():
        print(f"Error: manifest not found: {manifest_path}")
        sys.exit(1)

    manifest = load_manifest(manifest_path)
    node = manifest.get("node", "unknown")
    print(f"Manifest: {manifest_path.name}  (node: {node})")
    print(f"Migrations: {len(manifest['migrations'])}")

    # Default migrations dir is the parent of baselines/
    if args.migrations_dir:
        migrations_dirs = [Path(d).resolve() for d in args.migrations_dir]
    else:
        migrations_dirs = [manifest_path.parent.parent]
    print(f"Looking for files in: {', '.join(str(d) for d in migrations_dirs)}\n")

    entries = verify_checksums(manifest, migrations_dirs)
    stamp_database(entries, args.db_url, args.dry_run)


if __name__ == "__main__":
    main()
