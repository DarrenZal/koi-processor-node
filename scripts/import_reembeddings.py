#!/usr/bin/env python3
"""Import re-embedded vectors into personal_koi after they were computed elsewhere.

Reads JSONL files with {"id": ..., "embedding": [...]} and UPDATEs the
corresponding rows in the database.

Usage:
  python3 scripts/import_reembeddings.py --input-dir ./reembed_results [--db-url postgresql://...]
"""

import argparse
import asyncio
import json
import os
import time

import asyncpg


# Mapping: filename prefix -> (table, id_column, embedding_column)
TABLE_MAP = {
    "entity_registry": ("entity_registry", "id", "embedding"),
    "session_chunks": ("session_chunks", "id", "embedding"),
    "knowledge_facts": ("knowledge_facts", "id", "fact_embedding"),
    "koi_memory_chunks": ("koi_memory_chunks", "id", "embedding"),
}


async def import_file(conn, input_path: str, table: str, id_col: str, emb_col: str):
    """Import embeddings from JSONL into database table."""
    records = []
    with open(input_path) as f:
        for line in f:
            records.append(json.loads(line))

    if not records:
        print(f"  {os.path.basename(input_path)}: 0 records, skipping")
        return 0

    print(f"  {os.path.basename(input_path)}: {len(records)} records -> {table}.{emb_col}")
    t0 = time.time()

    # Batch UPDATE
    updated = 0
    batch_size = 100
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        async with conn.transaction():
            for rec in batch:
                result = await conn.execute(f"""
                    UPDATE {table}
                    SET {emb_col} = $1::vector
                    WHERE {id_col} = $2
                """, str(rec["embedding"]), int(rec["id"]))
                if "UPDATE 1" in result:
                    updated += 1

    elapsed = time.time() - t0
    print(f"    Updated {updated}/{len(records)} rows in {elapsed:.1f}s")
    return updated


async def main():
    parser = argparse.ArgumentParser(description="Import re-embeddings into personal_koi")
    # Either scan a directory of <prefix>_embeddings.jsonl files (legacy mode),
    # or import a single JSONL into a specified --table / --column.
    parser.add_argument("--input-dir", help="Directory of <prefix>_embeddings.jsonl files (legacy mode)")
    parser.add_argument("--input", help="Single JSONL input (use with --table)")
    parser.add_argument("--table", help="Target table (required with --input)")
    parser.add_argument("--id-col", default="id", help="Id column name (default: id)")
    parser.add_argument("--column", help="Target embedding column (overrides TABLE_MAP default)")
    parser.add_argument("--db-url", default=os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi"))
    args = parser.parse_args()

    if not (args.input_dir or args.input):
        parser.error("Specify --input-dir OR --input")
    if args.input and not args.table:
        parser.error("--input requires --table")

    conn = await asyncpg.connect(args.db_url)
    try:
        total = 0

        # Single-file mode
        if args.input:
            emb_col = args.column or "embedding"
            total += await import_file(conn, args.input, args.table, args.id_col, emb_col)
            count = await conn.fetchval(f"SELECT count(*) FROM {args.table} WHERE {emb_col} IS NOT NULL")
            print(f"\nTotal: {total} embeddings imported")
            print(f"Verification: {args.table}.{emb_col}: {count} non-null embeddings")
            return

        # Legacy directory-scan mode
        imported_prefixes = []
        for fname in sorted(os.listdir(args.input_dir)):
            if not fname.endswith("_embeddings.jsonl"):
                continue

            # Determine table from filename
            prefix = fname.replace("_embeddings.jsonl", "")
            if prefix not in TABLE_MAP:
                print(f"  WARNING: Unknown file prefix '{prefix}', skipping {fname}")
                continue

            table, id_col, emb_col = TABLE_MAP[prefix]
            if args.column:
                emb_col = args.column
            input_path = os.path.join(args.input_dir, fname)
            total += await import_file(conn, input_path, table, id_col, emb_col)
            imported_prefixes.append((prefix, emb_col))

        print(f"\nTotal: {total} embeddings imported")

        # Verify only the tables that actually received imports
        print("\nVerification:")
        for prefix, emb_col in imported_prefixes:
            table, _, _ = TABLE_MAP[prefix]
            count = await conn.fetchval(f"SELECT count(*) FROM {table} WHERE {emb_col} IS NOT NULL")
            print(f"  {table}.{emb_col}: {count} non-null embeddings")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
