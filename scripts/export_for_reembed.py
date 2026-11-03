#!/usr/bin/env python3
"""Export KOI tables for re-embedding.

Exports JSONL files (one per table that needs re-embedding):
  - entity_registry_for_reembed.jsonl
  - session_chunks_for_reembed.jsonl
  - knowledge_facts_for_reembed.jsonl
  - koi_memory_chunks_for_reembed.jsonl

Each line: {"id": ..., "text": "..."}
DOCUMENT mode only (no instruction prefix) — stored embeddings are documents.

Text formulas match the canonical fast_*_reembed.py scripts, preserving semantic
space consistency:
  - entity_registry:    "{entity_text}: {context}. {description}"  (8000 chars)
  - koi_memory_chunks:  "Page: {title}\n\n{text}" if title else text  (4000 chars)
  - session_chunks:     chunk_text  (2000 chars, legacy default)
  - knowledge_facts:    fact_text   (2000 chars, legacy default)

Usage:
  python3 scripts/export_for_reembed.py [--db-url postgresql://...] [--output-dir ./reembed_data]
"""

import argparse
import asyncio
import json
import os
import sys

import asyncpg


async def export_table(
    conn,
    output_dir: str,
    table: str,
    id_col: str,
    text_sql: str,
    filename: str,
    where: str = "",
    max_chars: int = 2000,
):
    """Export (id, text) pairs as JSONL."""
    sql = f"SELECT {id_col} AS id, {text_sql} AS text FROM {table}"
    if where:
        sql += f" WHERE {where}"
    rows = await conn.fetch(sql)
    path = os.path.join(output_dir, filename)
    count = 0
    with open(path, "w") as f:
        for row in rows:
            text = row["text"]
            if text and text.strip():
                f.write(json.dumps({"id": str(row["id"]), "text": text[:max_chars]}) + "\n")
                count += 1
    print(f"  {filename}: {count} rows exported{' (NULL-only)' if where else ''}")
    return count


async def main():
    parser = argparse.ArgumentParser(description="Export KOI tables for re-embedding")
    parser.add_argument("--db-url", default=os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi"))
    parser.add_argument("--output-dir", default="./reembed_data")
    parser.add_argument("--only-null", action="store_true",
                        help="Only export rows whose embedding column is NULL (incremental backfill).")
    parser.add_argument("--tables", default="entity_registry,session_chunks,knowledge_facts",
                        help="Comma-separated subset of tables to export. "
                             "Valid: entity_registry, session_chunks, knowledge_facts, koi_memory_chunks.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    selected = set(args.tables.split(","))

    # Canonical formula matches scripts/fast_entity_reembed.py (context + description, 8000 chars)
    entity_text_sql = """
        COALESCE(entity_text, '') || CASE
            WHEN COALESCE(metadata->>'context','') != '' OR COALESCE(description,'') != ''
            THEN ': ' || TRIM(BOTH ' ' FROM
                COALESCE(NULLIF(metadata->>'context',''),'') ||
                CASE WHEN COALESCE(metadata->>'context','') != '' AND COALESCE(description,'') != ''
                     THEN '. ' ELSE '' END ||
                COALESCE(description,''))
            ELSE ''
        END
    """.strip()

    # Canonical formula matches scripts/fast_chunk_reembed.py (title + text, 4000 chars)
    chunk_text_sql = """
        CASE WHEN COALESCE(content->>'title','') != ''
             THEN 'Page: ' || (content->>'title') || E'\n\n' || COALESCE(content->>'text','')
             ELSE COALESCE(content->>'text','')
        END
    """.strip()

    conn = await asyncpg.connect(args.db_url)
    try:
        print(f"Exporting from {args.db_url} to {args.output_dir}/{' (only-null)' if args.only_null else ''}")

        if "entity_registry" in selected:
            await export_table(
                conn, args.output_dir,
                table="entity_registry",
                id_col="id",
                text_sql=entity_text_sql,
                filename="entity_registry_for_reembed.jsonl",
                where="embedding IS NULL" if args.only_null else "",
                max_chars=8000,
            )

        if "session_chunks" in selected:
            await export_table(
                conn, args.output_dir,
                table="session_chunks",
                id_col="id",
                text_sql="chunk_text",
                filename="session_chunks_for_reembed.jsonl",
                where="embedding IS NULL" if args.only_null else "",
            )

        if "knowledge_facts" in selected:
            await export_table(
                conn, args.output_dir,
                table="knowledge_facts",
                id_col="id",
                text_sql="fact_text",
                filename="knowledge_facts_for_reembed.jsonl",
                where="fact_embedding IS NULL" if args.only_null else "",
            )

        if "koi_memory_chunks" in selected:
            await export_table(
                conn, args.output_dir,
                table="koi_memory_chunks",
                id_col="id",
                text_sql=chunk_text_sql,
                filename="koi_memory_chunks_for_reembed.jsonl",
                where="embedding IS NULL" if args.only_null else "",
                max_chars=4000,
            )

        print("\nDone. Re-embed the JSONL files (OpenAI or H200), then run:")
        print("  python3 scripts/import_reembeddings.py --input-dir <results_dir>")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
