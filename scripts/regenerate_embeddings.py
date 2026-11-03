#!/usr/bin/env python3
"""
Regenerate Embeddings Script

Re-embeds all documents in koi_memories table using real BGE model.
Replaces mock embeddings with actual semantic embeddings.

Usage:
    python scripts/regenerate_embeddings.py [OPTIONS]

Options:
    --apply             Embed and write (INSERT/UPDATE koi_embeddings, commit per batch)
    --batch-size N      Process N documents at a time (default: 32)
    --incremental       Only re-embed documents with null/mock embeddings
    --priority SOURCES  Comma-separated list of sources to prioritize
    --dry-run          Show what would be done without making changes (the default;
                       wins over --apply)

Without --apply it lists the documents in a read-only session and contacts no BGE
server. The database comes from POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
POSTGRES_USER and POSTGRES_PASSWORD (default eliza on localhost:5433), the BGE
server from BGE_SERVER_URL (default http://localhost:8090).
"""

import asyncio
import argparse
import logging
import sys
import os
from typing import List, Dict, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
BGE_SERVER_URL = os.getenv('BGE_SERVER_URL', 'http://localhost:8090')
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5433')),
    'dbname': os.getenv('POSTGRES_DB', 'eliza'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
}


def get_embedding(text: str) -> Optional[List[float]]:
    """Get embedding from BGE server"""
    try:
        response = requests.post(
            f"{BGE_SERVER_URL}/encode",
            json={"text": text},
            timeout=30
        )
        response.raise_for_status()
        data = response.json()
        return data['embedding']
    except Exception as e:
        logger.error(f"Error getting embedding: {e}")
        return None


def check_bge_server():
    """Check if BGE server is running and using real model"""
    try:
        response = requests.get(f"{BGE_SERVER_URL}/health", timeout=5)
        response.raise_for_status()
        health = response.json()

        if 'mock' in health.get('model', '').lower():
            logger.error("❌ BGE server is still using MOCK embeddings!")
            logger.error("Please ensure bge_server.py has been updated with real model")
            return False

        logger.info(f"✅ BGE server healthy: {health.get('model')}")
        logger.info(f"   Device: {health.get('device', 'unknown')}")
        return True
    except Exception as e:
        logger.error(f"❌ Cannot connect to BGE server at {BGE_SERVER_URL}: {e}")
        logger.error("Please start BGE server first: python src/core/bge_server.py")
        return False


def get_documents_to_embed(
    cursor,
    priority_sources: Optional[List[str]] = None,
    incremental: bool = False
) -> List[Dict]:
    """Fetch documents that need re-embedding"""

    if priority_sources:
        # Prioritize specific sources
        sources_filter = "AND m.metadata->>'source' IN (" + ','.join(f"'{s}'" for s in priority_sources) + ")"
    else:
        sources_filter = ""

    if incremental:
        # Only re-embed documents with null embeddings
        embedding_filter = "AND (e.dim_1024 IS NULL OR e.id IS NULL)"
    else:
        # Re-embed all documents
        embedding_filter = ""

    query = f"""
        SELECT
            m.id as memory_id,
            m.rid,
            m.content->>'text' as text,
            m.metadata->>'source' as source,
            m.metadata->>'url' as url,
            e.id as embedding_id
        FROM koi_memories m
        LEFT JOIN koi_embeddings e ON e.memory_id = m.id
        WHERE
            m.content->>'text' IS NOT NULL
            AND LENGTH(m.content->>'text') > 20
            {sources_filter}
            {embedding_filter}
        ORDER BY
            CASE m.metadata->>'source'
                WHEN 'website' THEN 1
                WHEN 'github' THEN 2
                ELSE 3
            END,
            m.rid
    """

    cursor.execute(query)
    return cursor.fetchall()


def update_embedding(cursor, memory_id: str, embedding_id: Optional[int], embedding: List[float]) -> bool:
    """Update or insert embedding for a document"""
    try:
        if embedding_id:
            # Update existing embedding
            cursor.execute(
                """
                UPDATE koi_embeddings
                SET dim_1024 = %s::vector(1024)
                WHERE id = %s
                """,
                (embedding, embedding_id)
            )
        else:
            # Insert new embedding
            cursor.execute(
                """
                INSERT INTO koi_embeddings (memory_id, dim_1024)
                VALUES (%s, %s::vector(1024))
                ON CONFLICT (memory_id) DO UPDATE
                SET dim_1024 = EXCLUDED.dim_1024
                """,
                (memory_id, embedding)
            )
        return True
    except Exception as e:
        logger.error(f"Error updating embedding for memory {memory_id}: {e}")
        return False


def regenerate_embeddings(
    batch_size: int = 32,
    incremental: bool = False,
    priority_sources: Optional[List[str]] = None,
    dry_run: bool = False
):
    """Main function to regenerate embeddings"""

    logger.info(
        f"Database: {DB_CONFIG['user']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        f"  BGE server: {BGE_SERVER_URL}"
    )

    # Check BGE server (a dry run does not contact it)
    if not dry_run and not check_bge_server():
        return 1

    # Connect to database
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        if dry_run:
            conn.set_session(readonly=True)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        logger.info(f"✅ Connected to database: {DB_CONFIG['dbname']}")
    except Exception as e:
        logger.error(f"❌ Cannot connect to database: {e}")
        return 1

    try:
        # Get documents
        documents = get_documents_to_embed(cursor, priority_sources, incremental)
        total_docs = len(documents)

        if total_docs == 0:
            logger.info("No documents need re-embedding")
            return 0

        logger.info(f"Found {total_docs} documents to re-embed")

        if dry_run:
            logger.info("DRY RUN - No changes will be made (pass --apply to re-embed)")
            for i, doc in enumerate(documents[:10]):
                logger.info(f"  [{i+1}] {doc['rid'][:12]}... ({doc['source']}) - {doc['text'][:80]}...")
            if total_docs > 10:
                logger.info(f"  ... and {total_docs - 10} more")
            return 0

        # Process in batches
        success_count = 0
        error_count = 0
        start_time = time.time()

        for i in range(0, total_docs, batch_size):
            batch = documents[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_docs + batch_size - 1) // batch_size

            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} docs)")

            for doc in batch:
                memory_id = doc['memory_id']
                rid = doc['rid']
                text = doc['text']
                source = doc.get('source', 'unknown')
                embedding_id = doc.get('embedding_id')

                # Get new embedding
                embedding = get_embedding(text)

                if embedding:
                    # Update database
                    if update_embedding(cursor, memory_id, embedding_id, embedding):
                        success_count += 1
                        logger.debug(f"  ✓ {rid[:12]}... ({source})")
                    else:
                        error_count += 1
                        logger.warning(f"  ✗ Failed to update {rid[:12]}...")
                else:
                    error_count += 1
                    logger.warning(f"  ✗ Failed to get embedding for {rid[:12]}...")

                # Small delay to avoid overwhelming BGE server
                time.sleep(0.1)

            # Commit after each batch
            conn.commit()

            # Progress update
            elapsed = time.time() - start_time
            docs_per_sec = success_count / elapsed if elapsed > 0 else 0
            eta_seconds = (total_docs - success_count) / docs_per_sec if docs_per_sec > 0 else 0

            logger.info(
                f"Progress: {success_count}/{total_docs} "
                f"({success_count*100/total_docs:.1f}%) "
                f"| {docs_per_sec:.1f} docs/sec "
                f"| ETA: {eta_seconds/60:.1f} min"
            )

        # Final summary
        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Re-embedding Complete!")
        logger.info(f"  ✓ Success: {success_count}")
        logger.info(f"  ✗ Errors: {error_count}")
        logger.info(f"  ⏱ Time: {elapsed/60:.1f} minutes")
        logger.info(f"  📊 Rate: {success_count/elapsed:.1f} docs/sec")
        logger.info("=" * 60)

        return 0 if error_count == 0 else 1

    finally:
        cursor.close()
        conn.close()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Regenerate embeddings for KOI memories using real BGE model',
        epilog='Without --apply this is a dry run: it lists the documents in a read-only '
               'session and contacts no BGE server.'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Embed the documents and INSERT/UPDATE koi_embeddings'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=32,
        help='Number of documents to process in each batch (default: 32)'
    )
    parser.add_argument(
        '--incremental',
        action='store_true',
        help='Only re-embed documents with null embeddings'
    )
    parser.add_argument(
        '--priority',
        type=str,
        help='Comma-separated list of sources to prioritize (e.g., website,github)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes (the default; wins over --apply)'
    )

    args = parser.parse_args(argv)

    # Parse priority sources
    priority_sources = None
    if args.priority:
        priority_sources = [s.strip() for s in args.priority.split(',')]

    # Run regeneration
    exit_code = regenerate_embeddings(
        batch_size=args.batch_size,
        incremental=args.incremental,
        priority_sources=priority_sources,
        dry_run=args.dry_run or not args.apply
    )

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
