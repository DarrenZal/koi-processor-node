#!/usr/bin/env python3
"""
Cleanup orphaned chunks - chunks in koi_memory_chunks without parent documents in koi_memories.

This script identifies and optionally removes orphaned chunks that were created when:
1. Parent document storage failed but chunk storage succeeded (before transaction fix)
2. Parent document was deleted but chunks remained

Usage:
    python cleanup_orphaned_chunks.py           # Preview mode - show orphans
    python cleanup_orphaned_chunks.py --apply   # Apply mode - backup and delete orphans
    python cleanup_orphaned_chunks.py --count   # Just show count of orphans
"""
import asyncio
import asyncpg
import os
import sys
import logging
import argparse
from datetime import datetime, timezone

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def get_orphaned_chunks(conn: asyncpg.Connection) -> list:
    """Find chunks in koi_memory_chunks that have no parent document in koi_memories"""
    orphans = await conn.fetch("""
        SELECT
            c.chunk_rid,
            c.document_rid,
            c.chunk_index,
            c.created_at,
            c.source_content_rid
        FROM koi_memory_chunks c
        LEFT JOIN koi_memories m ON c.document_rid = m.rid
        WHERE m.rid IS NULL
        ORDER BY c.created_at DESC
    """)
    return orphans


async def get_orphan_stats(conn: asyncpg.Connection) -> dict:
    """Get statistics about orphaned chunks"""
    stats = await conn.fetchrow("""
        SELECT
            COUNT(*) as total_orphans,
            COUNT(DISTINCT c.document_rid) as unique_parent_rids,
            MIN(c.created_at) as oldest_orphan,
            MAX(c.created_at) as newest_orphan
        FROM koi_memory_chunks c
        LEFT JOIN koi_memories m ON c.document_rid = m.rid
        WHERE m.rid IS NULL
    """)
    return dict(stats) if stats else {}


async def backup_orphans(conn: asyncpg.Connection, timestamp: str) -> str:
    """Create a backup table of orphaned chunks before deletion"""
    backup_table = f"koi_memory_chunks_orphans_{timestamp}"

    # Create backup table with orphaned chunks
    await conn.execute(f"""
        CREATE TABLE {backup_table} AS
        SELECT c.*
        FROM koi_memory_chunks c
        LEFT JOIN koi_memories m ON c.document_rid = m.rid
        WHERE m.rid IS NULL
    """)

    # Get count
    count = await conn.fetchval(f"SELECT COUNT(*) FROM {backup_table}")
    logger.info(f"Created backup table '{backup_table}' with {count} orphaned chunks")

    return backup_table


async def delete_orphans(conn: asyncpg.Connection) -> int:
    """Delete orphaned chunks from koi_memory_chunks"""
    result = await conn.execute("""
        DELETE FROM koi_memory_chunks
        WHERE chunk_rid IN (
            SELECT c.chunk_rid
            FROM koi_memory_chunks c
            LEFT JOIN koi_memories m ON c.document_rid = m.rid
            WHERE m.rid IS NULL
        )
    """)
    # Parse "DELETE N" to get count
    deleted_count = int(result.split()[1]) if result else 0
    return deleted_count


async def main():
    parser = argparse.ArgumentParser(
        description='Cleanup orphaned chunks in koi_memory_chunks'
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Actually delete orphans (creates backup first)'
    )
    parser.add_argument(
        '--count',
        action='store_true',
        help='Only show count of orphans'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=100,
        help='Maximum orphans to show in preview mode (default: 100)'
    )
    args = parser.parse_args()

    # Get database URL from environment
    db_url = os.getenv(
        'DATABASE_URL',
        os.getenv(
            'POSTGRES_URL',
            'postgresql://postgres:postgres@localhost:5433/eliza'
        )
    )

    logger.info(f"Connecting to database...")
    conn = await asyncpg.connect(db_url)

    try:
        # Get statistics
        stats = await get_orphan_stats(conn)
        total_orphans = stats.get('total_orphans', 0)
        unique_parents = stats.get('unique_parent_rids', 0)
        oldest = stats.get('oldest_orphan')
        newest = stats.get('newest_orphan')

        logger.info("=" * 60)
        logger.info("ORPHANED CHUNKS REPORT")
        logger.info("=" * 60)
        logger.info(f"Total orphaned chunks: {total_orphans}")
        logger.info(f"Unique missing parent RIDs: {unique_parents}")
        if oldest:
            logger.info(f"Oldest orphan: {oldest}")
        if newest:
            logger.info(f"Newest orphan: {newest}")
        logger.info("=" * 60)

        if total_orphans == 0:
            logger.info("No orphaned chunks found. Database is clean!")
            return

        if args.count:
            # Just show count and exit
            return

        if not args.apply:
            # Preview mode - show some orphans
            orphans = await get_orphaned_chunks(conn)
            preview_count = min(len(orphans), args.limit)

            logger.info(f"\nShowing first {preview_count} orphans:")
            logger.info("-" * 60)

            for orphan in orphans[:preview_count]:
                logger.info(
                    f"  chunk_rid: {orphan['chunk_rid']}\n"
                    f"    document_rid: {orphan['document_rid']}\n"
                    f"    chunk_index: {orphan['chunk_index']}\n"
                    f"    created_at: {orphan['created_at']}"
                )

            if len(orphans) > args.limit:
                logger.info(f"  ... and {len(orphans) - args.limit} more")

            logger.info("-" * 60)
            logger.info(
                "\nTo delete these orphans, run with --apply flag:\n"
                "  python cleanup_orphaned_chunks.py --apply"
            )
        else:
            # Apply mode - backup and delete
            timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

            logger.info(f"\nApplying cleanup...")

            # Create backup
            backup_table = await backup_orphans(conn, timestamp)

            # Delete orphans
            deleted_count = await delete_orphans(conn)

            logger.info(f"Deleted {deleted_count} orphaned chunks")
            logger.info(f"Backup saved to table: {backup_table}")

            # Verify
            remaining = await conn.fetchval("""
                SELECT COUNT(*) FROM koi_memory_chunks c
                LEFT JOIN koi_memories m ON c.document_rid = m.rid
                WHERE m.rid IS NULL
            """)
            logger.info(f"Remaining orphans after cleanup: {remaining}")

            if remaining == 0:
                logger.info("SUCCESS: All orphaned chunks have been cleaned up!")
            else:
                logger.warning(f"WARNING: {remaining} orphans still remain (may have been created during cleanup)")

    finally:
        await conn.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    asyncio.run(main())
