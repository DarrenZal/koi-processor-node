"""Monitoring and metrics for entity deduplication."""

import logging
from typing import Dict, Any, Optional
import os

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def log_resolver_stats(entity_resolver, logger: logging.Logger = None):
    """
    Log entity resolver statistics.

    Args:
        entity_resolver: EntityResolver instance
        logger: Logger to use (defaults to root logger)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    stats = entity_resolver.get_stats()

    logger.info("=" * 70)
    logger.info("ENTITY DEDUPLICATION STATS")
    logger.info("=" * 70)
    logger.info(f"Total lookups: {stats.get('total_lookups', 0)}")
    logger.info(
        f"Tier 1 (Exact):    {stats.get('tier1_exact_hits', 0):6d} "
        f"({stats.get('tier1_hit_rate', 0) * 100:5.1f}%)"
    )
    logger.info(
        f"Tier 2 (Semantic): {stats.get('tier2_semantic_hits', 0):6d} "
        f"({stats.get('tier2_hit_rate', 0) * 100:5.1f}%)"
    )
    logger.info(
        f"Tier 3 (New):      {stats.get('tier3_new_entities', 0):6d} "
        f"({stats.get('tier3_new_rate', 0) * 100:5.1f}%)"
    )
    if stats.get('race_condition_hits', 0) > 0:
        logger.info(f"Race conditions:   {stats.get('race_condition_hits', 0):6d}")
    if stats.get('embedding_errors', 0) > 0:
        logger.warning(f"Embedding errors:  {stats.get('embedding_errors', 0):6d}")
    logger.info("=" * 70)


def get_registry_summary(db_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get summary statistics from entity_registry.

    Args:
        db_config: Database connection config

    Returns:
        Dictionary with registry statistics
    """
    if not HAS_PSYCOPG2:
        return {"error": "psycopg2 not available"}

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM entity_registry_stats")
        result = cursor.fetchone()

        if result:
            return {
                "total_entities": result[0] or 0,
                "unique_types": result[1] or 0,
                "total_occurrences": result[2] or 0,
                "avg_occurrences": float(result[3]) if result[3] else 0.0,
                "max_occurrences": result[4] or 0,
                "oldest_entity": result[5].isoformat() if result[5] else None,
                "newest_entity": result[6].isoformat() if result[6] else None,
            }
        return {
            "total_entities": 0,
            "unique_types": 0,
            "total_occurrences": 0,
        }

    finally:
        cursor.close()
        conn.close()


def get_type_distribution(db_config: Dict[str, Any]) -> Dict[str, int]:
    """
    Get entity count by type.

    Args:
        db_config: Database connection config

    Returns:
        Dictionary mapping entity_type to count
    """
    if not HAS_PSYCOPG2:
        return {"error": "psycopg2 not available"}

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT entity_type, COUNT(*) as count
            FROM entity_registry
            GROUP BY entity_type
            ORDER BY count DESC
        """)

        return {row[0]: row[1] for row in cursor.fetchall()}

    finally:
        cursor.close()
        conn.close()


def get_top_entities(
    db_config: Dict[str, Any],
    entity_type: Optional[str] = None,
    limit: int = 10
) -> list:
    """
    Get most frequently occurring entities.

    Args:
        db_config: Database connection config
        entity_type: Optional type filter
        limit: Number of entities to return

    Returns:
        List of (entity_text, entity_type, occurrence_count) tuples
    """
    if not HAS_PSYCOPG2:
        return []

    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    try:
        if entity_type:
            cursor.execute("""
                SELECT entity_text, entity_type, occurrence_count
                FROM entity_registry
                WHERE entity_type = %s
                ORDER BY occurrence_count DESC
                LIMIT %s
            """, (entity_type.upper(), limit))
        else:
            cursor.execute("""
                SELECT entity_text, entity_type, occurrence_count
                FROM entity_registry
                ORDER BY occurrence_count DESC
                LIMIT %s
            """, (limit,))

        return [
            {
                "entity_text": row[0],
                "entity_type": row[1],
                "occurrence_count": row[2]
            }
            for row in cursor.fetchall()
        ]

    finally:
        cursor.close()
        conn.close()


def print_dedup_report(db_config: Dict[str, Any], entity_resolver=None):
    """
    Print a comprehensive deduplication report.

    Args:
        db_config: Database connection config
        entity_resolver: Optional EntityResolver instance for session stats
    """
    print("\n" + "=" * 70)
    print("ENTITY DEDUPLICATION REPORT")
    print("=" * 70)

    # Registry summary
    summary = get_registry_summary(db_config)
    print("\n[Registry Summary]")
    print(f"  Total entities:     {summary.get('total_entities', 0):,}")
    print(f"  Unique types:       {summary.get('unique_types', 0)}")
    print(f"  Total occurrences:  {summary.get('total_occurrences', 0):,}")
    print(f"  Avg occurrences:    {summary.get('avg_occurrences', 0):.2f}")
    print(f"  Max occurrences:    {summary.get('max_occurrences', 0):,}")

    # Type distribution
    types = get_type_distribution(db_config)
    print("\n[Entity Types]")
    for etype, count in list(types.items())[:10]:
        print(f"  {etype:<20} {count:>6,}")

    # Top entities
    top = get_top_entities(db_config, limit=10)
    print("\n[Top Entities by Occurrence]")
    for e in top:
        print(f"  {e['entity_text'][:30]:<30} ({e['entity_type']:<12}) {e['occurrence_count']:>4}")

    # Session stats if available
    if entity_resolver:
        stats = entity_resolver.get_stats()
        if stats.get('total_lookups', 0) > 0:
            print("\n[Session Statistics]")
            print(f"  Total lookups:      {stats.get('total_lookups', 0):,}")
            print(f"  Tier 1 (Exact):     {stats.get('tier1_exact_hits', 0):,} ({stats.get('tier1_hit_rate', 0)*100:.1f}%)")
            print(f"  Tier 2 (Semantic):  {stats.get('tier2_semantic_hits', 0):,} ({stats.get('tier2_hit_rate', 0)*100:.1f}%)")
            print(f"  Tier 3 (New):       {stats.get('tier3_new_entities', 0):,} ({stats.get('tier3_new_rate', 0)*100:.1f}%)")

    print("\n" + "=" * 70)
