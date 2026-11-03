"""
Polysemy-Aware Entity Resolution Module for Regen KOI

Provides entity resolution for labels with multiple type variants (polysemy),
supporting query/GraphRAG disambiguation without requiring data merging.

Ranking algorithm:
1. occurrence_count (highest first)
2. connectivity (relationship degree)
3. type_priority (configurable)
4. type_hint boost (if provided)

Example usage:
    from knowledge_graph.polysemy_resolver import resolve_entity_variants, EntityVariant

    # Basic resolution
    results = resolve_entity_variants("notion", db_config=db_config)

    # With type hint
    results = resolve_entity_variants(
        "ethereum",
        type_hint="TECHNOLOGY",
        db_config=db_config
    )

    # Access results
    for r in results:
        print(f"{r.label} ({r.entity_type}): score={r.score}")

Author: Claude Code
Date: 2025-12-24
Version: 2.0.0 (Module refactor)
"""

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
import os

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None


# Default type priority (higher = more preferred)
# Can be customized based on query intent
DEFAULT_TYPE_PRIORITY: Dict[str, int] = {
    'TECHNOLOGY': 100,
    'PROJECT': 90,
    'ORGANIZATION': 80,
    'CONCEPT': 70,
    'STANDARD': 60,
    'PERSON': 50,
    'PROCESS': 40,
    'MATERIAL': 35,
    'MODULE': 30,
    'LOCATION': 25,
    'EVENT': 20,
    'VALIDATOR': 15,
    'CREDIT_CLASS': 10,
    'GOVERNANCE_PROPOSAL': 5,
    'EVIDENCE': 4,
    'CLAIM': 3,
    'QUESTION': 2,
    'API_MESSAGE': 1,
    'LICENSE': 0,
    'KEEPER': 0,
}


@dataclass
class EntityVariant:
    """
    Represents a single entity variant with ranking information.

    Attributes:
        uri: Fuseki URI for the entity
        entity_text: Display name of the entity
        entity_type: Type classification (TECHNOLOGY, CONCEPT, etc.)
        occurrence_count: Number of times this entity appears in documents
        relationship_count: Number of relationships this entity participates in
        score: Computed ranking score
        score_breakdown: Human-readable explanation of score components
    """
    uri: str
    entity_text: str
    entity_type: str
    occurrence_count: int
    relationship_count: int
    score: float
    score_breakdown: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ResolutionResult:
    """
    Complete result of entity resolution.

    Attributes:
        query_label: The input label that was resolved
        type_hint: Optional type hint used for boosting
        variant_count: Total number of variants found
        winner: The highest-ranked variant (or None if no match)
        alternatives: Other variants sorted by score
        is_polysemy: True if multiple entity types exist for this label
        resolution_method: How the winner was determined
    """
    query_label: str
    type_hint: Optional[str]
    variant_count: int
    winner: Optional[EntityVariant]
    alternatives: List[EntityVariant]
    is_polysemy: bool
    resolution_method: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'query_label': self.query_label,
            'type_hint': self.type_hint,
            'variant_count': self.variant_count,
            'winner': self.winner.to_dict() if self.winner else None,
            'alternatives': [a.to_dict() for a in self.alternatives],
            'is_polysemy': self.is_polysemy,
            'resolution_method': self.resolution_method,
        }


def get_default_db_config() -> Dict[str, Any]:
    """
    Get database configuration from environment variables.

    Returns:
        Dictionary with PostgreSQL connection parameters.
    """
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", 5433)),
        "database": os.getenv("POSTGRES_DB", "eliza"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def _get_entity_variants_from_db(conn, label: str) -> List[Dict]:
    """
    Query database for all entity variants matching a label.

    Args:
        conn: psycopg2 database connection
        label: Entity label to search for

    Returns:
        List of dictionaries with entity data and relationship counts
    """
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    query = """
    WITH entity_matches AS (
        SELECT
            id,
            entity_text,
            entity_type,
            normalized_text,
            occurrence_count,
            fuseki_uri
        FROM entity_registry
        WHERE LOWER(TRIM(normalized_text)) = LOWER(TRIM(%s))
    ),
    rel_counts AS (
        SELECT
            e.id,
            COALESCE(subj.subj_count, 0) + COALESCE(obj.obj_count, 0) as relationship_count
        FROM entity_matches e
        LEFT JOIN (
            SELECT subject_entity_id, COUNT(*) as subj_count
            FROM koi_relationships
            GROUP BY subject_entity_id
        ) subj ON e.id = subj.subject_entity_id
        LEFT JOIN (
            SELECT object_entity_id, COUNT(*) as obj_count
            FROM koi_relationships
            GROUP BY object_entity_id
        ) obj ON e.id = obj.object_entity_id
    )
    SELECT
        e.id,
        e.entity_text,
        e.entity_type,
        e.normalized_text,
        e.occurrence_count,
        e.fuseki_uri,
        r.relationship_count
    FROM entity_matches e
    JOIN rel_counts r ON e.id = r.id
    ORDER BY e.occurrence_count DESC, r.relationship_count DESC
    """

    cursor.execute(query, (label,))
    return cursor.fetchall()


def compute_score(
    variant: Dict,
    type_hint: Optional[str] = None,
    type_priority: Optional[Dict[str, int]] = None
) -> tuple:
    """
    Compute ranking score for an entity variant.

    Scoring formula:
    - Base: occurrence_count * 1000 + relationship_count * 100
    - Type priority: + type_priority[type] * 10
    - Type hint match: + 50000 if matches hint

    Args:
        variant: Dictionary with entity data (occurrence_count, relationship_count, entity_type)
        type_hint: Optional type to boost (e.g., "TECHNOLOGY")
        type_priority: Custom type priority mapping (defaults to DEFAULT_TYPE_PRIORITY)

    Returns:
        Tuple of (score: float, breakdown: str)
    """
    if type_priority is None:
        type_priority = DEFAULT_TYPE_PRIORITY

    occ_score = variant['occurrence_count'] * 1000
    rel_score = variant['relationship_count'] * 100
    type_score = type_priority.get(variant['entity_type'], 0) * 10

    base_score = occ_score + rel_score + type_score

    reasons = []
    reasons.append(f"occ={variant['occurrence_count']}")
    reasons.append(f"rels={variant['relationship_count']}")
    reasons.append(f"type_pri={type_priority.get(variant['entity_type'], 0)}")

    # Boost if matches type hint
    if type_hint and variant['entity_type'].upper() == type_hint.upper():
        base_score += 50000
        reasons.append("type_hint_match=+50k")

    return base_score, ", ".join(reasons)


def resolve_entity_variants(
    label: str,
    type_hint: Optional[str] = None,
    limit: int = 10,
    db_config: Optional[Dict[str, Any]] = None,
    type_priority: Optional[Dict[str, int]] = None,
    conn=None
) -> List[Dict]:
    """
    Resolve a label to its entity variants with ranking scores.

    This is the main API function for the polysemy resolver.

    Args:
        label: The entity label to resolve (e.g., "notion", "ethereum")
        type_hint: Optional type to prefer (e.g., "TECHNOLOGY")
        limit: Maximum number of results to return
        db_config: Database configuration (uses env vars if not provided)
        type_priority: Custom type priority ranking (uses defaults if not provided)
        conn: Optional existing database connection (will create one if not provided)

    Returns:
        List of dictionaries with fields:
        - uri: Fuseki URI
        - entity_text: Display name
        - entity_type: Type classification
        - occurrence_count: Document frequency
        - relationship_count: Graph connectivity
        - score: Computed ranking score
        - score_breakdown: Explanation of score components

    Example:
        >>> results = resolve_entity_variants("ethereum", type_hint="TECHNOLOGY")
        >>> for r in results:
        ...     print(f"{r['entity_text']} ({r['entity_type']}): {r['score']}")
        Ethereum (TECHNOLOGY): 179200
        Ethereum (PROJECT): 17900
        Ethereum (ORGANIZATION): 9800
    """
    if psycopg2 is None:
        raise ImportError("psycopg2 is required. Install with: pip install psycopg2-binary")

    # Use provided connection or create new one
    close_conn = False
    if conn is None:
        if db_config is None:
            db_config = get_default_db_config()
        conn = psycopg2.connect(**db_config)
        close_conn = True

    try:
        variants = _get_entity_variants_from_db(conn, label)

        if not variants:
            return []

        # Compute scores for all variants
        scored_variants = []
        for v in variants:
            score, breakdown = compute_score(v, type_hint, type_priority)
            scored_variants.append({
                'uri': v['fuseki_uri'],
                'entity_text': v['entity_text'],
                'entity_type': v['entity_type'],
                'occurrence_count': v['occurrence_count'],
                'relationship_count': v['relationship_count'],
                'score': score,
                'score_breakdown': breakdown,
            })

        # Sort by score descending
        scored_variants.sort(key=lambda x: -x['score'])

        return scored_variants[:limit]

    finally:
        if close_conn:
            conn.close()


def resolve_entity(
    label: str,
    type_hint: Optional[str] = None,
    max_alternatives: int = 5,
    db_config: Optional[Dict[str, Any]] = None,
    type_priority: Optional[Dict[str, int]] = None,
    conn=None
) -> ResolutionResult:
    """
    Resolve a label to its best entity variant with full metadata.

    This function returns a structured ResolutionResult with the winner,
    alternatives, and resolution metadata.

    Args:
        label: The entity label to resolve
        type_hint: Optional type to prefer (e.g., "TECHNOLOGY")
        max_alternatives: Maximum number of alternative results
        db_config: Database configuration (uses env vars if not provided)
        type_priority: Custom type priority ranking
        conn: Optional existing database connection

    Returns:
        ResolutionResult with:
        - winner: The highest-ranked EntityVariant
        - alternatives: Other variants sorted by score
        - is_polysemy: True if multiple types exist
        - resolution_method: How the winner was determined

    Example:
        >>> result = resolve_entity("notion")
        >>> print(f"Winner: {result.winner.entity_text} ({result.winner.entity_type})")
        Winner: Notion (TECHNOLOGY)
        >>> print(f"Is polysemy: {result.is_polysemy}")
        Is polysemy: True
    """
    variants = resolve_entity_variants(
        label=label,
        type_hint=type_hint,
        limit=max_alternatives + 1,
        db_config=db_config,
        type_priority=type_priority,
        conn=conn
    )

    if not variants:
        return ResolutionResult(
            query_label=label,
            type_hint=type_hint,
            variant_count=0,
            winner=None,
            alternatives=[],
            is_polysemy=False,
            resolution_method="no_match"
        )

    # Convert to EntityVariant objects
    entity_variants = [
        EntityVariant(
            uri=v['uri'],
            entity_text=v['entity_text'],
            entity_type=v['entity_type'],
            occurrence_count=v['occurrence_count'],
            relationship_count=v['relationship_count'],
            score=v['score'],
            score_breakdown=v['score_breakdown']
        )
        for v in variants
    ]

    winner = entity_variants[0]
    alternatives = entity_variants[1:max_alternatives + 1]

    # Determine if polysemy exists
    unique_types = set(v['entity_type'] for v in variants)
    is_polysemy = len(unique_types) > 1

    # Determine resolution method
    if type_hint and winner.entity_type.upper() == type_hint.upper():
        resolution_method = "type_hint_match"
    elif winner.occurrence_count > sum(v['occurrence_count'] for v in variants) * 0.5:
        resolution_method = "dominant_occurrence"
    elif winner.relationship_count > sum(v['relationship_count'] for v in variants) * 0.5:
        resolution_method = "dominant_connectivity"
    else:
        resolution_method = "highest_combined_score"

    return ResolutionResult(
        query_label=label,
        type_hint=type_hint,
        variant_count=len(variants),
        winner=winner,
        alternatives=alternatives,
        is_polysemy=is_polysemy,
        resolution_method=resolution_method
    )


def get_type_conflicts(
    conn=None,
    db_config: Optional[Dict[str, Any]] = None,
    min_types: int = 2
) -> List[Dict]:
    """
    Get all labels that have multiple entity types (type conflicts/polysemy).

    Args:
        conn: Optional existing database connection
        db_config: Database configuration
        min_types: Minimum number of distinct types to be considered a conflict

    Returns:
        List of dictionaries with:
        - label: The entity label
        - type_count: Number of distinct types
        - types: List of types
        - total_occurrences: Sum of occurrence_count across all types
    """
    if psycopg2 is None:
        raise ImportError("psycopg2 is required. Install with: pip install psycopg2-binary")

    close_conn = False
    if conn is None:
        if db_config is None:
            db_config = get_default_db_config()
        conn = psycopg2.connect(**db_config)
        close_conn = True

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        query = """
        SELECT
            LOWER(TRIM(normalized_text)) as label,
            COUNT(DISTINCT entity_type) as type_count,
            array_agg(DISTINCT entity_type) as types,
            SUM(occurrence_count) as total_occurrences
        FROM entity_registry
        GROUP BY LOWER(TRIM(normalized_text))
        HAVING COUNT(DISTINCT entity_type) >= %s
        ORDER BY SUM(occurrence_count) DESC
        """

        cursor.execute(query, (min_types,))
        results = cursor.fetchall()

        return [dict(r) for r in results]

    finally:
        if close_conn:
            conn.close()


# Export main classes and functions
__all__ = [
    'EntityVariant',
    'ResolutionResult',
    'resolve_entity_variants',
    'resolve_entity',
    'get_type_conflicts',
    'compute_score',
    'get_default_db_config',
    'DEFAULT_TYPE_PRIORITY',
]
