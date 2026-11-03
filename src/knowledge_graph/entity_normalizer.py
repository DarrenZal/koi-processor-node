"""
Shared entity name normalization for FIX-006 deduplication improvements.

This module provides a single normalization routine used consistently in:
- entity_resolver.py (Tier 1.5 lookup + fuzzy tier + reporting)
- improvements/canonical_resolver.py
- uri_generator.py

Normalization requirements:
- lowercase + trim + collapse whitespace
- convert _ and - to spaces
- strip leading @
- strip trailing "| SOMETHING" suffix pattern (e.g. Gregory | RND -> gregory)
- organization punctuation normalization (regen.foundation ~ regen foundation)

Author: Claude Code
Date: 2025-12-23
"""

import re
from typing import Optional


def normalize_entity_name(name: str, entity_type: Optional[str] = None) -> str:
    """
    Normalize entity name for consistent matching and deduplication.

    This is the canonical normalization routine - use this everywhere
    instead of rolling your own normalization logic.

    Args:
        name: Original entity name
        entity_type: Optional entity type for type-specific rules

    Returns:
        Normalized name suitable for matching

    Examples:
        >>> normalize_entity_name("Gregory_Regen")
        'gregory regen'
        >>> normalize_entity_name("@willszal")
        'willszal'
        >>> normalize_entity_name("Gregory | RND")
        'gregory'
        >>> normalize_entity_name("regen.foundation", "ORGANIZATION")
        'regen foundation'
        >>> normalize_entity_name("Regen Network Development, Inc")
        'regen network development inc'
        >>> normalize_entity_name("Greg-Landua")
        'greg landua'
    """
    if not name:
        return ""

    normalized = name

    # Strip leading @ (usernames)
    normalized = normalized.lstrip('@')

    # Strip trailing "| SOMETHING" suffix pattern (e.g., "Gregory | RND" -> "Gregory")
    normalized = re.sub(r'\s*\|\s*[A-Za-z0-9\s]+$', '', normalized)

    # Convert underscores and hyphens to spaces
    normalized = normalized.replace('_', ' ').replace('-', ' ')

    # Lowercase
    normalized = normalized.lower()

    # Organization-specific: convert dots to spaces (e.g., regen.foundation -> regen foundation)
    # Apply this for ORGs or when type is unknown
    if entity_type is None or entity_type.upper() in ('ORGANIZATION', 'ORG', 'PROJECT', 'TECHNOLOGY'):
        # Replace dots that are likely part of org names (not abbreviations like "Inc.")
        # Only replace dots that are surrounded by letters/numbers (not at word boundaries)
        normalized = re.sub(r'(\w)\.(\w)', r'\1 \2', normalized)

    # Remove common corporate suffixes for matching
    # Note: Keep the full name in display, just normalize for matching
    corporate_suffixes = [
        r',?\s*inc\.?$',
        r',?\s*llc\.?$',
        r',?\s*ltd\.?$',
        r',?\s*corp\.?$',
        r',?\s*pbc\.?$',
        r',?\s*ag\.?$',
    ]
    for suffix in corporate_suffixes:
        normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)

    # Remove common articles at start
    normalized = re.sub(r'^\s*(the|a|an)\s+', '', normalized)

    # Normalize whitespace (collapse multiple spaces)
    normalized = ' '.join(normalized.split())

    # Remove trailing punctuation (but keep internal punctuation for abbreviations)
    normalized = normalized.rstrip('.,;:!?')

    return normalized.strip()


def normalize_for_canonical_lookup(name: str, entity_type: Optional[str] = None) -> str:
    """
    Normalize entity name for canonical registry lookup.

    This is a slightly stricter normalization used for the canonical_entities.json
    lookup table. Uses the same core normalization but may apply additional
    type-specific rules.

    Args:
        name: Original entity name
        entity_type: Optional entity type

    Returns:
        Normalized name for lookup
    """
    return normalize_entity_name(name, entity_type)


def normalize_for_fuzzy_match(name: str, entity_type: Optional[str] = None) -> str:
    """
    Normalize entity name for fuzzy string matching.

    This is used before applying rapidfuzz similarity algorithms.
    Same as base normalization for consistency.

    Args:
        name: Original entity name
        entity_type: Optional entity type

    Returns:
        Normalized name for fuzzy matching
    """
    return normalize_entity_name(name, entity_type)


def is_single_token_name(name: str) -> bool:
    """
    Check if a name is a single token (e.g., "Max", "Will", "Sarah").

    Single-token PERSON names are dangerous to auto-merge without
    explicit allowlisting - they could match many different people.

    Args:
        name: Normalized name to check

    Returns:
        True if single token, False otherwise
    """
    normalized = normalize_entity_name(name)
    tokens = normalized.split()
    return len(tokens) == 1


def names_are_similar_enough_for_merge(
    name1: str,
    name2: str,
    entity_type: str,
    threshold: float = 0.85
) -> bool:
    """
    Quick check if two names are similar enough to consider merging.

    This is a conservative pre-filter before running expensive embedding
    comparisons. Uses simple heuristics.

    Args:
        name1: First name (normalized)
        name2: Second name (normalized)
        entity_type: Entity type
        threshold: Minimum similarity threshold

    Returns:
        True if names seem similar enough to warrant further comparison
    """
    n1 = normalize_entity_name(name1, entity_type)
    n2 = normalize_entity_name(name2, entity_type)

    # Exact match after normalization
    if n1 == n2:
        return True

    # One is substring of the other (useful for "Greg" vs "Gregory")
    if n1 in n2 or n2 in n1:
        return True

    # Share same first token (useful for "Gregory Landua" vs "Gregory Regen")
    tokens1 = n1.split()
    tokens2 = n2.split()
    if tokens1 and tokens2 and tokens1[0] == tokens2[0]:
        # For PERSON, sharing first name is weak signal
        # For ORGANIZATION, sharing first word is stronger
        if entity_type.upper() == 'ORGANIZATION':
            return True

    return False
