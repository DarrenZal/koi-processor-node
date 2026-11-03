"""
Predicate validation and normalization for knowledge graph relationships.

Week 13: Shared module to avoid import tangles between prompt_builder.py and llm_extractor.py.
Provides canonical predicate allowlist and validation functions.

Week 16 (FIX-015): Added predicate-type constraints to prevent semantically invalid
relationships like "operates→CONCEPT" from being created.

Week 20 (FIX-015d): Relaxed constraints to allow legitimate patterns:
- VALIDATOR can author (e.g., governance proposals)
- VALIDATOR can be led by PERSON/ORGANIZATION

Week 20 (FIX-015e): Further constraint relaxations:
- leads: Added TECHNOLOGY as valid object (orgs/people lead tech projects)
- works_at: Added VALIDATOR as valid subject, TECHNOLOGY as valid object
"""

import os
import logging
from typing import Tuple, List, Dict, Any, Set, Optional

logger = logging.getLogger(__name__)

# ============================================================================
# CANONICAL PREDICATES (Week 12)
# Only these predicates should be emitted by extractors.
# This prevents regrowth of normalized/deprecated predicates.
# ============================================================================
CANONICAL_PREDICATES = {
    # Core relationships (high frequency)
    "supports", "uses", "mentions", "implements", "includes", "manages",
    "enables", "part_of", "requires", "provides", "associated_with",
    "located_in", "defines", "relates_to", "works_with", "represents",
    "contains", "addresses", "hosts", "validates", "governs",
    "participates_in", "leads", "monitors", "promotes", "performs",
    "focuses_on", "affects", "queries", "updates", "aligns_with",
    "is_a", "targets", "interacts_with", "contributes_to", "improves",
    "operates", "creates", "built_on", "proposes", "authored",
    "founded", "discusses",

    # Organization/People
    "member_of", "works_at", "employs", "advises",

    # Process/Action
    "executes", "processes", "generates", "analyzes", "evaluates",
    "measures", "deploys", "maintains", "funds", "connects",

    # Knowledge/Communication
    "describes", "explains", "documents", "announces",

    # Platform/Tool (Week 11 E402)
    "documents_on", "integrates_with", "powered_by", "communicates_via",

    # Regen Domain
    "anchors", "bridges", "delegates", "votes", "credits", "issues",
    "retires", "verifies", "registers", "approves", "mints", "burns",

    # Lifecycle
    "replaces", "upgrades",
}

# ============================================================================
# PREDICATE MAPPINGS (Week 12 normalization)
# Maps deprecated/variant predicates to canonical forms.
# ============================================================================
PREDICATE_MAPPINGS = {
    # Platform predicates
    "hosted_on": "uses",
    "published_on": "documents_on",
    "linked_to": "associated_with",
    "based_in": "located_in",

    # Role consolidation
    "founder_of": "founded",
    "is_founder_of": "founded",
    "is_ceo_of": "leads",
    "ceo_of": "leads",

    # Tense normalization
    "exploring": "discusses",
    "presented": "discusses",
    "presenting": "discusses",
    "discussed": "discusses",

    # Other variants
    "related_to": "relates_to",
    "works_on": "works_with",
    "part-of": "part_of",
    "is_part_of": "part_of",
    "member": "member_of",
    "employed_by": "works_at",
    "employes": "employs",  # typo fix

    # Week 20: High-volume non-canonical predicates
    "used_in": "uses",
    "handles": "processes",
    "powers": "powered_by",
    "managed_by": "manages",
    "implemented_in": "implements",
    "co_founded": "founded",
    "developed": "creates",
    "funded_by": "funds",
}


# ============================================================================
# PREDICATE TYPE CONSTRAINTS (Week 16 FIX-015)
# Defines valid subject/object types for predicates with semantic constraints.
# This prevents nonsensical relationships like "operates→CONCEPT".
# ============================================================================
PREDICATE_TYPE_CONSTRAINTS: Dict[str, Dict[str, Set[str]]] = {
    "operates": {
        # "operates" implies running/managing operational infrastructure
        # Relaxed: Allow most subject types except CONCEPT/EVENT (nonsense)
        # Strict on object: Can't operate abstract concepts, materials, locations
        "blocked_object_types": {"CONCEPT", "MATERIAL", "LOCATION", "EVENT"},
        "blocked_subject_types": {"CONCEPT", "EVENT"},
    },
    "founded": {
        # People AND organizations can found things
        # "Google founded DeepMind", "Regen Foundation founded Regen Registry"
        "valid_subject_types": {"PERSON", "ORGANIZATION"},
        "valid_object_types": {"ORGANIZATION", "PROJECT", "TECHNOLOGY"},
    },
    "works_at": {
        # People work at organizations, projects, or validators
        # FIX-015e: Added VALIDATOR as valid subject (validators have operational roles)
        # FIX-015e: Added TECHNOLOGY as valid object (people work on tech projects)
        "valid_subject_types": {"PERSON", "VALIDATOR"},
        "valid_object_types": {"ORGANIZATION", "PROJECT", "VALIDATOR", "TECHNOLOGY"},
    },
    "employs": {
        # Organizations employ people
        "valid_subject_types": {"ORGANIZATION"},
        "valid_object_types": {"PERSON"},
    },
    "member_of": {
        # People/orgs/validators are members of organizations/projects
        "valid_subject_types": {"PERSON", "ORGANIZATION", "VALIDATOR"},
        "valid_object_types": {"ORGANIZATION", "PROJECT"},
    },
    "leads": {
        # People AND organizations can lead things
        # "Regen Foundation leads the registry initiative"
        # FIX-015d: Added VALIDATOR as valid object (e.g., "Gregory Landua leads Chomp")
        # FIX-015e: Added TECHNOLOGY as valid object (e.g., "Regen Network leads Regen Ledger")
        "valid_subject_types": {"PERSON", "ORGANIZATION"},
        "valid_object_types": {"ORGANIZATION", "PROJECT", "EVENT", "PROCESS", "VALIDATOR", "TECHNOLOGY"},
    },
    "located_in": {
        # Things are located in locations (strict - no concepts/projects)
        "valid_object_types": {"LOCATION"},
    },
    "authored": {
        # People AND organizations can author things
        # Companies publish papers, proposals, etc.
        # FIX-015d: Added VALIDATOR (e.g., "Chomp authored Proposal #42")
        "valid_subject_types": {"PERSON", "ORGANIZATION", "VALIDATOR"},
    },
    "validates": {
        # Validators validate things
        "valid_subject_types": {"VALIDATOR", "ORGANIZATION", "TECHNOLOGY"},
    },
    "delegates": {
        # People/orgs delegate to validators
        "valid_object_types": {"VALIDATOR", "PERSON", "ORGANIZATION"},
    },
    "votes": {
        # People/orgs/validators vote on things
        "valid_subject_types": {"PERSON", "ORGANIZATION", "VALIDATOR"},
    },
}


def validate_relationship_types(
    predicate: str,
    subject_type: Optional[str],
    object_type: Optional[str],
    strict: bool = False
) -> Tuple[bool, Optional[str]]:
    """
    Validate that a relationship's types are compatible with the predicate.

    Args:
        predicate: The relationship predicate
        subject_type: Entity type of the subject (e.g., "PERSON", "ORGANIZATION")
        object_type: Entity type of the object
        strict: If True, reject invalid relationships. If False, log warning only.

    Returns:
        Tuple of (is_valid, reason)
        - is_valid: True if the relationship passes type constraints
        - reason: Description of why it failed (None if valid)
    """
    if predicate not in PREDICATE_TYPE_CONSTRAINTS:
        return (True, None)  # No constraints defined

    constraints = PREDICATE_TYPE_CONSTRAINTS[predicate]

    # Check blocked types first (hard blocks)
    if object_type and "blocked_object_types" in constraints:
        if object_type.upper() in constraints["blocked_object_types"]:
            reason = f"{predicate} cannot target {object_type}"
            if strict:
                logger.warning(f"[PredicateTypeGuard] BLOCKED: {reason}")
            else:
                logger.info(f"[PredicateTypeGuard] Would block: {reason}")
            return (False, reason)

    if subject_type and "blocked_subject_types" in constraints:
        if subject_type.upper() in constraints["blocked_subject_types"]:
            reason = f"{subject_type} cannot use {predicate}"
            if strict:
                logger.warning(f"[PredicateTypeGuard] BLOCKED: {reason}")
            else:
                logger.info(f"[PredicateTypeGuard] Would block: {reason}")
            return (False, reason)

    # Check valid types (allowlist)
    if object_type and "valid_object_types" in constraints:
        if object_type.upper() not in constraints["valid_object_types"]:
            reason = f"{predicate} expects object type in {constraints['valid_object_types']}, got {object_type}"
            if strict:
                logger.warning(f"[PredicateTypeGuard] BLOCKED: {reason}")
            else:
                logger.info(f"[PredicateTypeGuard] Would block: {reason}")
            return (False, reason)

    if subject_type and "valid_subject_types" in constraints:
        if subject_type.upper() not in constraints["valid_subject_types"]:
            reason = f"{predicate} expects subject type in {constraints['valid_subject_types']}, got {subject_type}"
            if strict:
                logger.warning(f"[PredicateTypeGuard] BLOCKED: {reason}")
            else:
                logger.info(f"[PredicateTypeGuard] Would block: {reason}")
            return (False, reason)

    return (True, None)


def validate_predicate(predicate: str, strict: bool = False) -> Tuple[str, bool]:
    """
    Validate and optionally normalize a predicate.

    Args:
        predicate: The predicate to validate
        strict: If True, reject non-canonical predicates (return fallback).
                If False, log and allow non-canonical predicates.

    Returns:
        Tuple of (normalized_predicate, is_canonical)
        - normalized_predicate: The predicate to use (canonical or original)
        - is_canonical: True if predicate is in the canonical set
    """
    if not predicate:
        return ("associated_with", False)

    # Normalize: lowercase, strip, replace spaces/hyphens with underscores
    normalized = predicate.lower().strip().replace(" ", "_").replace("-", "_")

    # Check if already canonical
    if normalized in CANONICAL_PREDICATES:
        return (normalized, True)

    # Check if there's a mapping
    if normalized in PREDICATE_MAPPINGS:
        mapped = PREDICATE_MAPPINGS[normalized]
        logger.info(f"[PredicateGuard] Mapped '{predicate}' -> '{mapped}'")
        return (mapped, True)

    # Non-canonical predicate
    logger.warning(f"[PredicateGuard] Non-canonical predicate: '{predicate}'")

    if strict:
        # In strict mode, fallback to generic predicate
        return ("associated_with", False)
    else:
        # In permissive mode, allow but flag as non-canonical
        return (normalized, False)


def filter_relationships(
    relationships: List[Dict[str, Any]],
    strict: bool = False,
    validate_types: bool = False,
    strict_types: bool = False
) -> List[Dict[str, Any]]:
    """
    Filter and validate relationships, applying predicate guard and optional type validation.

    Args:
        relationships: List of relationship dicts with 'predicate' key
        strict: If True, drop or remap non-canonical predicates
        validate_types: If True, also validate predicate-type constraints (Week 16)
        strict_types: If True, reject type-invalid relationships. If False, log only.

    Returns:
        Filtered list of relationships with validated predicates
    """
    if not relationships:
        return []

    validated = []
    rejected_count = 0
    mapped_count = 0
    type_rejected_count = 0

    for rel in relationships:
        if not isinstance(rel, dict):
            continue

        predicate = rel.get("predicate", "")
        original_predicate = predicate

        normalized, is_canonical = validate_predicate(predicate, strict=strict)

        if strict and not is_canonical:
            rejected_count += 1
            continue

        # Track if we mapped to a different predicate
        if normalized != original_predicate.lower().strip().replace(" ", "_").replace("-", "_"):
            mapped_count += 1

        # Type validation (Week 16 FIX-015)
        if validate_types:
            subject_type = rel.get("source_type") or rel.get("subject_type")
            object_type = rel.get("target_type") or rel.get("object_type")

            is_valid, reason = validate_relationship_types(
                normalized, subject_type, object_type, strict=strict_types
            )

            if not is_valid and strict_types:
                type_rejected_count += 1
                continue

        # Update predicate in relationship
        rel_copy = rel.copy()
        rel_copy["predicate"] = normalized
        validated.append(rel_copy)

    # Log summary
    if rejected_count > 0:
        logger.info(f"[PredicateGuard] Rejected {rejected_count} non-canonical predicates (strict mode)")
    if mapped_count > 0:
        logger.info(f"[PredicateGuard] Mapped {mapped_count} predicates to canonical forms")
    if type_rejected_count > 0:
        logger.info(f"[PredicateTypeGuard] Rejected {type_rejected_count} type-invalid relationships")

    return validated


def get_predicate_stats(relationships: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get statistics about predicates in a relationship list.

    Args:
        relationships: List of relationship dicts with 'predicate' key

    Returns:
        Dict with counts: canonical, mapped, non_canonical, total
    """
    stats = {
        "canonical": 0,
        "mapped": 0,
        "non_canonical": 0,
        "total": 0,
    }

    for rel in relationships:
        if not isinstance(rel, dict):
            continue

        predicate = rel.get("predicate", "")
        normalized = predicate.lower().strip().replace(" ", "_").replace("-", "_")

        stats["total"] += 1

        if normalized in CANONICAL_PREDICATES:
            stats["canonical"] += 1
        elif normalized in PREDICATE_MAPPINGS:
            stats["mapped"] += 1
        else:
            stats["non_canonical"] += 1

    return stats


# ============================================================================
# Module Exports
# ============================================================================

__all__ = [
    # Canonical predicates and mappings
    "CANONICAL_PREDICATES",
    "PREDICATE_MAPPINGS",
    # Week 16 FIX-015: Type constraints
    "PREDICATE_TYPE_CONSTRAINTS",
    # Validation functions
    "validate_predicate",
    "validate_relationship_types",
    "filter_relationships",
    "get_predicate_stats",
]
