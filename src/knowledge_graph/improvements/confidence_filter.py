"""
Confidence-based filtering for knowledge graph entities and relationships.

Filters out entities and relationships with confidence scores below configurable thresholds.
This complements pattern-based filtering (EntityQualityFilter) by catching semantically weak extractions.

Usage:
    from src.knowledge_graph.improvements import ConfidenceFilter

    filter = ConfidenceFilter(
        entity_threshold=0.70,
        relationship_threshold=0.80
    )

    # Check entity
    is_valid, reason = filter.filter_entity(
        name="Gregory Landua",
        entity_type="PERSON",
        confidence=0.85
    )
    # Returns: (True, None)

    # Check low-confidence entity
    is_valid, reason = filter.filter_entity(
        name="some ambiguous entity",
        entity_type="CONCEPT",
        confidence=0.45
    )
    # Returns: (False, "confidence_too_low")

Author: Claude Code
Date: 2025-12-08
Version: 1.0.0
"""

from typing import Tuple, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class ConfidenceFilter:
    """
    Filters entities and relationships based on confidence scores.

    This filter complements the pattern-based EntityQualityFilter by catching
    semantically weak extractions that have low confidence scores from the LLM.

    Attributes:
        entity_threshold: Minimum confidence score for entities (0.0-1.0)
        relationship_threshold: Minimum confidence score for relationships
        allow_null: Whether to allow entities without confidence scores
        strict_mode: Whether to require all entities to have confidence scores
        stats: Dictionary tracking filtering statistics
    """

    def __init__(
        self,
        entity_threshold: float = 0.70,
        relationship_threshold: float = 0.80,
        allow_null: bool = True,
        strict_mode: bool = False
    ):
        """
        Initialize confidence filter.

        Args:
            entity_threshold: Minimum confidence for entities (0.0-1.0)
            relationship_threshold: Minimum confidence for relationships
            allow_null: If True, entities without confidence are allowed
            strict_mode: If True, all entities must have confidence scores
        """
        # Validate thresholds
        if not (0.0 <= entity_threshold <= 1.0):
            raise ValueError(f"entity_threshold must be between 0.0 and 1.0, got {entity_threshold}")
        if not (0.0 <= relationship_threshold <= 1.0):
            raise ValueError(f"relationship_threshold must be between 0.0 and 1.0, got {relationship_threshold}")

        self.entity_threshold = entity_threshold
        self.relationship_threshold = relationship_threshold
        self.allow_null = allow_null
        self.strict_mode = strict_mode

        # Statistics
        self.stats = {
            'entities_checked': 0,
            'entities_blocked_low_confidence': 0,
            'entities_blocked_missing_confidence': 0,
            'entities_allowed': 0,
            'relationships_checked': 0,
            'relationships_blocked_low_confidence': 0,
            'relationships_blocked_missing_confidence': 0,
            'relationships_allowed': 0
        }

    def filter_entity(
        self,
        name: str,
        entity_type: str,
        confidence: Optional[float] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if entity meets confidence threshold.

        Args:
            name: Entity name
            entity_type: Entity type
            confidence: Confidence score (0.0-1.0) or None

        Returns:
            Tuple of (is_valid, block_reason)
            - (True, None) if entity passes
            - (False, reason) if entity should be blocked
        """
        self.stats['entities_checked'] += 1

        # Handle missing confidence
        if confidence is None:
            if self.strict_mode:
                self.stats['entities_blocked_missing_confidence'] += 1
                logger.debug(f"Blocked entity '{name}' ({entity_type}): missing_confidence_strict_mode")
                return False, "missing_confidence_strict_mode"
            elif self.allow_null:
                self.stats['entities_allowed'] += 1
                return True, None
            else:
                self.stats['entities_blocked_missing_confidence'] += 1
                logger.debug(f"Blocked entity '{name}' ({entity_type}): missing_confidence")
                return False, "missing_confidence"

        # Validate confidence range
        if not (0.0 <= confidence <= 1.0):
            logger.warning(f"Invalid confidence value: {confidence} for entity '{name}'")
            self.stats['entities_blocked_low_confidence'] += 1
            return False, "invalid_confidence_range"

        # Check threshold
        if confidence < self.entity_threshold:
            self.stats['entities_blocked_low_confidence'] += 1
            logger.debug(f"Blocked entity '{name}' ({entity_type}): confidence {confidence:.2f} < threshold {self.entity_threshold}")
            return False, "confidence_too_low"

        self.stats['entities_allowed'] += 1
        return True, None

    def filter_relationship(
        self,
        source: str,
        predicate: str,
        target: str,
        confidence: Optional[float] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if relationship meets confidence threshold.

        Args:
            source: Source entity name
            predicate: Relationship predicate
            target: Target entity name
            confidence: Confidence score or None

        Returns:
            Tuple of (is_valid, block_reason)
        """
        self.stats['relationships_checked'] += 1

        # Handle missing confidence
        if confidence is None:
            if self.strict_mode:
                self.stats['relationships_blocked_missing_confidence'] += 1
                logger.debug(f"Blocked relationship ({source})-[{predicate}]->({target}): missing_confidence_strict_mode")
                return False, "missing_confidence_strict_mode"
            elif self.allow_null:
                self.stats['relationships_allowed'] += 1
                return True, None
            else:
                self.stats['relationships_blocked_missing_confidence'] += 1
                logger.debug(f"Blocked relationship ({source})-[{predicate}]->({target}): missing_confidence")
                return False, "missing_confidence"

        # Validate confidence range
        if not (0.0 <= confidence <= 1.0):
            logger.warning(f"Invalid confidence value: {confidence} for relationship ({source})-[{predicate}]->({target})")
            self.stats['relationships_blocked_low_confidence'] += 1
            return False, "invalid_confidence_range"

        # Check threshold (relationships require higher confidence)
        if confidence < self.relationship_threshold:
            self.stats['relationships_blocked_low_confidence'] += 1
            logger.debug(f"Blocked relationship ({source})-[{predicate}]->({target}): confidence {confidence:.2f} < threshold {self.relationship_threshold}")
            return False, "confidence_too_low"

        self.stats['relationships_allowed'] += 1
        return True, None

    def get_stats(self) -> Dict[str, Any]:
        """
        Get filtering statistics.

        Returns:
            Dictionary with detailed statistics including counts and rates
        """
        stats = self.stats.copy()

        # Calculate entity rates
        entities_total = stats['entities_checked']
        if entities_total > 0:
            stats['entity_block_rate'] = round(
                (stats['entities_blocked_low_confidence'] + stats['entities_blocked_missing_confidence']) / entities_total,
                4
            )
            stats['entity_allow_rate'] = round(stats['entities_allowed'] / entities_total, 4)
        else:
            stats['entity_block_rate'] = 0.0
            stats['entity_allow_rate'] = 0.0

        # Calculate relationship rates
        relationships_total = stats['relationships_checked']
        if relationships_total > 0:
            stats['relationship_block_rate'] = round(
                (stats['relationships_blocked_low_confidence'] + stats['relationships_blocked_missing_confidence']) / relationships_total,
                4
            )
            stats['relationship_allow_rate'] = round(stats['relationships_allowed'] / relationships_total, 4)
        else:
            stats['relationship_block_rate'] = 0.0
            stats['relationship_allow_rate'] = 0.0

        # Add configuration info
        stats['config'] = {
            'entity_threshold': self.entity_threshold,
            'relationship_threshold': self.relationship_threshold,
            'allow_null': self.allow_null,
            'strict_mode': self.strict_mode
        }

        return stats

    def reset_stats(self):
        """Reset statistics counters."""
        self.stats = {
            'entities_checked': 0,
            'entities_blocked_low_confidence': 0,
            'entities_blocked_missing_confidence': 0,
            'entities_allowed': 0,
            'relationships_checked': 0,
            'relationships_blocked_low_confidence': 0,
            'relationships_blocked_missing_confidence': 0,
            'relationships_allowed': 0
        }

    def __repr__(self) -> str:
        return (
            f"ConfidenceFilter("
            f"entity_threshold={self.entity_threshold}, "
            f"relationship_threshold={self.relationship_threshold}, "
            f"allow_null={self.allow_null}, "
            f"strict_mode={self.strict_mode})"
        )


# Export
__all__ = ['ConfidenceFilter']
