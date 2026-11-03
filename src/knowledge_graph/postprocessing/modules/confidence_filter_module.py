"""
Confidence filtering module for pipeline.

Wraps the existing ConfidenceFilter to work within the pipeline framework.
"""

from typing import Dict, Any
import logging

from ..base import PostProcessingModule
from ..context import ProcessingContext

logger = logging.getLogger(__name__)


class ConfidenceFilterModule(PostProcessingModule):
    """
    Module that filters entities/relationships based on confidence scores.

    Configuration:
        entity_threshold: Minimum confidence for entities (default: 0.70)
        relationship_threshold: Minimum confidence for relationships (default: 0.80)
        allow_null: Allow entities without confidence scores (default: True)
        strict_mode: Require all entities to have confidence (default: False)
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        # Get thresholds from config
        self.entity_threshold = self.config.get('entity_threshold', 0.70)
        self.relationship_threshold = self.config.get('relationship_threshold', 0.80)
        self.allow_null = self.config.get('allow_null', True)
        self.strict_mode = self.config.get('strict_mode', False)

        # Try to import and use existing ConfidenceFilter
        self._filter = None
        try:
            from ...improvements import ConfidenceFilter
            self._filter = ConfidenceFilter(
                entity_threshold=self.entity_threshold,
                relationship_threshold=self.relationship_threshold,
                allow_null=self.allow_null,
                strict_mode=self.strict_mode
            )
        except ImportError:
            logger.warning("ConfidenceFilter not available, using inline implementation")

    def validate_config(self):
        """Validate threshold configuration."""
        entity_threshold = self.config.get('entity_threshold', 0.70)
        relationship_threshold = self.config.get('relationship_threshold', 0.80)

        if not (0.0 <= entity_threshold <= 1.0):
            raise ValueError(f"entity_threshold must be between 0.0 and 1.0, got {entity_threshold}")
        if not (0.0 <= relationship_threshold <= 1.0):
            raise ValueError(f"relationship_threshold must be between 0.0 and 1.0, got {relationship_threshold}")

    def get_name(self) -> str:
        return "ConfidenceFilter"

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Filter entities and relationships by confidence."""

        # Filter entities
        entities_to_remove = []
        for entity in context.entities:
            is_valid, reason = self._filter_entity(
                entity.name,
                entity.type,
                entity.confidence
            )

            if not is_valid:
                entities_to_remove.append((entity, reason))
                self.stats['entities_blocked'] += 1

        # Block low-confidence entities
        for entity, reason in entities_to_remove:
            context.block_entity(entity, reason, self.get_name())

        # Filter relationships
        relationships_to_remove = []
        for rel in context.relationships:
            is_valid, reason = self._filter_relationship(
                rel.source,
                rel.predicate,
                rel.target,
                rel.confidence
            )

            if not is_valid:
                relationships_to_remove.append((rel, reason))
                self.stats['relationships_blocked'] += 1

        # Block low-confidence relationships
        for rel, reason in relationships_to_remove:
            context.block_relationship(rel, reason, self.get_name())

        return context

    def _filter_entity(self, name: str, entity_type: str, confidence: float) -> tuple:
        """Filter a single entity by confidence."""
        if self._filter:
            return self._filter.filter_entity(name, entity_type, confidence)

        # Inline implementation
        if confidence is None:
            if self.strict_mode:
                return False, "missing_confidence_strict_mode"
            elif self.allow_null:
                return True, None
            else:
                return False, "missing_confidence"

        if not (0.0 <= confidence <= 1.0):
            return False, "invalid_confidence_range"

        if confidence < self.entity_threshold:
            return False, "confidence_too_low"

        return True, None

    def _filter_relationship(self, source: str, predicate: str, target: str, confidence: float) -> tuple:
        """Filter a single relationship by confidence."""
        if self._filter:
            return self._filter.filter_relationship(source, predicate, target, confidence)

        # Inline implementation
        if confidence is None:
            if self.strict_mode:
                return False, "missing_confidence_strict_mode"
            elif self.allow_null:
                return True, None
            else:
                return False, "missing_confidence"

        if not (0.0 <= confidence <= 1.0):
            return False, "invalid_confidence_range"

        if confidence < self.relationship_threshold:
            return False, "confidence_too_low"

        return True, None


# Export
__all__ = ['ConfidenceFilterModule']
