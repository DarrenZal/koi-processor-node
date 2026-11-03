"""
List splitter module for pipeline.

Splits entities that are actually lists into individual entities.
For example: "Gregory Landua, Will Szal, and Rowan Ellery" -> 3 separate PERSON entities
"""

from typing import Dict, Any, List, Optional
import re
import logging

from ..base import PostProcessingModule
from ..context import ProcessingContext, Entity

logger = logging.getLogger(__name__)


class ListSplitterModule(PostProcessingModule):
    """
    Module that splits list-like entities into individual entities.

    Detects patterns like:
    - "A, B, and C"
    - "A, B, C"
    - "A and B"
    - "A & B"
    - "A, B & C"

    Configuration:
        min_confidence_to_split: Only split high-confidence entities (default: 0.75)
        max_items: Maximum list items to split (default: 5)
        enabled_types: Entity types to split (default: ["PERSON", "ORGANIZATION", "PROJECT"])
        min_item_length: Minimum length for each split item (default: 2)
    """

    # List patterns - order matters (more specific first)
    LIST_PATTERNS = [
        # "A, B, and C" or "A, B and C"
        (r'^(.+?),\s*(.+?),?\s+and\s+(.+?)$', 3),
        # "A, B, & C" or "A, B & C"
        (r'^(.+?),\s*(.+?),?\s*&\s*(.+?)$', 3),
        # "A and B"
        (r'^(.+?)\s+and\s+(.+?)$', 2),
        # "A & B"
        (r'^(.+?)\s*&\s*(.+?)$', 2),
        # "A, B, C, D" (comma-separated list without conjunction)
        (r'^([^,]+),\s*([^,]+),\s*([^,]+),\s*([^,]+)$', 4),
        # "A, B, C" (simple three-item list)
        (r'^([^,]+),\s*([^,]+),\s*([^,]+)$', 3),
    ]

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        self.min_confidence = self.config.get('min_confidence_to_split', 0.75)
        self.max_items = self.config.get('max_items', 5)
        self.enabled_types = self.config.get('enabled_types', [
            "PERSON", "ORGANIZATION", "PROJECT"
        ])
        self.min_item_length = self.config.get('min_item_length', 2)

        # Compile patterns
        self._compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), count)
            for pattern, count in self.LIST_PATTERNS
        ]

    def get_name(self) -> str:
        return "ListSplitter"

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Split list-like entities into individual entities."""

        entities_to_remove = []
        entities_to_add = []

        for entity in context.entities:
            # Only split enabled types
            if entity.type.upper() not in [t.upper() for t in self.enabled_types]:
                continue

            # Only split high-confidence entities (to avoid false positives)
            if entity.confidence is not None and entity.confidence < self.min_confidence:
                continue

            # Try to split
            items = self._try_split(entity.name)

            if items and 1 < len(items) <= self.max_items:
                # Validate items
                valid_items = [
                    item.strip() for item in items
                    if len(item.strip()) >= self.min_item_length
                ]

                if len(valid_items) > 1:
                    # Split successful
                    entities_to_remove.append(entity)

                    for item in valid_items:
                        # Create new entity for each item
                        new_entity = Entity(
                            name=item,
                            type=entity.type,
                            confidence=entity.confidence,
                            metadata={
                                **entity.metadata,
                                'split_from': entity.name,
                                'split_by': self.get_name()
                            }
                        )
                        entities_to_add.append(new_entity)

                    self.stats['lists_split'] += 1
                    self.stats['entities_created'] += len(valid_items)
                    logger.debug(f"Split '{entity.name}' into {len(valid_items)} entities: {valid_items}")

        # Remove list entities
        for entity in entities_to_remove:
            context.remove_entity(entity, self.get_name())

        # Add individual entities
        if entities_to_add:
            context.add_entities(entities_to_add, self.get_name())

        return context

    def _try_split(self, text: str) -> Optional[List[str]]:
        """
        Try to split text as a list.

        Returns:
            List of items if split successful, None otherwise
        """
        text = text.strip()

        # Skip if too short
        if len(text) < 5:
            return None

        # Try each pattern
        for pattern, expected_count in self._compiled_patterns:
            match = pattern.match(text)
            if match:
                items = list(match.groups())
                # Clean up items
                items = [item.strip() for item in items if item]

                # Validate we got the expected number of items
                if len(items) >= 2:
                    return items

        return None

    def would_split(self, entity_name: str) -> bool:
        """Check if an entity name would be split (for testing)."""
        items = self._try_split(entity_name)
        return items is not None and len(items) > 1


# Export
__all__ = ['ListSplitterModule']
