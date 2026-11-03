"""
Document-level deduplication module for the post-processing pipeline.

Removes duplicate entities within a single document/context based on
normalized entity name and type. Keeps the first occurrence and blocks
subsequent duplicates to prevent inflated counts from chunk repetition.
"""

from typing import Dict, Any, Tuple
import logging

from ..base import PostProcessingModule
from ..context import ProcessingContext, Entity

logger = logging.getLogger(__name__)


class DocumentLevelDeduplicator(PostProcessingModule):
    """Prevents duplicate entities within the same document context."""

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._case_insensitive = self.config.get('case_insensitive', True)
        self._normalize_type_case = self.config.get('normalize_type_case', True)

    def get_name(self) -> str:
        return "DocumentLevelDeduplicator"

    def _make_key(self, entity: Entity) -> Tuple[str, str]:
        """Create a hashable key for an entity based on name and type."""
        name = entity.name.strip()
        etype = (entity.type or "").strip()

        if self._case_insensitive:
            name = name.lower()
        if self._normalize_type_case:
            etype = etype.upper()

        return name, etype

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Remove duplicate entities within the current document/context."""
        seen = {}
        deduped_entities = []
        duplicates = []

        for entity in context.entities:
            key = self._make_key(entity)
            if key in seen:
                duplicates.append(entity)
            else:
                seen[key] = entity
                deduped_entities.append(entity)

        # Replace entities with deduped list
        context.entities = deduped_entities

        # Track duplicate entities without removing the first occurrence
        if duplicates:
            for entity in duplicates:
                entity.metadata['blocked_by'] = self.get_name()
                entity.metadata['blocked_reason'] = "duplicate_in_document"
            context.blocked_entities.extend(duplicates)
            context.statistics[f"{self.get_name()}_blocked"] = context.statistics.get(f"{self.get_name()}_blocked", 0) + len(duplicates)
            context.increment_statistic("document_duplicates_removed", len(duplicates))

        # Track statistics
        duplicate_count = len(duplicates)
        self.stats['duplicates_removed'] += duplicate_count
        self.stats['unique_entities'] = len(context.entities)

        if duplicate_count > 0:
            logger.info(
                "Document-level deduplication removed %s duplicates (unique=%s)",
                duplicate_count,
                len(context.entities),
            )

        return context


__all__ = ["DocumentLevelDeduplicator"]
