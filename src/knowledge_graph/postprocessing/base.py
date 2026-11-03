"""
Base classes for post-processing pipeline modules.

All pipeline modules inherit from PostProcessingModule and implement
the process() method.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
from collections import defaultdict
import logging

from .context import ProcessingContext

logger = logging.getLogger(__name__)


class PostProcessingModule(ABC):
    """
    Abstract base class for post-processing modules.

    All modules must implement:
    - process(): Main processing logic
    - get_name(): Module identifier

    Optional overrides:
    - validate_config(): Validate module configuration
    - reset(): Reset module state
    """

    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize module with configuration.

        Args:
            config: Module-specific configuration dictionary
        """
        self.config = config or {}
        self.stats = defaultdict(int)
        self.enabled = self.config.get('enabled', True)

        # Validate configuration
        self.validate_config()

    @abstractmethod
    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Process entities/relationships in context.

        Args:
            context: Processing context with entities and relationships

        Returns:
            Modified context after processing
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        Return module name for logging/debugging.

        Returns:
            Module identifier (e.g., "ConfidenceFilter")
        """
        pass

    def validate_config(self):
        """
        Validate module configuration.

        Raises:
            ValueError: If configuration is invalid
        """
        # Override in subclasses if needed
        pass

    def get_statistics(self) -> Dict[str, Any]:
        """
        Return module-specific statistics.

        Returns:
            Dictionary of statistics
        """
        return {
            **dict(self.stats),
            'enabled': self.enabled
        }

    def reset(self):
        """Reset module state and statistics."""
        self.stats.clear()

    def __repr__(self) -> str:
        return f"{self.get_name()}(enabled={self.enabled})"


class PassthroughModule(PostProcessingModule):
    """
    A module that passes through all entities/relationships unchanged.

    Useful for testing and as a template for new modules.
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._name = config.get('name', 'Passthrough') if config else 'Passthrough'

    def get_name(self) -> str:
        return self._name

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Pass through all entities and relationships unchanged."""
        self.stats['entities_processed'] = len(context.entities)
        self.stats['relationships_processed'] = len(context.relationships)
        return context


class FilterModule(PostProcessingModule):
    """
    Base class for filter modules that block entities/relationships.

    Subclasses implement should_block_entity() and/or should_block_relationship().
    """

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Filter entities and relationships based on subclass logic."""

        # Filter entities
        entities_to_block = []
        for entity in context.entities:
            should_block, reason = self.should_block_entity(entity)
            if should_block:
                entities_to_block.append((entity, reason))
                self.stats['entities_blocked'] += 1

        for entity, reason in entities_to_block:
            context.block_entity(entity, reason, self.get_name())

        # Filter relationships
        relationships_to_block = []
        for rel in context.relationships:
            should_block, reason = self.should_block_relationship(rel)
            if should_block:
                relationships_to_block.append((rel, reason))
                self.stats['relationships_blocked'] += 1

        for rel, reason in relationships_to_block:
            context.block_relationship(rel, reason, self.get_name())

        return context

    def should_block_entity(self, entity) -> tuple:
        """
        Determine if an entity should be blocked.

        Args:
            entity: Entity to check

        Returns:
            Tuple of (should_block: bool, reason: str)
        """
        return False, ""

    def should_block_relationship(self, relationship) -> tuple:
        """
        Determine if a relationship should be blocked.

        Args:
            relationship: Relationship to check

        Returns:
            Tuple of (should_block: bool, reason: str)
        """
        return False, ""


class TransformModule(PostProcessingModule):
    """
    Base class for transform modules that modify entities/relationships.

    Subclasses implement transform_entity() and/or transform_relationship().
    """

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Transform entities and relationships based on subclass logic."""

        # Transform entities
        for entity in list(context.entities):
            transformed, new_entity = self.transform_entity(entity)
            if transformed and new_entity != entity:
                context.modify_entity(entity, new_entity, self.get_name())
                self.stats['entities_transformed'] += 1

        # Transform relationships
        for rel in context.relationships:
            transformed, new_rel = self.transform_relationship(rel)
            if transformed:
                # Update relationship in place
                rel.source = new_rel.source
                rel.predicate = new_rel.predicate
                rel.target = new_rel.target
                rel.confidence = new_rel.confidence
                rel.metadata.update(new_rel.metadata)
                self.stats['relationships_transformed'] += 1

        return context

    def transform_entity(self, entity):
        """
        Transform an entity.

        Args:
            entity: Entity to transform

        Returns:
            Tuple of (was_transformed: bool, new_entity: Entity)
        """
        return False, entity

    def transform_relationship(self, relationship):
        """
        Transform a relationship.

        Args:
            relationship: Relationship to transform

        Returns:
            Tuple of (was_transformed: bool, new_relationship: Relationship)
        """
        return False, relationship


# Export
__all__ = ['PostProcessingModule', 'PassthroughModule', 'FilterModule', 'TransformModule']
