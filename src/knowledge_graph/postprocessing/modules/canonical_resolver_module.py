"""
Canonical resolution module for pipeline.

Wraps the existing CanonicalResolver to work within the pipeline framework.
"""

from typing import Dict, Any
import logging
from pathlib import Path

from ..base import PostProcessingModule
from ..context import ProcessingContext, Entity

logger = logging.getLogger(__name__)


class CanonicalResolverModule(PostProcessingModule):
    """
    Module that resolves entity aliases to canonical names.

    For example:
    - "regen.network" -> "Regen Network"
    - "ecocredit" -> "Ecocredit Module"
    - "Gregory" -> "Gregory Landua"

    Configuration:
        registry_path: Path to canonical_entities.json (optional)
        update_relationships: Update relationship sources/targets (default: True)
        allow_type_mismatch: Resolve even when provided type differs (default: True)
        use_canonical_type: Replace entity type with canonical type when available (default: True)
    """

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        self._resolver = None
        self._update_relationships = self.config.get('update_relationships', True)
        self._allow_type_mismatch = self.config.get('allow_type_mismatch', True)
        self._use_canonical_type = self.config.get('use_canonical_type', True)

        # Try to import and use existing CanonicalResolver
        try:
            from ...improvements import CanonicalResolver

            registry_path = self.config.get('registry_path')
            if registry_path:
                self._resolver = CanonicalResolver(Path(registry_path))
            else:
                self._resolver = CanonicalResolver()
        except (ImportError, FileNotFoundError) as e:
            logger.warning(f"CanonicalResolver not available: {e}")
            logger.warning("Entity resolution will be skipped")

    def get_name(self) -> str:
        return "CanonicalResolver"

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Resolve entity aliases to canonical names."""

        if not self._resolver:
            logger.warning("CanonicalResolver not initialized, skipping resolution")
            return context

        # Track canonical mappings for relationship updates
        name_mappings = {}

        # Resolve entities
        for entity in list(context.entities):  # Copy to allow modification
            canonical_name, was_resolved = self._resolver.resolve(
                entity.name,
                entity.type,
                allow_type_mismatch=self._allow_type_mismatch
            )

            if was_resolved:
                canonical_type = self._resolver.get_canonical_type(canonical_name) if self._use_canonical_type else None
                target_type = canonical_type or entity.type
                name_changed = canonical_name != entity.name
                type_changed = target_type != entity.type

                # Track mapping only when name actually changes
                if name_changed:
                    name_mappings[entity.name] = canonical_name

                # Create modified entity with canonical name
                canonical_entity = Entity(
                    name=canonical_name,
                    type=target_type,
                    confidence=entity.confidence,
                    metadata={
                        **entity.metadata,
                        'original_name': entity.name,
                        'resolved_by': self.get_name()
                    }
                )

                if type_changed:
                    canonical_entity.metadata['original_type'] = entity.type

                if name_changed or type_changed:
                    context.modify_entity(entity, canonical_entity, self.get_name())
                    self.stats['entities_canonicalized'] += 1
                    if type_changed:
                        self.stats['types_aligned'] += 1

        # Update relationship sources and targets
        if self._update_relationships and name_mappings:
            for rel in context.relationships:
                # Update source
                if rel.source in name_mappings:
                    original_source = rel.source
                    rel.source = name_mappings[rel.source]
                    rel.metadata['original_source'] = original_source
                    self.stats['relationship_sources_updated'] += 1

                # Update target
                if rel.target in name_mappings:
                    original_target = rel.target
                    rel.target = name_mappings[rel.target]
                    rel.metadata['original_target'] = original_target
                    self.stats['relationship_targets_updated'] += 1

        return context

    def is_known_entity(self, entity_name: str) -> bool:
        """Check if an entity is in the canonical registry."""
        if not self._resolver:
            return False
        return self._resolver.is_known_entity(entity_name)

    def resolve(self, entity_name: str, entity_type: str = None) -> tuple:
        """Resolve a single entity name."""
        if not self._resolver:
            return entity_name, False
        return self._resolver.resolve(entity_name, entity_type)


# Export
__all__ = ['CanonicalResolverModule']
