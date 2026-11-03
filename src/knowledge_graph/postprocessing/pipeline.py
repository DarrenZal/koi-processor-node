"""
Pipeline orchestrator for multi-stage post-processing.

The orchestrator runs modules in sequence, tracks statistics,
and handles errors.
"""

from typing import List, Dict, Any, Optional
import logging
import json
from datetime import datetime
from pathlib import Path

from .base import PostProcessingModule
from .context import ProcessingContext, Entity, Relationship

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """
    Orchestrates multi-stage post-processing of extracted entities.

    Runs modules in sequence, allowing each to:
    - Block entities/relationships
    - Modify entities
    - Merge duplicates
    - Add new entities (e.g., from splitting)

    Example:
        pipeline = PipelineOrchestrator([
            ConfidenceFilterModule(),
            CanonicalResolverModule(),
            EntityQualityFilterModule(),
            ListSplitterModule()
        ])

        context = ProcessingContext(entities=[...])
        result = pipeline.process(context)
    """

    def __init__(
        self,
        modules: List[PostProcessingModule] = None,
        stop_on_error: bool = False
    ):
        """
        Initialize pipeline with modules.

        Args:
            modules: List of modules to run in sequence
            stop_on_error: If True, halt pipeline on first error
        """
        self.modules = modules or []
        self.stop_on_error = stop_on_error
        self.stats = {}
        self.execution_times = {}
        self._last_context = None

    def add_module(self, module: PostProcessingModule):
        """Add a module to the pipeline."""
        self.modules.append(module)

    def insert_module(self, index: int, module: PostProcessingModule):
        """Insert a module at a specific position."""
        self.modules.insert(index, module)

    def remove_module(self, module_name: str) -> bool:
        """Remove a module by name. Returns True if found and removed."""
        for i, module in enumerate(self.modules):
            if module.get_name() == module_name:
                self.modules.pop(i)
                return True
        return False

    def get_module(self, module_name: str) -> Optional[PostProcessingModule]:
        """Get a module by name."""
        for module in self.modules:
            if module.get_name() == module_name:
                return module
        return None

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """
        Run all modules in sequence.

        Args:
            context: Processing context with entities/relationships

        Returns:
            Modified context after all modules
        """
        logger.info(f"Starting pipeline with {len(self.modules)} modules")
        logger.info(f"Input: {len(context.entities)} entities, "
                   f"{len(context.relationships)} relationships")

        # Reset stats
        self.stats.clear()
        self.execution_times.clear()

        for i, module in enumerate(self.modules, 1):
            module_name = module.get_name()

            # Skip if disabled
            if not module.enabled:
                logger.info(f"[{i}/{len(self.modules)}] Skipping {module_name} (disabled)")
                continue

            logger.info(f"[{i}/{len(self.modules)}] Running {module_name}")

            # Measure execution time
            start_time = datetime.now()

            try:
                # Run module
                context = module.process(context)

                # Track execution time
                execution_time = (datetime.now() - start_time).total_seconds()
                self.execution_times[module_name] = execution_time

                # Track module statistics
                self.stats[module_name] = module.get_statistics()

                logger.info(f"  Completed in {execution_time:.3f}s")
                logger.info(f"  Entities: {len(context.entities)}, "
                           f"Blocked: {len(context.blocked_entities)}")

            except Exception as e:
                error_msg = f"Error in {module_name}: {str(e)}"
                logger.error(error_msg)
                context.errors.append(error_msg)

                if self.stop_on_error:
                    context.halt(f"Module {module_name} failed")
                    break

            # Check if pipeline should halt
            if context.should_halt:
                logger.warning(f"Pipeline halted by {module_name}")
                break

        logger.info(f"Pipeline complete. Output: {len(context.entities)} entities")

        # Store last context for debugging
        self._last_context = context

        return context

    def process_entities(
        self,
        entities: List[Dict[str, Any]],
        relationships: List[Dict[str, Any]] = None,
        metadata: Dict[str, Any] = None
    ) -> ProcessingContext:
        """
        Convenience method to process entities from dictionaries.

        Args:
            entities: List of entity dictionaries with 'name', 'type', 'confidence'
            relationships: Optional list of relationship dictionaries
            metadata: Optional source metadata

        Returns:
            ProcessingContext with results
        """
        # Convert dictionaries to Entity objects
        entity_objects = [
            Entity(
                name=e.get('name', ''),
                type=e.get('type', ''),
                confidence=e.get('confidence'),
                metadata=e.get('metadata', {})
            )
            for e in entities
        ]

        # Convert dictionaries to Relationship objects
        relationship_objects = []
        if relationships:
            relationship_objects = [
                Relationship(
                    source=r.get('source', r.get('subject', '')),
                    predicate=r.get('predicate', ''),
                    target=r.get('target', r.get('object', '')),
                    confidence=r.get('confidence'),
                    metadata=r.get('metadata', {})
                )
                for r in relationships
            ]

        # Create context
        context = ProcessingContext(
            entities=entity_objects,
            relationships=relationship_objects,
            source_metadata=metadata or {}
        )

        # Process
        return self.process(context)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics from all modules.

        Returns:
            Dictionary with per-module statistics and execution times
        """
        return {
            'modules': self.stats,
            'execution_times': self.execution_times,
            'total_time': sum(self.execution_times.values())
        }

    def get_last_context(self) -> Optional[ProcessingContext]:
        """Get the context from the last pipeline run."""
        return self._last_context

    def reset(self):
        """Reset all modules and pipeline statistics."""
        for module in self.modules:
            module.reset()
        self.stats.clear()
        self.execution_times.clear()
        self._last_context = None

    def __repr__(self) -> str:
        module_names = [m.get_name() for m in self.modules]
        return f"PipelineOrchestrator({module_names})"

    def __len__(self) -> int:
        return len(self.modules)

    def __iter__(self):
        return iter(self.modules)


class PipelineBuilder:
    """
    Builder for constructing pipelines from configuration.

    Example:
        builder = PipelineBuilder()
        builder.register_module('ConfidenceFilter', ConfidenceFilterModule)
        builder.register_module('EntityQualityFilter', EntityQualityFilterModule)

        pipeline = builder.from_config('pipeline_config.json')
    """

    def __init__(self):
        self._module_registry: Dict[str, type] = {}

    def register_module(self, name: str, module_class: type):
        """Register a module class by name."""
        self._module_registry[name] = module_class

    def get_registered_modules(self) -> List[str]:
        """Get list of registered module names."""
        return list(self._module_registry.keys())

    def from_config(self, config_path: str) -> PipelineOrchestrator:
        """
        Build pipeline from JSON configuration file.

        Args:
            config_path: Path to pipeline_config.json

        Returns:
            Configured PipelineOrchestrator
        """
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Pipeline config not found: {config_path}")

        with open(config_file) as f:
            config = json.load(f)

        return self.from_dict(config)

    def from_dict(self, config: Dict[str, Any]) -> PipelineOrchestrator:
        """
        Build pipeline from configuration dictionary.

        Args:
            config: Pipeline configuration dictionary

        Returns:
            Configured PipelineOrchestrator
        """
        pipeline_config = config.get('pipeline', {})
        stop_on_error = pipeline_config.get('stop_on_error', False)

        modules = []
        for module_config in pipeline_config.get('modules', []):
            module_name = module_config.get('name')
            module_enabled = module_config.get('enabled', True)
            module_params = module_config.get('config', {})

            if module_name not in self._module_registry:
                logger.warning(f"Unknown module: {module_name}, skipping")
                continue

            # Create module instance
            module_class = self._module_registry[module_name]
            module = module_class(config={**module_params, 'enabled': module_enabled})
            modules.append(module)

        return PipelineOrchestrator(modules=modules, stop_on_error=stop_on_error)


def create_default_pipeline() -> PipelineOrchestrator:
    """
    Create a pipeline with default modules.

    This is a convenience function for quick setup. For production use,
    prefer loading from configuration file.
    """
    # Import modules here to avoid circular imports
    from .modules import (
        ConfidenceFilterModule,
        CanonicalResolverModule,
        DocumentLevelDeduplicator,
        EntityQualityFilterModule,
        ListSplitterModule,
        OntologyNormalizerModule
    )

    return PipelineOrchestrator([
        ConfidenceFilterModule(),
        DocumentLevelDeduplicator(),
        CanonicalResolverModule(),
        EntityQualityFilterModule(),
        ListSplitterModule(),
        OntologyNormalizerModule()
    ])


def create_pipeline_from_config(config_path: str) -> PipelineOrchestrator:
    """
    Create a pipeline from configuration file.

    Args:
        config_path: Path to pipeline_config.json

    Returns:
        Configured PipelineOrchestrator
    """
    # Import modules here to avoid circular imports
    from .modules import (
        ConfidenceFilterModule,
        CanonicalResolverModule,
        DocumentLevelDeduplicator,
        EntityQualityFilterModule,
        ListSplitterModule,
        OntologyNormalizerModule
    )

    builder = PipelineBuilder()
    builder.register_module('ConfidenceFilter', ConfidenceFilterModule)
    builder.register_module('CanonicalResolver', CanonicalResolverModule)
    builder.register_module('DocumentLevelDeduplicator', DocumentLevelDeduplicator)
    builder.register_module('EntityQualityFilter', EntityQualityFilterModule)
    builder.register_module('ListSplitter', ListSplitterModule)
    builder.register_module('OntologyNormalizer', OntologyNormalizerModule)

    return builder.from_config(config_path)


# Export
__all__ = [
    'PipelineOrchestrator',
    'PipelineBuilder',
    'create_default_pipeline',
    'create_pipeline_from_config'
]
