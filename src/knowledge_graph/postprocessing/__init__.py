"""
Post-processing pipeline framework for knowledge graph quality control.

This module provides a modular, extensible framework for processing
extracted entities and relationships before insertion into the knowledge graph.

Components:
- ProcessingContext: Shared context object passed through all modules
- PostProcessingModule: Base class for all pipeline modules
- PipelineOrchestrator: Runs modules in sequence

Available Modules:
- ConfidenceFilterModule: Filters by confidence scores
- CanonicalResolverModule: Resolves entity aliases
- DocumentLevelDeduplicator: Removes duplicate entities within a document
- EntityQualityFilterModule: Pattern-based quality filtering
- ListSplitterModule: Splits list-like entities
- OntologyNormalizerModule: Normalizes entity types and predicates

Usage:
    from src.knowledge_graph.postprocessing import (
        PipelineOrchestrator,
        ProcessingContext,
        Entity,
        create_pipeline_from_config
    )

    # Create pipeline from config
    pipeline = create_pipeline_from_config('pipeline_config.json')

    # Or create manually
    from src.knowledge_graph.postprocessing.modules import (
        ConfidenceFilterModule,
        EntityQualityFilterModule
    )

    pipeline = PipelineOrchestrator([
        ConfidenceFilterModule({'entity_threshold': 0.70}),
        EntityQualityFilterModule()
    ])

    # Process entities
    context = ProcessingContext(entities=[
        Entity(name="Gregory Landua", type="PERSON", confidence=0.9),
        Entity(name="we", type="PERSON", confidence=0.85)
    ])

    result = pipeline.process(context)
    print(f"Valid entities: {len(result.entities)}")
    print(f"Blocked entities: {len(result.blocked_entities)}")
"""

from .context import Entity, Relationship, ProcessingContext
from .base import PostProcessingModule, PassthroughModule, FilterModule, TransformModule
from .pipeline import (
    PipelineOrchestrator,
    PipelineBuilder,
    create_default_pipeline,
    create_pipeline_from_config
)

__version__ = "1.0.0"

__all__ = [
    # Context
    'Entity',
    'Relationship',
    'ProcessingContext',
    # Base classes
    'PostProcessingModule',
    'PassthroughModule',
    'FilterModule',
    'TransformModule',
    # Pipeline
    'PipelineOrchestrator',
    'PipelineBuilder',
    'create_default_pipeline',
    'create_pipeline_from_config',
]
