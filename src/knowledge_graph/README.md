# Knowledge Graph Module

The Knowledge Graph module provides RDF-based knowledge graph integration with configurable quality controls via a modular post-processing pipeline.

## Overview

The `KnowledgeGraphIntegrator` class integrates extracted entities and relationships into an RDF knowledge graph. It supports two modes for quality control:

1. **Pipeline Mode** (default, recommended): Uses the modular post-processing pipeline
2. **Legacy Mode**: Uses individual filter classes directly

## Quick Start

```python
from knowledge_graph.graph_integration import KnowledgeGraphIntegrator

# Initialize with pipeline mode (default)
kg = KnowledgeGraphIntegrator(
    store_type="memory",  # or "postgresql", "sparql"
    enable_quality_controls=True,
    use_pipeline=True
)

# Process entities
entities = [
    {'name': 'Gregory Landua', 'type': 'PERSON', 'confidence': 0.9},
    {'name': 'Regen Network', 'type': 'ORGANIZATION', 'confidence': 0.85}
]

valid_entities = kg.process_entities_batch(entities)
print(f"Valid entities: {len(valid_entities)}")
```

## Graph Integration Modes

### Pipeline Mode (Recommended)

Uses the modular post-processing pipeline framework with 5 configurable modules:

```python
kg = KnowledgeGraphIntegrator(
    store_type="neo4j",
    enable_quality_controls=True,
    use_pipeline=True  # Enable pipeline mode
)

# Batch processing is more efficient
entities = [
    {'name': 'Gregory Landua', 'type': 'PERSON', 'confidence': 0.9},
    {'name': 'Regen Network', 'type': 'ORGANIZATION', 'confidence': 0.85}
]

valid_entities = kg.process_entities_batch(entities)
```

**Pipeline Modules** (executed in order):
1. **ConfidenceFilter** - Blocks low-confidence entities (default threshold: 0.70)
2. **CanonicalResolver** - Resolves known entity aliases to canonical names
3. **EntityQualityFilter** - Pattern-based filtering (pronouns, generics, URLs, etc.)
4. **ListSplitter** - Splits "Alice and Bob" into separate entities
5. **OntologyNormalizer** - Standardizes entity types (COMPANY → ORGANIZATION)

**Benefits**:
- Modular architecture (easy to add/remove modules)
- Configuration-driven (no code changes needed)
- Better performance (batch processing)
- Comprehensive statistics

### Legacy Mode

Uses individual filter classes directly (backward compatible):

```python
kg = KnowledgeGraphIntegrator(
    store_type="memory",
    enable_quality_controls=True,
    use_pipeline=False  # Legacy mode
)

# Single entity processing
result = kg.process_entity('Gregory Landua', 'PERSON', confidence=0.9)
if result:
    name, entity_type = result
    print(f"Valid: {name} ({entity_type})")
```

**When to use Legacy Mode**:
- Backward compatibility requirements
- Troubleshooting/debugging
- Gradual migration from older code

## Configuration

### Pipeline Configuration

Pipeline configuration is stored in `config/pipeline_config.json`:

```json
{
  "version": "1.0.0",
  "pipeline": {
    "stop_on_error": false,
    "modules": [
      {
        "name": "ConfidenceFilter",
        "enabled": true,
        "config": {
          "entity_threshold": 0.70,
          "relationship_threshold": 0.80,
          "allow_null": true
        }
      },
      {
        "name": "EntityQualityFilter",
        "enabled": true,
        "config": {
          "max_name_length": 80,
          "max_word_count": 8
        }
      }
    ]
  }
}
```

### Custom Configuration Path

```python
kg = KnowledgeGraphIntegrator(
    store_type="memory",
    use_pipeline=True,
    pipeline_config_path="/path/to/custom_config.json"
)
```

## Document Integration

Integrate documents with extracted metadata:

```python
document = {
    "rid": "orn:discourse:post:123",
    "title": "Carbon Credit Discussion",
    "content": "Discussion about carbon credits...",
    "source_type": "discourse",
    "metadata": {
        "author": "Alice",
        "published_at": "2024-01-15T10:00:00Z"
    }
}

extraction = {
    "extracted_entities": [
        {"name": "Alice", "type": "PERSON", "confidence": 0.9},
        {"name": "Carbon Credits", "type": "CONCEPT", "confidence": 0.85}
    ],
    "extracted_relationships": [
        {"subject": "Alice", "predicate": "mentions", "object": "Carbon Credits"}
    ]
}

report = kg.integrate_document(document, extraction)
print(f"Entities created: {len(report['entities_created'])}")
print(f"Entities blocked: {report['entities_blocked']}")
```

## Statistics

Get quality control statistics:

```python
stats = kg.get_quality_stats()
print(f"Mode: {stats['mode']}")  # 'pipeline' or 'legacy'
print(f"Total extracted: {stats['total_extracted']}")
print(f"Blocked: {stats['blocked_total']}")
print(f"Block rate: {stats['block_rate']}%")

# Pipeline-specific stats
if stats['mode'] == 'pipeline':
    pipeline_stats = stats['pipeline_statistics']
    print(f"Total time: {pipeline_stats['total_time']*1000:.2f}ms")
    for module, time in pipeline_stats['execution_times'].items():
        print(f"  {module}: {time*1000:.2f}ms")
```

Reset statistics:

```python
kg.reset_quality_stats()
```

## Quality Filtering Details

### What Gets Blocked

The pipeline blocks entities matching these patterns:

- **Pronouns**: we, they, it, this, that, etc.
- **Generic nouns**: user, community, project, organization, etc.
- **Technical patterns**: URLs, email addresses, blockchain addresses
- **Sentence-like**: Entities with >8 words or sentence structure
- **Low confidence**: Entities below threshold (default 0.70)
- **Tautological**: "organization" as type ORGANIZATION

### What Gets Transformed

- **Type normalization**: COMPANY → ORGANIZATION, PLACE → LOCATION
- **List splitting**: "Alice and Bob" → ["Alice", "Bob"]
- **Canonical resolution**: Known aliases to canonical names

## API Reference

### KnowledgeGraphIntegrator

```python
class KnowledgeGraphIntegrator:
    def __init__(
        self,
        store_type: str = "memory",
        store_config: Dict[str, Any] = None,
        enable_quality_controls: bool = True,
        use_pipeline: bool = True,
        pipeline_config_path: Optional[str] = None
    ):
        """
        Initialize knowledge graph integrator.

        Args:
            store_type: RDF store type ("memory", "postgresql", "sparql")
            store_config: Store-specific configuration
            enable_quality_controls: Enable quality filtering
            use_pipeline: Use pipeline mode (True) or legacy mode (False)
            pipeline_config_path: Custom pipeline config path
        """

    def process_entities_batch(
        self,
        entities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process batch of entities through pipeline."""

    def process_entity(
        self,
        entity_name: str,
        entity_type: str,
        confidence: Optional[float] = None
    ) -> Optional[Tuple[str, str]]:
        """Process single entity with quality controls."""

    def integrate_document(
        self,
        document: Dict[str, Any],
        extraction_metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Integrate document into knowledge graph."""

    def get_quality_stats(self) -> Dict[str, Any]:
        """Get quality control statistics."""

    def reset_quality_stats(self):
        """Reset quality statistics."""

    def query(self, sparql_query: str) -> List[Dict[str, Any]]:
        """Execute SPARQL query."""

    def export_graph(
        self,
        format: str = "turtle",
        file_path: Optional[str] = None
    ) -> str:
        """Export graph in specified format."""
```

## Testing

Run all tests:

```bash
# Framework tests
python -m pytest tests/test_pipeline_framework.py -v

# Module tests
python -m pytest tests/test_pipeline_modules.py -v
```

## Performance

Pipeline performance benchmarks:

- **29 entities**: ~1ms total
- **100 entities**: ~0.8ms total
- **Per-entity overhead**: ~0.008ms

Module breakdown (typical):
- ConfidenceFilter: ~0.12ms
- CanonicalResolver: ~0.01ms
- EntityQualityFilter: ~0.5ms (most complex)
- ListSplitter: ~0.01ms
- OntologyNormalizer: ~0.01ms

## Migration Guide

### Migrating from Legacy to Pipeline Mode

1. **Immediate**: Set `use_pipeline=True` (new default)
2. **Testing**: Run comparison tests to verify consistency
3. **Monitor**: Check quality stats for unexpected changes
4. **Tune**: Adjust pipeline config thresholds if needed

### Keeping Legacy Mode

If you need legacy mode temporarily:

```python
kg = KnowledgeGraphIntegrator(
    use_pipeline=False  # Explicit legacy mode
)
```

## File Structure

```
knowledge_graph/
├── graph_integration.py      # Main integrator class
├── config/
│   ├── pipeline_config.json  # Pipeline configuration
│   └── quality_config.json   # Quality control config
├── postprocessing/
│   ├── __init__.py          # Package exports
│   ├── context.py           # ProcessingContext, Entity, Relationship
│   ├── base.py              # PostProcessingModule base class
│   ├── pipeline.py          # PipelineOrchestrator
│   └── modules/
│       ├── __init__.py
│       ├── confidence.py    # ConfidenceFilterModule
│       ├── canonical.py     # CanonicalResolverModule
│       ├── quality.py       # EntityQualityFilterModule
│       ├── splitter.py      # ListSplitterModule
│       └── ontology.py      # OntologyNormalizerModule
└── improvements/            # Legacy filter classes
```

## Dependencies

Required:
- `rdflib` - RDF graph support
- `SPARQLWrapper` - SPARQL endpoint support

Optional:
- `rdflib-postgresql` - PostgreSQL store support
