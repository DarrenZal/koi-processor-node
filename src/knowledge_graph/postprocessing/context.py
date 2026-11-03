"""
Processing context for pipeline framework.

The context object is passed through all pipeline modules and contains:
- Input data (entities, relationships)
- Processing state (blocked, modified)
- Statistics and metadata
- Control flow (halt, errors)
"""

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime


@dataclass
class Entity:
    """Entity to be processed."""
    name: str
    type: str
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.name, self.type))

    def __eq__(self, other):
        if not isinstance(other, Entity):
            return False
        return self.name == other.name and self.type == other.type

    def to_dict(self) -> Dict[str, Any]:
        """Convert entity to dictionary."""
        return {
            'name': self.name,
            'type': self.type,
            'confidence': self.confidence,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Entity':
        """Create entity from dictionary."""
        return cls(
            name=data.get('name', ''),
            type=data.get('type', ''),
            confidence=data.get('confidence'),
            metadata=data.get('metadata', {})
        )


@dataclass
class Relationship:
    """Relationship to be processed."""
    source: str
    predicate: str
    target: str
    confidence: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.source, self.predicate, self.target))

    def __eq__(self, other):
        if not isinstance(other, Relationship):
            return False
        return (self.source == other.source and
                self.predicate == other.predicate and
                self.target == other.target)

    def to_dict(self) -> Dict[str, Any]:
        """Convert relationship to dictionary."""
        return {
            'source': self.source,
            'predicate': self.predicate,
            'target': self.target,
            'confidence': self.confidence,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Relationship':
        """Create relationship from dictionary."""
        return cls(
            source=data.get('source', ''),
            predicate=data.get('predicate', ''),
            target=data.get('target', ''),
            confidence=data.get('confidence'),
            metadata=data.get('metadata', {})
        )


@dataclass
class ProcessingContext:
    """
    Shared context for pipeline processing.

    This object is passed through all modules in the pipeline and contains
    the current state of processing.
    """

    # Input data
    entities: List[Entity] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    source_metadata: Dict[str, Any] = field(default_factory=dict)

    # Processing state
    blocked_entities: List[Entity] = field(default_factory=list)
    blocked_relationships: List[Relationship] = field(default_factory=list)
    modified_entities: Dict[str, Entity] = field(default_factory=dict)
    merged_groups: List[List[Entity]] = field(default_factory=list)

    # Statistics
    statistics: Dict[str, Any] = field(default_factory=dict)
    module_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Control flow
    should_halt: bool = False
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Metadata
    timestamp: datetime = field(default_factory=datetime.now)
    pipeline_version: str = "1.0.0"

    def block_entity(self, entity: Entity, reason: str, module_name: str):
        """Block an entity from being inserted."""
        self.blocked_entities.append(entity)
        self.entities = [e for e in self.entities if e != entity]

        # Track in statistics
        key = f"{module_name}_blocked"
        self.statistics[key] = self.statistics.get(key, 0) + 1

        # Store reason
        entity.metadata['blocked_by'] = module_name
        entity.metadata['blocked_reason'] = reason

    def block_relationship(self, relationship: Relationship, reason: str, module_name: str):
        """Block a relationship from being inserted."""
        self.blocked_relationships.append(relationship)
        self.relationships = [r for r in self.relationships if r != relationship]

        key = f"{module_name}_blocked_relationships"
        self.statistics[key] = self.statistics.get(key, 0) + 1

        relationship.metadata['blocked_by'] = module_name
        relationship.metadata['blocked_reason'] = reason

    def modify_entity(self, original: Entity, modified: Entity, module_name: str):
        """Modify an entity (e.g., canonicalize name)."""
        # Remove original, add modified
        self.entities = [e if e != original else modified for e in self.entities]

        # Track modification
        self.modified_entities[original.name] = modified

        key = f"{module_name}_modified"
        self.statistics[key] = self.statistics.get(key, 0) + 1

    def merge_entities(self, entities: List[Entity], canonical: Entity, module_name: str):
        """Merge multiple entities into a canonical entity."""
        self.merged_groups.append(entities)

        # Remove originals, add canonical
        entity_set = set(entities)
        self.entities = [e for e in self.entities if e not in entity_set]
        if canonical not in self.entities:
            self.entities.append(canonical)

        key = f"{module_name}_merged"
        self.statistics[key] = self.statistics.get(key, 0) + 1

    def add_entities(self, entities: List[Entity], module_name: str):
        """Add new entities (e.g., from splitting)."""
        self.entities.extend(entities)

        key = f"{module_name}_added"
        self.statistics[key] = self.statistics.get(key, 0) + len(entities)

    def remove_entity(self, entity: Entity, module_name: str):
        """Remove an entity without blocking it (e.g., when splitting)."""
        self.entities = [e for e in self.entities if e != entity]

        key = f"{module_name}_removed"
        self.statistics[key] = self.statistics.get(key, 0) + 1

    def add_statistic(self, key: str, value: Any):
        """Add a custom statistic."""
        self.statistics[key] = value

    def increment_statistic(self, key: str, amount: int = 1):
        """Increment a statistic counter."""
        self.statistics[key] = self.statistics.get(key, 0) + amount

    def halt(self, reason: str):
        """Stop pipeline processing."""
        self.should_halt = True
        self.errors.append(f"Pipeline halted: {reason}")

    def add_warning(self, warning: str):
        """Add a warning message."""
        self.warnings.append(warning)

    def add_error(self, error: str):
        """Add an error message."""
        self.errors.append(error)

    def get_summary(self) -> Dict[str, Any]:
        """Get processing summary."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'pipeline_version': self.pipeline_version,
            'total_entities': len(self.entities),
            'total_relationships': len(self.relationships),
            'blocked_entities': len(self.blocked_entities),
            'blocked_relationships': len(self.blocked_relationships),
            'modified_entities': len(self.modified_entities),
            'merged_groups': len(self.merged_groups),
            'errors': len(self.errors),
            'warnings': len(self.warnings),
            'halted': self.should_halt,
            'statistics': self.statistics
        }

    def clone(self) -> 'ProcessingContext':
        """Create a deep copy of the context."""
        return ProcessingContext(
            entities=[Entity(e.name, e.type, e.confidence, e.metadata.copy()) for e in self.entities],
            relationships=[Relationship(r.source, r.predicate, r.target, r.confidence, r.metadata.copy()) for r in self.relationships],
            source_metadata=self.source_metadata.copy(),
            blocked_entities=[Entity(e.name, e.type, e.confidence, e.metadata.copy()) for e in self.blocked_entities],
            blocked_relationships=[Relationship(r.source, r.predicate, r.target, r.confidence, r.metadata.copy()) for r in self.blocked_relationships],
            modified_entities={k: Entity(v.name, v.type, v.confidence, v.metadata.copy()) for k, v in self.modified_entities.items()},
            merged_groups=[[Entity(e.name, e.type, e.confidence, e.metadata.copy()) for e in group] for group in self.merged_groups],
            statistics=self.statistics.copy(),
            module_stats={k: v.copy() for k, v in self.module_stats.items()},
            should_halt=self.should_halt,
            errors=self.errors.copy(),
            warnings=self.warnings.copy(),
            timestamp=self.timestamp,
            pipeline_version=self.pipeline_version
        )


# Export
__all__ = ['Entity', 'Relationship', 'ProcessingContext']
