"""Data models for knowledge graph entities and relationships."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ResolvedEntity:
    """
    Result of entity resolution via EntityResolver.

    Attributes:
        fuseki_uri: The canonical URI for this entity (as str, not URIRef).
                   Wrap to URIRef only in graph_integration.py when adding to RDF.
        entity_id: The entity_registry.id for database references.
        tier: Resolution tier that matched:
              - 'tier1': Exact match (normalized text)
              - 'tier1_5': Canonical alias match
              - 'tier2': Semantic embedding match
              - 'tier3': Created new entity
        match_score: Similarity score (1.0 for exact/new, 0-1 for semantic)
        entity_text: The canonical entity text from the registry
    """
    fuseki_uri: str
    entity_id: int
    tier: str
    match_score: float = 1.0
    entity_text: Optional[str] = None
