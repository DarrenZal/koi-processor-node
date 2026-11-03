"""
Knowledge Graph Improvements Module

This module contains quality improvement components for the Regen KOI
knowledge graph extraction pipeline, adapted from proven techniques
in an earlier knowledge graph extraction system.

Components:
- EntityQualityFilter: Blocks low-quality entities (pronouns, generics, etc.)
- CanonicalResolver: Maps aliases to canonical entity names
- ConfidenceFilter: Filters entities/relationships by confidence score
- FuzzyDeduplicator: Merges similar entities (TODO)
"""

from .entity_quality_filter import EntityQualityFilter, FilterConfig
from .canonical_resolver import CanonicalResolver
from .confidence_filter import ConfidenceFilter

__all__ = ["EntityQualityFilter", "FilterConfig", "CanonicalResolver", "ConfidenceFilter"]
