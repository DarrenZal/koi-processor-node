"""
Canonical Resolver Module for Regen KOI

Resolves entity aliases to their canonical forms using a registry.
Helps deduplicate entities in the knowledge graph.

FIX-006: Uses shared entity_normalizer for consistent normalization across
all deduplication components.

Usage:
    from src.knowledge_graph.improvements import CanonicalResolver

    resolver = CanonicalResolver()

    # Resolve a single entity
    canonical, was_resolved = resolver.resolve("regen.network")
    # canonical = "Regen Network", was_resolved = True

    # Get canonical type
    entity_type = resolver.get_canonical_type("regen.network")
    # entity_type = "ORGANIZATION"

Author: Claude Code
Date: 2025-12-08
Updated: 2025-12-23 (FIX-006)
"""

import json
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from dataclasses import dataclass

# FIX-006: Import shared normalization
try:
    from ..entity_normalizer import normalize_for_canonical_lookup
except ImportError:
    # Fallback if import fails (e.g., running standalone)
    def normalize_for_canonical_lookup(name: str, entity_type: Optional[str] = None) -> str:
        """Fallback normalization if entity_normalizer not available."""
        return name.strip().lower()


@dataclass
class ResolverStats:
    """Statistics for resolver operations."""
    total_lookups: int = 0
    resolved: int = 0
    not_resolved: int = 0

    @property
    def resolution_rate(self) -> float:
        if self.total_lookups == 0:
            return 0.0
        return self.resolved / self.total_lookups * 100


class CanonicalResolver:
    """
    Resolves entity aliases to canonical forms using a registry.

    The resolver maintains a lookup table that maps all aliases
    (case-insensitive) to their canonical names and types.

    Attributes:
        registry: The full canonical entities registry
        alias_to_canonical: Reverse lookup from alias to (canonical, type)
        stats: Resolution statistics
    """

    def __init__(self, registry_path: Optional[Path] = None):
        """
        Initialize the canonical resolver.

        Args:
            registry_path: Path to canonical_entities.json.
                          If None, uses default location.
        """
        if registry_path is None:
            registry_path = Path(__file__).parents[3] / 'data' / 'canonical_entities.json'

        if not registry_path.exists():
            raise FileNotFoundError(f"Canonical registry not found: {registry_path}")

        with open(registry_path) as f:
            self.registry = json.load(f)

        # Build reverse lookup: alias -> (canonical, type, confidence)
        self.alias_to_canonical: Dict[str, Tuple[str, str, float]] = {}
        self._build_lookup_table()

        # Statistics
        self.stats = ResolverStats()

    def _build_lookup_table(self):
        """
        Build the reverse lookup table from aliases to canonical names.

        FIX-006: Uses normalize_for_canonical_lookup for consistent normalization.
        """

        for category, entities in self.registry.get('entities', {}).items():
            for entity_id, entity_data in entities.items():
                canonical = entity_data.get('canonical_name', '')
                entity_type = entity_data.get('entity_type', 'ENTITY')
                confidence = entity_data.get('confidence', 1.0)

                # FIX-006: Use shared normalization for canonical name
                canonical_key = normalize_for_canonical_lookup(canonical, entity_type)
                if canonical_key not in self.alias_to_canonical:
                    self.alias_to_canonical[canonical_key] = (canonical, entity_type, confidence)

                # FIX-006: Use shared normalization for all aliases
                for alias in entity_data.get('aliases', []):
                    alias_normalized = normalize_for_canonical_lookup(alias, entity_type)
                    # Only add if not already mapped (first mapping wins)
                    if alias_normalized not in self.alias_to_canonical:
                        self.alias_to_canonical[alias_normalized] = (canonical, entity_type, confidence)

    def resolve(
        self,
        entity_name: str,
        entity_type: Optional[str] = None,
        allow_type_mismatch: bool = False
    ) -> Tuple[str, bool]:
        """
        Resolve entity name to canonical form.

        FIX-006: Uses normalize_for_canonical_lookup for consistent matching.

        Args:
            entity_name: The entity name to resolve
            entity_type: Optional entity type for type-aware resolution
            allow_type_mismatch: Resolve even when provided type differs from canonical

        Returns:
            Tuple of (resolved_name, was_resolved)
            - If resolved: (canonical_name, True)
            - If not resolved: (original_name, False)
        """
        self.stats.total_lookups += 1

        # FIX-006: Use shared normalization for lookup
        lookup_key = normalize_for_canonical_lookup(entity_name, entity_type)

        if lookup_key in self.alias_to_canonical:
            canonical, canonical_type, confidence = self.alias_to_canonical[lookup_key]

            # If type provided, only resolve if types are compatible
            if entity_type and not allow_type_mismatch:
                if not self._types_compatible(entity_type, canonical_type):
                    self.stats.not_resolved += 1
                    return entity_name, False

            self.stats.resolved += 1
            return canonical, True

        self.stats.not_resolved += 1
        return entity_name, False

    def _types_compatible(self, provided_type: str, canonical_type: str) -> bool:
        """Check if entity types are compatible for resolution."""
        # Normalize types
        provided = provided_type.upper().replace('_', '')
        canonical = canonical_type.upper().replace('_', '')

        # Exact match
        if provided == canonical:
            return True

        # Compatible mappings
        compatible_groups = [
            {'ORGANIZATION', 'ORG', 'FORMALORGANIZATION', 'COMPANY'},
            {'PROJECT', 'PRODUCT', 'SOFTWARE', 'MODULE', 'TECHNOLOGY'},
            {'PERSON', 'HUMAN', 'HUMANACTOR'},
            {'CONCEPT', 'TOPIC', 'THEME'},
        ]

        for group in compatible_groups:
            if provided in group and canonical in group:
                return True

        return False

    def get_canonical_type(self, entity_name: str, entity_type: Optional[str] = None) -> Optional[str]:
        """
        Get canonical entity type for a name.

        FIX-006: Uses normalize_for_canonical_lookup for consistent matching.

        Args:
            entity_name: The entity name to look up
            entity_type: Optional type hint for normalization

        Returns:
            Canonical entity type if found, None otherwise
        """
        lookup_key = normalize_for_canonical_lookup(entity_name, entity_type)

        if lookup_key in self.alias_to_canonical:
            _, entity_type_result, _ = self.alias_to_canonical[lookup_key]
            return entity_type_result

        return None

    def get_confidence(self, entity_name: str, entity_type: Optional[str] = None) -> float:
        """
        Get confidence score for a canonical mapping.

        FIX-006: Uses normalize_for_canonical_lookup for consistent matching.

        Args:
            entity_name: The entity name to look up
            entity_type: Optional type hint for normalization

        Returns:
            Confidence score (0.0-1.0), 0.0 if not found
        """
        lookup_key = normalize_for_canonical_lookup(entity_name, entity_type)

        if lookup_key in self.alias_to_canonical:
            _, _, confidence = self.alias_to_canonical[lookup_key]
            return confidence

        return 0.0

    def is_known_entity(self, entity_name: str, entity_type: Optional[str] = None) -> bool:
        """
        Check if an entity name is in the canonical registry.

        FIX-006: Uses normalize_for_canonical_lookup for consistent matching.

        Args:
            entity_name: The entity name to check
            entity_type: Optional type hint for normalization

        Returns:
            True if entity is known (canonical or alias)
        """
        lookup_key = normalize_for_canonical_lookup(entity_name, entity_type)
        return lookup_key in self.alias_to_canonical

    def get_all_aliases(self, canonical_name: str) -> List[str]:
        """
        Get all aliases for a canonical name.

        Args:
            canonical_name: The canonical entity name

        Returns:
            List of aliases (including the canonical name itself)
        """
        aliases = []
        canonical_lower = canonical_name.strip().lower()

        for alias, (canonical, _, _) in self.alias_to_canonical.items():
            if canonical.lower() == canonical_lower:
                # Find original case version
                for category in self.registry.get('entities', {}).values():
                    for entity_data in category.values():
                        if entity_data.get('canonical_name', '').lower() == canonical_lower:
                            aliases = [entity_data['canonical_name']] + entity_data.get('aliases', [])
                            return aliases

        return aliases

    def resolve_batch(self, entities: List[Dict], allow_type_mismatch: bool = False) -> List[Dict]:
        """
        Resolve a batch of entities to canonical forms.

        Args:
            entities: List of entity dictionaries with 'name' and optionally 'type'
            allow_type_mismatch: Resolve even when provided type differs from canonical

        Returns:
            List of entities with names resolved to canonical forms
        """
        resolved = []

        for entity in entities:
            name = entity.get('name', '')
            entity_type = entity.get('type', '')

            canonical_name, was_resolved = self.resolve(
                name,
                entity_type,
                allow_type_mismatch=allow_type_mismatch
            )

            resolved_entity = entity.copy()
            resolved_entity['name'] = canonical_name
            if was_resolved:
                resolved_entity['_was_canonicalized'] = True
                resolved_entity['_original_name'] = name

            resolved.append(resolved_entity)

        return resolved

    def get_stats(self) -> Dict:
        """Get resolution statistics."""
        return {
            'total_lookups': self.stats.total_lookups,
            'resolved': self.stats.resolved,
            'not_resolved': self.stats.not_resolved,
            'resolution_rate': round(self.stats.resolution_rate, 2)
        }

    def reset_stats(self):
        """Reset resolution statistics."""
        self.stats = ResolverStats()

    def get_registry_stats(self) -> Dict:
        """Get statistics about the canonical registry."""
        stats = {
            'version': self.registry.get('version', 'unknown'),
            'last_updated': self.registry.get('last_updated', 'unknown'),
            'categories': {},
            'total_canonical': 0,
            'total_aliases': len(self.alias_to_canonical)
        }

        for category, entities in self.registry.get('entities', {}).items():
            category_count = len(entities)
            alias_count = sum(len(e.get('aliases', [])) for e in entities.values())
            stats['categories'][category] = {
                'canonical_count': category_count,
                'alias_count': alias_count
            }
            stats['total_canonical'] += category_count

        return stats


# Convenience function
def resolve_entity(
    name: str,
    entity_type: Optional[str] = None,
    allow_type_mismatch: bool = False
) -> Tuple[str, bool]:
    """
    Quick utility to resolve a single entity.

    Args:
        name: Entity name to resolve
        entity_type: Optional entity type
        allow_type_mismatch: Resolve even when provided type differs from canonical

    Returns:
        Tuple of (resolved_name, was_resolved)
    """
    resolver = CanonicalResolver()
    return resolver.resolve(name, entity_type, allow_type_mismatch=allow_type_mismatch)


# Demo function
def demo():
    """Demonstrate the canonical resolver."""

    print("=" * 60)
    print("CANONICAL RESOLVER DEMO")
    print("=" * 60)

    resolver = CanonicalResolver()

    # Show registry stats
    stats = resolver.get_registry_stats()
    print(f"\nRegistry Version: {stats['version']}")
    print(f"Last Updated: {stats['last_updated']}")
    print(f"Total Canonical Entries: {stats['total_canonical']}")
    print(f"Total Aliases Mapped: {stats['total_aliases']}")

    print("\nCategories:")
    for category, cat_stats in stats['categories'].items():
        print(f"  {category}: {cat_stats['canonical_count']} entries, {cat_stats['alias_count']} aliases")

    # Test resolution
    test_cases = [
        ("regen.network", None),
        ("RND", None),
        ("Regen Network Development", None),
        ("regen ledger", None),
        ("ecocredit", None),
        ("$REGEN", None),
        ("Greg Landua", "PERSON"),
        ("regenerative ag", None),
        ("Osmosis", "ORGANIZATION"),
        ("some-unknown-entity", None),
    ]

    print("\n" + "-" * 60)
    print("RESOLUTION TESTS")
    print("-" * 60)

    for name, entity_type in test_cases:
        canonical, resolved = resolver.resolve(name, entity_type)
        status = "resolved" if resolved else "NOT FOUND"
        print(f"  {name:35s} -> {canonical:25s} [{status}]")

    # Show final stats
    resolution_stats = resolver.get_stats()
    print("\n" + "-" * 60)
    print(f"Resolution Stats: {resolution_stats['resolved']}/{resolution_stats['total_lookups']} "
          f"({resolution_stats['resolution_rate']}%)")
    print("-" * 60)


if __name__ == "__main__":
    demo()
