"""Deterministic, content-addressable URI generation for entities.

FIX-006: Uses shared entity_normalizer for consistent normalization.
"""

import hashlib
import re
from typing import Dict, Tuple, Optional

# FIX-006: Import shared normalization
try:
    from .entity_normalizer import normalize_entity_name
    HAS_SHARED_NORMALIZER = True
except ImportError:
    HAS_SHARED_NORMALIZER = False


class DeterministicURIGenerator:
    """
    Generate deterministic URIs based on entity content.

    Same normalized name + type always produces same URI.
    This prevents duplicates at the RDF level - it's the "anti-duplication shield".

    Benefits:
    - Collision-resistant (SHA256)
    - Reproducible (same input -> same URI)
    - No need to query before generating
    - Works offline
    """

    BASE_URI = "https://regen.network"

    TYPE_PREFIXES = {
        # Core types
        "PERSON": "person",
        "ORGANIZATION": "org",
        "PROJECT": "project",
        "LOCATION": "location",
        "EVENT": "event",
        "CONCEPT": "concept",
        "CLAIM": "claim",
        "TECHNOLOGY": "tech",
        "METHODOLOGY": "methodology",
        "METRIC": "metric",
        "PRODUCT": "product",
        "DOCUMENT": "doc",
        "PROTOCOL": "protocol",
        "EVIDENCE": "evidence",
        "QUESTION": "question",

        # FIX-005: Domain types
        "CREDIT_CLASS": "credit-class",
        "GOVERNANCE_PROPOSAL": "proposal",
        "VALIDATOR": "validator",
        "MODULE": "module",
        "API_MESSAGE": "msg",
        "KEEPER": "keeper",

        # FIX-005: General types
        "LICENSE": "license",
        "STANDARD": "standard",
        "PROCESS": "process",
        "MATERIAL": "material",
    }

    def __init__(self, base_uri: str = None):
        """
        Initialize URI generator.

        Args:
            base_uri: Base URI for all entities (default: https://regen.network)
        """
        self.base_uri = base_uri or self.BASE_URI

    def normalize_name(self, name: str, entity_type: str = None) -> str:
        """
        Normalize entity name for consistent hashing.

        FIX-006: Now delegates to shared entity_normalizer for consistency
        across all deduplication components.

        Normalization rules:
        - Lowercase
        - Remove extra whitespace
        - Remove common articles (the, a, an)
        - Trim trailing punctuation
        - FIX-006 additions: strip @, convert _/- to spaces, strip "| SUFFIX"

        Args:
            name: Original entity name
            entity_type: Optional entity type for type-specific normalization

        Returns:
            Normalized name

        Examples:
            "The Regen Network" -> "regen network"
            "REGEN NETWORK  " -> "regen network"
            "Gregory_Regen" -> "gregory regen"
            "@willszal" -> "willszal"
            "Gregory | RND" -> "gregory"
        """
        # FIX-006: Use shared normalizer if available
        if HAS_SHARED_NORMALIZER:
            return normalize_entity_name(name, entity_type)

        # Fallback to original implementation for backward compatibility
        # Lowercase
        normalized = name.lower()

        # Remove common articles at start
        normalized = re.sub(r'^\s*(the|a|an)\s+', '', normalized)

        # Normalize whitespace (collapse multiple spaces)
        normalized = ' '.join(normalized.split())

        # Remove trailing punctuation (but keep internal punctuation)
        normalized = normalized.rstrip('.,;:!?')

        return normalized.strip()

    def generate_uri(self, name: str, entity_type: str) -> str:
        """
        Generate deterministic URI from name and type.

        Args:
            name: Entity name
            entity_type: Entity type (PERSON, ORGANIZATION, etc.)

        Returns:
            Content-addressable URI

        Examples:
            generate_uri("Regen Network", "ORGANIZATION")
            -> https://regen.network/org/a1b2c3d4e5f6g7h8

            generate_uri("Gregory Landua", "PERSON")
            -> https://regen.network/person/e5f6g7h8i9j0k1l2
        """
        # FIX-006: Pass entity_type to normalize_name for type-specific normalization
        normalized = self.normalize_name(name, entity_type)

        # Normalize type
        entity_type_upper = entity_type.upper()
        type_prefix = self.TYPE_PREFIXES.get(entity_type_upper, "entity")

        # Generate content hash
        # Format: "{normalized_name}:{entity_type}"
        content = f"{normalized}:{entity_type_upper}"
        hash_digest = hashlib.sha256(content.encode('utf-8')).hexdigest()

        # Use first 16 chars of hash
        # Collision probability: ~1 in 10^19 (astronomically low)
        short_hash = hash_digest[:16]

        # Build URI
        uri = f"{self.base_uri}/{type_prefix}/{short_hash}"

        return uri

    def generate_uri_with_metadata(
        self,
        name: str,
        entity_type: str
    ) -> Dict[str, str]:
        """
        Generate URI with metadata for debugging/provenance.

        Args:
            name: Entity name
            entity_type: Entity type

        Returns:
            Dictionary with URI and metadata:
            {
                "uri": "https://...",
                "normalized_name": "regen network",
                "hash": "a1b2c3d4...",
                "original_name": "Regen Network",
                "type": "ORGANIZATION"
            }
        """
        normalized = self.normalize_name(name)
        uri = self.generate_uri(name, entity_type)

        content = f"{normalized}:{entity_type.upper()}"
        full_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()

        return {
            "uri": uri,
            "normalized_name": normalized,
            "hash": full_hash,
            "original_name": name,
            "type": entity_type.upper()
        }

    def parse_uri(self, uri: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract type and hash from URI.

        Args:
            uri: Entity URI

        Returns:
            (type_prefix, hash) tuple

        Example:
            parse_uri("https://regen.network/org/a1b2c3d4e5f6g7h8")
            -> ("org", "a1b2c3d4e5f6g7h8")
        """
        parts = uri.replace(self.base_uri + "/", "").split("/")
        if len(parts) >= 2:
            return parts[0], parts[1]
        return None, None

    def get_type_from_prefix(self, prefix: str) -> Optional[str]:
        """
        Get entity type from URI prefix.

        Args:
            prefix: URI prefix (e.g., "org", "person")

        Returns:
            Entity type (e.g., "ORGANIZATION", "PERSON") or None
        """
        prefix_to_type = {v: k for k, v in self.TYPE_PREFIXES.items()}
        return prefix_to_type.get(prefix)
