"""
Ontology normalization module for pipeline.

Normalizes entity types and relationship predicates to standard ontology.
"""

from typing import Dict, Any
import logging

from ..base import PostProcessingModule
from ..context import ProcessingContext, Entity

logger = logging.getLogger(__name__)


class OntologyNormalizerModule(PostProcessingModule):
    """
    Module that normalizes entity types and relationship predicates.

    Standardizes:
    - Entity types: PERSON, ORGANIZATION, PROJECT, CONCEPT, EVENT, LOCATION
    - Relationship predicates: works_at, founded, mentions, etc.

    Configuration:
        type_mappings: Custom type normalization rules
        predicate_mappings: Custom predicate normalization rules
        normalize_case: Normalize to uppercase types (default: True)
    """

    # Default type mappings (source -> canonical)
    DEFAULT_TYPE_MAPPINGS = {
        # Person variations
        'INDIVIDUAL': 'PERSON',
        'PEOPLE': 'PERSON',
        'HUMAN': 'PERSON',
        'HUMANACTOR': 'PERSON',
        'HUMAN_ACTOR': 'PERSON',
        'ACTOR': 'PERSON',

        # Organization variations
        'ORG': 'ORGANIZATION',
        'COMPANY': 'ORGANIZATION',
        'FOUNDATION': 'ORGANIZATION',
        'CORPORATION': 'ORGANIZATION',
        'NONPROFIT': 'ORGANIZATION',
        'NON_PROFIT': 'ORGANIZATION',
        'FORMALORGANIZATION': 'ORGANIZATION',
        'FORMAL_ORGANIZATION': 'ORGANIZATION',

        # Project variations
        'REPO': 'PROJECT',
        'REPOSITORY': 'PROJECT',
        'SOFTWARE': 'PROJECT',
        'PRODUCT': 'PROJECT',
        # FIX-005: Removed 'MODULE': 'PROJECT' - MODULE is now canonical
        'PROTOCOL': 'PROJECT',

        # Concept variations
        'IDEA': 'CONCEPT',
        'TOPIC': 'CONCEPT',
        'THEME': 'CONCEPT',
        'METHODOLOGY': 'CONCEPT',
        'FRAMEWORK': 'CONCEPT',

        # Location variations
        'PLACE': 'LOCATION',
        'CITY': 'LOCATION',
        'COUNTRY': 'LOCATION',
        'REGION': 'LOCATION',
        'GPE': 'LOCATION',  # Geo-Political Entity

        # Event variations
        'MEETING': 'EVENT',
        'CONFERENCE': 'EVENT',
        'WORKSHOP': 'EVENT',

        # ====================================================================
        # FIX-005: Domain type variations (Regen/Cosmos)
        # ====================================================================

        # Credit class variations
        'CREDITCLASS': 'CREDIT_CLASS',
        'CREDIT_CLASS': 'CREDIT_CLASS',
        'ECOCREDIT': 'CREDIT_CLASS',
        'ECO_CREDIT': 'CREDIT_CLASS',

        # Governance proposal variations
        'GOVERNANCEPROPOSAL': 'GOVERNANCE_PROPOSAL',
        'GOVERNANCE_PROPOSAL': 'GOVERNANCE_PROPOSAL',
        'PROPOSAL': 'GOVERNANCE_PROPOSAL',
        'GOV_PROPOSAL': 'GOVERNANCE_PROPOSAL',

        # Module variations (now canonical, but map variations)
        'MODULE': 'MODULE',
        'COSMOS_MODULE': 'MODULE',
        'SDK_MODULE': 'MODULE',

        # API message variations
        'MESSAGE': 'API_MESSAGE',
        'API_MESSAGE': 'API_MESSAGE',
        'MSG': 'API_MESSAGE',
        'PROTOBUF_MESSAGE': 'API_MESSAGE',

        # Validator variations
        'VALIDATOR': 'VALIDATOR',
        'BLOCKVALIDATOR': 'VALIDATOR',
        'BLOCK_VALIDATOR': 'VALIDATOR',

        # Keeper variations
        'KEEPER': 'KEEPER',
        'SDK_KEEPER': 'KEEPER',

        # ====================================================================
        # FIX-005: General type variations
        # ====================================================================

        # License variations
        'LICENSE': 'LICENSE',
        'SOFTWARE_LICENSE': 'LICENSE',

        # Standard variations
        'STANDARD': 'STANDARD',
        'SPECIFICATION': 'STANDARD',

        # Process variations
        'PROCESS': 'PROCESS',
        'WORKFLOW': 'PROCESS',
        'PROCEDURE': 'PROCESS',

        # Material variations
        'MATERIAL': 'MATERIAL',
        'RESOURCE': 'MATERIAL',
        'SUBSTANCE': 'MATERIAL',

        # ====================================================================
        # WS3: regen-data-standards type aliases
        # ====================================================================
        'AGENT': 'AGENT',
        'WORK_ORDER': 'WORK_ORDER',
        'WORKORDER': 'WORK_ORDER',
        'PROJECTINFO': 'PROJECT',
        'CREDITPROJECTINFO': 'PROJECT',
        'CREDITCLASSINFO': 'CREDIT_CLASS',
        'CARBONCREDITCLASSINFO': 'CREDIT_CLASS',
        'VOICECOUNCILSESSION': 'EVENT',
        'COHERENCECHECK': 'PROCESS',
        'GOVERNANCEDECISION': 'GOVERNANCE_PROPOSAL',
        'GOVERNANCEPROCESS': 'PROCESS',
        'GOVERNANCESTAGE': 'PROCESS',
    }

    # Default predicate mappings (source -> canonical)
    DEFAULT_PREDICATE_MAPPINGS = {
        # Employment/affiliation
        'employed_by': 'works_at',
        'works_for': 'works_at',
        'affiliated_with': 'works_at',
        'employed_at': 'works_at',

        # Membership
        'member_of': 'part_of',
        'belongs_to': 'part_of',
        'in': 'part_of',

        # Creation/founding
        'created': 'founded',
        'established': 'founded',
        'started': 'founded',
        'launched': 'founded',

        # References
        'refers_to': 'mentions',
        'cites': 'mentions',
        'references': 'mentions',
        'discusses': 'mentions',

        # Location
        'based_in': 'located_in',
        'headquartered_in': 'located_in',
        'operates_in': 'located_in',

        # Collaboration
        'collaborates_with': 'works_with',
        'partners_with': 'works_with',
        'cooperates_with': 'works_with',

        # Support
        'backs': 'supports',
        'endorses': 'supports',
        'advocates_for': 'supports',
    }

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)

        self.normalize_case = self.config.get('normalize_case', True)

        # Merge custom mappings with defaults
        self.type_mappings = {
            **self.DEFAULT_TYPE_MAPPINGS,
            **self.config.get('type_mappings', {})
        }

        self.predicate_mappings = {
            **self.DEFAULT_PREDICATE_MAPPINGS,
            **self.config.get('predicate_mappings', {})
        }

        # Build case-insensitive lookup
        self._type_lookup = {k.upper(): v.upper() for k, v in self.type_mappings.items()}
        self._predicate_lookup = {k.lower(): v.lower() for k, v in self.predicate_mappings.items()}

    def get_name(self) -> str:
        return "OntologyNormalizer"

    def process(self, context: ProcessingContext) -> ProcessingContext:
        """Normalize entity types and relationship predicates."""

        # Normalize entity types
        for entity in list(context.entities):
            original_type = entity.type
            normalized_type = self._normalize_type(original_type)

            if normalized_type != original_type:
                # Create normalized entity
                normalized_entity = Entity(
                    name=entity.name,
                    type=normalized_type,
                    confidence=entity.confidence,
                    metadata={
                        **entity.metadata,
                        'original_type': original_type,
                        'normalized_by': self.get_name()
                    }
                )

                context.modify_entity(entity, normalized_entity, self.get_name())
                self.stats['types_normalized'] += 1

        # Normalize relationship predicates
        for rel in context.relationships:
            original_predicate = rel.predicate
            normalized_predicate = self._normalize_predicate(original_predicate)

            if normalized_predicate != original_predicate:
                rel.predicate = normalized_predicate
                rel.metadata['original_predicate'] = original_predicate
                rel.metadata['predicate_normalized_by'] = self.get_name()
                self.stats['predicates_normalized'] += 1

        return context

    # Prefixes to strip from entity types (WS3: regen-data-standards alignment)
    _TYPE_PREFIXES = [
        'https://framework.regen.network/schema/',
        'https://framework.regen.network/taxonomy/',
        'https://regen.network/koi#',
        'rfs:',
        'rft:',
        'koi:',
        'schema:',
    ]

    def _normalize_type(self, entity_type: str) -> str:
        """Normalize an entity type."""
        if not entity_type:
            return entity_type

        # Strip known namespace prefixes and full URIs (WS3)
        cleaned = entity_type.strip()
        for prefix in self._TYPE_PREFIXES:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix):]
                break

        # Clean and uppercase for lookup
        clean_type = cleaned.upper().replace(' ', '_')

        # Check if mapping exists
        if clean_type in self._type_lookup:
            return self._type_lookup[clean_type]

        # Return original (optionally uppercased)
        if self.normalize_case:
            return clean_type
        return entity_type

    def _normalize_predicate(self, predicate: str) -> str:
        """Normalize a relationship predicate."""
        if not predicate:
            return predicate

        # Clean predicate
        clean_predicate = predicate.strip().lower().replace(' ', '_')

        # Remove namespace prefix if present (e.g., "regen:works_at" -> "works_at")
        if ':' in clean_predicate:
            clean_predicate = clean_predicate.split(':')[-1]

        # Check if mapping exists
        if clean_predicate in self._predicate_lookup:
            return self._predicate_lookup[clean_predicate]

        return clean_predicate

    def get_canonical_type(self, entity_type: str) -> str:
        """Get the canonical form of an entity type (for external use)."""
        return self._normalize_type(entity_type)

    def get_canonical_predicate(self, predicate: str) -> str:
        """Get the canonical form of a predicate (for external use)."""
        return self._normalize_predicate(predicate)


# Export
__all__ = ['OntologyNormalizerModule']
