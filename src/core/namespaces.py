"""
Canonical namespace definitions for the KOI knowledge graph.

SINGLE SOURCE OF TRUTH for all namespace URIs.
All files that define RDF namespaces should import from here.

FIX-001 Conformance:
- HTTPS everywhere (no HTTP)
- Types/predicates use koi# namespace
- UPPERCASE type names
- Entity instances use existing pattern: https://regen.network/{type_prefix}/{hash}
"""

from rdflib import Namespace

# Canonical namespace definitions
KOI = Namespace("https://regen.network/koi#")  # Types and predicates only
REGEN = Namespace("https://regen.network/ontology#")  # Legacy - deprecated, use KOI
PROV = Namespace("http://www.w3.org/ns/prov#")
SCHEMA = Namespace("http://schema.org/")
DC = Namespace("http://purl.org/dc/elements/1.1/")

# regen-data-standards namespaces (WS3: namespace alignment)
RFS = Namespace("https://framework.regen.network/schema/")
RFT = Namespace("https://framework.regen.network/taxonomy/")

# Source-specific namespaces (all HTTPS)
DISCOURSE = Namespace("https://regen.network/koi/discourse#")
TWITTER = Namespace("https://regen.network/koi/twitter#")
MEDIUM = Namespace("https://regen.network/koi/medium#")
GITHUB = Namespace("https://regen.network/koi/github#")


def get_type_uri(entity_type: str) -> str:
    """
    Return canonical type URI for entity type.

    All types are UPPERCASE with no "Entity" suffix.

    Args:
        entity_type: Entity type string (e.g., "person", "ORGANIZATION")

    Returns:
        Full type URI (e.g., "https://regen.network/koi#PERSON")
    """
    return str(KOI[entity_type.upper()])


def get_predicate_uri(predicate: str) -> str:
    """
    Return canonical predicate URI.

    Predicates are lowercase snake_case.

    Args:
        predicate: Predicate string (already normalized)

    Returns:
        Full predicate URI
    """
    return str(KOI[predicate])
