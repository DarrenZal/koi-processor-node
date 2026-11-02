"""
BKC ontology registry.

Loads bkc-ontology.jsonld once at service startup and exposes:
- ALLOWED_ENTITY_TYPES: frozenset[str]  — 25 PascalCase type names
- ALLOWED_PREDICATES: frozenset[str]    — 39 snake_case predicate names
- PREDICATE_ALIASES: dict[str, tuple[str, bool]]
    alias -> (canonical_predicate, swap_direction)
- ONTOLOGY_VERSION: str

Fails fast if the file is missing or malformed — the crawler's commit-validation
story relies on a non-empty allow-list.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

_DEFAULT_PATH = Path(__file__).resolve().parent / "ontology" / "bkc-ontology.jsonld"

# Labels that differ from their @id local-name. The system uses these labels
# as the canonical string everywhere (e.g., "Location" not "Place",
# "related_to" not "related").
_LABEL_OVERRIDES_BY_IRI = {
    "schema:Place": "Location",
    "skos:Concept": "Concept",
    "skos:related": "related_to",
}

# Predicate aliases: caller-supplied name -> (canonical_name, swap_direction).
# Mirrors vault_parser.py behavior so crawler commit path accepts the same
# forgiving input without duplicating logic.
PREDICATE_ALIASES: dict[str, tuple[str, bool]] = {
    "documentedBy": ("documents", True),
    "implements": ("implemented_by", True),
    "protocol": ("implemented_by", True),
}

ALLOWED_ENTITY_TYPES: frozenset[str] = frozenset()
ALLOWED_PREDICATES: frozenset[str] = frozenset()
ONTOLOGY_VERSION: str = "unknown"


class OntologyLoadError(RuntimeError):
    """Raised when the ontology file can't be found or parsed."""


def _iri_local_name(iri: str) -> str:
    # "bkc:Practice" -> "Practice" ; "schema:Person" -> "Person"
    return iri.split(":", 1)[-1]


def _resolved_label(iri: str) -> str:
    """Canonical string used throughout the system for this IRI."""
    override = _LABEL_OVERRIDES_BY_IRI.get(iri)
    if override is not None:
        return override
    return _iri_local_name(iri)


def _extract_version(graph: Iterable[dict]) -> str:
    for entry in graph:
        if entry.get("@type") == "owl:Ontology":
            version = entry.get("owl:versionInfo")
            if isinstance(version, str) and version:
                return version
    return "unknown"


def load_ontology(path: Path | str | None = None) -> tuple[frozenset[str], frozenset[str], str]:
    """Parse the BKC ontology file and return (types, predicates, version).

    Raises OntologyLoadError on any failure.
    """
    target = Path(path or os.environ.get("BKC_ONTOLOGY_PATH") or _DEFAULT_PATH)
    if not target.exists():
        raise OntologyLoadError(f"BKC ontology file not found at {target}")
    try:
        doc = json.loads(target.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise OntologyLoadError(f"BKC ontology file is not valid JSON: {exc}") from exc

    graph = doc.get("@graph")
    if not isinstance(graph, list):
        raise OntologyLoadError("BKC ontology file missing top-level @graph array")

    types: set[str] = set()
    predicates: set[str] = set()
    for entry in graph:
        iri = entry.get("@id", "")
        kind = entry.get("@type")
        if kind == "owl:Class":
            types.add(_resolved_label(iri))
        elif kind == "owl:ObjectProperty":
            predicates.add(_resolved_label(iri))

    if not types or not predicates:
        raise OntologyLoadError(
            f"BKC ontology loaded but empty: {len(types)} types, {len(predicates)} predicates"
        )

    version = _extract_version(graph)
    return frozenset(types), frozenset(predicates), version


def init_registry(path: Path | str | None = None) -> None:
    """Populate module-level constants. Idempotent — safe to call at startup."""
    global ALLOWED_ENTITY_TYPES, ALLOWED_PREDICATES, ONTOLOGY_VERSION
    types, predicates, version = load_ontology(path)
    ALLOWED_ENTITY_TYPES = types
    ALLOWED_PREDICATES = predicates
    ONTOLOGY_VERSION = version
    logger.info(
        "ontology loaded: %d types, %d predicates, version %s",
        len(types),
        len(predicates),
        version,
    )


def canonicalize_predicate(name: str) -> tuple[str, bool]:
    """Return (canonical_predicate, swap_direction) for a caller-supplied name.

    Unknown predicates return (name, False); the caller still needs to check
    ALLOWED_PREDICATES membership for validation. Aliases apply their canonical
    name and signal whether subject/object should be swapped on write.
    """
    if name in PREDICATE_ALIASES:
        return PREDICATE_ALIASES[name]
    return name, False


# Load on import so misconfiguration fails before the HTTP server accepts
# traffic. Callers may re-invoke init_registry() with an explicit path in tests.
try:
    init_registry()
except OntologyLoadError:
    # Re-raise during module import so uvicorn startup fails loudly.
    raise
