"""
TerminusDB Schema for KOI Knowledge Graph — Phase 0 Evaluation

Defines: Entity, Assertion, SameAs, AllowedPredicate, SchemaVersion
Plus: canonical_object(), compute_assertion_hash(), canonicalize_schema()
"""

import hashlib
import json
from datetime import date
from typing import Optional, Set

from terminusdb_client import WOQLClient
from terminusdb_client.woqlschema import DocumentTemplate, LexicalKey, Schema

# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------
schema = Schema()


# ---------------------------------------------------------------------------
# Document templates
# ---------------------------------------------------------------------------

class Entity(DocumentTemplate):
    _schema = schema
    _key = LexicalKey(["rid"])

    rid: str  # Federation identity (was fuseki_uri)
    entity_text: str
    entity_type: str
    normalized_text: str
    occurrence_count: int
    phonetic_code: str
    aliases: Set[str]
    created_by: str
    created_at: str
    source: str
    first_seen_rid: str


class Assertion(DocumentTemplate):
    _schema = schema
    _key = LexicalKey(["assertion_hash"])

    # Deterministic ID
    assertion_hash: str

    subject_uri: str
    predicate: str

    # Object (entity ref or literal)
    object_kind: str            # "entity" or "literal"
    object_uri: str             # When object_kind="entity"
    literal_value: str          # When object_kind="literal"
    literal_datatype: str       # "xsd:integer", "xsd:string", "xsd:date", etc.
    literal_lang: str           # Language tag or ""

    # Provenance
    asserted_by: str            # Node RID
    asserted_at: str            # ISO timestamp (NOT part of hash)
    confidence: float
    source: str                 # "personal-vault", "federation:<peer>"
    source_rid: str             # Vault file or remote RID
    source_field: str           # YAML field name
    raw_value: str              # Original value

    status: str                 # "active", "superseded", "disputed", "retracted"

    # Precomputed for conflict queries
    normalized_object_key: str  # canonical_object_key() serialized


class SameAs(DocumentTemplate):
    _schema = schema
    _key = LexicalKey(["from_uri", "to_uri"])

    from_uri: str               # Minted or foreign URI
    to_uri: str                 # Canonical fuseki_uri
    asserted_by: str
    asserted_at: str
    confidence: float
    method: str                 # "exact", "fuzzy", "semantic", "manual"


class AllowedPredicate(DocumentTemplate):
    _schema = schema
    _key = LexicalKey(["predicate"])

    predicate: str
    description: str
    subject_types: Set[str]
    object_types: Set[str]


class SchemaVersion(DocumentTemplate):
    _schema = schema
    _key = LexicalKey(["version"])

    version: str
    schema_hash: str            # SHA256 of canonicalized schema
    committed_at: str
    description: str


# ---------------------------------------------------------------------------
# Canonical object serialization
# ---------------------------------------------------------------------------

def canonical_object(object_kind: str, object_uri: str,
                     literal_value: str, literal_datatype: str,
                     literal_lang: str = "") -> tuple:
    """Canonical serialization for hashing and conflict comparison.

    Examples:
      entity ref:  ("entity", "orn:personal-koi.entity:person-...", "", "")
      integer:     ("literal", "2017", "xsd:integer", "")
      string:      ("literal", "Costa Rica", "xsd:string", "")
      lang string: ("literal", "Costa Rica", "xsd:string", "es")
    """
    if object_kind == "entity":
        return ("entity", object_uri, "", "")
    else:
        return ("literal", str(literal_value), literal_datatype, literal_lang)


def normalize_literal(value, datatype: str) -> str:
    """Canonicalize literal by datatype before hashing.

    Ensures '2017' and '02017' and 2017 all hash identically for xsd:integer.
    """
    if datatype == "xsd:integer":
        return str(int(value))
    elif datatype in ("xsd:decimal", "xsd:float", "xsd:double"):
        return f"{float(value):.10g}"
    elif datatype == "xsd:boolean":
        return "true" if str(value).lower() in ("true", "1", "yes") else "false"
    elif datatype == "xsd:date":
        if isinstance(value, str):
            return date.fromisoformat(value).isoformat()
        return str(value)
    else:
        return str(value)


def canonical_object_key(assertion: dict) -> tuple:
    """Canonical comparison key for conflict detection.

    Uses normalize_literal() to match hash normalization.
    Accepts a dict with assertion fields.
    """
    if assertion["object_kind"] == "entity":
        return ("entity", assertion["object_uri"], "", "")
    else:
        norm = normalize_literal(assertion["literal_value"],
                                 assertion["literal_datatype"])
        return ("literal", norm, assertion["literal_datatype"],
                assertion.get("literal_lang", ""))


def serialize_object_key(key: tuple) -> str:
    """Serialize a canonical_object_key tuple to a string for storage."""
    return json.dumps(list(key), ensure_ascii=True)


# ---------------------------------------------------------------------------
# Assertion hash computation
# ---------------------------------------------------------------------------

def compute_assertion_hash(subject_uri: str, predicate: str,
                           object_kind: str, object_uri: str,
                           literal_value: str, literal_datatype: str,
                           literal_lang: str,
                           source: str, source_rid: str,
                           source_field: str, asserted_by: str) -> str:
    """Deterministic hash. Same fact from same source+asserter+evidence = same hash.

    Includes source_rid and source_field so distinct evidence from the same
    source/asserter produces separate assertions.
    Excludes asserted_at for idempotency across re-import/federation.
    """
    if object_kind == "literal":
        norm_value = normalize_literal(literal_value, literal_datatype)
    else:
        norm_value = ""
    obj = canonical_object(object_kind, object_uri, norm_value,
                           literal_datatype, literal_lang)
    payload = json.dumps(
        [subject_uri, predicate, list(obj),
         source, source_rid or "", source_field or "", asserted_by],
        sort_keys=True, ensure_ascii=True
    )
    return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Status lifecycle validation
# ---------------------------------------------------------------------------

VALID_TRANSITIONS = {
    "active":     {"superseded", "disputed", "retracted"},
    "disputed":   {"active", "retracted"},
    "superseded": set(),   # terminal
    "retracted":  set(),   # terminal
}

VALID_STATUSES = set(VALID_TRANSITIONS.keys())


def validate_status_transition(from_status: str, to_status: str) -> bool:
    """Check whether a status transition is allowed."""
    if from_status not in VALID_TRANSITIONS:
        return False
    return to_status in VALID_TRANSITIONS[from_status]


# ---------------------------------------------------------------------------
# Schema canonicalization
# ---------------------------------------------------------------------------

VOLATILE_KEYS = {
    "@metadata", "_last_modified", "_created", "_updated",
    "_instance_count", "_system",
}


def strip_volatile(obj):
    """Recursively strip volatile/non-semantic keys at all depths."""
    if isinstance(obj, dict):
        return {k: strip_volatile(v) for k, v in sorted(obj.items())
                if k not in VOLATILE_KEYS}
    elif isinstance(obj, list):
        return [strip_volatile(item) for item in obj]
    return obj


def canonicalize_schema(client: WOQLClient) -> str:
    """Get schema docs, recursively strip volatile fields, produce stable canonical form."""
    schema_docs = list(client.get_all_documents(graph_type="schema"))
    cleaned = [strip_volatile(doc) for doc in
               sorted(schema_docs, key=lambda d: d.get("@id", ""))]
    return json.dumps(cleaned, sort_keys=True, ensure_ascii=True)


def compute_schema_hash(client: WOQLClient) -> str:
    """SHA256 of the canonicalized schema."""
    return hashlib.sha256(canonicalize_schema(client).encode()).hexdigest()


def preflight_schema_check(local_client: WOQLClient, remote_client: WOQLClient):
    """Verify schemas match before push/pull. Raises on mismatch."""
    local_hash = compute_schema_hash(local_client)
    remote_hash = compute_schema_hash(remote_client)
    if local_hash != remote_hash:
        raise SchemaVersionMismatch(
            f"Schema hashes differ: local={local_hash[:12]}... "
            f"remote={remote_hash[:12]}... "
            f"Align schemas before push/pull."
        )


class SchemaVersionMismatch(Exception):
    pass


# ---------------------------------------------------------------------------
# RID validation
# ---------------------------------------------------------------------------

def validate_rid(uri: str) -> bool:
    """Validate that a URI looks like a valid RID."""
    if not uri or not isinstance(uri, str):
        return False
    return uri.startswith("orn:") and len(uri) > 4


# ---------------------------------------------------------------------------
# Helper: commit schema to a client
# ---------------------------------------------------------------------------

def commit_schema(client: WOQLClient, msg: str = "Commit KOI schema"):
    """Commit the module-level schema to the connected database."""
    schema.commit(client, commit_msg=msg)
