"""
TerminusDB adapter for KOI Knowledge Graph.

Provides idempotent upserts (called by outbox worker) and read queries
(called by API endpoints). Maps fuseki_uri <-> rid at the boundary.
"""

import logging
from typing import Optional

from terminusdb_client import WOQLClient

from scripts.terminusdb.schema import (
    Entity,
    Assertion,
    canonical_object_key,
    compute_assertion_hash,
    compute_schema_hash,
    serialize_object_key,
    schema,
    commit_schema,
)

logger = logging.getLogger(__name__)


def _fuseki_to_rid(entity_dict: dict) -> dict:
    """Map fuseki_uri -> rid for TerminusDB storage."""
    d = dict(entity_dict)
    if "fuseki_uri" in d and "rid" not in d:
        d["rid"] = d.pop("fuseki_uri")
    return d


def _rid_to_fuseki(tdb_dict: dict) -> dict:
    """Map rid -> fuseki_uri for API responses."""
    d = dict(tdb_dict)
    if "rid" in d and "fuseki_uri" not in d:
        d["fuseki_uri"] = d.pop("rid")
    # Strip TerminusDB internal fields
    d.pop("@id", None)
    d.pop("@type", None)
    return d


class TerminusDBAdapter:
    """Idempotent adapter for TerminusDB graph storage."""

    def __init__(self, url: str, db_name: str, team: str, key: str,
                 ensure_schema: bool = True):
        self.url = url
        self.db_name = db_name
        self.team = team
        self.key = key
        self.client = WOQLClient(url)
        self.client.connect(team=team, key=key, db=db_name)

        if ensure_schema:
            self._ensure_schema()

    def _ensure_schema(self):
        """Re-commit schema to ensure field renames (e.g. fuseki_uri→rid) are applied.

        If existing data uses old fuseki_uri field, schema commit will fail with a
        SchemaCheckFailure. In that case we log a clear error directing the user to
        re-import with --fresh, and mark the adapter as degraded (reads still work,
        writes will fail until schema is fixed).
        """
        try:
            commit_schema(self.client, "Ensure schema up-to-date (rid field)")
            logger.info("TerminusDB schema committed/verified")
            self._schema_ok = True
        except Exception as e:
            err = str(e).lower()
            if "no changes" in err or "same" in err:
                logger.debug("Schema already up-to-date")
                self._schema_ok = True
            elif "schemacheck" in err or "unknown_property" in err:
                logger.error(
                    "TerminusDB schema incompatible — existing data uses old field names. "
                    "Run: python -m scripts.terminusdb.import_from_postgres --fresh"
                )
                self._schema_ok = False
            else:
                logger.warning(f"Schema commit warning: {e}")
                self._schema_ok = True  # Optimistic — may work

    def _check_schema(self):
        """Raise if schema is known-incompatible."""
        if not getattr(self, '_schema_ok', True):
            raise RuntimeError(
                "TerminusDB schema incompatible. "
                "Run: python -m scripts.terminusdb.import_from_postgres --fresh"
            )

    # --- Idempotent upserts (called by outbox worker) ---

    def upsert_entity(self, entity_dict: dict) -> bool:
        """Upsert an entity document. Returns True if written, False if already exists."""
        self._check_schema()
        d = _fuseki_to_rid(entity_dict)
        rid = d.get("rid", "")

        ent = Entity()
        ent.rid = rid
        ent.entity_text = d.get("entity_text", "")
        ent.entity_type = d.get("entity_type", "")
        ent.normalized_text = d.get("normalized_text", "")
        ent.occurrence_count = d.get("occurrence_count", 0)
        ent.phonetic_code = d.get("phonetic_code", "")
        ent.aliases = set(d.get("aliases", []))
        ent.created_by = d.get("created_by", "")
        ent.created_at = d.get("created_at", "")
        ent.source = d.get("source", "")
        ent.first_seen_rid = d.get("first_seen_rid", "")

        try:
            self.client.update_document([ent], commit_msg=f"Upsert entity {rid[:40]}")
            return True
        except Exception as e:
            err = str(e).lower()
            if "already exists" in err or "same document" in err:
                logger.debug(f"Entity already up-to-date: {rid[:40]}")
                return False
            raise

    def upsert_assertion(self, assertion_dict: dict) -> bool:
        """Upsert an assertion document. Returns True if written."""
        self._check_schema()
        d = dict(assertion_dict)
        ahash = d.get("assertion_hash", "")

        doc = Assertion()
        for key, val in d.items():
            if hasattr(doc, key):
                setattr(doc, key, val)

        try:
            self.client.update_document([doc], commit_msg=f"Upsert assertion {ahash[:12]}")
            return True
        except Exception as e:
            err = str(e).lower()
            if "already exists" in err or "same document" in err:
                return False
            raise

    def retract_assertions_by_source(self, source_rid: str) -> int:
        """Retract (delete) all assertions from a given source_rid. Returns count deleted."""
        try:
            docs = list(self.client.query_document({
                "@type": "Assertion",
                "source_rid": source_rid,
            }))
        except Exception:
            docs = [
                d for d in self.client.get_all_documents()
                if d.get("@type", "").endswith("Assertion")
                and d.get("source_rid") == source_rid
            ]

        deleted = 0
        for doc in docs:
            doc_id = doc.get("@id")
            if doc_id:
                try:
                    self.client.delete_document([doc_id],
                                                commit_msg=f"Retract assertion from {source_rid}")
                    deleted += 1
                except Exception as e:
                    logger.warning(f"Failed to delete assertion {doc_id}: {e}")
        return deleted

    # --- Reads (called by API endpoints) ---

    def get_conflicts(self, entity_rid: Optional[str] = None,
                      limit: int = 50, offset: int = 0) -> tuple[list[dict], int]:
        """Get assertions with conflicting values for the same subject+predicate.

        Returns (conflicts_list, total_count).
        """
        try:
            query_filter = {"@type": "Assertion", "status": "active"}
            if entity_rid:
                query_filter["subject_uri"] = entity_rid
            assertions = list(self.client.query_document(query_filter))
        except Exception:
            assertions = [
                d for d in self.client.get_all_documents()
                if d.get("@type", "").endswith("Assertion")
                and d.get("status") == "active"
                and (entity_rid is None or d.get("subject_uri") == entity_rid)
            ]

        # Group by (subject_uri, predicate) and find groups with multiple distinct values
        groups: dict[tuple[str, str], list[dict]] = {}
        for a in assertions:
            key = (a.get("subject_uri", ""), a.get("predicate", ""))
            groups.setdefault(key, []).append(a)

        conflicts = []
        for (subj, pred), group in sorted(groups.items()):
            # Distinct normalized_object_key values
            distinct_values = set()
            for a in group:
                nok = a.get("normalized_object_key", "")
                if nok:
                    distinct_values.add(nok)
            if len(distinct_values) > 1:
                conflicts.append({
                    "subject_uri": subj,
                    "predicate": pred,
                    "assertion_count": len(group),
                    "distinct_values": len(distinct_values),
                    "assertions": [_rid_to_fuseki(a) for a in group],
                })

        total = len(conflicts)
        return conflicts[offset:offset + limit], total

    def get_assertions(self, entity_rid: str,
                       limit: int = 100, offset: int = 0) -> tuple[list[dict], int]:
        """Get all assertions about an entity. Returns (assertions, total_count)."""
        try:
            docs = list(self.client.query_document({
                "@type": "Assertion",
                "subject_uri": entity_rid,
            }))
        except Exception:
            docs = [
                d for d in self.client.get_all_documents()
                if d.get("@type", "").endswith("Assertion")
                and d.get("subject_uri") == entity_rid
            ]

        total = len(docs)
        page = docs[offset:offset + limit]
        return [_rid_to_fuseki(d) for d in page], total

    def get_entity(self, rid: str) -> Optional[dict]:
        """Get an entity by RID."""
        try:
            docs = list(self.client.query_document({
                "@type": "Entity",
                "rid": rid,
            }))
            if docs:
                return _rid_to_fuseki(docs[0])
        except Exception:
            pass

        # Fallback: try by document ID
        try:
            doc = self.client.get_document(f"Entity/{rid}")
            if doc:
                return _rid_to_fuseki(doc)
        except Exception:
            pass

        return None

    # --- Health/sync ---

    def health(self) -> dict:
        """Connection status, schema hash, doc counts."""
        try:
            s_hash = compute_schema_hash(self.client)
            result = {
                "terminusdb_reachable": True,
                "schema_hash": s_hash,
                "schema_ok": getattr(self, '_schema_ok', True),
            }
            if not getattr(self, '_schema_ok', True):
                result["schema_error"] = (
                    "Schema incompatible — run: "
                    "python -m scripts.terminusdb.import_from_postgres --fresh"
                )
            return result
        except Exception as e:
            return {
                "terminusdb_reachable": False,
                "error": str(e),
            }

    def schema_hash(self) -> str:
        return compute_schema_hash(self.client)
