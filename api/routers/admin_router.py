"""Admin endpoints — entity merge / redirect.

Provides ``POST /entities/merge`` (service-token gated): merge a *loser* entity
into a *survivor*, rewiring every entity-URI reference in one transaction, then
TOMBSTONING the loser (``entity_registry.merged_into = survivor``) instead of
hard-deleting it. The loser is never deleted because the ON DELETE CASCADE FKs
on ``entity_relationships`` / ``pending_relationships`` would otherwise wipe the
very rows the merge just rewired.

Also exposes ``GET /entities/{uri}/resolve`` to follow a merged_into redirect
chain to the canonical live URI.

Routes are prefix-relative — prefix "/entities" is applied at mount in
personal_ingest_api.py.

A "retype" (e.g. a Concept that should be a Person) is expressed as a merge:
the entity type is encoded in the fuseki_uri prefix (…entity:organization-… vs
…entity:person-…), so merging into a correctly-typed survivor URI re-types it.

Reference surface (verified live against personal_koi 2026-05-29, all keyed by
entity_registry.fuseki_uri):

  Collision-prone (rewire-then-dedup; have a UNIQUE constraint a blind rewrite
  could violate):
    * entity_relationships  UNIQUE(subject_uri, predicate, object_uri)  [FK CASCADE]
    * document_entity_links UNIQUE(document_rid, entity_uri)            [additive mention_count]
    * pending_relationships UNIQUE(COALESCE(subject_uri,''),
                                   COALESCE(object_uri,''), predicate,
                                   raw_unknown_label, unknown_side)     [FK CASCADE]

  Plain UPDATE (no UNIQUE on the URI columns):
    knowledge_facts(subject_uri,object_uri), claims(entity_uri,claimant_uri,
    operator_uri), claim_attestations(reviewer_uri), entity_rid_mappings(
    canonical_uri), intent_registry(entity_uri,publisher_uri), task_registry(
    owner_uri,project_uri), commitments(pledger_uri,evidence_uri),
    commitment_pools(steward_uri,bioregion_uri), signals(subject_uri),
    requirements(subject_uri), assertion_history(subject,object_uri)*,
    koi_extraction_records(subject_uri,object_uri)*    (* RDF mirror, empty today)

  Array columns (array_replace):
    claim_attestations.evidence_uris, task_registry.collaborator_uris

  JSONB metadata (text-replace of the embedded URI):
    claims.metadata + the other *.metadata columns + entity_registry.metadata
    (loser row excluded)

  Aliases: the loser's entity_text + aliases are unioned into the survivor's
  aliases so the survivor stays findable by the loser's name(s).
"""

import json
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.auth_deps import make_service_token_auth
from api.resolution_primitives import normalize_alias_list
from api.merge_reversal import (
    REVERSAL_SCHEMA, capture_reversal, unmerge, persona_merge_hazard,
)

logger = logging.getLogger(__name__)


# Plain text columns holding an entity fuseki_uri, with no UNIQUE constraint on
# the column — a blind UPDATE loser->survivor cannot violate a constraint.
# (table, column)
_PLAIN_REF_COLS: List[tuple] = [
    ("knowledge_facts", "subject_uri"),
    ("knowledge_facts", "object_uri"),
    ("claims", "entity_uri"),
    ("claims", "claimant_uri"),
    ("claims", "operator_uri"),         # RESTRICT FK -> entity_registry; survivor exists, safe
    ("claim_attestations", "reviewer_uri"),  # RESTRICT FK; safe
    ("entity_rid_mappings", "canonical_uri"),
    ("intent_registry", "entity_uri"),
    ("intent_registry", "publisher_uri"),
    ("task_registry", "owner_uri"),
    ("task_registry", "project_uri"),
    ("commitments", "pledger_uri"),
    ("commitments", "evidence_uri"),
    ("commitment_pools", "steward_uri"),
    ("commitment_pools", "bioregion_uri"),
    ("signals", "subject_uri"),
    ("requirements", "subject_uri"),
    ("assertion_history", "subject"),       # RDF mirror (empty today); column is `subject`
    ("assertion_history", "object_uri"),
    ("koi_extraction_records", "subject_uri"),
    ("koi_extraction_records", "object_uri"),
]

# text[] columns holding entity URIs — rewrite with array_replace.
_ARRAY_REF_COLS: List[tuple] = [
    ("claim_attestations", "evidence_uris"),
    ("task_registry", "collaborator_uris"),
]

# jsonb columns that may embed an entity URI as a string value/substring.
# Rewritten with a guarded text-REPLACE (URIs carry a unique hash suffix, so the
# loser URI is never a substring of a different entity's URI — REPLACE is safe).
_JSONB_METADATA_COLS: List[tuple] = [
    ("claims", "metadata"),
    ("claim_attestations", "metadata"),
    ("commitments", "metadata"),
    ("commitment_pools", "metadata"),
    ("intent_registry", "metadata"),
    ("requirements", "metadata"),
    ("signals", "metadata"),
    ("task_registry", "metadata"),
]


def _count(status: str) -> int:
    """Parse the affected-row count out of an asyncpg command status string.

    asyncpg returns e.g. ``'UPDATE 5'`` / ``'DELETE 2'`` from ``conn.execute``.
    """
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


# --- Closure ---------------------------------------------------------------
# The status lattice. Derived from the verdicts the research record actually
# produced, not invented up front:
#   INCONCLUSIVE — tested, but the instrument could not decide (e.g. the map
#                  construction was gauge-flat, so holonomy was zero *by
#                  construction* and the claim never got a fair test)
#   SCOPED       — dead in one regime, still standing in another. The load-bearing
#                  value: a binary OPEN/REFUTED forces a wrong answer for exactly
#                  the claims that matter ("dead on the single embedding-derived
#                  concept graph, still open on federated cross-schema data" is
#                  neither), and collapsing it to either loses the finding.
_CLOSURE_STATUSES = frozenset(
    {"OPEN", "SUPPORTED", "REFUTED", "SCOPED", "SUPERSEDED", "INCONCLUSIVE"})


class EntityCloseRequest(BaseModel):
    status: str = Field(..., description="One of: " + ", ".join(sorted(_CLOSURE_STATUSES)))
    rationale: str = Field(
        ..., min_length=1,
        description="Why. A closure act with no reason is not reviewable.")
    scope: Optional[str] = Field(
        None,
        description="REQUIRED when status=SCOPED: the regime in which the claim STILL STANDS.")
    evidence_uris: List[str] = Field(
        default_factory=list,
        description="What closed it: RIDs, arXiv URLs, repo paths, commit SHAs. Empty = a "
                    "bare assertion of authority (permitted, but legible as such).")
    authority: Optional[str] = Field(
        None, description="Under what authority the act was taken.")
    closed_by: Optional[str] = Field(
        None, description="Audit actor; defaults to the authenticated identity")
    dry_run: bool = Field(
        False, description="Validate + report inside a transaction, then ROLL BACK.")


class ClosureAct(BaseModel):
    id: Optional[int] = None
    status: str
    rationale: str
    scope: Optional[str] = None
    evidence_uris: List[str] = []
    authority: Optional[str] = None
    closed_by: str
    closed_at: Optional[str] = None


class EntityCloseResponse(BaseModel):
    entity_uri: str
    entity_text: Optional[str] = None
    dry_run: bool
    applied: bool
    previous_status: Optional[str] = None   # None = never closed before
    closure: Optional[ClosureAct] = None    # the act just recorded (= the new projection)
    closure_log_id: Optional[int] = None
    message: Optional[str] = None


class EntityClosureResponse(BaseModel):
    entity_uri: str
    entity_text: Optional[str] = None
    current: Optional[ClosureAct] = None    # the projection: latest act (None = never closed)
    history: List[ClosureAct] = []          # the append-only log, newest first
    is_closed: bool = False                 # standing is anything other than OPEN/never-closed


class ReprojectResponse(BaseModel):
    dry_run: bool
    entities_with_log: int                  # entities that have >=1 closure act
    healed: int                             # projections rebuilt (were missing or stale)
    already_current: int                    # projections that already matched the log
    healed_uris: List[str] = []


def _act_from_row(row) -> ClosureAct:
    """Build a ClosureAct from an entity_closure_log row."""
    return ClosureAct(
        id=row["id"],
        status=row["status"],
        rationale=row["rationale"],
        scope=row["scope"],
        evidence_uris=list(row["evidence_uris"] or []),
        authority=row["authority"],
        closed_by=row["closed_by"],
        closed_at=row["closed_at"].isoformat() if row["closed_at"] else None,
    )


class EntityUnmergeRequest(BaseModel):
    merge_log_id: int = Field(
        ..., description="entity_merge_log.id of the merge to reverse")
    unmerged_by: Optional[str] = Field(
        None, description="Audit actor; defaults to the authenticated identity")
    dry_run: bool = Field(
        ...,
        description="REQUIRED — no default, matching /merge. If true, perform the full "
                    "reversal inside a transaction, report per-table restored counts, then "
                    "ROLL BACK. Merges predating migration 118 (2026-09-02) are REFUSED "
                    "in either mode: the old log recorded only counts, so the row "
                    "identities an exact undo needs no longer exist, and a best-effort "
                    "reversal would repoint arbitrary rows and manufacture wrong "
                    "provenance while reporting success.")


class EntityUnmergeResponse(BaseModel):
    merge_log_id: int
    loser: str
    survivor: str
    dry_run: bool
    applied: bool
    restored: Dict[str, int] = {}
    not_reversed: Dict[str, Any] = {}


class EntityMergeRequest(BaseModel):
    allow_persona_merge: bool = Field(
        False,
        description="Override the persona guard. A '(via Platform)' row whose principal "
                    "has NO independent live row is the ONLY record of that referent -- "
                    "merging it erases a real person rather than deduplicating one. "
                    "21 Person rows and 9 Claim rows are in that state (measured "
                    "2026-09-02). Set true only after confirming the survivor is the "
                    "same referent.")
    survivor_uri: str = Field(..., description="fuseki_uri of the entity to KEEP")
    loser_uri: str = Field(..., description="fuseki_uri of the entity to merge in + tombstone")
    merged_by: Optional[str] = Field(
        None, description="Audit actor; defaults to the authenticated identity")
    dry_run: bool = Field(
        ...,
        description="REQUIRED — no default. If true, perform the full rewire inside a "
                    "transaction, report the per-table counts, then ROLL BACK — nothing is "
                    "committed. If false, the merge is applied and committed "
                    "(loser tombstoned; rewire is NOT easily reversible — colliding rows in "
                    "UNIQUE-constrained tables are deduped/deleted outright and only "
                    "per-table counts are logged, not row-level before-state). Every caller "
                    "must explicitly choose; there is no safe default for this operation.")


class EntityMergeResponse(BaseModel):
    survivor_uri: str
    loser_uri: str
    dry_run: bool
    applied: bool                         # True only if committed
    already_merged: bool = False
    rewired: Dict[str, Any] = {}          # per-table {rewired, deduped, merged, self_loops_deleted}
    total_refs_rewired: int = 0
    survivor_aliases_after: Optional[List[str]] = None
    merge_log_id: Optional[int] = None
    message: Optional[str] = None


class EntityRetypeRequest(BaseModel):
    uri: str = Field(..., description="fuseki_uri of the entity to re-type")
    new_type: str = Field(..., description="Target entity type (canonicalized server-side)")
    retyped_by: Optional[str] = Field(
        None, description="Audit actor; defaults to the authenticated identity")
    dry_run: bool = Field(
        False,
        description="If true, perform the full retype inside a transaction, report "
                    "what would change, then ROLL BACK — nothing is committed.")


class EntityRetypeResponse(BaseModel):
    old_uri: str
    new_uri: str
    old_type: Optional[str]
    new_type: str
    merged_into_existing: bool = False    # True when a live twin already held new_uri
    rewired: Dict[str, Any] = {}
    merge_log_id: Optional[int] = None
    applied: bool = False                 # True only if committed
    dry_run: bool = False
    already_typed: bool = False           # True when entity already has new_type (no-op)
    message: Optional[str] = None


def create_router(pool) -> APIRouter:
    """Return an APIRouter for admin (entity-merge) endpoints."""
    router = APIRouter(tags=["admin"])
    require_service_auth = make_service_token_auth(pool)

    async def _do_merge(conn, survivor: str, loser: str, merged_by: str) -> Dict[str, Any]:
        """Rewire every reference loser->survivor inside the caller's transaction.

        Returns the per-table counts dict. Assumes validation already passed
        (both exist, survivor live, loser not already merged, survivor != loser).
        """
        rewired: Dict[str, Any] = {}

        # --- 0. Union the loser's name + aliases into the survivor's aliases so
        #        the survivor stays findable by the loser's name(s). Mirrors the
        #        register_redirect_alias idiom in api/mediawiki_ingest.py.
        loser_row = await conn.fetchrow(
            "SELECT entity_text, COALESCE(aliases, '{}') AS aliases "
            "FROM entity_registry WHERE fuseki_uri = $1", loser)
        loser_text = loser_row["entity_text"] if loser_row else None
        loser_aliases = list(loser_row["aliases"]) if loser_row else []
        add_aliases = normalize_alias_list([loser_text] + loser_aliases)
        await conn.execute("""
            UPDATE entity_registry
            SET aliases = (
                SELECT ARRAY(
                    SELECT DISTINCT e FROM unnest(
                        array_cat(COALESCE(aliases, '{}'), $2::text[])
                    ) AS e
                    WHERE e IS NOT NULL AND e <> ''
                )
            )
            WHERE fuseki_uri = $1
        """, survivor, add_aliases)

        # --- 1. entity_relationships — rewire-then-dedup on UNIQUE(subj,pred,obj).
        #        NULL object_uri rows are never deduped: Postgres UNIQUE treats
        #        NULLs as distinct, so a blind rewrite of them can't collide.
        er: Dict[str, int] = {}
        er["subject_deduped"] = _count(await conn.execute("""
            DELETE FROM entity_relationships el
            WHERE el.subject_uri = $1
              AND EXISTS (SELECT 1 FROM entity_relationships e2
                          WHERE e2.subject_uri = $2
                            AND e2.predicate = el.predicate
                            AND e2.object_uri = el.object_uri)
        """, loser, survivor))
        er["subject_rewired"] = _count(await conn.execute(
            "UPDATE entity_relationships SET subject_uri = $2 WHERE subject_uri = $1",
            loser, survivor))
        er["object_deduped"] = _count(await conn.execute("""
            DELETE FROM entity_relationships el
            WHERE el.object_uri = $1
              AND EXISTS (SELECT 1 FROM entity_relationships e2
                          WHERE e2.object_uri = $2
                            AND e2.predicate = el.predicate
                            AND e2.subject_uri = el.subject_uri)
        """, loser, survivor))
        er["object_rewired"] = _count(await conn.execute(
            "UPDATE entity_relationships SET object_uri = $2 WHERE object_uri = $1",
            loser, survivor))
        er["self_loops_deleted"] = _count(await conn.execute(
            "DELETE FROM entity_relationships WHERE subject_uri = $1 AND object_uri = $1",
            survivor))
        rewired["entity_relationships"] = er

        # --- 2. document_entity_links — UNIQUE(document_rid, entity_uri),
        #        mention_count is additive. Fold the loser's count into a
        #        colliding survivor row, drop the colliding loser rows, then
        #        rewrite the rest.
        del_: Dict[str, int] = {}
        del_["merged"] = _count(await conn.execute("""
            UPDATE document_entity_links s
            SET mention_count = COALESCE(s.mention_count, 0) + COALESCE(l.mention_count, 0)
            FROM document_entity_links l
            WHERE l.entity_uri = $1 AND s.entity_uri = $2
              AND s.document_rid = l.document_rid
        """, loser, survivor))
        del_["deduped"] = _count(await conn.execute("""
            DELETE FROM document_entity_links l
            WHERE l.entity_uri = $1
              AND EXISTS (SELECT 1 FROM document_entity_links s
                          WHERE s.entity_uri = $2 AND s.document_rid = l.document_rid)
        """, loser, survivor))
        del_["rewired"] = _count(await conn.execute(
            "UPDATE document_entity_links SET entity_uri = $2 WHERE entity_uri = $1",
            loser, survivor))
        rewired["document_entity_links"] = del_

        # --- 3. pending_relationships — rewire-then-dedup. The UNIQUE index
        #        COALESCEs NULL subject/object to '', so dedup must match with
        #        the same COALESCE semantics (NULLs collide).
        pr: Dict[str, int] = {}
        pr["subject_deduped"] = _count(await conn.execute("""
            DELETE FROM pending_relationships el
            WHERE el.subject_uri = $1
              AND EXISTS (SELECT 1 FROM pending_relationships e2
                          WHERE e2.subject_uri = $2
                            AND e2.predicate = el.predicate
                            AND COALESCE(e2.object_uri,'') = COALESCE(el.object_uri,'')
                            AND e2.raw_unknown_label = el.raw_unknown_label
                            AND e2.unknown_side = el.unknown_side)
        """, loser, survivor))
        pr["subject_rewired"] = _count(await conn.execute(
            "UPDATE pending_relationships SET subject_uri = $2 WHERE subject_uri = $1",
            loser, survivor))
        pr["object_deduped"] = _count(await conn.execute("""
            DELETE FROM pending_relationships el
            WHERE el.object_uri = $1
              AND EXISTS (SELECT 1 FROM pending_relationships e2
                          WHERE e2.object_uri = $2
                            AND e2.predicate = el.predicate
                            AND COALESCE(e2.subject_uri,'') = COALESCE(el.subject_uri,'')
                            AND e2.raw_unknown_label = el.raw_unknown_label
                            AND e2.unknown_side = el.unknown_side)
        """, loser, survivor))
        pr["object_rewired"] = _count(await conn.execute(
            "UPDATE pending_relationships SET object_uri = $2 WHERE object_uri = $1",
            loser, survivor))
        pr["self_loops_deleted"] = _count(await conn.execute(
            "DELETE FROM pending_relationships WHERE subject_uri = $1 AND object_uri = $1",
            survivor))
        rewired["pending_relationships"] = pr

        # Deployments differ: a fresh personal install has no signals or requirements table.
        # A failed statement would abort the merge's transaction, so check first, as
        # capture_reversal does.
        async def _present(table: str, col: str) -> bool:
            return await conn.fetchval(
                "SELECT to_regclass($1) IS NOT NULL "
                "AND EXISTS (SELECT 1 FROM information_schema.columns "
                "            WHERE table_name = $2 AND column_name = $3)",
                table, table, col)

        # --- 4. Plain text columns (no UNIQUE on the URI column).
        for table, col in _PLAIN_REF_COLS:
            if not await _present(table, col):
                continue
            n = _count(await conn.execute(
                f"UPDATE {table} SET {col} = $2 WHERE {col} = $1", loser, survivor))
            if n:
                rewired.setdefault("plain", {})[f"{table}.{col}"] = n

        # --- 5. Array columns (text[]).
        for table, col in _ARRAY_REF_COLS:
            if not await _present(table, col):
                continue
            n = _count(await conn.execute(
                f"UPDATE {table} SET {col} = array_replace({col}, $1, $2) "
                f"WHERE $1 = ANY({col})", loser, survivor))
            if n:
                rewired.setdefault("arrays", {})[f"{table}.{col}"] = n

        # --- 6. JSONB metadata text-replace (guarded by LIKE).
        for table, col in _JSONB_METADATA_COLS:
            if not await _present(table, col):
                continue
            n = _count(await conn.execute(
                f"UPDATE {table} SET {col} = REPLACE({col}::text, $1, $2)::jsonb "
                f"WHERE {col}::text LIKE '%' || $1 || '%'", loser, survivor))
            if n:
                rewired.setdefault("jsonb_metadata", {})[f"{table}.{col}"] = n
        # entity_registry.metadata for OTHER rows (never the loser's own row).
        n = _count(await conn.execute(
            "UPDATE entity_registry SET metadata = REPLACE(metadata::text, $1, $2)::jsonb "
            "WHERE fuseki_uri <> $1 AND metadata::text LIKE '%' || $1 || '%'",
            loser, survivor))
        if n:
            rewired.setdefault("jsonb_metadata", {})["entity_registry.metadata"] = n

        # --- 7. Tombstone the loser (NEVER hard-delete — FK CASCADE would wipe
        #        the relationships just rewired).
        rewired["loser_tombstoned"] = _count(await conn.execute(
            "UPDATE entity_registry SET merged_into = $2, merged_at = NOW(), merged_by = $3 "
            "WHERE fuseki_uri = $1 AND merged_into IS NULL", loser, survivor, merged_by))

        return rewired

    async def _do_retype(
        conn, old_uri: str, old_type: Optional[str], new_type: str, retyped_by: str,
    ) -> tuple:
        """Re-type an entity inside the caller's transaction.

        Strategy (mirrors the operator's manual merge-into-correctly-typed-survivor
        procedure): the entity type is encoded in the fuseki_uri prefix, so a
        retype is a URI change. We (a) mint a new registry row at the new-typed
        URI copying the source row's data, then (b) reuse ``_do_merge`` to rewire
        every reference old->new and tombstone the old row. When a live row
        already occupies the new URI, we skip the insert and merge straight into
        it. When the deterministic URI is unchanged (type label differs only by
        case/prefix), we update the type in place.

        Assumes validation already passed (row exists, not tombstoned, new_type
        canonical + != old_type). Returns
        ``(new_uri, merged_into_existing, rewired, reversal)``.

        The 4th element is the undo payload, and it is built here rather than in
        ``_do_merge`` because three of the four branches do something _do_merge
        cannot see: the in-place branch performs no merge at all, the mint branch
        CREATES the survivor (so an undo must delete it), and the resurrection
        branch revives a tombstone (so an undo must re-tombstone it). Wiring the
        capture into _do_merge instead would silently miss the in-place branch
        and would still leave the mint branch's orphan behind.
        """
        # Deferred imports: generate_entity_uri lives in personal_ingest_api,
        # which imports this module (circular) — import inside the function body.
        from api.personal_ingest_api import (
            generate_entity_uri, get_first_significant_token, get_phonetic_code,
        )
        from api.entity_schema import get_schema_for_type

        src = await conn.fetchrow(
            "SELECT entity_text, normalized_text, koi_rid, wallet_address, phonetic_code "
            "FROM entity_registry WHERE fuseki_uri = $1", old_uri)
        entity_text = src["entity_text"]
        normalized = src["normalized_text"]

        new_uri = generate_entity_uri(entity_text, new_type)

        # Recompute phonetic_code for the NEW type's schema (Person-style types
        # index on it; non-phonetic types store NULL).
        new_schema = get_schema_for_type(new_type)
        phonetic_code = None
        if getattr(new_schema, "phonetic_matching", False):
            first_token = get_first_significant_token(
                normalized, new_schema.phonetic_stopwords)
            phonetic_code = get_phonetic_code(first_token)

        # Type label change that does NOT alter the deterministic URI (e.g. a
        # case-only difference like 'person' -> 'Person'): update in place.
        if new_uri == old_uri:
            await conn.execute(
                "UPDATE entity_registry SET entity_type = $1, phonetic_code = $2 "
                "WHERE fuseki_uri = $3", new_type, phonetic_code, old_uri)
            n_rid = _count(await conn.execute(
                "UPDATE entity_rid_mappings SET entity_type = $1 WHERE canonical_uri = $2",
                new_type, old_uri))
            rewired = {
                "in_place_type_update": 1,
                "entity_rid_mappings_type_updated": n_rid,
                "retype": {"from": old_type, "to": new_type},
            }
            # No merge occurred, so there are no rows to repoint and no tombstone
            # to lift -- but the type change still has to be undoable, and
            # unmerge() branches on this to skip the merge-undo entirely.
            reversal = {
                "schema": REVERSAL_SCHEMA,
                "loser": old_uri,
                "survivor": old_uri,
                "refs": {},
                "aliases_added": [],
                "retype": {
                    "branch": "in_place",
                    "from": old_type,
                    "to": new_type,
                    "prev_phonetic_code": src["phonetic_code"],
                },
            }
            return old_uri, False, rewired, reversal

        # merged_at/merged_by/entity_type/phonetic_code are read for the
        # resurrection branch below: reviving a tombstone is only undoable if the
        # state being overwritten was recorded first.
        twin = await conn.fetchrow(
            "SELECT merged_into, merged_at, merged_by, entity_type, phonetic_code "
            "FROM entity_registry WHERE fuseki_uri = $1", new_uri)

        retype_block: Dict[str, Any] = {
            "branch": None,
            "from": old_type,
            "to": new_type,
            "survivor_minted": False,
            "survivor_resurrected": False,
            "moved_koi_rid": None,
            "moved_wallet": None,
        }
        reversal: Dict[str, Any] = {}

        merged_into_existing = False
        if twin is not None and twin["merged_into"] is None:
            # A live entity already occupies the new-typed URI — merge into it.
            merged_into_existing = True
            retype_block["branch"] = "live_twin"
            reversal = await capture_reversal(conn, loser=old_uri, survivor=new_uri)
            rewired = await _do_merge(conn, survivor=new_uri, loser=old_uri, merged_by=retyped_by)
        elif twin is not None:
            # A TOMBSTONED row occupies the new-typed URI. The old code fell
            # through to the INSERT below and died on
            # entity_registry_fuseki_uri_key, making the retype permanently
            # impossible — a tombstone squatted the canonical slot forever.
            # Observed 2026-08-14 on both Signal and Notion.
            #
            # Follow the tombstone's merged_into chain. If it leads back to the
            # row being retyped, the tombstone is this entity's own former self:
            # an earlier merge ran in the opposite direction (e.g. Concept was
            # merged INTO SoftwareApplication). Resurrecting it and merging back
            # is exactly the reversal of that merge, so it is safe.
            #
            # If the chain leads anywhere else, the slot belongs to a different
            # entity's history and we must not guess — fail loudly.
            survivor_of_twin = twin["merged_into"]
            seen = {new_uri}
            while survivor_of_twin is not None and survivor_of_twin not in seen:
                seen.add(survivor_of_twin)
                nxt = await conn.fetchrow(
                    "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1",
                    survivor_of_twin)
                if nxt is None or nxt["merged_into"] is None:
                    break
                survivor_of_twin = nxt["merged_into"]

            if survivor_of_twin != old_uri:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"cannot retype to {new_uri}: that URI is held by a tombstoned "
                        f"row whose surviving entity is {survivor_of_twin}, not the row "
                        f"being retyped ({old_uri}). Merge the two entities explicitly "
                        f"first if they are the same thing."
                    ),
                )

            retype_block.update({
                "branch": "resurrected_tombstone",
                "survivor_resurrected": True,
                "resurrected_merged_into": twin["merged_into"],
                "resurrected_merged_at": twin["merged_at"],
                "resurrected_merged_by": twin["merged_by"],
                "resurrected_entity_type": twin["entity_type"],
                "resurrected_phonetic_code": twin["phonetic_code"],
            })
            await conn.execute(
                "UPDATE entity_registry "
                "SET merged_into = NULL, merged_at = NULL, merged_by = NULL, "
                "    entity_type = $1, phonetic_code = $2 "
                "WHERE fuseki_uri = $3",
                new_type, phonetic_code, new_uri)
            merged_into_existing = True
            reversal = await capture_reversal(conn, loser=old_uri, survivor=new_uri)
            rewired = await _do_merge(
                conn, survivor=new_uri, loser=old_uri, merged_by=retyped_by)
            rewired["resurrected_tombstone"] = 1
        else:
            # Mint the new-typed row by copying the source row server-side.
            # koi_rid + wallet_address carry UNIQUE partial indexes and the old
            # row is only tombstoned (not deleted) by _do_merge, so we MOVE
            # them: null them on the source first, then set them on the new row.
            moved_koi_rid = src["koi_rid"]
            moved_wallet = src["wallet_address"]
            if moved_koi_rid is not None or moved_wallet is not None:
                await conn.execute(
                    "UPDATE entity_registry SET koi_rid = NULL, wallet_address = NULL "
                    "WHERE fuseki_uri = $1", old_uri)
            await conn.execute("""
                INSERT INTO entity_registry (
                    fuseki_uri, entity_text, entity_type, normalized_text,
                    ledger_id, metadata_iri, admin_address, aliases, jurisdiction,
                    class_id, source, first_seen_rid, metadata, embedding,
                    vault_rid, phonetic_code, node_private, wallet_address, koi_rid,
                    description, embedding_3072, resolution_tier
                )
                SELECT $1, entity_text, $2, normalized_text,
                       ledger_id, metadata_iri, admin_address, aliases, jurisdiction,
                       class_id, source, first_seen_rid, metadata, embedding,
                       vault_rid, $3, node_private, $4, $5,
                       description, embedding_3072, 'manual'
                FROM entity_registry WHERE fuseki_uri = $6
            """, new_uri, new_type, phonetic_code, moved_wallet, moved_koi_rid, old_uri)
            retype_block.update({
                "branch": "mint",
                "survivor_minted": True,
                "moved_koi_rid": moved_koi_rid,
                "moved_wallet": moved_wallet,
            })
            # Captured AFTER the INSERT (the survivor must exist to read its
            # aliases) but BEFORE _do_merge rewires anything.
            reversal = await capture_reversal(conn, loser=old_uri, survivor=new_uri)
            rewired = await _do_merge(conn, survivor=new_uri, loser=old_uri, merged_by=retyped_by)

        # _do_merge rewrites entity_rid_mappings.canonical_uri old->new but NOT
        # the type column — bring it to the new type here.
        n_rid = _count(await conn.execute(
            "UPDATE entity_rid_mappings SET entity_type = $1 WHERE canonical_uri = $2",
            new_type, new_uri))
        rewired["entity_rid_mappings_type_updated"] = n_rid
        rewired["retype"] = {"from": old_type, "to": new_type}
        reversal["retype"] = retype_block
        return new_uri, merged_into_existing, rewired, reversal

    def _total(rewired: Dict[str, Any]) -> int:
        total = 0
        for v in rewired.values():
            if isinstance(v, dict):
                total += sum(x for x in v.values() if isinstance(x, int))
            elif isinstance(v, int):
                total += v
        return total

    @router.post("/unmerge", response_model=EntityUnmergeResponse)
    async def unmerge_entities(
        body: EntityUnmergeRequest,
        identity: str = Depends(require_service_auth),
    ):
        """Reverse a merge recorded with reversal data (migration 118+).

        This route exists because the capability without it is unreachable:
        unmerge() was written, tested and committed with no HTTP surface at all
        -- a correctly-built function with no caller, which is the same defect
        this session spent the day cataloguing elsewhere. Caught by the operator
        asking whether the route appeared in /openapi.json.
        """
        async with pool.acquire() as conn:
            tx = conn.transaction()
            await tx.start()
            try:
                result = await unmerge(
                    conn, body.merge_log_id, body.unmerged_by or identity)
                # Surface what the reversal did NOT restore, so a caller is never
                # told "reverted" when array/jsonb/no-PK references still point at
                # the survivor.
                rev = await conn.fetchval(
                    "SELECT reversal FROM entity_merge_log WHERE id = $1",
                    body.merge_log_id)
                if isinstance(rev, str):
                    rev = json.loads(rev)
                not_reversed = (rev or {}).get("not_reversed", {})

                if body.dry_run:
                    await tx.rollback()
                else:
                    await tx.commit()
            except ValueError as e:
                await tx.rollback()
                raise HTTPException(status_code=409, detail=str(e))
            except Exception:
                await tx.rollback()
                raise

        return EntityUnmergeResponse(
            merge_log_id=body.merge_log_id,
            loser=result["loser"],
            survivor=result["survivor"],
            dry_run=body.dry_run,
            applied=not body.dry_run,
            restored=result["restored"],
            not_reversed=not_reversed,
        )

    @router.post("/merge", response_model=EntityMergeResponse)
    async def merge_entities(
        body: EntityMergeRequest,
        identity: str = Depends(require_service_auth),
    ):
        survivor = body.survivor_uri
        loser = body.loser_uri
        merged_by = body.merged_by or identity

        if survivor == loser:
            raise HTTPException(status_code=400,
                                detail="survivor_uri and loser_uri are identical")

        async with pool.acquire() as conn:
            s_row = await conn.fetchrow(
                "SELECT fuseki_uri, merged_into FROM entity_registry WHERE fuseki_uri = $1",
                survivor)
            l_row = await conn.fetchrow(
                "SELECT fuseki_uri, merged_into FROM entity_registry WHERE fuseki_uri = $1",
                loser)
            if s_row is None:
                raise HTTPException(status_code=404, detail=f"survivor not found: {survivor}")
            if l_row is None:
                raise HTTPException(status_code=404, detail=f"loser not found: {loser}")

            # Survivor must be live (not itself a tombstone) — merging into a
            # tombstone would create a stale chain.
            if s_row["merged_into"] is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"survivor is itself merged into {s_row['merged_into']}; "
                           f"merge into the live entity instead")

            # Idempotency on the loser.
            if l_row["merged_into"] is not None:
                if l_row["merged_into"] == survivor:
                    prior = await conn.fetchrow(
                        "SELECT id, rewired FROM entity_merge_log "
                        "WHERE loser_uri = $1 AND survivor_uri = $2 "
                        "ORDER BY merged_at DESC LIMIT 1", loser, survivor)
                    # asyncpg returns the jsonb `rewired` column as a JSON string;
                    # decode it back to a dict for the response model.
                    prior_rewired: Dict[str, Any] = {}
                    if prior and prior["rewired"]:
                        raw = prior["rewired"]
                        prior_rewired = json.loads(raw) if isinstance(raw, str) else raw
                    return EntityMergeResponse(
                        survivor_uri=survivor, loser_uri=loser, dry_run=body.dry_run,
                        applied=False, already_merged=True,
                        rewired=prior_rewired,
                        merge_log_id=(prior["id"] if prior else None),
                        message="loser already merged into survivor (no-op)")
                raise HTTPException(
                    status_code=409,
                    detail=f"loser already merged into {l_row['merged_into']} "
                           f"(not {survivor})")

            tx = conn.transaction()
            await tx.start()
            try:
                # Capture the undo BEFORE rewiring. A merge is a blind UPDATE:
                # afterwards the loser's rows are indistinguishable from rows
                # the survivor already held, so the identities an undo needs are
                # destroyed by the act of merging. This is deliberately NOT in a
                # try/except -- a merge that cannot be reversed must fail rather
                # than silently record itself as one that can. That is the whole
                # difference between this and the 262 pre-118 merges.
                # The checks above ran without locks. Lock both rows in a fixed order and
                # re-check, so opposing merges cannot each tombstone the other's survivor.
                locked = {r["fuseki_uri"]: r["merged_into"] for r in await conn.fetch(
                    "SELECT fuseki_uri, merged_into FROM entity_registry "
                    "WHERE fuseki_uri = ANY($1::text[]) ORDER BY fuseki_uri FOR UPDATE",
                    [survivor, loser])}
                if locked.get(survivor) is not None or locked.get(loser) is not None:
                    raise HTTPException(
                        status_code=409,
                        detail="survivor or loser was merged concurrently; retry")

                # The except HTTPException below rolls back; rolling back here as well
                # makes asyncpg raise and turns this refusal into a 500.
                if not body.allow_persona_merge:
                    hazard = await persona_merge_hazard(conn, loser)
                    if hazard:
                        raise HTTPException(status_code=409, detail=hazard)

                reversal = await capture_reversal(conn, loser=loser, survivor=survivor)

                rewired = await _do_merge(conn, survivor, loser, merged_by)
                survivor_aliases = await conn.fetchval(
                    "SELECT aliases FROM entity_registry WHERE fuseki_uri = $1", survivor)

                merge_log_id: Optional[int] = None
                if not body.dry_run:
                    merge_log_id = await conn.fetchval("""
                        INSERT INTO entity_merge_log
                            (survivor_uri, loser_uri, rewired, merged_by, reversal)
                        VALUES ($1, $2, $3::jsonb, $4, $5::jsonb)
                        RETURNING id
                    """, survivor, loser, json.dumps(rewired), merged_by,
                         json.dumps(reversal))

                if body.dry_run:
                    await tx.rollback()
                else:
                    await tx.commit()
            except HTTPException:
                await tx.rollback()
                raise
            except Exception as e:
                await tx.rollback()
                logger.exception("entity merge failed loser=%s survivor=%s", loser, survivor)
                raise HTTPException(status_code=500, detail=f"merge failed: {e}")

        total = _total(rewired)
        logger.info(
            "entity_merge %s loser=%s -> survivor=%s total_refs=%d merge_log_id=%s by=%s",
            "DRY_RUN" if body.dry_run else "APPLIED",
            loser, survivor, total, merge_log_id, merged_by)
        return EntityMergeResponse(
            survivor_uri=survivor, loser_uri=loser, dry_run=body.dry_run,
            applied=(not body.dry_run), already_merged=False,
            rewired=rewired, total_refs_rewired=total,
            survivor_aliases_after=list(survivor_aliases) if survivor_aliases else [],
            merge_log_id=merge_log_id,
            message=("dry run — rolled back, nothing committed"
                     if body.dry_run else "merge applied"))

    @router.post("/retype", response_model=EntityRetypeResponse)
    async def retype_entity(
        body: EntityRetypeRequest,
        identity: str = Depends(require_service_auth),
    ):
        from api.entity_schema import canonicalize_entity_type, get_entity_schemas

        old_uri = body.uri
        retyped_by = body.retyped_by or identity

        canon_new_type = (
            canonicalize_entity_type(body.new_type) if body.new_type else body.new_type)
        if not canon_new_type or canon_new_type not in get_entity_schemas():
            raise HTTPException(
                status_code=422,
                detail=f"unknown entity type: {body.new_type!r} "
                       f"(canonicalized to {canon_new_type!r})")

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT entity_type, merged_into FROM entity_registry "
                "WHERE fuseki_uri = $1", old_uri)
            if row is None:
                raise HTTPException(status_code=404, detail=f"entity not found: {old_uri}")
            if row["merged_into"] is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"entity is merged into {row['merged_into']}; "
                           f"retype the live entity instead")

            old_type = row["entity_type"]
            # No-op when the stored type already equals the requested canonical
            # type. The comparison is against the RAW stored type on purpose:
            # a prefixed/variant spelling (e.g. 'schema:Concept') is NOT already
            # typed as 'Concept' and must fold.
            if old_type == canon_new_type:
                return EntityRetypeResponse(
                    old_uri=old_uri, new_uri=old_uri, old_type=old_type,
                    new_type=canon_new_type, merged_into_existing=False,
                    rewired={}, merge_log_id=None, applied=False,
                    dry_run=body.dry_run, already_typed=True,
                    message="entity already has the requested type (no-op)")

            tx = conn.transaction()
            await tx.start()
            try:
                new_uri, merged_into_existing, rewired, reversal = await _do_retype(
                    conn, old_uri, old_type, canon_new_type, retyped_by)

                merge_log_id: Optional[int] = None
                if not body.dry_run:
                    # reversal is persisted here, not inside _do_merge: a retype
                    # needs to record things _do_merge cannot know (whether the
                    # survivor was minted, what type the rid mappings held). The
                    # first 142 retypes wrote no reversal at all and are
                    # permanently irreversible.
                    merge_log_id = await conn.fetchval("""
                        INSERT INTO entity_merge_log
                            (survivor_uri, loser_uri, rewired, merged_by, reversal)
                        VALUES ($1, $2, $3::jsonb, $4, $5::jsonb)
                        RETURNING id
                    """, new_uri, old_uri, json.dumps(rewired), retyped_by,
                        json.dumps(reversal, default=str))

                if body.dry_run:
                    await tx.rollback()
                else:
                    await tx.commit()
            except HTTPException:
                await tx.rollback()
                raise
            except Exception as e:
                await tx.rollback()
                logger.exception(
                    "entity retype failed uri=%s new_type=%s", old_uri, canon_new_type)
                raise HTTPException(status_code=500, detail=f"retype failed: {e}")

        logger.info(
            "entity_retype %s uri=%s -> %s (%s -> %s) merged_into_existing=%s "
            "merge_log_id=%s by=%s",
            "DRY_RUN" if body.dry_run else "APPLIED",
            old_uri, new_uri, old_type, canon_new_type, merged_into_existing,
            merge_log_id, retyped_by)
        return EntityRetypeResponse(
            old_uri=old_uri, new_uri=new_uri, old_type=old_type,
            new_type=canon_new_type, merged_into_existing=merged_into_existing,
            rewired=rewired, merge_log_id=merge_log_id,
            applied=(not body.dry_run), dry_run=body.dry_run, already_typed=False,
            message=("dry run — rolled back, nothing committed"
                     if body.dry_run else "retype applied"))

    # ------------------------------------------------------------------
    # Closure — the non-monotone operation this surface was missing.
    #
    # Asserting and relating are MONOTONE: they commute, they are order-independent,
    # they never retract a conclusion. Closure (REFUTED / SCOPED / SUPERSEDED /
    # authority change) is NOT, and by CALM (Hellerstein 2010; Ameloot/Neven/Van den
    # Bussche, JACM 2013) it admits no coordination-free implementation. We do not
    # pretend otherwise — we split it:
    #   * entity_closure_log        — APPEND-ONLY. One row per attributable act.
    #                                 Never UPDATEd, never DELETEd. Stays monotone.
    #   * metadata->'closure'       — the DERIVED PROJECTION of the latest act.
    #                                 Non-monotone, but a deterministic function of
    #                                 the log, so always recomputable and never
    #                                 authoritative on its own.
    # Replay determinism without pretending to schedule invariance. Reopening is
    # just another act (status=OPEN); nothing is ever overwritten.
    #
    # Without this, a refutation could only be appended as *facts* while the
    # hypothesis kept full retrieval standing — which is how a killed experiment
    # (holonomy on the RAGE concept graph, permutation p = 0.980, 2026-06-29) was
    # re-proposed on 2026-07-14 with nothing in the substrate able to object.
    # See migration 107 and rage-research/notes/confluence-audit.md.
    # ------------------------------------------------------------------

    @router.get("/closed", response_model=List[EntityClosureResponse])
    async def list_closed_entities(status: Optional[str] = None, limit: int = 100):
        """Entities whose latest act is a non-OPEN closure.

        Backs the proposal-time gate: 'is there a closed hypothesis overlapping
        what I am about to propose?'
        """
        if status is not None and status not in _CLOSURE_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"unknown status {status!r}; expected one of {sorted(_CLOSURE_STATUSES)}")
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (l.entity_uri) l.*, e.entity_text
                FROM entity_closure_log l
                JOIN entity_registry e ON e.fuseki_uri = l.entity_uri
                ORDER BY l.entity_uri, l.closed_at DESC, l.id DESC
            """)
        out: List[EntityClosureResponse] = []
        for r in rows:
            if r["status"] == "OPEN":            # reopened — no longer closed
                continue
            if status is not None and r["status"] != status:
                continue
            out.append(EntityClosureResponse(
                entity_uri=r["entity_uri"], entity_text=r["entity_text"],
                current=_act_from_row(r), history=[], is_closed=True))
        return out[:limit]

    @router.post("/reproject-closures", response_model=ReprojectResponse)
    async def reproject_closures(
        dry_run: bool = False,
        identity: str = Depends(require_service_auth),
    ):
        """Rebuild metadata->'closure' from entity_closure_log for every entity
        that has closure acts. Idempotent.

        The projection is a CACHE of the log's latest act. Concurrent metadata
        upserts from other pipelines (session extraction, entity sync) can reset
        an entity's metadata to '{}' and silently drop the projection — the standing
        then vanishes from the retrieval surfaces even though the LOG is intact.
        This is the read-model rebuild: the log is the source of truth, so this can
        run any time, after any bulk operation, or on a schedule. It never invents
        a closure — an entity with no log rows is left untouched.
        """
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (entity_uri) *
                FROM entity_closure_log
                ORDER BY entity_uri, closed_at DESC, id DESC
            """)
            healed: List[str] = []
            already = 0
            for r in rows:
                projected = _act_from_row(r).model_dump(exclude={"id"})
                cur_raw = await conn.fetchval(
                    "SELECT metadata -> 'closure' FROM entity_registry WHERE fuseki_uri = $1",
                    r["entity_uri"])
                cur = json.loads(cur_raw) if isinstance(cur_raw, str) else cur_raw
                if cur == projected:
                    already += 1
                    continue
                if not dry_run:
                    await conn.execute("""
                        UPDATE entity_registry
                        SET metadata = COALESCE(metadata, '{}'::jsonb)
                                       || jsonb_build_object('closure', $2::jsonb),
                            updated_at = NOW()
                        WHERE fuseki_uri = $1
                    """, r["entity_uri"], json.dumps(projected))
                healed.append(r["entity_uri"])
        logger.info("reproject_closures %s: %d healed / %d already-current / %d total",
                    "DRY_RUN" if dry_run else "APPLIED", len(healed), already, len(rows))
        return ReprojectResponse(
            dry_run=dry_run, entities_with_log=len(rows),
            healed=len(healed), already_current=already, healed_uris=healed)

    @router.get("/{uri:path}/closure", response_model=EntityClosureResponse)
    async def get_entity_closure(uri: str):
        """Current standing + the full append-only history of closure acts."""
        async with pool.acquire() as conn:
            ent = await conn.fetchrow(
                "SELECT entity_text FROM entity_registry WHERE fuseki_uri = $1", uri)
            if ent is None:
                raise HTTPException(status_code=404, detail=f"entity not found: {uri}")
            rows = await conn.fetch(
                "SELECT * FROM entity_closure_log WHERE entity_uri = $1 "
                "ORDER BY closed_at DESC, id DESC", uri)
        history = [_act_from_row(r) for r in rows]
        current = history[0] if history else None
        return EntityClosureResponse(
            entity_uri=uri, entity_text=ent["entity_text"],
            current=current, history=history,
            is_closed=bool(current and current.status != "OPEN"))

    @router.post("/{uri:path}/close", response_model=EntityCloseResponse)
    async def close_entity(
        uri: str,
        body: EntityCloseRequest,
        identity: str = Depends(require_service_auth),
    ):
        """Record an attributable, versioned closure act against an entity.

        Appends to entity_closure_log (never overwrites) and re-projects
        metadata->'closure'. Idempotency is deliberately NOT enforced: re-closing is
        a new act, because *who* closed it and *on what evidence* is the point.
        """
        status = body.status.upper().strip()
        if status not in _CLOSURE_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"unknown status {body.status!r}; expected one of "
                       f"{sorted(_CLOSURE_STATUSES)}")
        if status == "SCOPED" and not (body.scope or "").strip():
            raise HTTPException(
                status_code=400,
                detail="status=SCOPED requires `scope`: the regime in which the claim still "
                       "stands. Without it a scoped kill degrades to a wrong binary.")
        closed_by = body.closed_by or identity

        async with pool.acquire() as conn:
            ent = await conn.fetchrow(
                "SELECT entity_text, merged_into FROM entity_registry WHERE fuseki_uri = $1",
                uri)
            if ent is None:
                raise HTTPException(status_code=404, detail=f"entity not found: {uri}")
            if ent["merged_into"] is not None:
                raise HTTPException(
                    status_code=409,
                    detail=f"entity is a tombstone (merged into {ent['merged_into']}); "
                           f"close the live entity instead")

            previous_status = await conn.fetchval(
                "SELECT status FROM entity_closure_log WHERE entity_uri = $1 "
                "ORDER BY closed_at DESC, id DESC LIMIT 1", uri)

            tx = conn.transaction()
            await tx.start()
            try:
                row = await conn.fetchrow("""
                    INSERT INTO entity_closure_log
                        (entity_uri, status, rationale, scope, evidence_uris, authority, closed_by)
                    VALUES ($1, $2, $3, $4, $5::text[], $6, $7)
                    RETURNING *
                """, uri, status, body.rationale, body.scope,
                     list(body.evidence_uris or []), body.authority, closed_by)
                act = _act_from_row(row)

                # Re-project the latest act onto the entity. Shallow jsonb merge —
                # every other metadata key is preserved.
                await conn.execute("""
                    UPDATE entity_registry
                    SET metadata = COALESCE(metadata, '{}'::jsonb)
                                   || jsonb_build_object('closure', $2::jsonb),
                        updated_at = NOW()
                    WHERE fuseki_uri = $1
                """, uri, json.dumps(act.model_dump(exclude={"id"})))

                if body.dry_run:
                    await tx.rollback()
                else:
                    await tx.commit()
            except HTTPException:
                await tx.rollback()
                raise
            except Exception as e:
                await tx.rollback()
                logger.exception("entity close failed uri=%s status=%s", uri, status)
                raise HTTPException(status_code=500, detail=f"close failed: {e}")

        logger.info("entity_close %s uri=%s %s->%s by=%s",
                    "DRY_RUN" if body.dry_run else "APPLIED", uri,
                    previous_status or "(never closed)", status, closed_by)
        return EntityCloseResponse(
            entity_uri=uri, entity_text=ent["entity_text"], dry_run=body.dry_run,
            applied=(not body.dry_run), previous_status=previous_status,
            closure=act, closure_log_id=(None if body.dry_run else act.id),
            message=("dry run — rolled back, nothing committed" if body.dry_run
                     else f"closed {previous_status or '(never closed)'} -> {status}"))

    @router.get("/{uri:path}/resolve")
    async def resolve_entity_redirect(uri: str):
        """Follow the merged_into chain to the canonical live URI (max 16 hops)."""
        async with pool.acquire() as conn:
            current = uri
            chain = [current]
            for _ in range(16):
                row = await conn.fetchrow(
                    "SELECT merged_into FROM entity_registry WHERE fuseki_uri = $1", current)
                if row is None:
                    if current == uri:
                        raise HTTPException(status_code=404, detail=f"entity not found: {uri}")
                    break
                if row["merged_into"] is None:
                    return {"input_uri": uri, "canonical_uri": current,
                            "redirected": current != uri, "chain": chain}
                current = row["merged_into"]
                if current in chain:  # cycle guard
                    raise HTTPException(status_code=409,
                                        detail=f"merged_into cycle detected: {chain + [current]}")
                chain.append(current)
            raise HTTPException(status_code=409,
                                detail=f"merged_into chain too long (>16 hops): {chain}")

    return router
