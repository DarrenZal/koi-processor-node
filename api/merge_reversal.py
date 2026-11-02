"""Capture enough state to undo an entity merge, and undo it.

WHY THIS IS NOT DERIVABLE AFTER THE FACT
----------------------------------------
A merge is a blind `UPDATE ... SET col = survivor WHERE col = loser`. Once it
runs, the loser's rows are indistinguishable from rows the survivor already
held. entity_merge_log.rewired records COUNTS ("2 document links rewired"), not
identities, so it cannot say which 2 to send back. The information required to
undo a merge is destroyed by performing it -- unless captured beforehand.

Hence capture_reversal() runs INSIDE the merge transaction, before any rewiring.
If the capture fails, the merge fails with it: a merge that cannot be undone
should not silently proceed as one that can.

WHAT THE 262 PRE-118 MERGES GET
--------------------------------
Nothing. They are irreversible and unmerge() refuses them by design. A
best-effort reversal there would repoint arbitrary rows and manufacture wrong
provenance quietly, which is strictly worse than an error message -- the
operator would believe a split had been restored when it had not.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

REVERSAL_SCHEMA = 2

# Schema 1 = merge-only capture (the 57 rows written 2026-09-02).
# Schema 2 adds the optional "retype" block: /entities/retype performs a merge
# whose survivor may not have existed beforehand, so an undo has to do more than
# repoint rows back -- see _apply_retype_reversal.
#
# Both are still applied. Bumping REVERSAL_SCHEMA alone would have made unmerge()
# refuse every one of the 57 existing captures, turning a widening change into a
# silent loss of the reversibility it was meant to extend.
_SUPPORTED_SCHEMAS = (1, 2)

# The set of columns a merge repoints is DERIVED from the merge's own list, not
# restated here. An earlier version of this module duplicated a partial copy (17
# of 21 columns) and would therefore have silently failed to restore signals,
# requirements, commitment_pools and assertion_history -- an undo that reports
# success while leaving references pointing at the survivor is worse than one
# that refuses. Importing means the capture cannot drift from the rewire.
#
# The three collision-prone tables are handled separately by the merge (rewire-
# then-dedupe) and are appended here explicitly.
def _n(result: Optional[str]) -> int:
    """Row count from an asyncpg status tag ('UPDATE 3' -> 3, 'DELETE 1' -> 1)."""
    try:
        return int(result.split()[-1]) if result else 0
    except (ValueError, IndexError, AttributeError):
        return 0


def _ref_cols() -> List[tuple]:
    from api.routers.admin_router import _PLAIN_REF_COLS

    collision = [
        ("entity_relationships", "subject_uri"),
        ("entity_relationships", "object_uri"),
        ("document_entity_links", "entity_uri"),
        ("pending_relationships", "subject_uri"),
        ("pending_relationships", "object_uri"),
    ]
    return list(dict.fromkeys(list(_PLAIN_REF_COLS) + collision))


async def persona_merge_hazard(conn, loser: str) -> Optional[str]:
    """Refuse to merge away a persona that is the ONLY record of its principal.

    Rows shaped "Name (via Platform)" are platform sender-name artifacts, and the
    obvious cleanup -- fold every "(via X)" row into its apparent principal --
    is safe only when that principal independently exists. Measured 2026-09-02:

        Person personas   54   principal exists 33   PRINCIPAL ABSENT 21
        Claim  personas    9   principal exists  0   PRINCIPAL ABSENT  9

    For those 21, the persona IS the canonical row. "Corin Ashdown (via Hylo)"
    is the only Ashdown in the graph; a cleanup pass merging it into its
    "apparent principal" would have merged a real person into somebody else --
    most likely another person with the same first name -- and erased her,
    along with 4 document links.
    Building Hylo carries 47, Collaborative Technology Alliance 17.

    The 9 Claim rows are not personas at all: "(via X)" there denotes citation
    provenance and entity_text is a whole sentence. A regex-driven cleanup hits
    both classes identically.

    This cannot be expressed as an entity_non_match veto, which relates two
    URIs -- the hazard is exactly that the second URI does not exist.

    Operator finding, 2026-09-02, after the 57-merge Batch A run.
    """
    row = await conn.fetchrow(
        """
        SELECT entity_text, entity_type,
               lower(trim(regexp_replace(entity_text, '\\s*\\(via [^)]*\\)\\s*$', ''))) AS principal
        FROM entity_registry WHERE fuseki_uri = $1
        """,
        loser,
    )
    if row is None or " (via " not in (row["entity_text"] or ""):
        return None

    principal_exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM entity_registry WHERE normalized_text = $1 "
        "AND merged_into IS NULL AND fuseki_uri <> $2)",
        row["principal"], loser,
    )
    if principal_exists:
        return None

    links = await conn.fetchval(
        "SELECT count(*) FROM document_entity_links WHERE entity_uri = $1", loser)
    return (
        f"{row['entity_text']!r} is a '(via …)' persona whose principal "
        f"({row['principal']!r}) has NO independent live row -- so this persona is the "
        f"only record of it in the graph, carrying {links} document link(s). Merging it "
        f"away erases a real referent rather than deduplicating one. 21 Person rows and "
        f"9 Claim rows are in this state. Pass allow_persona_merge=true only if you have "
        f"confirmed the survivor is genuinely the same referent."
    )


async def capture_reversal(conn, loser: str, survivor: str) -> Dict[str, Any]:
    """Snapshot what a merge is about to change. Call BEFORE rewiring."""
    refs: Dict[str, List[str]] = {}
    no_pk: Dict[str, int] = {}
    for table, col in _ref_cols():
        # Check existence FIRST rather than catching a failure. Deployments
        # differ (see api/capabilities.py profiles), so a missing table is
        # expected -- but `try: SELECT / except: continue` is NOT a safe way to
        # tolerate it. A failed statement ABORTS the enclosing transaction, and
        # since this runs inside the merge's transaction, every subsequent
        # statement would fail with InFailedSQLTransactionError. `except` is not
        # a savepoint. (Cost this exact bug here on 2026-09-02; the same class is
        # recorded in the project memory after /register-entity returned HTTP 200
        # success=true while silently discarding ~1,924 registrations.)
        exists = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM information_schema.columns "
            "            WHERE table_name = $2 AND column_name = $3)",
            table, table, col,
        )
        if not exists:
            continue

        # Row-id capture needs an `id` primary key. Verified 2026-09-02:
        # assertion_history and koi_extraction_records have none (both are the
        # RDF mirror, empty today). "Empty today" is not a guarantee, so rows
        # there are recorded as an explicit GAP rather than skipped silently --
        # an earlier version of this loop checked `id` on 9 tables and assumed
        # all 26, which is how the omission would have gone unnoticed.
        has_id = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_name = $1 AND column_name = 'id')",
            table,
        )
        if not has_id:
            n = await conn.fetchval(
                f"SELECT count(*) FROM {table} WHERE {col} = $1", loser  # noqa: S608
            )
            if n:
                no_pk[f"{table}.{col}"] = n
            continue

        # Ids are captured AS TEXT because the types are not uniform: 12 of
        # these tables use integer, knowledge_facts uses uuid (migration 079).
        # Storing native values breaks json.dumps on the uuid, and a `::bigint[]`
        # cast on restore breaks the other way. Text is the one representation
        # that round-trips both, and `id::text = ANY(...)` restores either.
        rows = await conn.fetch(
            f"SELECT id::text AS id FROM {table} WHERE {col} = $1", loser  # noqa: S608
        )
        if rows:
            refs[f"{table}.{col}"] = [r["id"] for r in rows]

    surv = await conn.fetchrow(
        "SELECT COALESCE(aliases,'{}') AS aliases FROM entity_registry WHERE fuseki_uri=$1",
        survivor,
    )
    lose = await conn.fetchrow(
        "SELECT entity_text, COALESCE(aliases,'{}') AS aliases "
        "FROM entity_registry WHERE fuseki_uri=$1",
        loser,
    )
    survivor_aliases = set(surv["aliases"]) if surv else set()

    # Only what the merge will ADD. Removing an alias the survivor already owned
    # would strip something the merge never contributed.
    from api.resolution_primitives import normalize_alias_list

    incoming = set(
        normalize_alias_list(
            [lose["entity_text"]] + list(lose["aliases"]) if lose else []
        )
    )
    aliases_added = sorted(a for a in incoming if a and a not in survivor_aliases)

    return {
        "schema": REVERSAL_SCHEMA,
        "loser": loser,
        "survivor": survivor,
        "refs": refs,
        "aliases_added": aliases_added,
        # Array and JSONB columns are rewritten by the merge with array_replace
        # and a text-REPLACE. Those are NOT captured for reversal here: undoing
        # them needs the pre-image, not a row id. Recorded as an explicit
        # not-covered list so unmerge REPORTS the gap instead of implying a
        # complete restoration. If either is ever non-empty for a real merge,
        # that is the signal to extend this capture rather than to trust it.
        "not_reversed": {
            "array_columns": ["claim_attestations.evidence_uris",
                              "task_registry.collaborator_uris"],
            "jsonb_metadata": "*.metadata text-REPLACE of the embedded URI",
            # Rows in tables with no `id` primary key. Empty => nothing at risk.
            "rows_in_tables_without_pk": no_pk,
        },
        "deletions": {},  # merge dedupe/self-loop deletions; see unmerge()
    }


async def unmerge(conn, merge_log_id: int, unmerged_by: str = "operator") -> Dict[str, Any]:
    """Reverse one merge. Refuses anything it cannot reverse exactly.

    Runs in the caller's transaction so a partial reversal rolls back whole.
    """
    row = await conn.fetchrow(
        "SELECT id, survivor_uri, loser_uri, reversal, reverted_at "
        "FROM entity_merge_log WHERE id = $1",
        merge_log_id,
    )
    if row is None:
        raise ValueError(f"no entity_merge_log row with id={merge_log_id}")
    if row["reverted_at"] is not None:
        raise ValueError(
            f"merge {merge_log_id} was already reverted at {row['reverted_at']}"
        )
    if row["reversal"] is None:
        raise ValueError(
            f"merge {merge_log_id} is NOT REVERSIBLE: it predates migration 118 "
            f"(2026-09-02), which is when merges began capturing the row identities "
            f"an undo requires. The prior log recorded only counts, and a blind "
            f"UPDATE destroys the identities. Refusing rather than guessing -- a "
            f"best-effort reversal would repoint arbitrary rows and manufacture "
            f"wrong provenance silently."
        )

    rev = row["reversal"]
    if isinstance(rev, str):
        rev = json.loads(rev)
    if rev.get("schema") not in _SUPPORTED_SCHEMAS:
        raise ValueError(
            f"merge {merge_log_id} reversal schema {rev.get('schema')} not in "
            f"{_SUPPORTED_SCHEMAS}; refusing to apply an undo written by different code"
        )

    survivor, loser = row["survivor_uri"], row["loser_uri"]
    restored: Dict[str, int] = {}
    retype = rev.get("retype") or {}

    # An in-place retype ('person' -> 'Person') changes the type label without
    # changing the deterministic URI, so no merge happened: nothing was rewired
    # and the row was never tombstoned. Running the merge-undo below would fail
    # at the "un-tombstone exactly 1 loser" assertion, which is why this returns
    # early rather than falling through.
    if retype.get("branch") == "in_place":
        n = _n(await conn.execute(
            "UPDATE entity_registry SET entity_type = $1, phonetic_code = $2 "
            "WHERE fuseki_uri = $3 AND entity_type = $4",
            retype["from"], retype.get("prev_phonetic_code"), survivor, retype["to"],
        ))
        if n != 1:
            raise ValueError(
                f"merge {merge_log_id}: in-place retype undo expected to restore exactly "
                f"1 row to {retype['from']!r}, affected {n}. The type has changed again "
                f"since. Rolling back."
            )
        restored["entity_type_restored"] = n
        restored["entity_rid_mappings_type_restored"] = _n(await conn.execute(
            "UPDATE entity_rid_mappings SET entity_type = $1 WHERE canonical_uri = $2",
            retype["from"], survivor,
        ))
        await conn.execute(
            "UPDATE entity_merge_log SET reverted_at = NOW() WHERE id = $1", merge_log_id
        )
        logger.info("unmerge %s: in-place retype %s -> %s restored on %s",
                    merge_log_id, retype["to"], retype["from"], survivor)
        return {"merge_log_id": merge_log_id, "loser": loser, "survivor": survivor,
                "restored": restored}

    # 1. Un-tombstone the loser. FIRST, before any link is repointed at it:
    #    migration 132's trigger on document_entity_links rewrites a link that
    #    names a merged-away URI to its survivor, so repointing while the loser
    #    is still tombstoned would send every link straight back to the survivor
    #    and report it restored.
    result = await conn.execute(
        "UPDATE entity_registry SET merged_into = NULL, merged_at = NULL, merged_by = NULL "
        "WHERE fuseki_uri = $1 AND merged_into = $2",
        loser, survivor,
    )
    restored["loser_restored"] = int(result.split()[-1]) if result else 0
    if restored["loser_restored"] != 1:
        raise ValueError(
            f"merge {merge_log_id}: expected to un-tombstone exactly 1 loser row, "
            f"affected {restored['loser_restored']}. The loser may have been merged "
            f"onward into a third entity. Rolling back."
        )

    # 2. Repoint exactly the captured rows back to the loser. The `= survivor`
    #    guard means a row someone has since moved elsewhere is left alone
    #    rather than yanked back -- and the count difference makes that visible.
    for key, ids in (rev.get("refs") or {}).items():
        table, col = key.split(".", 1)
        if (table, col) not in _ref_cols():
            raise ValueError(f"reversal names an unexpected column: {key}")
        result = await conn.execute(
            f"UPDATE {table} SET {col} = $1 "  # noqa: S608
            f"WHERE id::text = ANY($2::text[]) AND {col} = $3",
            loser, [str(i) for i in ids], survivor,
        )
        n = int(result.split()[-1]) if result else 0
        restored[key] = n
        if n != len(ids):
            logger.warning(
                "unmerge %s: %s restored %d of %d rows -- the rest were changed "
                "after the merge and were deliberately left alone",
                merge_log_id, key, n, len(ids),
            )

    # 3. Remove only the aliases this merge added.
    added = rev.get("aliases_added") or []
    if added:
        await conn.execute(
            """
            UPDATE entity_registry
            SET aliases = ARRAY(SELECT e FROM unnest(COALESCE(aliases,'{}')) e
                                WHERE e <> ALL($2::text[]))
            WHERE fuseki_uri = $1
            """,
            survivor, added,
        )
    restored["aliases_removed"] = len(added)

    # 4. Retype-specific restoration. A retype is a merge whose survivor URI
    #    encodes the NEW type, so undoing the rewiring is necessary but not
    #    sufficient: the survivor row itself may have been created (or
    #    resurrected) by the retype, and entity_rid_mappings carries a type
    #    column that _do_merge does not touch.
    if retype:
        restored["entity_rid_mappings_type_restored"] = _n(await conn.execute(
            "UPDATE entity_rid_mappings SET entity_type = $1 WHERE canonical_uri = $2",
            retype["from"], loser,
        ))

        # koi_rid / wallet_address carry UNIQUE partial indexes, so the mint
        # branch MOVED them off the source row. Move them back before the
        # survivor is deleted, or the unique values are destroyed with it.
        if retype.get("moved_koi_rid") is not None or retype.get("moved_wallet") is not None:
            await conn.execute(
                "UPDATE entity_registry SET koi_rid = NULL, wallet_address = NULL "
                "WHERE fuseki_uri = $1", survivor)
            restored["identifiers_moved_back"] = _n(await conn.execute(
                "UPDATE entity_registry SET koi_rid = $1, wallet_address = $2 "
                "WHERE fuseki_uri = $3",
                retype.get("moved_koi_rid"), retype.get("moved_wallet"), loser))

        if retype.get("survivor_minted"):
            # THE reason this branch exists. _do_retype INSERTs a new-typed row
            # copying entity_text, normalized_text and both embeddings. Restoring
            # the loser without deleting it leaves TWO live rows sharing a
            # normalized_text, both embedded, both competing in exact, fuzzy and
            # ANN resolution -- manufacturing the duplicate identity a retype
            # exists to remove, while reporting a successful undo.
            leftover = await conn.fetchval(
                "SELECT count(*) FROM entity_registry WHERE merged_into = $1", survivor)
            if leftover:
                raise ValueError(
                    f"merge {merge_log_id}: refusing to delete minted survivor "
                    f"{survivor} -- {leftover} row(s) are still merged into it. "
                    f"Unmerge those first."
                )
            n_del = _n(await conn.execute(
                "DELETE FROM entity_registry WHERE fuseki_uri = $1", survivor))
            if n_del != 1:
                raise ValueError(
                    f"merge {merge_log_id}: expected to delete exactly 1 minted "
                    f"survivor row ({survivor}), deleted {n_del}. Rolling back."
                )
            restored["minted_survivor_deleted"] = n_del

        elif retype.get("survivor_resurrected"):
            # The survivor URI was held by a TOMBSTONE that _do_retype revived.
            # Put it back the way it was, or the undo leaves a row live that was
            # tombstoned before the retype ran.
            # merged_at is bound through ::text::timestamptz because the payload
            # was serialized with json.dumps(default=str), so it comes back as a
            # string; asyncpg binds by inferred type and would reject a str for a
            # timestamptz parameter.
            restored["survivor_re_tombstoned"] = _n(await conn.execute(
                "UPDATE entity_registry SET merged_into = $1, merged_at = $2::text::timestamptz, "
                "merged_by = $3, entity_type = $4, phonetic_code = $5 "
                "WHERE fuseki_uri = $6",
                retype.get("resurrected_merged_into"),
                (str(retype["resurrected_merged_at"])
                 if retype.get("resurrected_merged_at") is not None else None),
                retype.get("resurrected_merged_by"),
                retype.get("resurrected_entity_type"),
                retype.get("resurrected_phonetic_code"),
                survivor,
            ))

    await conn.execute(
        "UPDATE entity_merge_log SET reverted_at = NOW() WHERE id = $1", merge_log_id
    )

    logger.info("unmerge %s: %s <- %s, restored %s", merge_log_id, loser, survivor, restored)
    return {"merge_log_id": merge_log_id, "loser": loser, "survivor": survivor,
            "restored": restored}
