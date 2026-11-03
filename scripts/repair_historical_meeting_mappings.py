#!/usr/bin/env python3
"""Plan, apply, verify, or roll back the historical Meeting mapping split.

The repair is deliberately narrow: only mappings in a canonical URI group
whose note names carry more than one leading ISO date are eligible.  The date
partition matching the existing registry entity stays in place; every other
date partition gets one explicitly registered Meeting identity.  Multiple
artifacts for the same date continue to share that identity.

``plan`` is read-only. ``apply`` verifies both immutable input digests,
recomputes the live mutation set, snapshots the rows it may change, and then
uses the live registration API. ``rollback`` restores those mappings and
source-scoped relationship rows and tombstones (never deletes) newly created
Meeting entities.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import asyncpg


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.personal_ingest_api import generate_entity_uri  # noqa: E402
from api.vault_parser import batch_resolve_by_vault_path, batch_resolve_entities  # noqa: E402
from scripts.backfill_meeting_attendance import (  # noqa: E402
    _api_json,
    _connect,
    _log,
    _new_log,
    _pending_promotions,
    _verify_api,
    write_private_json,
)
from scripts.meeting_attendee_parser import CorpusError, load_snapshot  # noqa: E402


PLAN_SCHEMA_VERSION = 1
ISO_DATE_RE = re.compile(r"^([12]\d{3}-[01]\d-[0-3]\d)(?:\s|$)")
SAFE_RUN_ID_RE = re.compile(r"^meeting_mapping_repair_\d{8}T\d{6}$")
LOCK_KEY = 0x4D454554  # "MEET"


class RepairError(RuntimeError):
    """A classification, drift, apply, or verification invariant failed."""


def leading_meeting_date(name: str | None) -> str | None:
    match = ISO_DATE_RE.match((name or "").strip())
    return match.group(1) if match else None


def representative_sort_key(row: Mapping[str, Any]) -> tuple[bool, bool, int, int]:
    """Prefer a primary note over transcripts, parentheticals, and long names."""
    name = str(row["name"])
    return (
        "transcript" in name.casefold(),
        bool(re.search(r"\([^)]*\)", name)),
        len(name),
        int(row["id"]),
    )


def choose_representative(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    candidates = [dict(row) for row in rows]
    if not candidates:
        raise RepairError("Cannot choose a representative from an empty date partition")
    return min(candidates, key=representative_sort_key)


def classify_mapping_rows(
    mappings: Iterable[Mapping[str, Any]],
    registry_names: Mapping[str, str],
) -> dict[str, Any]:
    """Classify mappings and derive the exact dated partitions to move."""
    rows = [dict(row) for row in mappings]
    by_uri: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        row["leading_date"] = leading_meeting_date(row.get("name"))
        by_uri[str(row["canonical_uri"])].append(row)

    dateless_singletons = []
    dateless_collapsed = []
    dated_singletons = []
    collapsed_groups = []
    moves = []

    for canonical_uri, group in sorted(by_uri.items()):
        group = sorted(group, key=lambda row: int(row["id"]))
        if len(group) == 1:
            (dateless_singletons if group[0]["leading_date"] is None else dated_singletons).append(
                group[0]
            )
            continue

        dateless = [row for row in group if row["leading_date"] is None]
        if dateless:
            dateless_collapsed.extend(dateless)
            continue

        partitions: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in group:
            partitions[str(row["leading_date"])].append(row)
        if len(partitions) == 1:
            # A legitimate same-date, multi-artifact group is not a date collapse.
            collapsed_groups.append(
                {
                    "canonical_uri": canonical_uri,
                    "anchor_date": next(iter(partitions)),
                    "date_partitions": 1,
                    "mapping_rows": len(group),
                    "same_date_only": True,
                }
            )
            continue

        registry_name = registry_names.get(canonical_uri)
        anchor_date = leading_meeting_date(registry_name)
        if anchor_date is None:
            raise RepairError(
                f"Collapsed canonical {canonical_uri} has no dated registry identity: {registry_name!r}"
            )
        if anchor_date not in partitions:
            raise RepairError(
                f"Registry date {anchor_date} is absent from mappings for {canonical_uri}"
            )

        group_plan = {
            "canonical_uri": canonical_uri,
            "registry_name": registry_name,
            "anchor_date": anchor_date,
            "date_partitions": len(partitions),
            "mapping_rows": len(group),
            "same_date_only": False,
            "partitions": [],
        }
        for meeting_date, partition in sorted(partitions.items()):
            representative = choose_representative(partition)
            expected_uri = (
                canonical_uri
                if meeting_date == anchor_date
                else generate_entity_uri(representative["name"], "Meeting")
            )
            group_plan["partitions"].append(
                {
                    "meeting_date": meeting_date,
                    "canonical_uri": expected_uri,
                    "representative_mapping_id": representative["id"],
                    "representative_name": representative["name"],
                    "mapping_ids": [row["id"] for row in partition],
                    "vault_paths": [row["vault_path"] for row in partition],
                    "is_anchor": meeting_date == anchor_date,
                }
            )
            if meeting_date == anchor_date:
                continue
            for row in partition:
                moves.append(
                    {
                        **row,
                        "old_canonical_uri": canonical_uri,
                        "expected_canonical_uri": expected_uri,
                        "representative_name": representative["name"],
                    }
                )
        collapsed_groups.append(group_plan)

    if dateless_collapsed:
        offenders = ", ".join(
            f"{row['id']}:{row['vault_path']}" for row in dateless_collapsed
        )
        raise RepairError(
            "Dateless Meeting mapping found inside a collapsed group; manual review required: "
            + offenders
        )

    return {
        "dated_singletons": dated_singletons,
        "dateless_singletons": dateless_singletons,
        "dateless_collapsed": dateless_collapsed,
        "collapsed_groups": collapsed_groups,
        "moves": sorted(moves, key=lambda row: (row["leading_date"], int(row["id"]))),
    }


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=_json_default,
    ).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _canonical_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted([dict(row) for row in rows], key=_json_bytes)


def _plan_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(plan)
    payload.pop("generated_at", None)
    payload.pop("plan_digest", None)
    return payload


def compute_plan_digest(plan: Mapping[str, Any]) -> str:
    return _digest(_plan_payload(plan))


def load_plan(path: Path, expected_digest: str | None = None) -> dict[str, Any]:
    try:
        plan = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RepairError(f"Cannot load repair plan {path}: {exc}") from exc
    if not isinstance(plan, dict) or plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise RepairError(f"{path}: unsupported Meeting repair plan schema")
    actual = compute_plan_digest(plan)
    if plan.get("plan_digest") != actual:
        raise RepairError(f"{path}: plan digest mismatch")
    if expected_digest is not None and expected_digest != actual:
        raise RepairError(f"{path}: expected plan digest {expected_digest}, got {actual}")
    return plan


def require_confirmation(confirmed: bool, action: str) -> None:
    if not confirmed:
        raise RepairError(f"Refusing {action} without the literal --{action} switch")


async def _global_counts(conn: asyncpg.Connection) -> dict[str, int]:
    row = await conn.fetchrow(
        """
        SELECT
          (SELECT count(*) FROM entity_registry WHERE entity_type='Meeting'
             AND merged_into IS NULL) AS meeting_live,
          (SELECT count(*) FROM entity_rid_mappings WHERE entity_type='Meeting') AS meeting_mappings,
          (SELECT count(DISTINCT canonical_uri) FROM entity_rid_mappings
             WHERE entity_type='Meeting') AS meeting_mapping_uris,
          (SELECT count(*) FROM entity_relationships WHERE predicate='attended') AS attended_total,
          (SELECT count(*) FROM pending_relationships) AS pending_total
        """
    )
    return {key: int(row[key]) for key in row.keys()}


async def _meeting_mapping_rows(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT id, vault_rid, vault_path, canonical_uri, entity_type, name,
               content_hash, sync_status, last_synced, created_at, visibility_scope
        FROM entity_rid_mappings
        WHERE entity_type='Meeting'
        ORDER BY id
        """
    )
    return [dict(row) for row in rows]


async def _registry_names(
    conn: asyncpg.Connection, mappings: Iterable[Mapping[str, Any]]
) -> dict[str, str]:
    uris = sorted({str(row["canonical_uri"]) for row in mappings})
    rows = await conn.fetch(
        """
        SELECT fuseki_uri, entity_text FROM entity_registry
        WHERE fuseki_uri=ANY($1::text[]) AND merged_into IS NULL
        """,
        uris,
    )
    names = {str(row["fuseki_uri"]): str(row["entity_text"]) for row in rows}
    missing = sorted(set(uris) - set(names))
    if missing:
        raise RepairError(f"Meeting mappings reference missing or tombstoned entities: {missing}")
    return names


def _snapshot_records(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for record in snapshot["records"]:
        path = str(record["vault_path"])
        if path in records:
            raise RepairError(f"Frozen snapshot contains duplicate vault path: {path}")
        records[path] = dict(record)
    return records


async def _resolve_attendees(
    conn: asyncpg.Connection,
    moves: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    slots = [
        (move, slot)
        for move in moves
        for slot in move["snapshot_record"]["attendees"]
        if slot["usable"]
    ]
    targets = sorted({(slot["target_name"], slot["type_hint"]) for _, slot in slots})
    resolved = await batch_resolve_entities(conn, targets)
    paths = sorted(
        {
            slot["target_vault_path"]
            for _, slot in slots
            if slot.get("target_vault_path")
        }
    )
    path_resolved = await batch_resolve_by_vault_path(conn, paths)

    expected_edges: set[tuple[str, str]] = set()
    expected_pending: set[tuple[str, str]] = set()
    for move, slot in slots:
        person_uri = (
            resolved.get((slot["target_name"].lower(), slot["type_hint"]))
            or resolved.get((slot["target_name"].lower(), None))
            or path_resolved.get(slot.get("target_vault_path"))
        )
        if person_uri:
            expected_edges.add((person_uri, move["expected_canonical_uri"]))
        else:
            expected_pending.add((move["expected_canonical_uri"], slot["target_name"]))
    return (
        [
            {"subject_uri": subject, "object_uri": obj}
            for subject, obj in sorted(expected_edges)
        ],
        [
            {"object_uri": obj, "raw_unknown_label": label}
            for obj, label in sorted(expected_pending)
        ],
    )


async def build_plan(snapshot: Mapping[str, Any], conn: asyncpg.Connection) -> dict[str, Any]:
    mappings = await _meeting_mapping_rows(conn)
    registry_names = await _registry_names(conn, mappings)
    classification = classify_mapping_rows(mappings, registry_names)
    records_by_path = _snapshot_records(snapshot)

    moves = []
    for move in classification["moves"]:
        record = records_by_path.get(str(move["vault_path"]))
        if record is None:
            raise RepairError(f"Affected Meeting is absent from frozen snapshot: {move['vault_path']}")
        if record.get("entity_type") != "Meeting":
            raise RepairError(f"Affected snapshot record is not a Meeting: {move['vault_path']}")
        moves.append({**move, "snapshot_record": record})

    expected_uris = sorted({move["expected_canonical_uri"] for move in moves})
    if len(expected_uris) != len(
        {
            (move["old_canonical_uri"], move["leading_date"])
            for move in moves
        }
    ):
        raise RepairError("Two distinct date partitions generated the same Meeting URI")
    existing = await conn.fetch(
        "SELECT fuseki_uri, entity_text, entity_type, merged_into FROM entity_registry "
        "WHERE fuseki_uri=ANY($1::text[])",
        expected_uris,
    )
    if existing:
        raise RepairError(
            "A planned new Meeting URI already exists; reclassification is required: "
            + json.dumps([dict(row) for row in existing], default=_json_default)
        )

    affected_paths = [str(move["vault_path"]) for move in moves]
    existing_source_edges = int(
        await conn.fetchval(
            "SELECT count(*) FROM entity_relationships WHERE source_rid=ANY($1::text[])",
            affected_paths,
        )
    )
    existing_source_pending = int(
        await conn.fetchval(
            "SELECT count(*) FROM pending_relationships WHERE source_rid=ANY($1::text[])",
            affected_paths,
        )
    )
    if existing_source_edges or existing_source_pending:
        raise RepairError(
            "Affected source paths already own live relationship rows; re-plan their replacement "
            f"explicitly (edges={existing_source_edges}, pending={existing_source_pending})"
        )

    expected_edges, expected_pending = await _resolve_attendees(conn, moves)
    new_entities = []
    for expected_uri in expected_uris:
        representative = next(
            move for move in moves if move["expected_canonical_uri"] == expected_uri
        )
        new_entities.append(
            {
                "expected_uri": expected_uri,
                "name": representative["representative_name"],
                "old_canonical_uri": representative["old_canonical_uri"],
                "meeting_date": representative["leading_date"],
            }
        )
    collateral = await _pending_promotions(
        conn,
        [
            (
                {"name": item["name"], "expected_uri": item["expected_uri"]},
                "Meeting",
            )
            for item in new_entities
        ],
    )
    if collateral:
        raise RepairError(
            "New Meeting registration would promote unrelated pending rows; manual review required: "
            + json.dumps(collateral, default=_json_default)
        )

    # Strip the full frozen record into only the fields needed to apply and verify.
    planned_moves = []
    for move in moves:
        record = move["snapshot_record"]
        planned_moves.append(
            {
                key: move[key]
                for key in (
                    "id",
                    "vault_rid",
                    "vault_path",
                    "canonical_uri",
                    "entity_type",
                    "name",
                    "content_hash",
                    "sync_status",
                    "last_synced",
                    "created_at",
                    "visibility_scope",
                    "leading_date",
                    "old_canonical_uri",
                    "expected_canonical_uri",
                    "representative_name",
                )
            }
            | {
                "source_sha256": record["source_sha256"],
                "attendees": [slot["raw"] for slot in record["attendees"]],
                "usable_attendee_slots": sum(
                    1 for slot in record["attendees"] if slot["usable"]
                ),
            }
        )

    moved_ids = {int(move["id"]) for move in moves}
    preserved = _canonical_rows(row for row in mappings if int(row["id"]) not in moved_ids)
    baseline = await _global_counts(conn)
    mutation_set = {
        "moves": planned_moves,
        "new_entities": new_entities,
        "expected_edges": expected_edges,
        "expected_pending": expected_pending,
    }
    cross_date_groups = [
        group for group in classification["collapsed_groups"] if not group["same_date_only"]
    ]
    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_digest": snapshot["snapshot_digest"],
        "meeting_mapping_digest": _digest(_canonical_rows(mappings)),
        "preserved_mapping_digest": _digest(preserved),
        "mutation_set_digest": _digest(mutation_set),
        "baseline": baseline,
        **mutation_set,
        "classification": {
            "dated_singletons": len(classification["dated_singletons"]),
            "dateless_singletons": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "vault_path": row["vault_path"],
                    "canonical_uri": row["canonical_uri"],
                }
                for row in classification["dateless_singletons"]
            ],
            "dateless_collapsed": [],
            "cross_date_groups": cross_date_groups,
        },
        "summary": {
            "mapping_rows": len(mappings),
            "unique_mapping_uris": len({row["canonical_uri"] for row in mappings}),
            "mapping_excess": len(mappings)
            - len({row["canonical_uri"] for row in mappings}),
            "cross_date_groups": len(cross_date_groups),
            "mappings_to_move": len(planned_moves),
            "new_meetings": len(new_entities),
            "usable_attendee_slots": sum(
                move["usable_attendee_slots"] for move in planned_moves
            ),
            "expected_new_attended_edges": len(expected_edges),
            "expected_new_pending": len(expected_pending),
            "post_mapping_uris": baseline["meeting_mapping_uris"] + len(new_entities),
            "post_mapping_excess": len(mappings)
            - (baseline["meeting_mapping_uris"] + len(new_entities)),
            "post_meeting_live": baseline["meeting_live"] + len(new_entities),
            "post_attended_total": baseline["attended_total"] + len(expected_edges),
            "post_pending_total": baseline["pending_total"] + len(expected_pending),
        },
    }
    plan["plan_digest"] = compute_plan_digest(plan)
    return plan


async def _acquire_lock(conn: asyncpg.Connection) -> None:
    if not await conn.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_KEY):
        raise RepairError("Another historical Meeting repair holds the advisory lock")


async def _create_backups(
    conn: asyncpg.Connection, plan: Mapping[str, Any], run_id: str
) -> dict[str, str]:
    if not SAFE_RUN_ID_RE.fullmatch(run_id):
        raise RepairError(f"Unsafe backup run id: {run_id}")
    tables = {
        "registry": f"entity_registry_backup_{run_id}",
        "mappings": f"entity_rid_mappings_backup_{run_id}",
        "relationships": f"entity_relationships_backup_{run_id}",
        "pending": f"pending_relationships_backup_{run_id}",
    }
    for table in tables.values():
        if await conn.fetchval("SELECT to_regclass($1)", f"public.{table}"):
            raise RepairError(f"Backup table already exists: {table}")
    old_uris = sorted({move["old_canonical_uri"] for move in plan["moves"]})
    new_uris = sorted({item["expected_uri"] for item in plan["new_entities"]})
    paths = [move["vault_path"] for move in plan["moves"]]
    await conn.execute(
        f"CREATE TABLE {tables['registry']} AS SELECT * FROM entity_registry "
        "WHERE fuseki_uri=ANY($1::text[])",
        old_uris + new_uris,
    )
    await conn.execute(
        f"CREATE TABLE {tables['mappings']} AS SELECT * FROM entity_rid_mappings "
        "WHERE vault_path=ANY($1::text[])",
        paths,
    )
    await conn.execute(
        f"CREATE TABLE {tables['relationships']} AS SELECT * FROM entity_relationships "
        "WHERE source_rid=ANY($1::text[])",
        paths,
    )
    await conn.execute(
        f"CREATE TABLE {tables['pending']} AS SELECT * FROM pending_relationships "
        "WHERE source_rid=ANY($1::text[])",
        paths,
    )
    return tables


def _registration_payload(move: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "vault_rid": move["vault_rid"],
        "vault_path": move["vault_path"],
        "entity_type": "Meeting",
        # Same-date secondary artifacts deliberately resolve to the partition's
        # representative, after which the mapping's original display name is restored.
        "name": move["representative_name"],
        "content_hash": move["source_sha256"][:16],
        "publication_scope": "local_graph",
        "visibility_scope": move["visibility_scope"] or "public",
        "force_type": True,
        "exact_only": True,
        "frontmatter": {"attendees": move["attendees"]},
    }


async def _fresh_plan_matches(
    snapshot: Mapping[str, Any], plan: Mapping[str, Any], conn: asyncpg.Connection
) -> dict[str, Any]:
    fresh = await build_plan(snapshot, conn)
    if fresh["mutation_set_digest"] != plan["mutation_set_digest"]:
        raise RepairError("Live Meeting mutation set drifted; create a new plan")
    if fresh["meeting_mapping_digest"] != plan["meeting_mapping_digest"]:
        raise RepairError("Live Meeting mappings drifted; create a new plan")
    return fresh


async def verify_applied(
    conn: asyncpg.Connection, plan: Mapping[str, Any]
) -> dict[str, Any]:
    moved_ids = [int(move["id"]) for move in plan["moves"]]
    rows = await conn.fetch(
        """
        SELECT id, vault_rid, vault_path, canonical_uri, entity_type, name,
               content_hash, sync_status, last_synced, created_at, visibility_scope
        FROM entity_rid_mappings WHERE id=ANY($1::int[]) ORDER BY id
        """,
        moved_ids,
    )
    by_id = {int(row["id"]): row for row in rows}
    if set(by_id) != set(moved_ids):
        raise RepairError("One or more moved Meeting mappings are missing")
    for move in plan["moves"]:
        row = by_id[int(move["id"])]
        if (
            row["canonical_uri"] != move["expected_canonical_uri"]
            or row["name"] != move["name"]
            or row["vault_path"] != move["vault_path"]
            or row["entity_type"] != "Meeting"
        ):
            raise RepairError(f"Unexpected repaired mapping {move['id']}: {dict(row)}")

    entity_rows = await conn.fetch(
        """
        SELECT fuseki_uri, entity_text, entity_type, merged_into
        FROM entity_registry WHERE fuseki_uri=ANY($1::text[])
        """,
        [item["expected_uri"] for item in plan["new_entities"]],
    )
    entities = {row["fuseki_uri"]: row for row in entity_rows}
    for expected in plan["new_entities"]:
        row = entities.get(expected["expected_uri"])
        if (
            row is None
            or row["entity_text"] != expected["name"]
            or row["entity_type"] != "Meeting"
            or row["merged_into"] is not None
            or leading_meeting_date(row["entity_text"]) != expected["meeting_date"]
        ):
            raise RepairError(f"Unexpected repaired entity: {expected}; actual={dict(row) if row else None}")

    current_mappings = await _meeting_mapping_rows(conn)
    preserved = _canonical_rows(row for row in current_mappings if int(row["id"]) not in set(moved_ids))
    if _digest(preserved) != plan["preserved_mapping_digest"]:
        raise RepairError("A Meeting mapping outside the repair set changed")

    new_uris = [item["expected_uri"] for item in plan["new_entities"]]
    edge_rows = await conn.fetch(
        """
        SELECT subject_uri, object_uri FROM entity_relationships
        WHERE predicate='attended' AND object_uri=ANY($1::text[])
        """,
        new_uris,
    )
    actual_edges = {(row["subject_uri"], row["object_uri"]) for row in edge_rows}
    expected_edges = {
        (row["subject_uri"], row["object_uri"]) for row in plan["expected_edges"]
    }
    if actual_edges != expected_edges:
        raise RepairError(
            f"Repaired attended edge set mismatch: expected={len(expected_edges)}, actual={len(actual_edges)}"
        )

    pending_rows = await conn.fetch(
        """
        SELECT object_uri, raw_unknown_label FROM pending_relationships
        WHERE predicate='attended' AND object_uri=ANY($1::text[])
        """,
        new_uris,
    )
    actual_pending = {(row["object_uri"], row["raw_unknown_label"]) for row in pending_rows}
    expected_pending = {
        (row["object_uri"], row["raw_unknown_label"]) for row in plan["expected_pending"]
    }
    if actual_pending != expected_pending:
        raise RepairError(
            f"Repaired pending set mismatch: expected={len(expected_pending)}, actual={len(actual_pending)}"
        )

    cross_date_mapping_groups = int(
        await conn.fetchval(
            """
            WITH dated AS (
              SELECT canonical_uri,
                     substring(name FROM '^([12][0-9]{3}-[01][0-9]-[0-3][0-9])') AS d
              FROM entity_rid_mappings WHERE entity_type='Meeting'
            )
            SELECT count(*) FROM (
              SELECT canonical_uri FROM dated GROUP BY canonical_uri
              HAVING count(DISTINCT d) FILTER (WHERE d IS NOT NULL) > 1
            ) groups
            """
        )
    )
    if cross_date_mapping_groups != 0:
        raise RepairError(f"{cross_date_mapping_groups} cross-date Meeting mapping groups remain")

    date_mismatched_edges = int(
        await conn.fetchval(
            """
            SELECT count(*)
            FROM entity_relationships rel
            JOIN entity_rid_mappings map ON map.vault_path=rel.source_rid
            JOIN entity_registry ent ON ent.fuseki_uri=rel.object_uri
            WHERE rel.predicate='attended'
              AND map.entity_type='Meeting'
              AND map.name ~ '^[12][0-9]{3}-[01][0-9]-[0-3][0-9]'
              AND ent.entity_text ~ '^[12][0-9]{3}-[01][0-9]-[0-3][0-9]'
              AND substring(map.name FROM 1 FOR 10) <> substring(ent.entity_text FROM 1 FOR 10)
            """
        )
    )
    if date_mismatched_edges:
        raise RepairError(f"{date_mismatched_edges} date-mismatched attended edges remain")

    counts = await _global_counts(conn)
    summary = plan["summary"]
    exact_targets = {
        "meeting_live": summary["post_meeting_live"],
        "meeting_mappings": summary["mapping_rows"],
        "meeting_mapping_uris": summary["post_mapping_uris"],
        "attended_total": summary["post_attended_total"],
        "pending_total": summary["post_pending_total"],
    }
    if counts != exact_targets:
        raise RepairError(f"Post-repair global counts drifted: expected={exact_targets}, actual={counts}")
    return {
        "counts": counts,
        "moved_mappings": len(moved_ids),
        "new_meetings": len(entity_rows),
        "new_attended_edges": len(actual_edges),
        "new_pending": len(actual_pending),
        "cross_date_mapping_groups": cross_date_mapping_groups,
        "date_mismatched_edges": date_mismatched_edges,
    }


async def apply_plan(
    snapshot: Mapping[str, Any],
    plan: Mapping[str, Any],
    database: str,
    api_url: str,
    log_path: Path,
) -> dict[str, Any]:
    await _verify_api(api_url)
    conn = await _connect(database)
    try:
        await _acquire_lock(conn)
        fresh = await _fresh_plan_matches(snapshot, plan, conn)
        run_id = "meeting_mapping_repair_" + datetime.now().strftime("%Y%m%dT%H%M%S")
        with _new_log(log_path) as log:
            _log(log, "preflight", plan_digest=plan["plan_digest"], baseline=fresh["baseline"])
            backups = await _create_backups(conn, plan, run_id)
            _log(log, "backups_created", run_id=run_id, tables=backups)
            endpoint = f"{api_url.rstrip('/')}/register-entity"
            try:
                for number, move in enumerate(plan["moves"], 1):
                    _log(
                        log,
                        "register_start",
                        number=number,
                        total=len(plan["moves"]),
                        mapping_id=move["id"],
                        vault_path=move["vault_path"],
                    )
                    response = await _api_json(endpoint, _registration_payload(move))
                    if (
                        not response.get("success")
                        or response.get("canonical_uri") != move["expected_canonical_uri"]
                    ):
                        raise RepairError(
                            f"Unexpected registration response for {move['vault_path']}: {response}"
                        )
                    # Preserve the artifact's own name after using the date
                    # partition representative as the exact resolution key.
                    result = await conn.execute(
                        """
                        UPDATE entity_rid_mappings SET name=$1
                        WHERE id=$2 AND vault_rid=$3 AND canonical_uri=$4
                        """,
                        move["name"],
                        int(move["id"]),
                        move["vault_rid"],
                        move["expected_canonical_uri"],
                    )
                    if result != "UPDATE 1":
                        raise RepairError(f"Could not restore mapping name for id {move['id']}")
                    _log(log, "register_ok", mapping_id=move["id"], response=response)
                verification = await verify_applied(conn, plan)
            except Exception as exc:
                _log(log, "apply_failed", error=str(exc), rollback_run_id=run_id)
                raise
            _log(log, "verification", result=verification)
            return {
                "run_id": run_id,
                "backup_tables": backups,
                "verification": verification,
            }
    finally:
        await conn.close()


async def rollback_plan(
    plan: Mapping[str, Any], database: str, run_id: str
) -> dict[str, Any]:
    if not SAFE_RUN_ID_RE.fullmatch(run_id):
        raise RepairError(f"Unsafe rollback run id: {run_id}")
    tables = {
        "registry": f"entity_registry_backup_{run_id}",
        "mappings": f"entity_rid_mappings_backup_{run_id}",
        "relationships": f"entity_relationships_backup_{run_id}",
        "pending": f"pending_relationships_backup_{run_id}",
    }
    conn = await _connect(database)
    try:
        await _acquire_lock(conn)
        for table in tables.values():
            if not await conn.fetchval("SELECT to_regclass($1)", f"public.{table}"):
                raise RepairError(f"Rollback backup table is missing: {table}")
        paths = [move["vault_path"] for move in plan["moves"]]
        new_uris = [item["expected_uri"] for item in plan["new_entities"]]
        old_uri_by_new = {
            item["expected_uri"]: item["old_canonical_uri"] for item in plan["new_entities"]
        }
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM entity_relationships WHERE source_rid=ANY($1::text[]) "
                "OR object_uri=ANY($2::text[])",
                paths,
                new_uris,
            )
            await conn.execute(
                "DELETE FROM pending_relationships WHERE source_rid=ANY($1::text[]) "
                "OR object_uri=ANY($2::text[])",
                paths,
                new_uris,
            )
            await conn.execute(
                f"""
                INSERT INTO entity_relationships SELECT * FROM {tables['relationships']}
                ON CONFLICT (subject_uri, predicate, object_uri) DO UPDATE SET
                  confidence=EXCLUDED.confidence, source=EXCLUDED.source,
                  source_rid=EXCLUDED.source_rid, source_field=EXCLUDED.source_field,
                  raw_value=EXCLUDED.raw_value, updated_at=EXCLUDED.updated_at
                """
            )
            # Backup rows are source-scoped and were all removed above, so no
            # expression-index conflict is possible here.
            await conn.execute(
                f"INSERT INTO pending_relationships SELECT * FROM {tables['pending']}"
            )
            await conn.execute(
                f"""
                INSERT INTO entity_rid_mappings SELECT * FROM {tables['mappings']}
                ON CONFLICT (vault_rid) DO UPDATE SET
                  vault_path=EXCLUDED.vault_path,
                  canonical_uri=EXCLUDED.canonical_uri,
                  entity_type=EXCLUDED.entity_type,
                  name=EXCLUDED.name,
                  content_hash=EXCLUDED.content_hash,
                  sync_status=EXCLUDED.sync_status,
                  last_synced=EXCLUDED.last_synced,
                  created_at=EXCLUDED.created_at,
                  visibility_scope=EXCLUDED.visibility_scope
                """
            )
            for new_uri, old_uri in old_uri_by_new.items():
                await conn.execute(
                    """
                    UPDATE entity_registry
                    SET merged_into=$1, merged_at=NOW(), merged_by=$2, updated_at=NOW()
                    WHERE fuseki_uri=$3 AND merged_into IS NULL
                    """,
                    old_uri,
                    f"rollback:{run_id}",
                    new_uri,
                )
        return {
            "run_id": run_id,
            "restored_mappings": len(plan["moves"]),
            "tombstoned_new_entities": len(new_uris),
            "backup_tables_retained": tables,
        }
    finally:
        await conn.close()


def _summary(plan: Mapping[str, Any]) -> None:
    print(
        json.dumps(
            {
                "summary": plan["summary"],
                "classification": {
                    "dateless_singletons": plan["classification"]["dateless_singletons"],
                    "cross_date_groups": len(plan["classification"]["cross_date_groups"]),
                },
                "snapshot_digest": plan["snapshot_digest"],
                "meeting_mapping_digest": plan["meeting_mapping_digest"],
                "mutation_set_digest": plan["mutation_set_digest"],
                "plan_digest": plan["plan_digest"],
                "baseline": plan["baseline"],
            },
            indent=2,
            sort_keys=True,
        )
    )


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="Build a read-only immutable repair plan")
    plan.add_argument("--snapshot", type=Path, required=True)
    plan.add_argument("--expected-snapshot-digest", required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--database", default="personal_koi")

    apply = commands.add_parser("apply", help="Apply a verified repair plan")
    apply.add_argument("--snapshot", type=Path, required=True)
    apply.add_argument("--expected-snapshot-digest", required=True)
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--expected-plan-digest", required=True)
    apply.add_argument("--database", default="personal_koi")
    apply.add_argument("--api-url", default="http://127.0.0.1:8351")
    apply.add_argument("--log", type=Path, required=True)
    apply.add_argument("--apply", action="store_true", dest="confirmed")

    verify = commands.add_parser("verify", help="Verify an applied repair plan")
    verify.add_argument("--plan", type=Path, required=True)
    verify.add_argument("--expected-plan-digest", required=True)
    verify.add_argument("--database", default="personal_koi")

    rollback = commands.add_parser("rollback", help="Restore backups and tombstone new entities")
    rollback.add_argument("--plan", type=Path, required=True)
    rollback.add_argument("--expected-plan-digest", required=True)
    rollback.add_argument("--run-id", required=True)
    rollback.add_argument("--database", default="personal_koi")
    rollback.add_argument("--rollback", action="store_true", dest="confirmed")
    return parser


async def _main() -> int:
    args = build_cli().parse_args()
    if args.command == "plan":
        try:
            snapshot = load_snapshot(args.snapshot, args.expected_snapshot_digest)
        except CorpusError as exc:
            raise RepairError(str(exc)) from exc
        conn = await _connect(args.database)
        try:
            plan = await build_plan(snapshot, conn)
        finally:
            await conn.close()
        write_private_json(plan, args.output)
        _summary(plan)
        return 0

    plan = load_plan(args.plan, args.expected_plan_digest)
    if args.command == "apply":
        require_confirmation(args.confirmed, "apply")
        try:
            snapshot = load_snapshot(args.snapshot, args.expected_snapshot_digest)
        except CorpusError as exc:
            raise RepairError(str(exc)) from exc
        if plan["snapshot_digest"] != snapshot["snapshot_digest"]:
            raise RepairError("Plan was not built from this frozen snapshot")
        result = await apply_plan(snapshot, plan, args.database, args.api_url, args.log)
    elif args.command == "rollback":
        require_confirmation(args.confirmed, "rollback")
        result = await rollback_plan(plan, args.database, args.run_id)
    else:
        conn = await _connect(args.database)
        try:
            result = await verify_applied(conn, plan)
        finally:
            await conn.close()
    print(json.dumps(result, indent=2, sort_keys=True, default=_json_default))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(_main()))
    except (RepairError, CorpusError) as exc:
        raise SystemExit(f"ERROR: {exc}")
