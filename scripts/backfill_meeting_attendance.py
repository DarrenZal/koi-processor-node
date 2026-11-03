#!/usr/bin/env python3
"""Plan, apply, and verify the frozen Meeting attendance backfill.

``plan`` is read-only. ``apply`` refuses to run without both checksums and the
literal ``--apply`` switch, re-plans against the live DB immediately before any
write, snapshots affected rows, then calls the live registration API. Private
plans and logs are always mode 0600.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import asyncpg


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.personal_ingest_api import generate_entity_uri  # noqa: E402
from api.vault_parser import batch_resolve_by_vault_path, batch_resolve_entities  # noqa: E402
from scripts.meeting_attendee_parser import (  # noqa: E402
    CorpusError,
    _parse_frontmatter,
    load_snapshot,
)


PLAN_SCHEMA_VERSION = 1
_SAFE_RUN_ID = re.compile(r"^meeting_backfill_\d{8}T\d{6}$")


class BackfillError(RuntimeError):
    """A plan, precondition, apply, or verification invariant failed."""


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
        raise BackfillError(f"Cannot load backfill plan {path}: {exc}") from exc
    if not isinstance(plan, dict) or plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise BackfillError(f"{path}: unsupported backfill plan schema")
    embedded = plan.get("plan_digest")
    actual = compute_plan_digest(plan)
    if embedded != actual:
        raise BackfillError(f"{path}: plan digest mismatch: embedded={embedded}, actual={actual}")
    if expected_digest is not None and actual != expected_digest:
        raise BackfillError(f"{path}: expected plan digest {expected_digest}, got {actual}")
    return plan


def write_private_json(value: Mapping[str, Any], output: Path) -> None:
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise BackfillError(f"Refusing to overwrite immutable output: {output}")
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, default=_json_default)
            + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output)
        os.chmod(output, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def require_apply(confirmed: bool) -> None:
    if not confirmed:
        raise BackfillError("Refusing live writes without the literal --apply switch")


def derive_entity_rid(vault_name: str, entity_type: str, entity_id: str) -> str:
    """Mirror personal-koi-mcp generateEntityRID exactly."""
    normalized_type = re.sub(r"^schema:", "", entity_type)
    normalized_id = entity_id.lower().replace(" ", "-")
    return f"orn:obsidian.entity:{vault_name}/{normalized_type}/{normalized_id}"


def _selected_records(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        record
        for record in snapshot["records"]
        if any(slot["usable"] for slot in record["attendees"])
    ]


def select_unmapped_records(
    snapshot: Mapping[str, Any], mapped_paths: Iterable[str]
) -> list[dict[str, Any]]:
    mapped = set(mapped_paths)
    return [record for record in _selected_records(snapshot) if record["vault_path"] not in mapped]


def _record_dict(row: asyncpg.Record) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _canonical_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows],
        key=lambda row: _json_bytes(row),
    )


async def _global_counts(conn: asyncpg.Connection) -> dict[str, int]:
    row = await conn.fetchrow("""
        SELECT
          (SELECT count(*) FROM entity_registry) AS registry_total,
          (SELECT count(*) FROM entity_registry WHERE merged_into IS NULL) AS registry_live,
          (SELECT count(*) FROM entity_registry WHERE entity_type='Meeting' AND merged_into IS NULL) AS meeting_live,
          (SELECT count(*) FROM entity_registry WHERE entity_type='Person' AND merged_into IS NULL) AS person_live,
          (SELECT count(*) FROM entity_rid_mappings) AS mappings_total,
          (SELECT count(*) FROM entity_relationships) AS relationships_total,
          (SELECT count(*) FROM entity_relationships WHERE predicate='attended') AS attended_total,
          (SELECT count(*) FROM pending_relationships) AS pending_total
    """)
    return {key: int(row[key]) for key in row.keys()}


async def _old_false_edge_count(conn: asyncpg.Connection) -> int:
    backup_exists = await conn.fetchval(
        "SELECT to_regclass('public.entity_relationships_backup_meeting_identity_20260822')"
    )
    if not backup_exists:
        raise BackfillError("Required prior-unit edge backup table is missing")
    return int(await conn.fetchval("""
        SELECT count(*)
        FROM entity_relationships_backup_meeting_identity_20260822 old
        JOIN entity_relationships live
          ON live.subject_uri=old.subject_uri
         AND live.predicate=old.predicate
         AND live.object_uri=old.object_uri
    """))


def _scan_person(snapshot: Mapping[str, Any], vault_path: str) -> dict[str, Any]:
    vault_root = Path(snapshot["source_root"]).resolve()
    absolute = (vault_root / vault_path).resolve()
    try:
        absolute.relative_to(vault_root)
    except ValueError as exc:
        raise BackfillError(f"Person path escapes vault: {vault_path}") from exc
    if not absolute.is_file() or not vault_path.startswith("People/"):
        raise BackfillError(f"Unsafe or missing Person note: {vault_path}")
    content = absolute.read_text(encoding="utf-8")
    frontmatter = _parse_frontmatter(content, vault_path) or {}
    explicit_type = str(frontmatter.get("@type") or frontmatter.get("type") or "Person")
    if re.sub(r"^schema:", "", explicit_type) != "Person":
        raise BackfillError(f"{vault_path}: explicit type is not Person: {explicit_type}")
    name = str(frontmatter.get("name") or absolute.stem).strip()
    if not name:
        raise BackfillError(f"{vault_path}: empty Person name")
    entity_id = str(frontmatter.get("@id") or frontmatter.get("id") or name)
    source_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
    return {
        "vault_path": vault_path,
        "vault_rid": derive_entity_rid("Notes", "Person", entity_id),
        "name": name,
        "expected_uri": generate_entity_uri(name, "Person"),
        "content_hash": source_sha256[:16],
        "source_sha256": source_sha256,
    }


async def _pending_promotions(
    conn: asyncpg.Connection,
    entities: list[tuple[dict[str, Any], str]],
) -> list[dict[str, Any]]:
    promotions = []
    consumed_ids: set[int] = set()
    for entity, entity_type in entities:
        rows = await conn.fetch("""
            SELECT id, subject_uri, object_uri, predicate, raw_unknown_label,
                   unknown_side, target_type_hint, source_rid, source_field,
                   similarity(LOWER(raw_unknown_label), LOWER($1)) AS sim
            FROM pending_relationships
            WHERE similarity(LOWER(raw_unknown_label), LOWER($1)) >= 0.8
              AND (target_type_hint IS NULL OR target_type_hint=$2)
            ORDER BY sim DESC
        """, entity["name"], entity_type)
        rows = [row for row in rows if row["id"] not in consumed_ids]
        if not rows or float(rows[0]["sim"]) < 0.85:
            continue
        if len(rows) > 1 and float(rows[0]["sim"]) - float(rows[1]["sim"]) < 0.05:
            continue
        row = _record_dict(rows[0])
        if row["unknown_side"] == "object":
            subject_uri, object_uri = row["subject_uri"], entity["expected_uri"]
        else:
            subject_uri, object_uri = entity["expected_uri"], row["object_uri"]
        consumed_ids.add(row["id"])
        promotions.append({
            "entity_type": entity_type,
            "entity_name": entity["name"],
            "pending_id": row["id"],
            "subject_uri": subject_uri,
            "predicate": row["predicate"],
            "object_uri": object_uri,
            "source_rid": row["source_rid"],
            "raw_unknown_label": row["raw_unknown_label"],
            "similarity": float(row["sim"]),
        })
    return promotions


async def build_plan(snapshot: Mapping[str, Any], conn: asyncpg.Connection) -> dict[str, Any]:
    selected = _selected_records(snapshot)
    selected_paths = [record["vault_path"] for record in selected]
    mapping_records = await conn.fetch("""
        SELECT id, vault_rid, vault_path, canonical_uri, entity_type, name,
               content_hash, sync_status, last_synced, created_at, visibility_scope
        FROM entity_rid_mappings
        WHERE vault_path=ANY($1::text[])
        ORDER BY vault_path, id
    """, selected_paths)
    protected_mappings = _canonical_rows(_record_dict(row) for row in mapping_records)
    mapped_paths = {row["vault_path"] for row in protected_mappings}
    meetings_source = select_unmapped_records(snapshot, mapped_paths)

    slot_rows = [
        (record, slot)
        for record in meetings_source
        for slot in record["attendees"]
        if slot["usable"]
    ]
    targets = sorted({
        (slot["target_name"], slot["type_hint"])
        for _, slot in slot_rows
    })
    resolved = await batch_resolve_entities(conn, targets)
    target_paths = sorted({
        slot["target_vault_path"]
        for _, slot in slot_rows
        if slot["target_vault_path"]
    })
    path_resolved = await batch_resolve_by_vault_path(conn, target_paths)

    unresolved_slots = []
    resolved_by_slot: dict[tuple[str, int], str] = {}
    for record, slot in slot_rows:
        uri = (
            resolved.get((slot["target_name"].lower(), slot["type_hint"]))
            or resolved.get((slot["target_name"].lower(), None))
            or path_resolved.get(slot["target_vault_path"])
        )
        key = (record["vault_path"], slot["index"])
        if uri:
            resolved_by_slot[key] = uri
        else:
            unresolved_slots.append((record, slot))

    person_paths_by_target: dict[tuple[str, str], str] = {}
    for _, slot in unresolved_slots:
        path = slot["target_vault_path"]
        if not path or not path.startswith("People/"):
            continue
        absolute = Path(snapshot["source_root"]) / path
        if not absolute.is_file():
            continue
        key = (slot["target_name"].lower(), slot["type_hint"])
        previous = person_paths_by_target.setdefault(key, path)
        if previous != path:
            raise BackfillError(f"One unresolved target maps to two Person notes: {key}")

    people = sorted(
        [_scan_person(snapshot, path) for path in set(person_paths_by_target.values())],
        key=lambda person: person["vault_path"].encode("utf-8"),
    )
    person_uri_by_key = {
        key: next(p["expected_uri"] for p in people if p["vault_path"] == path)
        for key, path in person_paths_by_target.items()
    }
    for record, slot in unresolved_slots:
        key = (slot["target_name"].lower(), slot["type_hint"])
        uri = person_uri_by_key.get(key)
        if uri:
            resolved_by_slot[(record["vault_path"], slot["index"])] = uri

    meetings = []
    unresolved_output = []
    expected_edges: set[tuple[str, str]] = set()
    expected_pending: set[tuple[str, str]] = set()
    for record in meetings_source:
        expected_uri = generate_entity_uri(record["meeting_name"], "Meeting")
        attendees = [slot["raw"] for slot in record["attendees"]]
        attendee_uris = []
        pending_labels = []
        for slot in record["attendees"]:
            if not slot["usable"]:
                continue
            person_uri = resolved_by_slot.get((record["vault_path"], slot["index"]))
            if person_uri:
                attendee_uris.append({"slot": slot["index"], "uri": person_uri})
                expected_edges.add((person_uri, expected_uri))
            else:
                pending_labels.append({"slot": slot["index"], "label": slot["target_name"]})
                expected_pending.add((expected_uri, slot["target_name"]))
                unresolved_output.append({
                    "meeting_path": record["vault_path"],
                    "slot": slot["index"],
                    "raw": slot["raw"],
                    "target_name": slot["target_name"],
                    "target_vault_path": slot["target_vault_path"],
                })
        meetings.append({
            "vault_path": record["vault_path"],
            "vault_rid": derive_entity_rid("Notes", "Meeting", record["meeting_name"]),
            "name": record["meeting_name"],
            "expected_uri": expected_uri,
            "content_hash": record["source_sha256"][:16],
            "attendees": attendees,
            "expected_attendees": attendee_uris,
            "expected_pending": pending_labels,
        })

    people_paths = [person["vault_path"] for person in people]
    existing_people_mappings = await conn.fetch("""
        SELECT vault_path FROM entity_rid_mappings WHERE vault_path=ANY($1::text[])
    """, people_paths)
    if existing_people_mappings:
        raise BackfillError("A planned Person path became mapped during plan construction")

    registration_order = [(person, "Person") for person in people]
    registration_order += [(meeting, "Meeting") for meeting in meetings]
    pending_promotions = await _pending_promotions(conn, registration_order)
    old_false_edges = await _old_false_edge_count(conn)
    if old_false_edges != 0:
        raise BackfillError(f"Prior unit's deleted false edges reappeared: {old_false_edges}")

    mutation_set = {
        "protected_mappings": protected_mappings,
        "people": people,
        "meetings": meetings,
        "unresolved": unresolved_output,
        "pending_promotions": pending_promotions,
    }
    plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_digest": snapshot["snapshot_digest"],
        "mutation_set_digest": _digest(mutation_set),
        "baseline": await _global_counts(conn),
        "protected_mapping_digest": _digest(protected_mappings),
        "old_false_edge_count": old_false_edges,
        **mutation_set,
        "summary": {
            "attended_corpus_meetings": len(selected),
            "protected_mapped_paths": len(mapped_paths),
            "meeting_registrations": len(meetings),
            "attendee_entries": len(slot_rows),
            "person_registrations": len(people),
            "expected_meeting_edges": len(expected_edges),
            "expected_meeting_pending": len(expected_pending),
            "expected_collateral_promotions": len(pending_promotions),
        },
    }
    plan["plan_digest"] = compute_plan_digest(plan)
    return plan


async def _connect(database: str) -> asyncpg.Connection:
    if "://" in database:
        return await asyncpg.connect(database)
    return await asyncpg.connect(database=database)


def _url_json(url: str, payload: Mapping[str, Any] | None = None, timeout: int = 180) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if data is None else "POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read())
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        raise BackfillError(f"API request failed for {url}: {exc}") from exc


async def _api_json(url: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return await asyncio.to_thread(_url_json, url, payload)


async def _verify_api(api_url: str) -> None:
    health = await _api_json(f"{api_url.rstrip('/')}/health")
    if health.get("status") != "healthy" or health.get("database") != "connected":
        raise BackfillError(f"API is not healthy: {health}")
    schema = await _api_json(f"{api_url.rstrip('/')}/openapi.json")
    properties = (
        schema.get("components", {}).get("schemas", {})
        .get("RegisterEntityRequest", {}).get("properties", {})
    )
    if "exact_only" not in properties:
        raise BackfillError("Live API does not expose RegisterEntityRequest.exact_only")


def _new_log(path: Path):
    path = path.expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    return os.fdopen(fd, "w", encoding="utf-8")


def _log(handle, event: str, **values: Any) -> None:
    row = {"at": datetime.now(timezone.utc).isoformat(), "event": event, **values}
    handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=_json_default) + "\n")
    handle.flush()


async def _create_backups(conn: asyncpg.Connection, plan: Mapping[str, Any], run_id: str) -> dict[str, str]:
    if not _SAFE_RUN_ID.fullmatch(run_id):
        raise BackfillError(f"Unsafe backup run id: {run_id}")
    tables = {
        "registry": f"entity_registry_backup_{run_id}",
        "mappings": f"entity_rid_mappings_backup_{run_id}",
        "relationships": f"entity_relationships_backup_{run_id}",
        "pending": f"pending_relationships_backup_{run_id}",
    }
    for table in tables.values():
        if await conn.fetchval("SELECT to_regclass($1)", f"public.{table}"):
            raise BackfillError(f"Backup table already exists: {table}")
    uris = [item["expected_uri"] for item in plan["people"] + plan["meetings"]]
    paths = [item["vault_path"] for item in plan["people"] + plan["meetings"]]
    paths += [item["vault_path"] for item in plan["protected_mappings"]]
    sources = paths + [item["source_rid"] for item in plan["pending_promotions"]]
    await conn.execute(
        f"CREATE TABLE {tables['registry']} AS SELECT * FROM entity_registry WHERE fuseki_uri=ANY($1::text[])",
        uris,
    )
    await conn.execute(
        f"CREATE TABLE {tables['mappings']} AS SELECT * FROM entity_rid_mappings WHERE vault_path=ANY($1::text[])",
        paths,
    )
    await conn.execute(
        f"CREATE TABLE {tables['relationships']} AS SELECT * FROM entity_relationships WHERE source_rid=ANY($1::text[])",
        sources,
    )
    await conn.execute(f"CREATE TABLE {tables['pending']} AS TABLE pending_relationships")
    return tables


def _registration_payload(item: Mapping[str, Any], entity_type: str) -> dict[str, Any]:
    payload = {
        "vault_rid": item["vault_rid"],
        "vault_path": item["vault_path"],
        "entity_type": entity_type,
        "name": item["name"],
        "content_hash": item["content_hash"],
        "publication_scope": "local_graph",
        "visibility_scope": "public",
        "force_type": True,
        "exact_only": True,
    }
    if entity_type == "Meeting":
        payload["frontmatter"] = {"attendees": item["attendees"]}
    return payload


async def verify_applied(conn: asyncpg.Connection, plan: Mapping[str, Any]) -> dict[str, Any]:
    protected_paths = sorted({row["vault_path"] for row in plan["protected_mappings"]})
    protected_now = await conn.fetch("""
        SELECT id, vault_rid, vault_path, canonical_uri, entity_type, name,
               content_hash, sync_status, last_synced, created_at, visibility_scope
        FROM entity_rid_mappings WHERE vault_path=ANY($1::text[])
        ORDER BY vault_path, id
    """, protected_paths)
    protected_rows = _canonical_rows(_record_dict(row) for row in protected_now)
    if _digest(protected_rows) != plan["protected_mapping_digest"]:
        raise BackfillError("A protected historical Meeting mapping changed")

    all_items = [(person, "Person") for person in plan["people"]]
    all_items += [(meeting, "Meeting") for meeting in plan["meetings"]]
    item_paths = [item["vault_path"] for item, _ in all_items]
    rows = await conn.fetch("""
        SELECT m.vault_path, m.vault_rid, m.canonical_uri,
               e.entity_text, e.entity_type, e.merged_into
        FROM entity_rid_mappings m
        JOIN entity_registry e ON e.fuseki_uri=m.canonical_uri
        WHERE m.vault_path=ANY($1::text[])
    """, item_paths)
    by_path = {row["vault_path"]: row for row in rows}
    if len(by_path) != len(all_items):
        raise BackfillError(f"Expected {len(all_items)} new mappings, found {len(by_path)}")
    for item, entity_type in all_items:
        row = by_path[item["vault_path"]]
        if (
            row["vault_rid"] != item["vault_rid"]
            or row["canonical_uri"] != item["expected_uri"]
            or row["entity_text"] != item["name"]
            or row["entity_type"] != entity_type
            or row["merged_into"] is not None
        ):
            raise BackfillError(f"Unexpected mapping/entity for {item['vault_path']}: {dict(row)}")

    meeting_paths = [meeting["vault_path"] for meeting in plan["meetings"]]
    relationship_rows = await conn.fetch("""
        SELECT subject_uri, predicate, object_uri, source, source_rid, source_field
        FROM entity_relationships WHERE source_rid=ANY($1::text[])
    """, meeting_paths)
    if any(row["predicate"] != "attended" for row in relationship_rows):
        raise BackfillError("A Meeting backfill source produced a non-attended relationship")
    actual_edges = {(row["subject_uri"], row["object_uri"]) for row in relationship_rows}
    expected_edges = {
        (attendee["uri"], meeting["expected_uri"])
        for meeting in plan["meetings"]
        for attendee in meeting["expected_attendees"]
    }
    if actual_edges != expected_edges:
        raise BackfillError(
            f"Meeting edge set mismatch: expected={len(expected_edges)}, actual={len(actual_edges)}"
        )

    pending_rows = await conn.fetch("""
        SELECT object_uri, raw_unknown_label
        FROM pending_relationships WHERE source_rid=ANY($1::text[])
    """, meeting_paths)
    actual_pending = {(row["object_uri"], row["raw_unknown_label"]) for row in pending_rows}
    expected_pending = {
        (meeting["expected_uri"], pending["label"])
        for meeting in plan["meetings"]
        for pending in meeting["expected_pending"]
    }
    if actual_pending != expected_pending:
        raise BackfillError(
            f"Meeting pending set mismatch: expected={len(expected_pending)}, actual={len(actual_pending)}"
        )

    for promotion in plan["pending_promotions"]:
        pending_exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pending_relationships WHERE id=$1)",
            promotion["pending_id"],
        )
        edge_exists = await conn.fetchval("""
            SELECT EXISTS(
              SELECT 1 FROM entity_relationships
              WHERE subject_uri=$1 AND predicate=$2 AND object_uri=$3
            )
        """, promotion["subject_uri"], promotion["predicate"], promotion["object_uri"])
        if pending_exists or not edge_exists:
            raise BackfillError(f"Expected pending promotion did not complete: {promotion}")

    old_false_edges = await _old_false_edge_count(conn)
    if old_false_edges != 0:
        raise BackfillError(f"Prior false edges reappeared: {old_false_edges}")
    return {
        "counts": await _global_counts(conn),
        "new_mappings": len(all_items),
        "meeting_edges": len(actual_edges),
        "meeting_pending": len(actual_pending),
        "collateral_promotions": len(plan["pending_promotions"]),
        "old_false_edges": old_false_edges,
        "protected_mapping_digest": _digest(protected_rows),
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
        fresh = await build_plan(snapshot, conn)
        if fresh["mutation_set_digest"] != plan["mutation_set_digest"]:
            raise BackfillError(
                "Live mutation set drifted since plan; create a new plan before applying"
            )
        run_id = "meeting_backfill_" + datetime.now().strftime("%Y%m%dT%H%M%S")
        with _new_log(log_path) as log:
            _log(log, "preflight", fresh_counts=fresh["baseline"], plan_digest=plan["plan_digest"])
            tables = await _create_backups(conn, plan, run_id)
            _log(log, "backups_created", tables=tables)

            endpoint = f"{api_url.rstrip('/')}/register-entity"
            for entity_type, items in (("Person", plan["people"]), ("Meeting", plan["meetings"])):
                for number, item in enumerate(items, 1):
                    payload = _registration_payload(item, entity_type)
                    _log(log, "register_start", entity_type=entity_type, number=number,
                         total=len(items), vault_path=item["vault_path"])
                    response = await _api_json(endpoint, payload)
                    if not response.get("success") or response.get("canonical_uri") != item["expected_uri"]:
                        raise BackfillError(
                            f"Unexpected registration response for {item['vault_path']}: {response}"
                        )
                    _log(log, "register_ok", entity_type=entity_type, number=number,
                         vault_path=item["vault_path"], response=response)
                _log(log, "phase_complete", entity_type=entity_type, count=len(items))

            verification = await verify_applied(conn, plan)
            _log(log, "verification", result=verification)
            return {"run_id": run_id, "backup_tables": tables, "verification": verification}
    finally:
        await conn.close()


def _summary(plan: Mapping[str, Any]) -> None:
    print(json.dumps({
        "summary": plan["summary"],
        "snapshot_digest": plan["snapshot_digest"],
        "mutation_set_digest": plan["mutation_set_digest"],
        "plan_digest": plan["plan_digest"],
        "baseline": plan["baseline"],
        "protected_mapping_digest": plan["protected_mapping_digest"],
    }, indent=2, sort_keys=True))


def build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan", help="Build a read-only exact mutation plan")
    plan.add_argument("--snapshot", type=Path, required=True)
    plan.add_argument("--expected-snapshot-digest", required=True)
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--database", default="personal_koi")

    apply = commands.add_parser("apply", help="Apply a verified immutable plan")
    apply.add_argument("--snapshot", type=Path, required=True)
    apply.add_argument("--expected-snapshot-digest", required=True)
    apply.add_argument("--plan", type=Path, required=True)
    apply.add_argument("--expected-plan-digest", required=True)
    apply.add_argument("--database", default="personal_koi")
    apply.add_argument("--api-url", default="http://127.0.0.1:8351")
    apply.add_argument("--log", type=Path, required=True)
    apply.add_argument("--apply", action="store_true", dest="confirmed")

    verify = commands.add_parser("verify", help="Verify an already-applied plan")
    verify.add_argument("--snapshot", type=Path, required=True)
    verify.add_argument("--expected-snapshot-digest", required=True)
    verify.add_argument("--plan", type=Path, required=True)
    verify.add_argument("--expected-plan-digest", required=True)
    verify.add_argument("--database", default="personal_koi")
    return parser


async def _main() -> int:
    args = build_cli().parse_args()
    try:
        snapshot = load_snapshot(args.snapshot, args.expected_snapshot_digest)
    except CorpusError as exc:
        raise BackfillError(str(exc)) from exc

    if args.command == "plan":
        conn = await _connect(args.database)
        try:
            plan = await build_plan(snapshot, conn)
        finally:
            await conn.close()
        write_private_json(plan, args.output)
        _summary(plan)
        return 0

    plan = load_plan(args.plan, args.expected_plan_digest)
    if plan["snapshot_digest"] != snapshot["snapshot_digest"]:
        raise BackfillError("Plan was not built from this frozen snapshot")
    if args.command == "apply":
        require_apply(args.confirmed)
        result = await apply_plan(snapshot, plan, args.database, args.api_url, args.log)
        print(json.dumps(result, indent=2, sort_keys=True, default=_json_default))
        return 0

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
    except (BackfillError, CorpusError) as exc:
        raise SystemExit(f"ERROR: {exc}")
