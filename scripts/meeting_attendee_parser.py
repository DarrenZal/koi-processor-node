#!/usr/bin/env python3
"""Canonical, deterministic parser for the private Meeting attendee corpus.

The frozen JSON is the only supported input to the Meeting attendance backfill.
Attendee names stay in the operator's private backup directory; the repository
stores only aggregate counts and digests in a contract fixture.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import yaml

# Direct ``python scripts/...`` execution puts scripts/, not the repository
# root, on sys.path. Tests import from the root and would otherwise miss this.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.vault_parser import extract_vault_path_from_wikilink, parse_wikilink


SCHEMA_VERSION = 1
DEFAULT_CONTRACT = Path("tests/fixtures/meeting_attendee_corpus_contract.json")
_FRONTMATTER = re.compile(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|\Z)", re.DOTALL)


class CorpusError(ValueError):
    """The corpus or a frozen snapshot violates the canonical contract."""


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _snapshot_payload(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot)
    payload.pop("generated_at", None)
    payload.pop("snapshot_digest", None)
    return payload


def compute_snapshot_digest(snapshot: Mapping[str, Any]) -> str:
    """Digest every deterministic snapshot field, excluding timestamp/digest."""
    return _sha256(_json_bytes(_snapshot_payload(snapshot)))


def _parse_frontmatter(text: str, vault_path: str) -> dict[str, Any] | None:
    match = _FRONTMATTER.match(text)
    if not match:
        return None
    try:
        value = yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError as exc:
        raise CorpusError(f"{vault_path}: invalid YAML frontmatter: {exc}") from exc
    if not isinstance(value, dict):
        raise CorpusError(f"{vault_path}: frontmatter must be a mapping")
    return value


def _attendee_slot(index: int, item: Any) -> dict[str, Any]:
    raw = None if item is None else str(item)
    value = "" if raw is None else raw.strip()
    slot: dict[str, Any] = {
        "index": index,
        "raw": raw,
        "value": value,
        "usable": bool(value),
        "target_name": None,
        "type_hint": None,
        "target_vault_path": None,
    }
    if value:
        target_name, type_hint = parse_wikilink(value)
        slot.update(
            target_name=target_name,
            type_hint=type_hint or "Person",
            target_vault_path=extract_vault_path_from_wikilink(value),
        )
    return slot


def parse_corpus(vault_root: Path) -> dict[str, Any]:
    """Parse ``<vault_root>/Meetings`` into a deterministic private snapshot."""
    vault_root = vault_root.expanduser().resolve()
    meetings_root = vault_root / "Meetings"
    if not meetings_root.is_dir():
        raise CorpusError(f"Meeting corpus not found: {meetings_root}")

    paths = sorted(
        meetings_root.rglob("*.md"),
        key=lambda path: path.relative_to(vault_root).as_posix().encode("utf-8"),
    )
    records: list[dict[str, Any]] = []
    row_material: list[bytes] = []
    stats = {
        "file_count": len(paths),
        "frontmatter_count": 0,
        "no_frontmatter_count": 0,
        "attendees_field_count": 0,
        "without_attendees_field_count": 0,
        "all_entry_count": 0,
        "null_entry_count": 0,
        "unusable_entry_count": 0,
        "nonempty_entry_count": 0,
        "attended_meeting_count": 0,
    }

    for path in paths:
        vault_path = path.relative_to(vault_root).as_posix()
        content = path.read_text(encoding="utf-8")
        frontmatter = _parse_frontmatter(content, vault_path)
        if frontmatter is None:
            stats["no_frontmatter_count"] += 1
            attendee_values: list[Any] = []
            frontmatter_evidence: dict[str, Any] = {}
        else:
            stats["frontmatter_count"] += 1
            frontmatter_evidence = {
                key: str(frontmatter[key])
                for key in ("name", "title", "@type", "type")
                if key in frontmatter and frontmatter[key] is not None
            }
            if "attendees" not in frontmatter:
                stats["without_attendees_field_count"] += 1
                attendee_values = []
            else:
                stats["attendees_field_count"] += 1
                attendee_values = frontmatter["attendees"]
                if not isinstance(attendee_values, list):
                    raise CorpusError(
                        f"{vault_path}: attendees must be a YAML list, got "
                        f"{type(attendee_values).__name__}"
                    )

        slots = [_attendee_slot(index, item) for index, item in enumerate(attendee_values)]
        stats["all_entry_count"] += len(slots)
        stats["null_entry_count"] += sum(slot["raw"] is None for slot in slots)
        stats["unusable_entry_count"] += sum(not slot["usable"] for slot in slots)
        stats["nonempty_entry_count"] += sum(slot["usable"] for slot in slots)
        if any(slot["usable"] for slot in slots):
            stats["attended_meeting_count"] += 1

        for slot in slots:
            if slot["usable"]:
                row_material.append(
                    f"{vault_path}\0{slot['index']}\0{slot['value']}\n".encode("utf-8")
                )

        records.append(
            {
                "vault_path": vault_path,
                "meeting_name": path.stem,
                "entity_type": "Meeting",
                "source_sha256": _sha256(content.encode("utf-8")),
                "frontmatter_evidence": frontmatter_evidence,
                "attendees": slots,
            }
        )

    snapshot: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source_root": str(vault_root),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stats": stats,
        "ordered_nonempty_row_digest": _sha256(b"".join(row_material)),
        "records": records,
    }
    snapshot["snapshot_digest"] = compute_snapshot_digest(snapshot)
    return snapshot


def load_snapshot(path: Path, expected_digest: str | None = None) -> dict[str, Any]:
    try:
        snapshot = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CorpusError(f"Cannot load frozen snapshot {path}: {exc}") from exc
    if not isinstance(snapshot, dict) or snapshot.get("schema_version") != SCHEMA_VERSION:
        raise CorpusError(f"{path}: unsupported snapshot schema")
    embedded = snapshot.get("snapshot_digest")
    actual = compute_snapshot_digest(snapshot)
    if embedded != actual:
        raise CorpusError(f"{path}: snapshot digest mismatch: embedded={embedded}, actual={actual}")
    if expected_digest is not None and actual != expected_digest:
        raise CorpusError(f"{path}: expected digest {expected_digest}, got {actual}")
    return snapshot


def validate_contract(snapshot: Mapping[str, Any], contract_path: Path) -> None:
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CorpusError(f"Cannot load parser contract {contract_path}: {exc}") from exc
    if contract.get("schema_version") != snapshot.get("schema_version"):
        raise CorpusError("snapshot schema does not match parser contract")
    for key, expected in contract.get("stats", {}).items():
        actual = snapshot.get("stats", {}).get(key)
        if actual != expected:
            raise CorpusError(f"contract stats.{key}: expected {expected}, got {actual}")
    expected_row_digest = contract.get("ordered_nonempty_row_digest")
    if expected_row_digest != snapshot.get("ordered_nonempty_row_digest"):
        raise CorpusError(
            "ordered non-empty row digest does not match the committed corpus contract"
        )
    expected_snapshot_digest = contract.get("snapshot_digest")
    if expected_snapshot_digest and expected_snapshot_digest != snapshot.get("snapshot_digest"):
        raise CorpusError("snapshot digest does not match the committed corpus contract")


def write_snapshot(snapshot: Mapping[str, Any], output: Path) -> None:
    output = output.expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise CorpusError(f"Refusing to overwrite frozen snapshot: {output}")
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, output)
        os.chmod(output, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def _report(snapshot: Mapping[str, Any]) -> None:
    print(json.dumps({
        "stats": snapshot["stats"],
        "ordered_nonempty_row_digest": snapshot["ordered_nonempty_row_digest"],
        "snapshot_digest": snapshot["snapshot_digest"],
    }, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    freeze = commands.add_parser("freeze", help="Parse the vault and write one immutable snapshot")
    freeze.add_argument("--vault-root", type=Path, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    freeze.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)

    verify = commands.add_parser("verify", help="Verify a previously frozen snapshot")
    verify.add_argument("--snapshot", type=Path, required=True)
    verify.add_argument("--expected-digest")
    verify.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "freeze":
        snapshot = parse_corpus(args.vault_root)
        validate_contract(snapshot, args.contract)
        write_snapshot(snapshot, args.output)
    else:
        snapshot = load_snapshot(args.snapshot, args.expected_digest)
        validate_contract(snapshot, args.contract)
    _report(snapshot)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CorpusError as exc:
        raise SystemExit(f"ERROR: {exc}")
