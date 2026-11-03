import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.meeting_attendee_parser import (
    CorpusError,
    compute_snapshot_digest,
    load_snapshot,
    parse_corpus,
    write_snapshot,
)


def _note(root: Path, relative: str, content: str) -> None:
    path = root / "Meetings" / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_negative_malformed_frontmatter_fails_closed(tmp_path):
    _note(tmp_path, "bad.md", "---\nattendees: [\n---\n")
    with pytest.raises(CorpusError, match="invalid YAML"):
        parse_corpus(tmp_path)


def test_negative_scalar_attendees_is_not_silently_split(tmp_path):
    _note(tmp_path, "bad.md", "---\nattendees: Alice, Bob\n---\n")
    with pytest.raises(CorpusError, match="must be a YAML list"):
        parse_corpus(tmp_path)


def test_negative_body_attendees_text_is_not_frontmatter(tmp_path):
    _note(tmp_path, "body.md", "# Notes\nattendees:\n  - Alice\n")
    snapshot = parse_corpus(tmp_path)
    assert snapshot["stats"]["no_frontmatter_count"] == 1
    assert snapshot["stats"]["all_entry_count"] == 0


def test_null_slots_are_counted_but_not_usable(tmp_path):
    _note(
        tmp_path,
        "2026-01-01 Alpha Meeting.md",
        '---\nattendees:\n  -\n  - "[[People/Alice|Alice]]"\n---\n',
    )
    snapshot = parse_corpus(tmp_path)
    assert snapshot["stats"]["all_entry_count"] == 2
    assert snapshot["stats"]["null_entry_count"] == 1
    assert snapshot["stats"]["unusable_entry_count"] == 1
    assert snapshot["stats"]["nonempty_entry_count"] == 1
    assert snapshot["records"][0]["attendees"][0]["raw"] is None
    assert snapshot["records"][0]["attendees"][1]["target_name"] == "Alice"
    assert snapshot["records"][0]["attendees"][1]["target_vault_path"] == "People/Alice.md"


def test_frontmatter_name_and_type_cannot_replace_filename_identity(tmp_path):
    _note(
        tmp_path,
        "2026-02-26 MycoFi Meeting.md",
        "---\nname: MycoFi Meeting\n'@type': schema:Event\nattendees:\n  - Alice\n---\n",
    )
    record = parse_corpus(tmp_path)["records"][0]
    assert record["meeting_name"] == "2026-02-26 MycoFi Meeting"
    assert record["entity_type"] == "Meeting"
    assert record["frontmatter_evidence"] == {
        "name": "MycoFi Meeting",
        "@type": "schema:Event",
    }


def test_paths_and_rows_are_deterministic(tmp_path):
    _note(tmp_path, "z.md", "---\nattendees:\n  - Zed\n---\n")
    _note(tmp_path, "a.md", "---\nattendees:\n  - Alice\n---\n")
    first = parse_corpus(tmp_path)
    second = parse_corpus(tmp_path)
    assert [r["vault_path"] for r in first["records"]] == [
        "Meetings/a.md",
        "Meetings/z.md",
    ]
    assert first["ordered_nonempty_row_digest"] == second["ordered_nonempty_row_digest"]
    assert first["snapshot_digest"] == second["snapshot_digest"]


def test_negative_tampered_snapshot_is_rejected(tmp_path):
    _note(tmp_path, "meeting.md", "---\nattendees:\n  - Alice\n---\n")
    snapshot = parse_corpus(tmp_path)
    output = tmp_path / "snapshot.json"
    write_snapshot(snapshot, output)
    assert output.stat().st_mode & 0o777 == 0o600
    data = json.loads(output.read_text(encoding="utf-8"))
    data["records"][0]["attendees"][0]["value"] = "Mallory"
    output.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(CorpusError, match="snapshot digest mismatch"):
        load_snapshot(output)


def test_snapshot_digest_ignores_only_generation_timestamp(tmp_path):
    _note(tmp_path, "meeting.md", "---\nattendees: []\n---\n")
    snapshot = parse_corpus(tmp_path)
    original = snapshot["snapshot_digest"]
    snapshot["generated_at"] = "later"
    assert compute_snapshot_digest(snapshot) == original
    snapshot["stats"]["file_count"] += 1
    assert compute_snapshot_digest(snapshot) != original


def test_direct_cli_entrypoint_can_import_repo_modules():
    result = subprocess.run(
        [sys.executable, "scripts/meeting_attendee_parser.py", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Canonical, deterministic parser" in result.stdout
