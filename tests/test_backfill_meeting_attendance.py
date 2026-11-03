import json
import stat

import pytest

from scripts.backfill_meeting_attendance import (
    BackfillError,
    compute_plan_digest,
    derive_entity_rid,
    load_plan,
    require_apply,
    select_unmapped_records,
    write_private_json,
)


def _snapshot():
    return {
        "records": [
            {"vault_path": "Meetings/a.md", "attendees": [{"usable": True}]},
            {"vault_path": "Meetings/b.md", "attendees": [{"usable": True}]},
            {"vault_path": "Meetings/empty.md", "attendees": [{"usable": False}]},
        ]
    }


def _plan():
    plan = {
        "schema_version": 1,
        "generated_at": "now",
        "snapshot_digest": "snapshot",
        "mutation_set_digest": "mutation",
        "items": [1],
    }
    plan["plan_digest"] = compute_plan_digest(plan)
    return plan


def test_negative_apply_requires_literal_confirmation():
    with pytest.raises(BackfillError, match="--apply"):
        require_apply(False)


def test_negative_tampered_plan_is_rejected(tmp_path):
    path = tmp_path / "plan.json"
    write_private_json(_plan(), path)
    data = json.loads(path.read_text(encoding="utf-8"))
    data["items"].append(2)
    path.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(BackfillError, match="plan digest mismatch"):
        load_plan(path)


def test_negative_wrong_expected_plan_digest_is_rejected(tmp_path):
    path = tmp_path / "plan.json"
    plan = _plan()
    write_private_json(plan, path)
    with pytest.raises(BackfillError, match="expected plan digest"):
        load_plan(path, "0" * 64)


def test_selection_excludes_mapped_and_non_attended_records():
    selected = select_unmapped_records(_snapshot(), {"Meetings/a.md"})
    assert [record["vault_path"] for record in selected] == ["Meetings/b.md"]


def test_mcp_compatible_entity_rid_derivation():
    assert derive_entity_rid("Notes", "Meeting", "2026-01-30 Harbour Meeting") == (
        "orn:obsidian.entity:Notes/Meeting/2026-01-30-harbour-meeting"
    )
    assert derive_entity_rid("Notes", "schema:Person", "Corin Ash-Fenwick") == (
        "orn:obsidian.entity:Notes/Person/corin-ash-fenwick"
    )


def test_private_plan_file_is_0600_and_immutable(tmp_path):
    path = tmp_path / "plan.json"
    write_private_json(_plan(), path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(BackfillError, match="overwrite"):
        write_private_json(_plan(), path)
